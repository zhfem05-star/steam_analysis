# 수집 파이프라인 명세서

2026-11-23 기준. 정기 수집 DAG 4개의 수집 주기와 데이터를 정리한다.
설계 배경과 결정 과정은 [pipeline_plan.md](pipeline_plan.md), 규모 수치는 같은 문서 8·9절의 2026-11-22 실측 기준.
API별 요청률과 차단 기준은 [api_rate_limits.md](api_rate_limits.md) 참고.

## 1. 요약

| DAG | 수집 데이터 | 주기 (UTC) | 한국 시간 | 대상 | 1회 규모 |
|---|---|---|---|---|---|
| `steam_catalog_01_app_list` | 게임 목록 | 매일 06:00 | 15:00 | 전체 게임 | 약 20만 건, API 5회, 약 8초 |
| `steam_catalog_02_store_prices` | KR 가격·할인·리뷰 요약 | 매일 18:30 | 03:30 (다음 날) | 전체 게임 | 약 20만 건, API 202회, 약 6분 |
| `steam_players_01_hourly` | 동접자 수 | 매시 정각 | 매시 정각 | top 100 + 관리 목록 활성 게임 | top 100 1회 + 개별 최대 약 2,100회 |
| `steam_players_02_daily_scan` | 동접자 스캔·관리 목록 갱신 | 매일 21:00 | 06:00 | 스캔 일정이 도래한 게임 | 최대 30,000회, 약 100분 |

- 공통 처리 흐름: Steam API → producer task → Kafka 토픽 → consumer task → S3(`steam-raw`) parquet → Silver task → PostgreSQL
- 수집 대상은 모두 게임만. DLC·소프트웨어·영상·하드웨어 제외
- 시각은 모두 UTC로 저장
- 4개 DAG 모두 `catchup=False`, `max_active_runs=1`, 실패 시 Slack 알림

### 하루 실행 순서 (UTC)

```text
00:00 ─ 매시 동접자 (매시 정각 반복) ────────────────────────────────▶
06:00   게임 목록        → dim_apps 갱신
18:30   가격·리뷰 요약   → 가격·리뷰 요약 구간 갱신 (Steam 할인 변경 17~18시 UTC 이후)
21:00   동접자 스캔      → 가격 수집 결과로 우선 스캔 표시 → 스캔 → 관리 목록 재배분
```

스캔이 21:00에 도는 이유는 당일 가격 수집에서 확인한 할인 시작·리뷰 급증을 우선 스캔에 반영하기 위해서다.
스캔이 끝나면 재배분된 관리 목록이 다음 정시 동접자 수집부터 적용된다.

## 2. 게임 목록 `steam_catalog_01_app_list`

| 항목 | 내용 |
|---|---|
| 목적 | 수집 대상 전체 게임 목록 유지, 목록에서 사라진 게임 확인 |
| 주기 | 매일 06:00 UTC |
| API | `IStoreService/GetAppList` (`include_games=true`), 페이지당 5만 건 |
| 수집 방식 | 매일 전체 순회. 증분(`if_modified_since`)만으로는 목록에서 빠진 게임을 알 수 없어 사용하지 않음 |
| Kafka 토픽 | `steam.app_list` (key `appid`) |
| S3 원본 | `steam-raw/app_list/{ts_nodash}/` + `_manifest.json` |
| DB | `dim_apps` upsert |

### 수집 필드

| 필드 | 설명 |
|---|---|
| `appid` | 게임 ID |
| `name` | 게임 이름 |
| `last_modified` | 스토어 정보 최종 수정 시각 |
| `price_change_number` | 가격 변경 일련번호 |

### DB 반영 `dim_apps`

- 스냅샷에 있는 게임은 이름·수정 시각 갱신, `last_seen_at` 갱신
- 처음 나온 게임은 `first_seen_at` 기록
- 스냅샷에서 사라진 게임은 `is_listed = FALSE`, `delisted_at` 기록
- 동접자 스캔의 신규 게재 판정과 관리 목록 등록에 사용

## 3. KR 가격·리뷰 요약 `steam_catalog_02_store_prices`

| 항목 | 내용 |
|---|---|
| 목적 | 과거 값을 다시 조회할 수 없는 가격·할인 이력 보존 |
| 주기 | 매일 18:30 UTC. Steam 할인은 보통 17~18시 UTC에 바뀌어 그 이후 수집 |
| API | `IStoreQueryService/Query`, 국가 `KR`, 페이지당 최대 1,000건 |
| 수집 방식 | 매일 전체 순회 |
| Kafka 토픽 | `steam.prices` (key `appid`) |
| S3 원본 | `steam-raw/store_prices/{ts_nodash}/` + `_manifest.json`. 일별 전체 스냅샷 보관 |
| DB | `fact_price_intervals`, `fact_review_summary_intervals` 변경 구간 |

### 수집 필드

| 필드 | 설명 |
|---|---|
| `appid`, `name` | 게임 ID·이름 |
| `is_free` | 무료 여부 |
| `has_price` | KR 가격 존재 여부. FALSE면 무료 또는 KR 구매 불가 |
| `original_price_cents` | 정가 (Steam 응답 단위 그대로) |
| `final_price_cents` | 할인 적용 최종가 |
| `discount_pct` | 할인율 |
| `discount_end_at` | 할인 종료 시각 (진행 중인 할인 중 가장 이른 종료) |
| `review_count` | 리뷰 수 (`reviews.summary_filtered`) |
| `percent_positive` | 긍정 비율 |
| `review_score` | Steam 리뷰 등급 |

### DB 반영: 변경 구간

매일 같은 값을 반복 저장하지 않고 값이 바뀐 시점만 구간으로 남긴다.

| 컬럼 | 의미 |
|---|---|
| `valid_from` | 이 값이 처음 관측된 스냅샷 시각 |
| `valid_to` | 다른 값 또는 미관측이 처음 확인된 스냅샷 시각. NULL이면 현재 유효 |
| `last_seen_at` | 이 값이 마지막으로 관측된 스냅샷 시각 |

- 값이 같으면 `last_seen_at`만 갱신, 바뀌거나 사라지면 구간을 닫고 새 구간 추가
- 실제 변경 시각은 직전 구간 `last_seen_at`과 `valid_from` 사이
- 게임당 열린 구간은 최대 1개 (unique index)

## 4. 매시 동접자 `steam_players_01_hourly`

| 항목 | 내용 |
|---|---|
| 목적 | 현재 값만 제공되는 동접자 수의 시계열 보존 |
| 주기 | 매시 정각. 수집 슬롯은 `data_interval_end` (예: 07:00 실행 → 07:00 슬롯) |
| API | ① `ISteamChartsService/GetGamesByConcurrentPlayers` 1회 (top 100)<br>② `ISteamUserStats/GetNumberOfCurrentPlayers` 게임별 개별 호출 |
| 대상 | top 100 전체 + `player_watchlist` 활성 게임 중 이번 top 100에 없는 게임 |
| 요청률 | 개별 호출은 초당 5건 token bucket. 11-22 실측 299건 약 61초 |
| Kafka 토픽 | `steam.players` (top 100과 개별 호출을 한 토픽으로) |
| S3 원본 | `steam-raw/players/dt=YYYY-MM-DD/hour=HH/` + `_manifest.json` |
| DB | `fact_player_counts` (월별 파티션 자동 생성), `player_watchlist` 최근 값 |

### 수집 필드 → `fact_player_counts`

| 필드 | 설명 |
|---|---|
| `app_id` | 게임 ID |
| `collected_at` | 수집 슬롯 (정시) |
| `fetched_at` | 실제 호출 시각 |
| `player_count` | 동접자 수. 데이터 없음·실패는 0이 아닌 NULL |
| `status` | `ok`, `not_found` (404·`result 42`, 데이터 없는 게임), `error` (수집 실패) |
| `source` | `top100` 또는 `individual`. 두 API 값 차이를 나중에 검증하기 위해 기록 |
| `rank`, `peak_in_game` | top 100 응답에만 있는 순위·당일 최고 동접 |

- 키 `(app_id, collected_at)`. 같은 슬롯 재실행은 덮어쓰기
- 전체 기간 보관, 정리는 추후 결정

### 관리 목록 반영

- top 100에 처음 들어온 게임은 `first_top100_at`과 함께 고정 등록. 순위에서 빠져도 계속 수집
- 활성 게임의 `last_player_count`, `last_observed_at` 갱신

## 5. 동접자 스캔 `steam_players_02_daily_scan`

전체 20만 게임을 매시 조회할 수 없으므로 매일 일부만 스캔해 매시 수집 대상(관리 목록)을 고른다.

| 항목 | 내용 |
|---|---|
| 주기 | 매일 21:00 UTC |
| API | `ISteamUserStats/GetNumberOfCurrentPlayers`, 초당 5건 |
| 대상 | 비활성 게임 중 `next_scan_at`이 지난 게임. 1회 최대 30,000건 |
| 정렬 | 우선 스캔 표시 → 리뷰 수 많은 순 → 예정 시각 이른 순 |
| Kafka 토픽 | `steam.player_scan` |
| S3 원본 | `steam-raw/player_scan/{ts_nodash}/` + `_manifest.json` |
| DB | `player_watchlist` 스캔 일정·활성 여부 |

### 처리 단계

| task | 내용 |
|---|---|
| `sync_watchlist` | `dim_apps` 신규 게임 등록. 신규 게재·할인 시작·리뷰 수 급증(20건 이상) 게임을 우선 스캔 표시 |
| `produce_scan` | 대상 게임 개별 호출 → Kafka |
| `publish_scan` | Kafka → S3 |
| `apply_scan` | 결과로 다음 스캔 일정 계산 |
| `rebalance` | 호출 예산으로 매시 수집 대상 재선정 |

### 스캔 일정 규칙

| 스캔 결과 | 다음 스캔 |
|---|---|
| 저조 (데이터 없음 또는 동접 10명 미만) | 간격 2배 (1 → 2 → 4 … 최대 30일) |
| 저조 아님 | 간격 1일로 복귀 |
| 수집 실패 | 상태 유지, 다음 날 재시도 |

### 매시 수집 대상 선정

- 고정 대상: top 100 진입 이력 또는 `manual` 등록 게임은 항상 활성
- 예산 대상: 나머지 중 최근 7일 최고 동접 상위 N개(기본 2,000, 최소 1명)
- 예산에서 빠진 게임은 다음 날부터 다시 스캔 대상

### 조정용 Airflow Variable

| Variable | 기본값 | 의미 |
|---|---|---|
| `player_watchlist_budget` | 2000 | 예산 대상 게임 수 |
| `player_scan_max_per_run` | 30000 | 1회 최대 스캔 수 |
| `player_scan_low_threshold` | 10 | 저조 판단 동접자 수 |

- 첫 전체 스캔은 약 20만 건을 하루 30,000건씩 나눠 약 7일 소요

## 6. 공통 품질 장치

| 장치 | 내용 |
|---|---|
| 완료 판정 | producer가 Kafka 전송 확인 후 `_manifest.json` 기록. Silver는 manifest 건수와 고유 `appid` 수가 같을 때만 반영 |
| 중복 제거 | Kafka는 at-least-once. Silver에서 `appid` 기준 중복 제거 |
| 재적용 방지 | `silver_snapshot_log`에 적용한 스냅샷 기록, 같은 스냅샷 재실행은 건너뜀 |
| 순서 역전 차단 | 게임 목록·가격은 이미 적용한 스냅샷보다 오래된 스냅샷 거부 |
| 급감 차단 | 게임 목록·가격은 직전 대비 90% 미만이면 적용 중단 (API 이상 응답 의심) |
| 요청률 제어 | 동접자 호출은 초당 5건. 429 수신 시 `Retry-After` 대기 후 요청률 절반, 연속 3회면 실행 중단 |
| 실패 한도 | 동접자 개별 호출 연속 실패 20회 또는 실패율 20% 초과 시 manifest를 남기지 않고 실패 |
| 늦은 실행 차단 | 매시 수집은 슬롯보다 45분 넘게 늦으면 실패. 현재 값을 과거 슬롯에 기록하지 않기 위함 |

## 7. 정기 수집하지 않는 데이터

과거 값을 나중에 다시 조회할 수 있어 요청 시에만 수집한다 (5단계 이후 구현 예정).

| 데이터 | API | 방식 |
|---|---|---|
| 리뷰 상세 | `store/appreviews` 날짜 구간 | 요청한 게임·언어·기간 |
| 리뷰 평가 추이 | `store/appreviewhistogram` | 요청한 게임·언어. 출시 이후 월별·최근 30일 일별 긍정·부정 수 |
| 게임 메타 | `store/api/appdetails` | 신규 등록 시 1회 + 주 1회 (예정) |
| 뉴스·패치노트 | `ISteamNews/GetNewsForApp` | 필요 시 추가 |

### 교체 예정 구 DAG (일시정지)

| DAG | 수집 데이터 | 대체 |
|---|---|---|
| `steam_bronze_01_discount_game_extract` | 할인 게임 목록 | 가격 정기 수집 |
| `steam_bronze_02_review_extract` | `tracked_games` 한국어·영어 리뷰 전체 | on-demand 리뷰 DAG |
| `steam_bronze_04_app_details_extract` | `tracked_games` 게임 메타 | 게임 메타 수집 재구성 |

## 8. 운영 상태 (2026-11-23)

- 4개 정기 DAG는 2026-11-22 수동 실행으로 검증만 끝났고 아직 일시정지 상태
- 켜기 전 정리 필요: 검증 때 미래 날짜로 실행한 스냅샷 3개가 예약 실행 이름과 겹침
  - 게임 목록 `20261122T060000` (11-23 06:00 실행과 겹침)
  - 가격 `20261122T183000`, `20261123T183000` (11-23·11-24 18:30 실행과 겹침)
  - 그대로 켜면 해당 회차가 "이미 적용"으로 건너뛰어짐
