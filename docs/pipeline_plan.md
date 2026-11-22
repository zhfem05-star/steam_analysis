# 파이프라인 재구성 계획

2026-11-18 확정. Silver 레이어부터 다시 구축하고 Bronze는 수집 방식만 재편한다.

## 1. 분류 기준

- **정기 적재:** API가 현재 값만 제공해 놓친 시점을 복구할 수 없는 데이터
- **On-demand:** 과거 값을 다시 조회할 수 있는 데이터. 분석 요청이 있을 때만 수집
- 정기 적재 데이터는 추적 시작 이후만 존재하므로 대상 범위를 넓게 유지

## 2. 데이터별 수집 방식

| 데이터 | API | 방식 | 주기 | 대상 |
|---|---|---|---|---|
| 게임 목록 | `IStoreService/GetAppList` | 정기 | 매일 06:00 UTC | 전체 게임 (DLC·소프트웨어·영상·하드웨어 제외). 목록에서 사라진 게임을 알기 위해 증분 대신 전체 |
| 가격·할인·리뷰 수 | `IStoreQueryService/Query` | 정기 | 매일 18:30 UTC | 전체 게임 KR, 페이지당 1,000건 |
| 동접자 top 100 | `ISteamChartsService/GetGamesByConcurrentPlayers` | 정기 | 1시간 | 호출 1회로 100개 |
| 동접자 관리 목록 | `ISteamUserStats/GetNumberOfCurrentPlayers` | 정기 | 1시간 | `player_watchlist` 활성 게임 |
| 동접자 스캔 | `ISteamUserStats/GetNumberOfCurrentPlayers` | 정기 | 매일 21:00 UTC | 스캔 일정이 도래한 게임만 (4절) |
| 게임 메타 | `store/api/appdetails` | 정기 (저빈도) | 신규 등록 시 1회 + 주 1회 | 관리 목록과 분석 요청 게임 |
| 리뷰 상세 | `store/appreviews` 날짜 구간 | On-demand | 요청 시 | 요청한 게임·언어·기간 |
| 리뷰 평가 추이 | `store/appreviewhistogram` | On-demand | 요청 시 | 요청한 게임·언어 |
| 뉴스·패치노트 | `ISteamNews/GetNewsForApp` | On-demand | 요청 시 | 필요 시 추가 |

- 리뷰 요약 일별 적재는 하지 않음. `appreviewhistogram`이 출시 이후 월별과 최근 30일 일별 긍정·부정 수를 제공함을 2026-11-18 CS2로 확인
- `appreviews`, `appreviewhistogram`, `appdetails`는 비공식 엔드포인트라 응답 구조 변경 감시 필요
- `Query`의 가격·리뷰 수 필드와 페이지당 최대 건수는 구현 전 실측 필요

## 3. 처리 구조

```mermaid
flowchart LR
    API[Steam API] --> P[Producer task]
    P --> K[Kafka 토픽]
    K --> C[Consumer task]
    C --> RAW[SeaweedFS steam-raw]
    RAW --> S[Silver 변환]
    S --> DB[PostgreSQL]
    DB --> M[dbt 집계·마트]
```

- **Kafka 유지:** producer-consumer 구조로 외부 연계를 쉽게 하고 API 대기 작업의 버퍼로 사용
- **Spark 제거:** consumer는 Airflow task에서 토픽을 끝까지 읽고 parquet로 S3 저장소에 저장한 뒤 offset commit
- **S3 저장소:** MinIO 공식 이미지 배포 중단으로 로컬은 SeaweedFS로 교체. Airflow 연결 ID `s3_storage`, 운영은 같은 연결을 AWS S3로 전환
- **전달 보장:** at-least-once. 중복은 Silver에서 키 기준으로 제거
- **토픽:** 데이터 종류별 1개, 메시지 key는 `appid`

| 토픽 | 생산자 |
|---|---|
| `steam.app_list` | 게임 목록 |
| `steam.prices` | 가격·할인·리뷰 수 |
| `steam.players` | top 100과 관리 목록 동접자를 한 토픽으로 병합 |
| `steam.player_scan` | 동접자 스캔 |
| `steam.app_details` | 게임 메타 |
| `steam.reviews` | 리뷰 상세 |
| `steam.review_histogram` | 리뷰 평가 추이 |

- **DAG 연결:** 정기 스냅샷은 producer → consumer → Silver를 한 DAG 안의 task로 연결. Airflow Dataset 이벤트는 Silver가 읽을 스냅샷 경로를 전달하지 못해 보류 (3단계에서 변경)
- **On-demand DAG:** `schedule=None`, 파라미터 `app_ids`, `languages`, `start_date`, `end_date`. 날짜 구간 체크포인트로 중단 후 재개

## 4. 동접자 관리 목록과 스캔 최적화

목표는 전체 게임을 매일 조회하지 않고 IP 차단 위험 없이 관리 목록을 유지하는 것

### 컨트롤 테이블 `player_watchlist`

| 컬럼 | 용도 |
|---|---|
| `appid` | 게임 ID |
| `source` | `scan` 자동 등록, `manual` 수동 등록, `top100` 순위 진입 |
| `first_top100_at` | top 100 최초 진입 시각. 값이 있으면 고정 수집 대상 |
| `is_active` | 1시간 수집 대상 여부 |
| `last_player_count`, `recent_peak` | 최근 스캔·수집 값 |
| `next_scan_at` | 다음 스캔 예정 시각 |
| `scan_interval_days` | 현재 스캔 간격 |
| `consecutive_low_scans` | 연속 저조 스캔 횟수 |

### 스캔 대상 줄이기

- **간격 차등:** 스캔 결과가 낮을수록 다음 스캔 간격을 늘림 (예: 1일 → 2일 → 4일 … 최대 30일). 다시 오르면 1일로 복귀
- **우선 스캔:** 신규 출시, 최근 할인 시작, `Query` 리뷰 수 급증 게임은 간격과 무관하게 다음 스캔에 포함
- **사전 필터:** 매일 받는 `Query` 리뷰 수를 활동성 신호로 사용. 리뷰가 거의 없는 게임은 긴 간격에서 시작
- **결과:** 매일 스캔 대상은 `next_scan_at`이 도래한 게임만이라 전체 게임 수보다 훨씬 적음

### 관리 목록 선정: 기준값 대신 호출 예산

- 고정 기준값(예: 100명) 대신 시간당 호출 예산 `N`으로 관리 목록 크기를 정함
- 스캔 결과를 최근 피크 기준으로 정렬해 상위 `N`개를 활성화하고 나머지는 비활성
- `manual` 등록 게임은 예산과 별도로 항상 활성

### top 100 경계 게임 처리

- top 100 경계의 게임은 순위에서 갑자기 빠졌다 다시 들어오기도 함. 빠진 동안 수집이 끊기면 시계열에 구멍이 생김
- top 100에 한 번이라도 들어온 게임은 매시 수집 때 `player_watchlist`에 `first_top100_at`과 함께 즉시 등록
- 이 게임은 순위를 벗어나도 계속 수집. 예산 정렬과 저조 스캔으로 자동 비활성하지 않고 수동으로만 해제
- 매시 수집 순서: top 100 호출 → 신규 진입 게임 등록 → 활성 관리 목록 중 이번 top 100 응답에 없는 게임만 개별 호출. 같은 게임 중복 호출 없음
- 동접자 팩트에 값 출처(`top100`, `individual`)를 남겨 두 API 값 차이를 나중에 검증
- `N`과 간격 규칙은 Airflow Variable로 두고 첫 전체 스캔 분포를 보고 조정

### 요청률 제어

- 모든 동접자 호출이 공유하는 token bucket. 기본 초당 5건 (2026-11-18 측정 무오류 상한 초당 40건의 1/8)
- 429 수신 시 `Retry-After`만큼 대기 후 요청률 절반으로 감소, 연속 429 시 해당 실행 중단과 알림
- 최초 전체 스캔은 여러 날에 나눠 실행

## 5. Silver 원칙

- **스냅샷 팩트 (동접자·가격):** append-only, 키 `(app_id, collected_at)`. 수집 실패는 0이 아닌 NULL
- **리뷰:** `(appid, recommendationid)`당 최신본 1행 upsert. 요청 단위 처리, Bronze 전체 재스캔 없음
- **역할:** Silver는 정제·중복 제거·타입 통일까지. 일별 리뷰 수·플레이타임 등 집계는 dbt로 이동
- **시각:** 모두 UTC 저장

### 동접자 저장 구조

관리 게임 N개 × 수집 시각 M개만큼 증가. 3,000개 매시 기준 연 약 2,600만 행으로 추정

| 레이어 | 위치 | 단위 | 정렬 | 용도 |
|---|---|---|---|---|
| Bronze | `steam-raw/players/dt=YYYY-MM-DD/hour=HH/` | 매시 1파일, 그 시각 전체 게임 | 없음 | 원본 보관·재처리 |
| Silver | `steam-silver/players/month=YYYY-MM/` | 월 1파일, 매일 재작성 | `app_id`, `collected_at` | 전체 시계열 보관본 |
| DB | `fact_player_counts` | 1시간 1행, 전체 기간 | 월별 파티션 | 분석 조회 |
| dbt | 동접자 일별 집계 | 하루 1행 | - | 장기 추이·마트 |

- 게임별 파일은 만들지 않음. S3 객체는 append 불가이고 작은 파일이 대량 발생
- DB 인덱스 `(app_id, collected_at)`. 1시간 단위 원본을 기간 제한 없이 보관
- parquet은 조회 경로가 아닌 보관본. DB 재구축과 외부 내보내기에 사용
- 정렬된 parquet은 row group의 `app_id` min/max 통계로 게임별 부분 읽기 가능
- 용량 문제 발생 시 오래된 월 파티션을 분리하고 parquet 직접 조회 또는 on-demand 복원 DAG 추가

## 6. 제거 대상

- 형태소 파이프라인: `dags/gold_morphemes_DAG.py`, `plugins/operators/gold_reviews_to_morphemes.py`, `init-db/04_create_gold_morphemes.sql`, 관련 의존성
- Spark: `plugins/operators/spark_reviews_kafka_to_s3.py`, `plugins/hooks/spark_hook.py`, Compose의 Spark 서비스, Airflow 이미지 JDK, 관련 통합 테스트
- 매일 리뷰 수집 DAG: on-demand DAG로 대체
- `silver_review_daily`, `silver_review_playtime`: dbt 모델로 대체

## 7. 작업 순서

각 단계 완료 시 커밋

1. 형태소·Spark 제거, MinIO → SeaweedFS 교체 (2026-11-19 완료, 2026-11-22 컨테이너 검증)
2. Kafka consumer task (토픽 → S3 parquet) (`KafkaTopicToS3Operator`, 2026-11-22 통합 테스트 통과)
3. 게임 목록·가격 정기 수집 (`Query` 실측 포함, 2026-11-22 구현·실제 API 1회 실행). 상세는 아래 "3단계 실측과 결정"
4. 동접자 top 100·관리 목록·스캔과 `player_watchlist` (2026-11-22 구현·실제 API 실행). 상세는 아래 "4단계 실측과 결정"
5. On-demand 리뷰·평가 추이 DAG
6. Silver 스키마 (`init-db`)와 operator, 테스트
7. dbt staging·마트 재작성
8. README·PIPELINE 갱신

## 8. 3단계 실측과 결정 (2026-11-22)

### API 실측

| API | 결과 |
|---|---|
| `GetAppList` 게임만 | 201,890~201,892개, 페이지당 5만 건 × 5회, 약 20MB, 약 8초. 필드 `appid`, `name`, `last_modified`, `price_change_number` |
| `GetAppList` `if_modified_since` 24시간 | 1,693개 (증분만으로는 목록 이탈을 알 수 없어 미사용) |
| `Query` 이름순 전체 순회 | 201,011건, 중복 0, 페이지당 최대 1,000건(5,000 요청 시에도 1,000), `start=100000`에서도 응답. 202회, 4.5~6분 |
| `Query` 응답 | 가격 있음 119,033, 무료 24,082, 할인 9,778, 리뷰 요약 전 항목. 게임 아닌 항목 2건 |
| `GetItems` | 요청당 250개까지 (300개 400 오류). 전체 조회에 800회 이상 필요해 미사용 |
| `GetAppList`만 있음 / `Query`만 있음 | 880개 / 1개 (출시 예정·KR 비공개 추정) |

### 결정

- **가격 지역:** KR만
- **저장 방식:** PostgreSQL에는 값이 바뀐 구간만 저장, 일별 전체 스냅샷은 S3 parquet에 보관
- **완료 판정:** producer가 전송 확인 후 manifest 기록, Silver는 manifest 건수와 고유 appid 수 일치 시에만 적용
- **오적용 차단:** 같은 스냅샷 재적용 건너뜀, 오래된 스냅샷 거부, 직전 대비 90% 미만 급감 시 중단
- **DAG 연결:** producer → consumer → Silver를 한 DAG에서 연결 (Dataset 보류)
- **API key:** Compose `AIRFLOW_VAR_STEAM_API_KEY`로 Variable 자동 등록, Hook 로그·예외에서 key 가림

### 실제 실행 (2026-11-22, 기존 볼륨 환경)

| 실행 | 결과 |
|---|---|
| 게임 목록 1회 | 201,892건 발행·저장, `dim_apps` 201,892행. 같은 스냅샷 Silver 재실행은 건너뜀 |
| 가격 1회차 | 201,011건, 가격·리뷰 구간 각 201,011행 신규 |
| 가격 2회차 (약 5분 뒤) | 가격 touch 201,010 / close 1 / insert 1 (다른 게임 1개씩 이탈·신규), 리뷰 touch 200,940 / close 71 / insert 71 |
| 불변 조건 | 게임당 열린 구간 1개 이하, `valid_to < valid_from` 0건, 오늘 Airflow 로그에 API key 포함 0건 |
| 용량 | 가격 구간 29MB, 리뷰 구간 25MB, `dim_apps` 25MB (1회차 기준) |

### 남은 일

- 기존 `steam_bronze_01_discount_game_extract`·`fact_price_history`·`tracked_games`는 4·5단계에서 정리 (지금은 그대로 동작)
- dbt 마트는 아직 `fact_price_history`를 사용. 7단계에서 `fact_price_intervals`로 전환
- 가격 구간의 실제 할인 시작·종료 추적은 18:30 UTC 정기 실행이 쌓인 뒤 확인

## 9. 4단계 실측과 결정 (2026-11-22)

### API 실측

| API | 결과 |
|---|---|
| `GetGamesByConcurrentPlayers` | 1회 100개, 약 0.2초, 7.6KB. 응답 `last_update`는 호출 시점보다 약 4분 이전. 100위 동접 약 13,000명 |
| top 100 값과 개별 API 값 | 하위 순위는 거의 같고 상위는 1~5% 차이 (갱신 시점 차이 추정). 그래서 값 출처를 `source`로 남김 |
| `GetNumberOfCurrentPlayers` 데이터 없는 게임 | HTTP 404 + `result: 42`. 실패가 아닌 `not_found`로 구분 |

### 결정

- **테이블:** 기존 `fact_concurrent_players`(serial id, 시간대 없는 timestamp, 테스트 400행)는 파티션 구조와 맞지 않아 새 `fact_player_counts`를 만듦. 기존 테이블은 7단계 dbt 전환 후 제거
- **수집 슬롯:** `collected_at`은 정시 슬롯, `fetched_at`은 실제 호출 시각. 슬롯보다 45분 넘게 늦은 실행은 실패
- **관리 목록 범위:** `player_watchlist`에 전체 게임의 스캔 일정을 함께 둠 (약 20만 행)
- **활성 게임 스캔 제외:** 매시 수집 값으로 `recent_peak`를 갱신하므로 일별 스캔 대상에서 뺌
- **우선 스캔:** 직전 상태가 있는 변화만 포함. 최초 카탈로그 적재 직후 전체 게임이 우선 대상이 되는 문제를 실행 중 발견해 수정 (201,797 → 1)
- **최초 전체 스캔:** 별도 분할 로직 없이 1회 최대 건수(기본 30,000)와 리뷰 수 순 정렬로 약 7일에 나눠 진행
- **스캔 시각:** 21:00 UTC. 18:30 가격 수집 결과(할인 시작·리뷰 급증)를 당일 반영

### 실제 실행 (2026-11-22)

| 실행 | 결과 |
|---|---|
| 매시 06:00 슬롯 | top 100 100건 적재, 고정 등록 100개, 월 파티션 `fact_player_counts_y2026m11` 자동 생성 |
| 스캔 (최대 300건 임시 설정) | 신규 등록 201,797개, 300건 조회 62초 (실패 0, 429 0), 저조 2개, 예산 활성 299개 |
| 매시 07:00 슬롯 | top 100 + 개별 299건, 개별 호출 약 61초(초당 약 4.9건), 실패 0, 429 0, 관리 목록 최근 값 399개 갱신 |
| 불변 조건 | 고정 대상 중 비활성 0, 활성 중 우선 스캔 표시 0, 실행 로그에 API key 포함 0건 |

### 남은 일

- 예산이 다 차지 않은 초기에는 동접 4명 같은 게임도 활성. 전체 스캔이 진행되면 상위 게임으로 교체됨
- 예산 N·저조 기준값은 첫 전체 스캔 분포를 보고 조정
- Silver 월별 parquet 보관본(5절)은 6단계에서 구현
- dbt 동접자 마트는 아직 `fact_concurrent_players`를 사용. 7단계에서 `fact_player_counts`로 전환
