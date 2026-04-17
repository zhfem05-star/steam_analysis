# Steam 데이터 파이프라인 구조

## 전체 흐름

```
Steam API
  └─ Bronze Layer (S3 steam-raw) ── 원시 데이터 그대로 저장
       └─ Silver Layer ─────────── 변환·적재 (PostgreSQL / S3 steam-silver)
            └─ Gold Layer ──────── 분석용 집계·마트 (PostgreSQL / dbt)
```

---

## 레이어별 저장소 요약

| 레이어 | 저장소 | 데이터 |
|--------|--------|--------|
| **Bronze** | `S3 steam-raw` | 원시 JSON/Parquet (할인게임 목록, 리뷰 청크, 동접자 수, 앱 상세) |
| **Silver** | `S3 steam-silver` | 정제된 리뷰 Parquet (언어·월별 파티셔닝) |
| **Silver** | `PostgreSQL` | `dim_games`, `dim_genres`, `dim_game_genres`, `fact_price_history`, `fact_concurrent_players`, `fact_review_daily` |
| **Gold** | `PostgreSQL` | `gold_review_morphemes` (형태소 빈도), `mart_*` (dbt 마트 테이블) |

### PostgreSQL 테이블 전체 목록

```
[컨트롤]
  tracked_games                ← 수집 대상 게임 목록 및 상태

[Silver - 디멘전]
  dim_games                    ← 게임 기본 정보 (이름, 개발사, 플랫폼 등)
  dim_genres                   ← 장르 코드·이름
  dim_game_genres              ← 게임-장르 N:M 관계

[Silver - 팩트]
  fact_price_history           ← 일별 가격·할인 스냅샷
  fact_concurrent_players      ← 일별 동접자 수 스냅샷
  fact_review_daily            ← 언어·날짜별 리뷰 작성 수 집계

[Gold]
  gold_review_morphemes        ← 게임·언어별 형태소 빈도 (워드클라우드용)
  mart_game_overview           ← 게임 종합 정보 (dim + 최신 가격 + 장르)
  mart_player_daily_avg        ← 동접자 일평균
  mart_discount_analysis       ← 할인율 이력 집계
  mart_discount_player_trend   ← 할인 구간별 동접자 수 추이
  mart_discount_review_trend   ← 할인 구간별 리뷰 수 추이 (A/B 비교)
```

### S3 버킷 구조

```
steam-raw/                              ← Bronze 원시 데이터
  discount_games/
    {YYYYMMDD_HHMM}_discount_game_data.json
  reviews/
    {YYYYMMDD_HHMM}/
      {appid}_chunk_{N:03d}.parquet
  player_counts/
    {YYYYMMDD_HHMM}_player_counts.json
  appdetails/
    {YYYYMMDD_HHMM}/
      {appid}.parquet

steam-silver/                           ← Silver 정제 데이터 (대용량 리뷰만 S3 저장)
  reviews/
    appid={appid}/
      language={lang}/                  ← 경로만으로 언어 구분 (korean/english)
        year={Y}/month={M}/
          {filename}.parquet
```

---

## Bronze Layer

### 01. 할인 게임 수집 `steam_bronze_01_discount_game_extract`

**스케줄:** 매일 08:00 UTC (한국시간 오후 5시)

| Task | 설명 | 출력 |
|------|------|------|
| `fetch_discount_games` | Steam Query API로 할인 중인 게임 목록 조회 (최대 400개, 1% 이상 할인, KR 기준) | `S3: discount_games/{YYYYMMDD_HHMM}_discount_game_data.json` |
| `push_app_ids` | S3 JSON에서 appid 목록 추출 → XCom push | XCom: `app_ids` 리스트 |
| `upsert_tracked_games` | appid 목록을 `tracked_games` 컨트롤 테이블에 UPSERT | PostgreSQL: `tracked_games` |

**수집 필드 (store_items[]):**
```
appid, name, best_purchase_option.original_price_in_cents,
best_purchase_option.final_price_in_cents, best_purchase_option.discount_pct,
best_purchase_option.active_discounts[].discount_end_date
```

**Silver 연결:** → `steam_silver_02_fact_price` (가격·할인 이력)

---

### 02. 리뷰 수집 `steam_bronze_02_review_extract`

**스케줄:** 매일 08:00 UTC

| Task | 설명 | 출력 |
|------|------|------|
| `collect_reviews` | `tracked_games`의 is_active=TRUE, collect_reviews=TRUE 게임 대상으로 한국어·영어 리뷰 증분 수집 | `S3: reviews/{YYYYMMDD_HHMM}/{appid}_chunk_{N:03d}.parquet` |

**수집 필드:**
```
recommendationid, author (struct), language, review,
timestamp_created, timestamp_updated, voted_up,
votes_up, votes_funny, steam_purchase, received_for_free,
written_during_early_access, appid (추가)
```

**수집 방식:**
- `tracked_games.review_cursors`에 언어별 cursor 저장 → 다음 실행 시 이어서 수집 (증분)
- 1000건마다 Parquet 청크 파일 1개 생성
- 수집 완료 후 `tracked_games.reviews_collected_at` 갱신

**Silver 연결:** → `steam_silver_03_fact_reviews` (S3 silver 파티셔닝)

---

### 03. 동접자 수 수집 `steam_bronze_03_player_count_extract`

**스케줄:** 매일 08:00 UTC

| Task | 설명 | 출력 |
|------|------|------|
| `collect_player_counts` | `tracked_games`의 is_active=TRUE, collect_players=TRUE 게임 대상으로 현재 동접자 수 수집 | `S3: player_counts/{YYYYMMDD_HHMM}_player_counts.json` |

**수집 필드:**
```
appid, player_count, collected_at
```

**Silver 연결:** → `steam_silver_04_fact_players` (동접자 수 이력)

---

### 04. 앱 상세 정보 수집 `steam_bronze_04_app_details_extract`

**스케줄:** 매일 09:00 UTC (01번 완료 후)

| Task | 설명 | 출력 |
|------|------|------|
| `collect_app_details` | `tracked_games`의 is_active=TRUE 게임 대상으로 appdetails API 호출, appid 1개당 파일 1개 저장 | `S3: appdetails/{YYYYMMDD_HHMM}/{appid}.parquet` |

**수집 필드:**
```
appid, name, type, is_free, required_age, short_description,
developers, publishers,
genres: [{genre_id, genre_name}],
platforms_windows, platforms_mac, platforms_linux,
header_image, release_date_coming_soon, release_date_str,
recommendations_total, collected_at
```

**Silver 연결:** → `steam_silver_01_dim_games` (게임·장르 디멘전)

---

## Silver Layer

### 01. 게임·장르 디멘전 `steam_silver_01_dim_games`

**스케줄:** 매일 10:00 UTC  
**소스:** `S3 steam-raw: appdetails/{YYYYMMDD}/`

| 적재 테이블 | 주요 컬럼 | 처리 내용 |
|------------|---------|---------|
| `dim_games` | app_id, name, type, is_free, developers, publishers, release_date, platforms_*, header_image_url, total_recommendations | release_date 문자열 → DATE 파싱, UPSERT |
| `dim_genres` | genre_id, genre_name | genres 배열에서 추출, UPSERT |
| `dim_game_genres` | app_id, genre_id | 게임-장르 N:M 관계, UPSERT |

---

### 02. 가격·할인 이력 `steam_silver_02_fact_price`

**스케줄:** 매일 09:00 UTC  
**소스:** `S3 steam-raw: discount_games/{YYYYMMDD}/`

| 적재 테이블 | 주요 컬럼 | 처리 내용 |
|------------|---------|---------|
| `fact_price_history` | app_id, currency(KRW), initial_price, final_price, discount_percent, is_free, discount_end_at, collected_at | 가격 단위(원), 할인 종료일 Unix→datetime 변환, UPSERT |

---

### 03. 리뷰 파티셔닝 `steam_silver_03_fact_reviews`

**스케줄:** 매일 09:00 UTC  
**소스:** `S3 steam-raw: reviews/{YYYYMMDD}/`

| 적재 위치 | 파티션 구조 | 처리 내용 |
|----------|-----------|---------|
| `S3 steam-silver: reviews/` | `appid={id}/language={lang}/year={Y}/month={M}/{filename}.parquet` | author struct 펼치기, timestamp → UTC datetime, year·month 파티션 컬럼 추가 |

---

### 04. 동접자 수 이력 `steam_silver_04_fact_players`

**스케줄:** 매일 09:00 UTC  
**소스:** `S3 steam-raw: player_counts/{YYYYMMDD}/`

| 적재 테이블 | 주요 컬럼 | 처리 내용 |
|------------|---------|---------|
| `fact_concurrent_players` | app_id, concurrent_in_game, collected_at | 같은 날 복수 스냅샷 모두 적재, UPSERT |

---

### 05. 리뷰 일별 집계 `steam_silver_05_review_daily`

**스케줄:** 매일 10:00 UTC  
**소스:** `S3 steam-silver: reviews/appid={id}/language={lang}/...`

| 적재 테이블 | 주요 컬럼 | 처리 내용 |
|------------|---------|---------|
| `fact_review_daily` | app_id, language, review_date, review_count | Silver S3 리뷰 전체 읽어 언어·날짜별 건수 집계, UPSERT |

- `tracked_games`의 is_active=TRUE 게임 전체 대상
- `timestamp_created` 기준으로 날짜 집계 (UTC)
- 한국어(`korean`) / 영어(`english`) 각각 별도 행으로 저장

**Gold 연결:** → `steam_gold_01_review_morphemes`, `steam_gold_02_dbt_marts`

---

## Gold Layer

### 01. 형태소 빈도 집계 `steam_gold_01_review_morphemes`

**스케줄:** 매일 11:00 UTC (Silver 03 완료 후)  
**소스:** `S3 steam-silver: reviews/`

| 적재 테이블 | 주요 컬럼 | 처리 내용 |
|------------|---------|---------|
| `gold_review_morphemes` | app_id, language, morpheme, frequency, review_appearances, earliest_review_at, latest_review_at | 리뷰 전문에서 형태소 추출 후 빈도 집계, UPSERT |

**언어별 분석 방식:**

| 언어 | 라이브러리 | 추출 대상 |
|------|----------|---------|
| 한국어 | `kiwipiepy (Kiwi)` | 일반명사(NNG) + 고유명사(NNP), 2글자 이상 |
| 영어 | 정규식 | 알파벳 단어, 불용어 제거, 3글자 이상 |

- 게임·언어별 상위 1,000개 형태소만 저장 (`top_n=1000`)
- 형태소 최대 길이 100자 제한
- Superset 워드클라우드 시각화에 활용

---

### 02. dbt 마트 빌드 `steam_gold_02_dbt_marts`

**스케줄:** 매일 12:00 UTC (Silver + Gold 01 완료 후)  
**실행 방식:** DockerOperator → `steam-dbt:latest` 컨테이너 실행 후 자동 종료

#### Staging (VIEW)

Silver PostgreSQL 테이블을 그대로 참조하는 가벼운 뷰. 데이터 저장 없음.

| 스테이징 뷰 | 소스 테이블 |
|------------|----------|
| `stg_games` | `dim_games` |
| `stg_price_history` | `fact_price_history` |
| `stg_concurrent_players` | `fact_concurrent_players` |
| `stg_review_daily` | `fact_review_daily` |

#### Marts (TABLE)

분석·시각화 목적의 영구 집계 테이블.

| 마트 테이블 | 설명 | 주요 컬럼 |
|-----------|------|---------|
| `mart_game_overview` | 게임 종합 정보 | app_id, name, genres, 최신 가격·할인율, 플랫폼, 추천 수 |
| `mart_player_daily_avg` | 동접자 일평균 | app_id, date, daily_avg_players |
| `mart_discount_analysis` | 할인 이력 집계 | app_id, 할인 횟수, 평균·최대 할인율, 역대 최저가 |
| `mart_discount_player_trend` | 할인 구간별 동접자 추이 | app_id, date, is_discounted, concurrent_players, ab_group |
| `mart_discount_review_trend` | 할인 구간별 리뷰 추이 (A/B) | app_id, date, total/korean/english 리뷰 수, is_discounted, ab_group, baseline_7d_avg |

**`mart_discount_review_trend` 활용 예시:**
- X축: 날짜, Y축: 리뷰 수, 할인 구간 하이라이트
- `ab_group = 'discount'` vs `'normal'` 리뷰 수 평균 비교
- `baseline_7d_avg`: 할인 시작 전 7일 평균 (비교 기준선)

---

## 컨트롤 테이블 `tracked_games`

Bronze 파이프라인 전체가 이 테이블을 기준으로 수집 대상을 결정한다.

| 컬럼 | 역할 |
|------|------|
| `appid` | 수집 대상 게임 식별자 |
| `is_active` | FALSE면 모든 수집에서 제외 |
| `collect_reviews` | FALSE면 02번 리뷰 DAG에서 제외 |
| `collect_players` | FALSE면 03번 동접자 DAG에서 제외 |
| `review_cursors` | 언어별 Steam 리뷰 페이지네이션 cursor (증분 수집용) |
| `reviews_collected_at` | 마지막 리뷰 수집 완료 시각 |
| `last_discounted` | 마지막으로 할인 확인된 날짜 |

