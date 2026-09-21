# 코드 전체 검수 로드맵

전체 데이터를 초기화(2026-09-19)하고 처음부터 코드를 검수하기 위한 로드맵.
Bronze -> Silver -> Gold/Mart 순서로, 하위 레이어가 의존하는 순서대로 진행한다.
챕터 하나가 끝나면 `type(chN): description` 형식으로 커밋한다.

## ch1. Bronze - 수집 훅/오퍼레이터

- [ ] `plugins/hooks/steam_api.py` - appreviews/appdetails 파라미터, 커서 페이지네이션 로직 검수
  - [x] `date_range_type`/`day_range`/`start_date`/`end_date` 반영 여부 (10만 건 커서 캡 우회용)
  - [x] `purchase_type`, `language` 기본값이 문서와 실제로 일치하는지
- [ ] `plugins/hooks/s3_hook.py`
- [ ] `plugins/hooks/kafka_hook.py`
- [ ] `plugins/operators/steam_api_to_s3.py`
- [ ] `plugins/operators/steam_app_details_to_s3.py`
- [ ] `plugins/operators/steam_players_to_s3.py`
- [ ] `plugins/operators/steam_reviews_to_kafka.py` / `spark_reviews_kafka_to_s3.py`
- [ ] `plugins/operators/upsert_tracked_games.py` - 컨트롤 테이블 upsert 로직
- [ ] `dags/bronze_api_extract_*.py` 4개 - 스케줄/의존성 확인

## ch2. Silver - 정제 오퍼레이터

- [ ] `plugins/operators/silver_reviews_to_s3.py`
- [ ] `plugins/operators/silver_appdetails_to_dim.py`
- [ ] `plugins/operators/silver_discount_to_fact_price.py`
- [ ] `plugins/operators/silver_players_to_fact.py`
- [ ] `plugins/operators/silver_review_daily_to_fact.py` - 전체 재집계 방식이 중복 카운팅 여지 없는지
- [ ] `plugins/operators/silver_review_playtime_to_fact.py`
- [ ] `dags/silver_*.py` 6개 - 스케줄/의존성 확인

## ch3. Gold/Mart

- [ ] `plugins/operators/gold_reviews_to_morphemes.py`
- [ ] `dags/gold_dbt_marts_DAG.py`, `dags/gold_morphemes_DAG.py`
- [ ] dbt 프로젝트 (mart_* 6개 모델)

## ch4. 인프라/운영

- [ ] `plugins/hooks/snowflake_hook.py`, `plugins/hooks/spark_hook.py`
- [ ] `plugins/callbacks/slack_callback.py`
- [ ] DB 이중화/failover (`DB_FAILOVER_RUNBOOK.txt`, replica 설정)
- [x] `# Steam Store API review (비공식).txt` 문서 최신화 (이번에 확인한 실제 기본값 반영)
