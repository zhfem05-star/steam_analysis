# 코드 전체 검수 로드맵

전체 데이터를 초기화(2026-09-19)하고 처음부터 코드를 검수하기 위한 로드맵.
Bronze -> Silver -> Gold/Mart 순서로, 하위 레이어가 의존하는 순서대로 진행.
챕터 하나가 끝나면 `type(chN): description` 형식으로 커밋한다.

## ch1. Bronze - 수집 훅/오퍼레이터

- [ ] `plugins/hooks/steam_api.py` - appreviews/appdetails 파라미터, 커서 페이지네이션 로직 검수
  - [x] `date_range_type`/`start_date`/`end_date` 반영 여부 (10만 건 커서 캡 우회용)
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


## Review collection integrity (implementation vs. integration)

- [x] Date-window and continuous backfill/incremental checkpoints (JSONB v3).
- [x] Kafka per-message acknowledgements before checkpoint persistence.
- [x] Overlapping timestamp boundaries with bounded cursor rollover replay.
- [x] Cross-file review-ID dedup for daily counts, playtime and morphemes.
- [x] Silver source-key identity to avoid overwriting different collection runs.
- [x] Production DAG wiring and service-free pytest verification.
- [x] Small Steam probe: positive start_date works; zero can disable date filtering.
- [x] 2026-10-16 live Steam: CS2 English fixed 10-minute range, 3 distinct reviews; bounds, descending order and collector completion verified (1 passed). This does not establish full-history completeness.
- [x] 2026-10-16 Docker: fixed-input Kafka/Postgres checkpoint integration and Kafka/Spark/MinIO multi-batch delivery (2 passed); offline suite in the same container (40 passed). See [runtime evidence](review_checkpoints.md#2026-10-16-docker-런타임-검증).
- [ ] Real Postgres/Kafka/Spark/MinIO restart and failure integration tests.
- [ ] Real collection beyond 100,000 reviews; compare distinct IDs and boundary timestamps.
- [ ] Verify broker retention covers backfill duration and benchmark full-history Silver scans.

Rollout and test commands: [review_checkpoints.md](review_checkpoints.md).

## 제출 준비: 성능 근거 점검 (2026-10-16)

- [x] README에 프로젝트 목적·Git 이력 기준 개발 기간, 현재 구조·설정·실행 순서, 테스트 결과 링크, 새 DB 실험과 알려진 한계 정리. 신규 환경 전체 설치와 본인 기여 최종 확인은 완료로 표시하지 않음
- [x] 남아 있는 task 로그와 개선 커밋을 제한된 범위에서 확인하고 [성능 근거 점검](performance_evidence.md)에 기록
- [x] 2026-10-16 고정 1만 행·5회·TEMP 테이블로 DB 호출 방식 새로 측정. executemany는 행별 execute와 유사, execute_values는 개선 후보로 확인. 과거 성과 및 운영 코드 적용과 구분
- [ ] 약 2GB → 800MB, 6,140초 → 5,000초, DB 210초 → 18초 및 500 → 5,800 rows/s의 원본 측정 조건 확인. 현재는 수치 근거 미확인으로 보류
- [ ] 포트폴리오에서 확인된 개선 구현과 미확인 성능 수치를 구분하고 본인 기여 범위 최종 확인
