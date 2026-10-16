# 리뷰 수집 체크포인트와 무결성

## 현재 동작

`tracked_games.review_cursors` JSONB 안에 언어별 상태를 저장한다. 컬럼 추가나 SQL 데이터 변환은 필요 없다.

운영 DAG는 `collection_mode="continuous"`를 사용한다.

- 최초 과거 수집: 2000-01-01 UTC부터 첫 실행의 `data_interval_end`까지 수집한다. 재시작해도 최초 종료 시각을 유지한다.
- 증분 수집: 과거 수집 완료 후, `collected_through - 86400초`부터 새 실행의 `data_interval_end`까지 수집한다. 하루 겹침은 늦게 나타나는 리뷰를 다시 확인하기 위한 기본값이며 조정 가능하다.
- 중단된 증분 구간은 원래 종료 시각까지 먼저 완료한다. 이후 최신 실행 시각까지 추가 구간을 처리한다.
- `collected_through`는 해당 구간 전체의 Kafka 전송 결과를 확인한 후에만 전진한다. `reviews_collected_at`은 작업 시각이며 데이터 워터마크가 아니다.

상태는 다음 구조다. `backfill`과 `incremental` 안에는 cursor, 날짜 범위, 현재 구간, 건수, 마지막 리뷰 시각, 완료 여부가 들어간다.

```json
{
  "english": {
    "version": 3,
    "mode": "continuous",
    "start_date": 946684800,
    "filter_type": "recent",
    "date_range_type": "include",
    "window_days": 30,
    "overlap_seconds": 1,
    "incremental_overlap_seconds": 86400,
    "backfill": {"...": "최초 과거 수집의 고정 범위와 진행 상태"},
    "incremental": null,
    "collected_through": null
  }
}
```

`fixed` 모드는 start_date/end_date가 고정된 한 번의 수집을 위한 호환 모드다. 기존 문자열 cursor는 날짜 없는 fixed 모드에서 읽을 수 있다. 구형 날짜 체크포인트 또는 설정이 달라진 상태는 v3 수집 범위를 처음부터 재수집한다. 기존 cursor에서 날짜 범위를 추측해서 이어가지 않는다.

## 경계 겹침과 중복 제거

30일 구간을 최신부터 과거로 이동한다. 인접 구간은 1초를 겹친다. 한 cursor에서 5만 건을 읽으면 마지막으로 받은 리뷰 시각에 1초를 더한 값을 새 종료 시각으로 삼고 cursor를 초기화한다. 따라서 이미 읽은 구간 전체를 절반으로 나눠 반복하지 않는다.

이 방식은 recent 정렬과 날짜 필터를 전제로 한다. 응답의 timestamp_created가 없거나, 범위를 벗어나거나, 최신순을 어기면 오류로 중단한다. 한 시각에 리뷰가 너무 많아 경계를 전진시킬 수 없는 경우에도 완료로 기록하지 않는다. 실패 전에 Kafka가 확인한 진행 상태로 재개한다.

Bronze는 경계 중복과 재시도 중복을 보존한다. 수집한 리뷰 ID가 빠지지 않았는지 비교할 때 사용할 수 있다. 다만 중복이 존재한다는 사실만으로 완전성을 입증할 수는 없다.

`utils/review_dedup.py`에서 `appid + recommendationid`로 중복을 제거한다.

1. timestamp_updated가 가장 최신인 리뷰를 선택한다.
2. 수정 시각이 같으면 `_collected_at`이 최신인 수집본을 선택한다. `_collected_at`은 UTC Unix 마이크로초다.
3. 예전 파일처럼 수집 시각이 없으면 정렬된 원본 파일 순서를 사용한다.
4. ID가 없거나 비어 있으면 조용히 합치지 않고 실패한다.

Silver 변환에서는 청크 내부 중복을 제거한다. 일별 건수·플레이타임·Gold 형태소 집계에서는 해당 앱의 파일들을 합친 뒤 파일과 실행을 가로질러 중복 제거한다. 언어가 바뀐 리뷰도 최신본을 선택한 후 언어별로 나눈다. 기존에 사라진 그룹이나 단어가 남지 않도록 집계 교체는 DB 트랜잭션 안에서 수행한다.

Silver 파일명에는 전체 Bronze 원본 키의 해시를 넣는다. 다른 실행의 같은 청크 번호가 서로 덮어쓰지 않고, 같은 원본 재처리는 같은 키에 저장된다. 과거 이름의 파일은 자동 삭제하지 않는다. 집계 시 새 파일과 함께 ID로 중복 제거한다.

## Kafka와 DAG

- Kafka producer는 `acks="all"`을 사용한다.
- flush 후 전송 Future 전부의 성공을 확인한다. 하나라도 실패하면 해당 체크포인트를 저장하지 않는다.
- 페이지 기준 1,000건마다, 구간 전환·완료 시, 종료 시 체크포인트를 저장한다.
- 전송 후 DB 저장 전에 중단되면 중복 발행은 가능하다. 정확히 한 번 전송을 보장하는 구조는 아니다.
- 앱별로 Kafka hook을 분리하며, 일부 앱 실패도 수집 task 실패로 처리한다.
- Bronze DAG는 `max_active_runs=1`이다. 수집 task가 실패해도 `all_done`으로 Kafka→Bronze task를 실행해 이미 발행한 데이터를 처리한다.
- Spark는 트리거당 최대 10,000개 offset을 읽도록 설정했다. 대량 backfill을 한 번에 driver 메모리로 모으는 것을 줄인다. [Spark 공식 옵션](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- Silver DAG는 `reviews/` 전체를 조회한다. 지연된 Bronze 파일을 날짜별 실행 prefix 때문에 놓치지 않게 한다. 현재는 전체 조회·재처리 비용이 발생하므로 향후 원본 처리 목록을 관리하는 최적화가 필요하다.

## 확인한 범위

- 오프라인 pytest: 실제 프로젝트 모듈, Kafka hook 코드, Polars 연산을 실행한다. Airflow adapter와 외부 서비스만 대체한다.
- 재시작 전후 ID 합집합, 경계 중복, 비동기 전송 실패, 워터마크, 중복에 영향받지 않는 집계 등을 검증한다.
- 실제 Steam 소량 확인: CS2 영어 리뷰를 조건별 10건씩 조회했다. `start_date=0`에서 과거 end_date가 무시된 응답이 관측됐다. `1`과 `946684800`에서는 요청 범위 안의 최신순 응답을 받았다. 따라서 날짜 수집 시작 시각 0은 거부한다.
- 실제 10만 건 초과 수집과 DB/Kafka/Spark/MinIO 통합 검증은 아직 실행하지 않았다.

```powershell
.venv-review/Scripts/python.exe -m pytest --confcutdir=tests/offline tests/offline -q
```

테스트 환경: Python 3.12, pytest 9.1.1, Polars 2.0.0, kafka-python 3.0.11. 운영 환경 의존성과의 통합 확인은 별도로 필요하다.

## 집에서 적용할 순서

1. 관련 DAG를 pause하고 실행 중인 수집 작업을 종료한 뒤 `tracked_games`의 appid/review_cursors/reviews_collected_at을 백업한다.
2. hook, utils, operator, DAG를 함께 배포한다. SQL 스키마 변경은 없다.
3. 테스트할 게임이 tracked_games에 활성 상태로 등록되어 있는지 확인한다. continuous 모드의 명시적 app_ids도 DB에서 상태를 읽는다. fixed 모드의 명시적 app_ids는 기존처럼 상태 읽기를 생략하므로 재시작 검증에 사용하지 않는다.
4. 운영 전체 수집 전에 한 게임·한 언어로 시작한다. 중간 실패 후 같은 구간/cursor로 재개되는지 확인한다.
5. 다음 통합 테스트를 서비스가 준비된 컨테이너에서 실행한다. 테스트는 임의 appid의 컨트롤 행을 만들었다가 정리하며 Kafka에는 테스트 리뷰를 발행한다.

```sh
docker exec airflow-scheduler pytest /opt/airflow/tests/integration/operators/test_review_checkpoint_resume_integration.py -m integration -v
docker exec airflow-scheduler pytest /opt/airflow/tests/integration/operators/test_reviews_kafka_to_s3_integration.py -m integration -v
```

```sql
SELECT appid, review_cursors, reviews_collected_at
FROM tracked_games
WHERE appid = 730;
```

6. 10만 건을 넘는 게임에서 경계 전후 ID·작성 시각, 원본 건수와 distinct ID 수, Silver/집계 결과를 비교한다. 실패·재시도 전후 distinct ID가 사라지지 않는지 확인한다.
7. Kafka 보관 기간이 과거 수집 소요 시간보다 긴지, broker 복제 설정이 필요한 내구성을 충족하는지 확인한다. producer ACK는 S3 적재 완료를 의미하지 않는다.
8. 기존에 덮어써진 Silver 파일은 남아 있는 Bronze 전체를 다시 처리해 복구한다. 이미 삭제된 Bronze까지 코드가 복원할 수는 없다.

구버전 operator는 v3 객체 상태를 읽을 수 없다. 롤백 시에는 DAG를 멈추고 백업 체크포인트와 호환되는 코드로 함께 되돌려야 한다.

## 남은 범위

증분 범위는 작성 시각을 기준으로 한다. 하루 겹침보다 늦게 나타나는 오래된 리뷰, 오래된 리뷰의 수정·삭제를 전부 탐지하는 별도 reconciliation은 아직 없다. API가 노출하는 리뷰와 현재 언어/구매/기본 필터 범위 안에서의 수집이다. 원본 보존과 중복 제거가 실제 서비스의 완전한 이력을 보장하는 것은 아니다.

Silver 및 집계가 앱 단위로 원본을 다시 읽는 비용, Kafka 보관 기간, 실제 API 날짜 경계와 장기 cursor 동작은 운영 규모에서 추가 검증한다.