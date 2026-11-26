FROM apache/airflow:2.8.1-python3.10

# Spark 제거로 JDK 설치 불필요
# 추가 패키지는 이미지에 버전 고정 설치
# - 이전 방식(compose _PIP_ADDITIONAL_REQUIREMENTS)은 컨테이너 기동마다 설치해 재시작이 느리고 PyPI 장애 시 기동 실패
# - apache-airflow를 함께 고정해 의존성 해석 중 Airflow 자체가 바뀌지 않게 함
# - 버전은 2026-11-26 로컬 컨테이너 실사용 버전
RUN pip install --no-cache-dir \
    "apache-airflow==2.8.1" \
    "apache-airflow-providers-docker==3.9.1" \
    "apache-airflow-providers-snowflake==5.2.1" \
    "kafka-python==3.0.13" \
    "polars==2.0.0" \
    "pyarrow==14.0.2" \
    "pytest==9.2.0"
