FROM ghcr.io/dbt-labs/dbt-postgres:1.8.0

WORKDIR /usr/app/dbt

# 프로젝트 파일 복사
COPY steam_dbt/ .

# profiles.yml 위치 지정
ENV DBT_PROFILES_DIR=/usr/app/dbt

# 의존성 설치 (packages.yml 있을 경우)
RUN dbt deps --profiles-dir /usr/app/dbt || true
