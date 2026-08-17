# Steam 게임 분석 프로젝트

## 프로젝트 구조

```
steam-project/
├── docker-compose.yml          # 전체 인프라 정의
├── .env.example                # 환경변수 템플릿
├── .env                        # 실제 환경변수 (git 제외)
├── init-db/
│   └── 01_create_tables.sql    # 분석DB 초기 테이블
├── dags/                       # Airflow DAG 파일들
├── plugins/                    # Airflow 커스텀 플러그인
├── scripts/                    # 유틸리티 스크립트
└── logs/                       # Airflow 로그
```

## 컨테이너 구성

| 컨테이너 | 역할 | 포트 | 접속 정보 |
|-----------|------|------|-----------|
| airflow-webserver | Airflow UI | localhost:8080 | admin / admin |
| airflow-scheduler | DAG 실행 | - | - |
| airflow-postgres | Airflow 메타DB | localhost:5433 | airflow / airflow |
| analytics-postgres | 분석용 DB | localhost:5434 | analytics / analytics |
| minio | S3 대체 스토리지 | localhost:9000 (API), localhost:9001 (콘솔) | minioadmin / minioadmin |

## 실행 방법

### 1. 환경변수 설정
```bash
cp .env.example .env
# .env 파일을 열고 STEAM_API_KEY 입력
```

### 2. 컨테이너 실행
```bash
# 최초 실행 (초기화 포함)
docker-compose up airflow-init
docker-compose up -d

# 상태 확인
docker-compose ps
```

### 3. 접속 확인
- Airflow UI: http://localhost:8080 (admin / admin)
- MinIO 콘솔: http://localhost:9001 (minioadmin / minioadmin)
- 분석DB 접속: `psql -h localhost -p 5434 -U analytics -d steam_analytics`

### 4. 종료
```bash
docker-compose down          # 컨테이너만 종료 (데이터 유지)
docker-compose down -v       # 컨테이너 + 볼륨 삭제 (데이터 초기화)
```

## AWS 전환 시 변경 포인트

| 로컬 | AWS | 변경 내용 |
|------|-----|-----------|
| MinIO | S3 | endpoint_url만 제거하면 됨 |
| analytics-postgres | Snowflake / Redshift | Connection string 변경 |
| LocalExecutor | CeleryExecutor 또는 ECS | 스케일 필요 시 |
