# AWS 이관 계획

2026-11-26 작성. 로컬 Docker Compose 파이프라인을 AWS Lightsail 서버 1대로 옮긴다.
수집 주기·데이터는 [collection_spec.md](collection_spec.md), 요청률 기준은 [api_rate_limits.md](api_rate_limits.md) 참고.

## 1. 결정

| 항목 | 결정 | 근거 |
|---|---|---|
| 위치 | AWS Lightsail, 서울 리전 (ap-northeast-2) | PC를 켜 둘 필요 없음. 같은 사양 중 가장 저렴하고 스냅샷 복구·사양 변경이 쉬움 |
| 범위 | 정기 수집 4개 DAG 전부 서버에서 실행 | 24시간 실행이 필요한 것은 매시 수집 하나라 일별 작업을 같이 두어도 추가 비용 거의 0. 가격은 하루 놓치면 복구 불가 |
| 사양 | **4GB (월 약 $23.5)** | 비용 우선으로 2GB(월 약 $11.8)를 먼저 검토했으나 로컬 실측 최대 1,973MB·유휴 1,568MB로 2GB 가용분(약 1.65GB) 초과 (11절) |
| 실행 구조 | Airflow LocalExecutor, 서버 1대 | 동시 task 최대 2개, 병목은 Steam 요청 제한이라 분산 worker 불필요 |
| 네트워크 | VPC 직접 구성 없음 | 서버 1대라 Lightsail 기본 네트워크로 충분. NAT Gateway(월 약 $43 추정)도 불필요 |
| 접속 | **들어오는 포트 0개, Tailscale 사설망으로 SSH** | PC 해킹·분실 대비. 무료 |
| 객체 저장소 | 1차는 서버 안 SeaweedFS 유지 | 코드 변경 없음. AWS S3 전환은 10절 후속 과제 |
| 조작 방식 | AWS CLI (Claude가 실행), 비용 발생 명령은 실행 전 사용자 확인 | 계정·결제·IAM 사용자·인증 입력은 사용자가 직접 |

### 가격 (2026-11 서울 리전 공식 가격 파일)

| 번들 | 시간당 | 월 (730시간) | 포함 |
|---|---|---|---|
| Linux 2GB (2 vCPU) | $0.01612 | 약 $11.8 | 디스크 60GB, 공인 IPv4, 전송 3TB |
| Linux 4GB (2 vCPU) | $0.03225 | 약 $23.5 | 디스크 80GB, 공인 IPv4, 전송 4TB |

- 신규 계정 무료 플랜 크레딧이 Lightsail에 적용되는지는 계정 생성 시 확인

### 검토 후 제외한 방식

| 방식 | 제외 이유 |
|---|---|
| 매시 수집만 클라우드, 일별은 로컬 저녁 실행 | 현재 구조 그대로면 서버 크기가 같아 절약 없음. 가볍게 다시 만들면 관리 목록 동기화·Kafka 예외가 생김. PC를 못 켠 날 가격 손실 |
| Lambda로 매시 수집 | 15분 실행 한도(순차 호출 약 4,500건), 공용 IP로 Steam 요청 제한 공유 가능성 |
| 라즈베리 파이 | 내구성 있는 SSD 비용(약 40만 원)이 Lightsail 약 1~3년치. ARM 이미지 재빌드 필요 |
| EC2 + SSM Session Manager | 들어오는 포트 0개·키 없는 접속은 가장 깔끔하지만 월 약 $40~44 (t3a/t3.medium + EBS + IPv4). 비용 우선으로 제외 |
| bastion(중개 서버) | PC가 해킹되면 bastion 키도 같이 유출되어 효과가 없음. 서버·비용만 늘어남 |
| Celery·Kubernetes 분산 실행 | 컨테이너·클러스터 비용 증가. 기존 `terraform/`(GKE)은 이 설계의 잔재 |

## 2. 목표 구성

```text
Lightsail 4GB (Ubuntu 24.04 LTS, 고정 IP)
├─ Lightsail 방화벽: 들어오는 포트 전부 닫음
├─ Tailscale (사설망, SSH·Airflow UI 접속 경로)
├─ Docker Compose
│  ├─ airflow-scheduler   (LocalExecutor, dbt DockerOperator용 docker.sock)
│  ├─ airflow-webserver   (평소 중지, 필요할 때만 기동해 메모리 여유 확보)
│  ├─ airflow-postgres    (메타DB)
│  ├─ analytics-postgres  (분석 DB)
│  ├─ kafka               (KRaft 단일 브로커, 힙 상한)
│  └─ seaweedfs           (steam-raw 등 S3 호환 버킷)
├─ swap 2GB (메모리 순간 초과 대비)
└─ Lightsail 자동 스냅샷 (매일, 7개 보관)
```

- 컨테이너 포트는 모두 `127.0.0.1`에만 바인딩. 방화벽 설정이 실수로 바뀌어도 DB·UI가 외부에 노출되지 않게 함
- 분석 DB 복제본(`analytics-postgres-replica`)은 서버에서 실행하지 않음. 백업은 스냅샷과 `pg_dump`로 대체
- Airflow 동시 실행 상한: `AIRFLOW__CORE__PARALLELISM=4`, `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=2`

## 3. 보안

| 위험 | 대응 |
|---|---|
| 인터넷에서 서버로 무차별 접속 | Lightsail 방화벽에서 22번 포함 전부 닫음. 접속은 Tailscale 사설망 안에서만 |
| PC 해킹으로 키·인증 유출 | Tailscale 로그인 계정에 MFA, 의심 시 관리 페이지에서 PC를 사설망에서 즉시 제거. SSH 키에 암호 설정, 서버는 비밀번호 로그인 비활성 |
| PC 고장·분실 | 다른 기기에서 Lightsail 콘솔 로그인 → 방화벽 22번을 잠시 열고 브라우저 SSH로 복구 |
| AWS 계정 탈취·악용 (가장 큰 위험) | 루트 계정 MFA·평소 미사용, CLI용 IAM 사용자는 Lightsail·예산 권한만, 예산 알림 |
| 서버 안 비밀 값 | Steam API key·Slack webhook은 서버 `.env`(권한 600)에만. 유출 시 재발급 |
| 공개 저장소의 하드코딩 값 | Fernet 키·DB·관리자 비밀번호를 `.env`로 이동 (5절) |
| 보안 패치 | `unattended-upgrades` 자동 적용 |

## 4. 사전 준비

### 사용자

1. AWS 계정 생성, 결제 수단 등록, 루트 계정 MFA 설정
2. IAM 사용자 생성 (콘솔 로그인 불필요, access key만)
   - 권한: Lightsail 전체(`lightsail:*`), 예산(`budgets:*`)
   - 루트 계정 access key는 만들지 않음
3. 이 PC 터미널에서 `aws configure` 직접 실행 (region `ap-northeast-2`, output `json`)
   - access key·secret은 채팅에 붙여 넣지 않음
4. Tailscale 계정 생성 (MFA 있는 로그인 수단 사용), 이 PC에 Tailscale 설치·로그인
5. 서버용 Tailscale 인증 키(1회용, 만료 짧게) 발급 → 서버 설정 단계에서 직접 입력 또는 안전한 경로로 전달

### Claude

1. AWS CLI v2 설치 (`winget install Amazon.AWSCLI`, 사용자 승인 후)
2. `aws sts get-caller-identity`로 인증 확인. 출력의 계정 ID는 대화에 노출하지 않음

## 5. 저장소 변경 (서버 배포 전)

브랜치 `feat/aws-deploy` (`feat/player-watchlist`에서 분기)

| 변경 | 이유 | 상태 |
|---|---|---|
| Fernet 키·DB 비밀번호·관리자 계정을 `.env`에서만 읽기 (`${변수:?}`, 기본값 없음) | 기존 `docker-compose-local.yml` 하드코딩. 공개 저장소라 서버에서 그대로 쓰면 안 됨. 값이 없으면 기동 거부 | 완료 |
| `.env.example` 추가 | 저장소에 없었음. MinIO 항목 제거, `S3_ACCESS_KEY`·`S3_SECRET_KEY` 포함, 값은 비움 | 완료 |
| 포트 `127.0.0.1` 바인딩 (로컬 compose) | 외부 노출 차단. 로컬도 `localhost` 접속은 그대로 | 완료 |
| Kafka 이미지 `apache/kafka:4.3.1` 고정 | 재배포 시 예고 없는 버전 변경 방지 | 완료 |
| Kafka `KAFKA_HEAP_OPTS=-Xmx256m -Xms256m` | 기본 1GB 고정. 11절 테스트에서 컨테이너 약 500MB로 정상 동작 | 완료 |
| Kafka `KAFKA_LOG_DIRS=/var/lib/kafka/data` | 기본값 `/tmp/kafka-logs`가 볼륨 밖이라 재생성 시 토픽·offset 유실. 재생성 후 토픽 유지 확인 | 완료 |
| Airflow `PARALLELISM=4`, `MAX_ACTIVE_TASKS_PER_DAG=2` | 메모리 급증 방지 | 완료 |
| Python 패키지를 `airflow/Dockerfile`에 버전 고정 설치 | 기존 `_PIP_ADDITIONAL_REQUIREMENTS`는 기동마다 설치. scheduler healthy까지 수 분 → 21초 | 완료 |
| 서버용 override `docker-compose-server.yml` | 상시 서비스 `restart: unless-stopped`, replica·webserver를 profile(`replica`·`ui`)로 기본 실행 제외 | 완료 |
| 로그 정리 DAG `maintenance_01_log_cleanup` | 매일 03:20 UTC, 보관 기간(Variable `log_retention_days`, 기본 14일) 지난 로그와 빈 디렉터리 삭제. 최근 1시간 안에 생긴 빈 디렉터리는 보존 | 완료 |
| dbt `profiles.yml` DB 계정 하드코딩 | 서버에서 새 비밀번호를 쓰면 dbt 접속 실패. dbt DAG는 7단계 재작성 대상(네트워크명 문제 포함)이라 그때 `env_var`로 변경. 그전까지 서버에서 dbt DAG는 일시정지 유지 | 보류 |
| GCP용 `terraform/` 정리 | 현재 계획과 무관한 GKE 설정. 삭제 또는 Lightsail용으로 재작성 | 미정 |
| 배포·운영 절차 문서 `docs/aws_operations.md` | 접속, 배포, 백업, 복구 절차. 실제 서버 구성 후 작성 | 대기 |

- 로컬 환경도 같은 변경을 받음. 2026-11-26 로컬 재생성 후 전 컨테이너 healthy, 오프라인 테스트 100 passed, ruff 통과, DAG import 오류 0
- Windows Docker Desktop에서 `docker compose build`가 bake 권한 오류로 실패 → `docker build -t steam-airflow:latest airflow`로 빌드. 서버(Linux)에서는 compose build 사용 예정
- Compose 프로젝트명: dbt DAG가 네트워크 `steam-project_default`를 가정하므로 서버는 `-p steam-project`로 실행 (로컬 기존 컨테이너는 `steam_analysis`)

## 6. 이관 절차

| 단계 | 작업 | 실행 | 비용 |
|---|---|---|---|
| 1 | ~~로컬 2GB 메모리 제한 테스트로 사양 결정 (11절)~~ 완료: 4GB | Claude | 없음 |
| 2 | ~~5절 저장소 변경~~ 완료 (dbt·terraform·운영 문서 제외) | Claude | 없음 |
| 3 | 월 예산 알림 설정 ($30, 실제·예상 80% 도달 시 메일) | Claude | 없음 |
| 4 | Lightsail SSH 키 생성 (암호 설정), 개인 키는 `~/.ssh/`에 저장 | Claude | 없음 |
| 5 | 번들·Ubuntu 24.04 블루프린트 ID 조회 (`get-bundles`, `get-blueprints`) | Claude | 없음 |
| 6 | **서버 생성** (`create-instances`, 서울 a 영역) | Claude, **사용자 확인 후** | 월 약 $23.5 시작 |
| 7 | 고정 IP 할당·연결 | Claude | 연결 상태면 무료 |
| 8 | 서버 초기 설정: 패키지 업데이트, swap 2GB, Docker Engine·Compose, Tailscale 설치·등록, SSH 비밀번호 로그인 비활성 | Claude (설정 중에만 22번 임시 개방) | 없음 |
| 9 | Tailscale 경유 SSH 확인 후 **방화벽 22번 닫기** (들어오는 포트 0개) | Claude | 없음 |
| 10 | 자동 스냅샷 활성화 (매일) | Claude, 사용자 확인 후 | 스냅샷 용량 과금 (소액) |
| 11 | 저장소 clone (`feat/aws-deploy`), `.env`를 SSH로 복사 (권한 600) | Claude | 없음 |
| 12 | 이미지 빌드, `airflow-init`, 전체 기동 | Claude | 없음 |
| 13 | 데이터 이전 (7절) | Claude | 없음 |
| 14 | 검증 (8절) | Claude | 없음 |
| 15 | 서버 4개 DAG unpause, 로컬은 계속 일시정지 | Claude, 사용자 확인 후 | 없음 |

## 7. 데이터 이전

| 대상 | 방식 | 필요 여부 |
|---|---|---|
| 분석 DB (`steam_analytics`, 현재 402MB) | 로컬 `pg_dump -Fc` → SSH 복사 → 서버 `pg_restore` | 필수. `player_watchlist` 스캔 일정, 가격·리뷰 요약 구간, `silver_snapshot_log` 포함 |
| SeaweedFS 원본 (`steam-raw`, 현재 약 16MB) | 로컬 버킷 내용을 받아 서버 버킷에 업로드 | 선택. 재처리용 원본 보관 목적 |
| Airflow 메타DB | 옮기지 않음 | 실행 기록만 있음. 연결·Variable은 환경 변수로 자동 등록, 임시 Variable은 삭제 상태 |

- 테스트 스냅샷 이름 충돌(`app_list 20261122T060000`, `store_prices 20261122T183000`·`20261123T183000`)은 해당 예약 날짜가 지나 해소됨. 이후 예약 실행의 스냅샷 이름은 11-25 이후 날짜라 충돌 없음
- `silver_snapshot_log`의 순서 역전 차단은 실제 수집 시각 기준이라 이전 후 새 스냅샷이 정상 적용됨

## 8. 검증 (unpause 전)

| 검증 | 방법 | 기준 |
|---|---|---|
| 컨테이너 | `docker compose ps`, 메모리 `docker stats` | 전부 healthy, swap 사용 지속 증가 없음 |
| 외부 노출 | 다른 네트워크에서 고정 IP로 22·8080·5434 접속 시도 | 전부 연결 불가 |
| S3·Kafka 연결 | `s3_storage_smoke_test` DAG 수동 실행 | 성공 |
| **서버 IP에서 Steam API** | 동접자 API 소량(예: 300건, 초당 5건) 호출 | 429·403 0건. 데이터센터 IP는 집 IP와 다르게 제한될 수 있어 반드시 확인 |
| 매시 수집 | `steam_players_01_hourly` 1회 수동 실행 | 이전한 관리 목록 기준 활성 게임 수만큼 적재 |
| 가격·게임 목록 | 각 1회 수동 실행 | 직전 대비 급감 차단에 걸리지 않고 구간 갱신 |
| 재부팅 | `sudo reboot` 후 확인 | 컨테이너·Tailscale 자동 기동, scheduler healthy |
| Slack 알림 | 일부러 실패하는 task 1회 | 알림 수신 |

## 9. 운영

| 항목 | 방법 |
|---|---|
| 배포 | 서버에서 `git pull` 후 `docker compose up -d --build`. DAG·plugin은 볼륨 마운트라 pull만으로 반영 |
| Airflow UI | 필요할 때 webserver 기동 → Tailscale 주소로 접속 → 끝나면 중지 |
| 실패 알림 | 기존 Slack 콜백 (`SLACK_WEBHOOK_URL`) |
| 백업 | Lightsail 자동 스냅샷 매일, 주 1회 `pg_dump` 파일을 로컬로 내려받기 |
| 디스크 | 월 1회 `df -h` 확인. 데이터 증가 추정 월 수백 MB |
| 비용 | 예산 알림 + 월 1회 Billing 확인 |

### 롤백

- 서버 장애가 길어지면 로컬에서 재가동: 서버 `pg_dump` → 로컬 복원 → 로컬 DAG unpause
- 서버와 로컬이 동시에 수집하지 않도록 한쪽은 항상 일시정지
- 서버 삭제는 사용자 확인 후에만. 삭제 전 최종 스냅샷 또는 `pg_dump` 보관

## 10. 후속 과제

| 과제 | 시점 |
|---|---|
| 2GB(월 약 $11.8) 축소 재검토 | scheduler 메모리 절감(DAG 파싱 프로세스 축소, 유휴 시 메모리 반환) 후 11절 방식으로 재측정 |
| SeaweedFS → AWS S3 전환 | 서버 디스크와 별개인 원본 보관이 필요해질 때. 버킷 이름이 전역 고유여야 해서 `plugins/hooks/s3_hook.py`의 고정 버킷명을 설정값으로 변경 필요 |
| 동시 호출 전환 | 매시 개별 호출 약 5,000건 초과 시 ([api_rate_limits.md](api_rate_limits.md) 7절) |

## 11. 진행 기록

| 날짜 | 내용 |
|---|---|
| 2026-11-25 | 서울 리전 가격 조사, 로컬 유휴 메모리 실측 (합계 약 1.9GB: webserver 639MB, scheduler 594MB, kafka 416MB, seaweedfs 145MB, postgres 2개 70MB) |
| 2026-11-26 | Lightsail·LocalExecutor·Tailscale 접속 결정, EC2+SSM·bastion·분산 실행 제외. 브랜치 `feat/aws-deploy` 생성 |
| 2026-11-26 | 로컬 2GB 메모리 제한 테스트: 최대 1,973MB로 2GB 부족, 4GB로 결정. Kafka 로그 디렉터리 문제 발견 (아래) |
| 2026-11-26 | 5절 저장소 변경 적용·로컬 검증 (비밀 값 `.env` 이동, 포트 127.0.0.1, Kafka 고정·힙·로그 경로, 패키지 이미지 고정, 서버 override, 로그 정리 DAG) |

### 로컬 2GB 메모리 제한 테스트

- 목적: 2GB 서버에서 OS·Docker 몫(약 250~300MB)을 뺀 약 1.65GB 안에서 파이프라인이 도는지 확인
- 방법: 컨테이너별 메모리 상한(swap 없음)을 건 상태에서 `airflow dags test`로 실제 API 실행, `docker stats` 2초 간격 기록. webserver는 띄우지 않음
- 설정: Kafka 힙 `-Xmx256m -Xms256m`, Airflow `PARALLELISM=4`·`MAX_ACTIVE_TASKS_PER_DAG=2`, SeaweedFS `GOMEMLIMIT=240MiB`

#### 1차 상한에서 실패

| 컨테이너 | 상한 | 결과 |
|---|---|---|
| seaweedfs | 180MB | 161MB에서 GC 과부하로 응답 지연, healthcheck 실패 → 300MB·`GOMEMLIMIT`로 해결 |
| airflow-scheduler | 800MB | 유휴 565MB + 가격 task 시작 30초 만에 OOM 강제 종료 |
| kafka | 400MB | 가격 20만 건 발행 중 395MB를 넘어 OOM 종료. 힙(256MB) 외 네트워크 버퍼·메타데이터 메모리 사용 |

#### 실측 최대 메모리 (scheduler 1,400MB·kafka 800MB로 올린 뒤)

| 구간 | scheduler | kafka | analytics-postgres | seaweedfs | airflow-postgres | **합계 최대** |
|---|---|---|---|---|---|---|
| 가격 (20.2만 건) | 1,000MB | 476MB | 142MB | 158MB | 35MB | **1,680MB** |
| 게임 목록 (20.3만 건) | 1,068MB | 476MB | 161MB | 159MB | 33MB | **1,797MB** |
| 매시 수집 + 스캔 동시 시작 | 1,213MB | 490MB | 167MB | 158MB | 34MB | **1,973MB** |
| 스캔 단독 (3,000건) | 1,011MB | 500MB | 170MB | 162MB | 35MB | 1,813MB |
| 실행 후 유휴 | 789MB | 494MB | 163MB | 162MB | 36MB | 1,568MB |

- 전 구간 성공: 가격 201,637건 발행·구간 갱신, 게임 목록 202,517건, 매시 401건(개별 301건, 실패·429 0), 스캔 3,000건(실패·429 0)
- 실행 후에도 scheduler가 789MB로 남아 유휴 합계가 1.5GB대
- `airflow dags test`는 task를 한 프로세스에서 실행. 실제 LocalExecutor는 task마다 감독 프로세스가 추가되어 실측보다 커질 수 있음

#### 결론: 4GB 필요

- 2GB 서버 가용 약 1.65GB 대비 최대 1,973MB, 유휴 1,568MB. swap 2GB를 두어도 평소부터 swap을 계속 쓰는 상태가 되어 매시 수집이 느려지거나 OOM으로 슬롯을 잃을 위험
- 4GB(가용 약 3.6GB)는 최대치 대비 1.6GB 이상 여유
- 2GB 재검토는 scheduler 메모리 절감(DAG 파싱 프로세스 축소 등) 후 10절 후속 과제로

#### 테스트 중 발견한 기존 설정 문제 (5절에 반영)

| 문제 | 영향 | 조치 |
|---|---|---|
| Kafka 로그 디렉터리가 기본값 `/tmp/kafka-logs` | 마운트한 볼륨(`/var/lib/kafka/data`)을 쓰지 않아 컨테이너 재생성 때마다 토픽·consumer offset 유실 | `KAFKA_LOG_DIRS=/var/lib/kafka/data` |
| Kafka 기본 힙 `-Xmx1G -Xms1G` | 기동 즉시 힙 1GB 확보 | `KAFKA_HEAP_OPTS=-Xmx256m -Xms256m`, 컨테이너 메모리 약 500MB |
| SeaweedFS 메모리 상한 근처 GC 과부하 | 응답 지연·healthcheck 실패 | 상한을 둘 때 `GOMEMLIMIT`을 상한의 80% 정도로 |

#### 부수 효과

- 이번 스캔·재배분으로 로컬 DB의 매시 수집 대상이 399개 → 2,107개(예산 2,000 + 고정 107)로 늘어남. 서버로 이전할 데이터에 그대로 포함
- 스캔 실제 속도는 초당 약 3.7건 (3,000건 13.6분). 비인기 게임 응답이 느려 순차 호출 기준 초당 5건에 못 미침. 하루 30,000건이면 약 2시간 15분
- 임시 Variable `player_scan_max_per_run`은 테스트 후 삭제
