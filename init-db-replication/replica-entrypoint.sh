#!/bin/bash
set -e

if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
    echo "복제본 데이터 디렉토리가 비어있음 → pg_basebackup으로 primary 복제 시작"
    until pg_isready -h analytics-postgres -U replicator; do
        echo "primary 대기 중..."
        sleep 2
    done

    PGPASSWORD="$REPLICATOR_PASSWORD" pg_basebackup \
        -h analytics-postgres \
        -D "$PGDATA" \
        -U replicator \
        -Fp -Xs -P -R

    chmod 700 "$PGDATA"
fi

exec docker-entrypoint.sh postgres