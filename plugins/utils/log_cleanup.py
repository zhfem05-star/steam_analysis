"""
Airflow task 로그 보관 기간 관리

- Airflow 2.8은 로그 자동 삭제 기능이 없어 디스크가 계속 증가
- 보관 기간이 지난 로그 파일과 비게 된 디렉터리만 삭제
- 기준 경로 밖은 건드리지 않음
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import NamedTuple

# 방금 생성된 빈 디렉터리는 실행 중인 task가 곧 로그를 쓸 수 있어 남겨 둠
EMPTY_DIR_GRACE_SEC = 3_600


class CleanupResult(NamedTuple):
    """
    로그 정리 결과

    :param deleted_files: 삭제한 파일 수
    :param deleted_bytes: 삭제한 파일 크기 합계
    :param deleted_dirs: 삭제한 빈 디렉터리 수
    """

    deleted_files: int
    deleted_bytes: int
    deleted_dirs: int


def cleanup_old_logs(
    log_dir: Path,
    retention_days: int,
    now: float | None = None,
) -> CleanupResult:
    """
    보관 기간이 지난 로그 파일과 빈 하위 디렉터리 삭제

    - 수정 시각이 now - retention_days보다 오래된 파일만 삭제
    - 빈 디렉터리는 최근 EMPTY_DIR_GRACE_SEC 안에 바뀌지 않은 것만 삭제
    - 기준 디렉터리 자체는 비어도 남김

    :param log_dir: Airflow 로그 기준 디렉터리
    :param retention_days: 보관 일수
    :param now: 기준 시각 Unix 초 (테스트용, 기본 현재 시각)
    :return: 삭제 건수와 크기
    :raises ValueError: 보관 일수가 1 미만
    :raises FileNotFoundError: 로그 디렉터리 없음
    """
    if retention_days < 1:
        raise ValueError(f"로그 보관 일수는 1 이상이어야 합니다: retention_days={retention_days}")
    if not log_dir.is_dir():
        raise FileNotFoundError(f"로그 디렉터리 없음: {log_dir}")

    current = time.time() if now is None else now
    cutoff = current - retention_days * 86_400
    deleted_files = 0
    deleted_bytes = 0
    # 이번 정리로 내용이 비워진 디렉터리. 삭제로 mtime이 방금 갱신되어도 정리 대상
    emptied_by_cleanup: set[Path] = set()

    for path in log_dir.rglob("*"):
        if not path.is_file() or path.is_symlink():
            continue
        stat = path.stat()
        if stat.st_mtime >= cutoff:
            continue
        path.unlink()
        deleted_files += 1
        deleted_bytes += stat.st_size
        emptied_by_cleanup.add(path.parent)

    # 깊은 디렉터리부터 확인해야 상위 디렉터리도 비었을 때 함께 삭제됨
    deleted_dirs = 0
    directories = sorted(
        (path for path in log_dir.rglob("*") if path.is_dir() and not path.is_symlink()),
        key=lambda path: len(path.parts),
        reverse=True,
    )
    for directory in directories:
        if any(directory.iterdir()):
            continue
        is_old = directory.stat().st_mtime < current - EMPTY_DIR_GRACE_SEC
        if not (is_old or directory in emptied_by_cleanup):
            continue
        directory.rmdir()
        deleted_dirs += 1
        emptied_by_cleanup.add(directory.parent)

    return CleanupResult(deleted_files, deleted_bytes, deleted_dirs)
