"""
Airflow 로그 정리 오프라인 검증

- 보관 기간 경계: 기준보다 오래된 파일만 삭제
- 빈 디렉터리 정리와 기준 디렉터리 보존
- 잘못된 설정 거부
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from utils.log_cleanup import cleanup_old_logs

_NOW = 1_800_000_000.0
_DAY = 86_400


def _write(path: Path, age_days: float, size: int = 10) -> None:
    """지정한 나이(일)의 수정 시각으로 파일 생성"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    mtime = _NOW - age_days * _DAY
    os.utime(path, (mtime, mtime))


def test_deletes_only_files_older_than_retention(tmp_path: Path) -> None:
    old = tmp_path / "dag_id=a" / "run_id=old" / "task_id=t" / "attempt=1.log"
    recent = tmp_path / "dag_id=a" / "run_id=new" / "task_id=t" / "attempt=1.log"
    _write(old, age_days=15, size=100)
    _write(recent, age_days=13)

    result = cleanup_old_logs(tmp_path, retention_days=14, now=_NOW)

    assert not old.exists(), "보관 기간(14일)이 지난 15일 전 로그는 삭제되어야 함"
    assert recent.exists(), "13일 전 로그는 보관 기간 안이라 남아야 함"
    assert result.deleted_files == 1, f"삭제 파일 수 기대 1, 실제 {result.deleted_files}"
    assert result.deleted_bytes == 100, f"삭제 크기 기대 100, 실제 {result.deleted_bytes}"


def test_removes_empty_directories_but_keeps_base(tmp_path: Path) -> None:
    _write(tmp_path / "dag_id=a" / "run_id=old" / "task_id=t" / "attempt=1.log", age_days=30)
    _write(tmp_path / "dag_id=b" / "run_id=new" / "task_id=t" / "attempt=1.log", age_days=1)

    result = cleanup_old_logs(tmp_path, retention_days=14, now=_NOW)

    assert not (tmp_path / "dag_id=a").exists(), "모든 파일이 지워진 DAG 디렉터리는 상위까지 삭제되어야 함"
    assert (tmp_path / "dag_id=b").exists(), "보관 중인 로그가 있는 디렉터리는 남아야 함"
    assert result.deleted_dirs == 3, f"삭제 디렉터리 기대 3(task·run·dag), 실제 {result.deleted_dirs}"

    cleanup_old_logs(tmp_path / "dag_id=b", retention_days=14, now=_NOW + 30 * _DAY)
    assert (tmp_path / "dag_id=b").is_dir(), "기준 디렉터리 자체는 비어도 남아야 함"


def test_keeps_recent_empty_directory(tmp_path: Path) -> None:
    fresh = tmp_path / "dag_id=c" / "run_id=running" / "task_id=t"
    fresh.mkdir(parents=True)
    stale = tmp_path / "dag_id=d" / "run_id=stale" / "task_id=t"
    stale.mkdir(parents=True)
    old_mtime = _NOW - 2 * 3_600
    for directory in (stale, stale.parent, stale.parent.parent):
        os.utime(directory, (old_mtime, old_mtime))
    for directory in (fresh, fresh.parent, fresh.parent.parent):
        os.utime(directory, (_NOW - 60, _NOW - 60))
    _write(tmp_path / "dag_id=a" / "attempt=1.log", age_days=30)

    cleanup_old_logs(tmp_path, retention_days=14, now=_NOW)

    assert fresh.is_dir(), "1시간 안에 생성된 빈 디렉터리는 실행 중 task가 쓸 수 있어 남아야 함"
    assert not (tmp_path / "dag_id=d").exists(), "2시간 넘게 비어 있던 디렉터리는 삭제되어야 함"


def test_rejects_invalid_settings(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="1 이상"):
        cleanup_old_logs(tmp_path, retention_days=0, now=_NOW)
    with pytest.raises(FileNotFoundError, match="로그 디렉터리 없음"):
        cleanup_old_logs(tmp_path / "missing", retention_days=14, now=_NOW)
