"""Non-blocking process lock for scheduler-safe one-shot runs.

asset-version: v1.0
updated: 2026-07-14
owner_surface: Meme Cohort Observatory
behavior_change: Prevent overlapping local collector processes without waiting.
rollback: Revert the collector implementation commit; the inert lock file may remain.
"""

import fcntl
import os
from pathlib import Path


class SingleInstanceLock:
    def __init__(self, path):
        self.path = Path(path)
        self._handle = None

    def acquire(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            handle.close()
            return False
        handle.seek(0)
        handle.truncate()
        handle.write(str(os.getpid()) + "\n")
        handle.flush()
        self._handle = handle
        return True

    def release(self):
        if self._handle is None:
            return
        try:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        finally:
            self._handle.close()
            self._handle = None

    def __enter__(self):
        if not self.acquire():
            return None
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()
