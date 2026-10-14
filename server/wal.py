"""Write-Ahead Log (WAL) implementation with crash-recovery replay."""

import json
import os
import threading
import time
from typing import Optional, Any
from .store import Store


class WAL:
    """
    Append-only Write-Ahead Log for Mini-Redis persistence and crash recovery.
    Every write mutation is flushed and fsynced to guarantee durability across crashes.
    """

    def __init__(self, file_path: str, sync_on_write: bool = True):
        self.file_path = file_path
        self.sync_on_write = sync_on_write
        self._lock = threading.Lock()
        self._file = None

        # Ensure directory exists
        dirname = os.path.dirname(self.file_path)
        if dirname and not os.path.exists(dirname):
            os.makedirs(dirname, exist_ok=True)

        # Open in append mode
        self._file = open(self.file_path, "a+", encoding="utf-8")

    def _append(self, record: dict) -> None:
        with self._lock:
            if self._file is None or self._file.closed:
                self._file = open(self.file_path, "a+", encoding="utf-8")
            line = json.dumps(record, separators=(",", ":")) + "\n"
            self._file.write(line)
            self._file.flush()
            if self.sync_on_write:
                try:
                    os.fsync(self._file.fileno())
                except OSError:
                    pass

    def log_set(
        self,
        key: str,
        value: str,
        version: int,
        ttl: Optional[float] = None,
        expires_at: Optional[float] = None,
    ) -> None:
        now = time.time()
        exp = expires_at if expires_at is not None else ((now + ttl) if ttl else None)
        entry = {
            "op": "SET",
            "key": key,
            "val": value,
            "ver": version,
            "exp": exp,
            "ts": now,
        }
        self._append(entry)

    def log_replset(
        self,
        key: str,
        value: str,
        version: int,
        ttl: Optional[float] = None,
        expires_at: Optional[float] = None,
    ) -> None:
        now = time.time()
        exp = expires_at if expires_at is not None else ((now + ttl) if ttl else None)
        entry = {
            "op": "REPLSET",
            "key": key,
            "val": value,
            "ver": version,
            "exp": exp,
            "ts": now,
        }
        self._append(entry)

    def log_del(self, key: str) -> None:
        entry = {
            "op": "DEL",
            "key": key,
            "ts": time.time(),
        }
        self._append(entry)

    def log_expire(self, key: str, seconds: float) -> None:
        now = time.time()
        entry = {
            "op": "EXPIRE",
            "key": key,
            "sec": seconds,
            "exp": now + seconds,
            "ts": now,
        }
        self._append(entry)

    def log_touch(self, key: str) -> None:
        entry = {
            "op": "TOUCH",
            "key": key,
            "ts": time.time(),
        }
        self._append(entry)

    def replay(self, store: Store) -> int:
        """
        Replay log entries from the WAL file into the provided Store.
        Gracefully handles trailing corrupted/incomplete lines resulting from a crash.
        Returns the number of successfully applied operations.
        """
        with self._lock:
            # Flush current file handle before reading
            if self._file and not self._file.closed:
                self._file.flush()

            if not os.path.exists(self.file_path):
                return 0

            replayed_count = 0
            now = time.time()

            with open(self.file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        # Corrupted or partially written line from crash; safely stop or skip
                        continue

                    op = record.get("op")
                    key = record.get("key")
                    if not key:
                        continue

                    exp = record.get("exp")
                    remaining_ttl = None
                    if exp is not None:
                        remaining_ttl = exp - now
                        if remaining_ttl <= 0:
                            # Already expired in the real world
                            if op in ("SET", "REPLSET"):
                                # If it's already expired, do not add or delete if present
                                store.del_key(key)
                                continue

                    if op == "SET":
                        store.set(
                            key=key,
                            value=record.get("val", ""),
                            ttl=remaining_ttl,
                            version=record.get("ver"),
                        )
                        replayed_count += 1
                    elif op == "REPLSET":
                        store.replset(
                            key=key,
                            value=record.get("val", ""),
                            version=record.get("ver", 1),
                            ttl=remaining_ttl,
                        )
                        replayed_count += 1
                    elif op == "DEL":
                        store.del_key(key)
                        replayed_count += 1
                    elif op == "EXPIRE":
                        if remaining_ttl is not None and remaining_ttl > 0:
                            store.expire(key, remaining_ttl)
                        else:
                            store.del_key(key)
                        replayed_count += 1
                    elif op == "TOUCH":
                        store.touch(key)
                        replayed_count += 1

            return replayed_count

    def compact(self, store: Store) -> None:
        """
        Compact the WAL by replacing it with a snapshot of all current valid keys.
        """
        with self._lock:
            temp_path = self.file_path + ".tmp"
            now = time.time()
            snap = store.snapshot()

            with open(temp_path, "w", encoding="utf-8") as f:
                for key, data in snap.items():
                    exp = data.get("expires_at")
                    if exp is not None and exp <= now:
                        continue
                    entry = {
                        "op": "SET",
                        "key": key,
                        "val": data["value"],
                        "ver": data["version"],
                        "exp": exp,
                        "ts": data.get("updated_at", now),
                    }
                    f.write(json.dumps(entry, separators=(",", ":")) + "\n")
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    pass

            if self._file and not self._file.closed:
                self._file.close()

            os.replace(temp_path, self.file_path)
            self._file = open(self.file_path, "a+", encoding="utf-8")

    def close(self) -> None:
        """Close WAL file descriptor."""
        with self._lock:
            if self._file and not self._file.closed:
                try:
                    self._file.flush()
                    if self.sync_on_write:
                        os.fsync(self._file.fileno())
                except OSError:
                    pass
                self._file.close()
                self._file = None
