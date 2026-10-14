"""Thread-safe in-memory store supporting LRU eviction and TTL expiration."""

from collections import OrderedDict
from dataclasses import dataclass
import threading
import time
from typing import Optional, Dict, List, Tuple, Any, Callable


@dataclass
class Record:
    """Internal key-value record with versioning and expiration metadata."""
    key: str
    value: str
    version: int
    created_at: float
    updated_at: float
    expires_at: Optional[float] = None

    def is_expired(self, now: Optional[float] = None) -> bool:
        if self.expires_at is None:
            return False
        current_time = now if now is not None else time.time()
        return current_time >= self.expires_at


class Store:
    """Thread-safe in-memory key-value store with LRU eviction and TTL expiration."""

    def __init__(self, capacity: Optional[int] = None):
        """
        Initialize the store.

        :param capacity: Maximum number of keys allowed. If exceeded, the least
                         recently used (LRU) key is evicted. None means unlimited.
        """
        self.capacity: Optional[int] = capacity
        self.lock = threading.RLock()
        self._data: OrderedDict[str, Record] = OrderedDict()
        self.on_evict: Optional[Callable[[str, Record], None]] = None

    def set(
        self,
        key: str,
        value: str,
        ttl: Optional[float] = None,
        version: Optional[int] = None,
    ) -> int:
        """
        Store or update a key-value pair.
        Returns the new version number.
        """
        with self.lock:
            now = time.time()
            expires_at = (now + ttl) if ttl is not None and ttl > 0 else None

            if key in self._data:
                rec = self._data[key]
                new_version = version if version is not None else (rec.version + 1)
                rec.value = str(value)
                rec.version = new_version
                rec.updated_at = now
                rec.expires_at = expires_at
                self._data.move_to_end(key, last=True)
                return new_version
            else:
                # Need to add new key: check capacity
                if self.capacity is not None and len(self._data) >= self.capacity:
                    evicted_key, evicted_rec = self._data.popitem(last=False)
                    if self.on_evict:
                        self.on_evict(evicted_key, evicted_rec)

                new_version = version if version is not None else 1
                rec = Record(
                    key=key,
                    value=str(value),
                    version=new_version,
                    created_at=now,
                    updated_at=now,
                    expires_at=expires_at,
                )
                self._data[key] = rec
                self._data.move_to_end(key, last=True)
                return new_version

    def replset(
        self,
        key: str,
        value: str,
        version: int,
        ttl: Optional[float] = None,
    ) -> bool:
        """
        Version-aware replica update.
        Rejects stale writes where version <= existing.version.
        Returns True if updated, False if ignored.
        """
        with self.lock:
            now = time.time()
            if key in self._data:
                rec = self._data[key]
                if rec.is_expired(now):
                    del self._data[key]
                elif version <= rec.version:
                    # Stale or duplicate write - reject
                    return False

            # If adding new key and at capacity, evict LRU
            if key not in self._data and self.capacity is not None and len(self._data) >= self.capacity:
                evicted_key, evicted_rec = self._data.popitem(last=False)
                if self.on_evict:
                    self.on_evict(evicted_key, evicted_rec)

            expires_at = (now + ttl) if ttl is not None and ttl > 0 else None
            rec = Record(
                key=key,
                value=str(value),
                version=version,
                created_at=now,
                updated_at=now,
                expires_at=expires_at,
            )
            self._data[key] = rec
            self._data.move_to_end(key, last=True)
            return True

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve value for key. Passively evicts if expired.
        Updates LRU recency on access.
        """
        with self.lock:
            if key not in self._data:
                return None
            rec = self._data[key]
            if rec.is_expired():
                del self._data[key]
                return None
            self._data.move_to_end(key, last=True)
            return rec.value

    def touch(self, key: str) -> bool:
        """Update LRU recency for a key without returning value."""
        with self.lock:
            if key not in self._data:
                return False
            rec = self._data[key]
            if rec.is_expired():
                del self._data[key]
                return False
            self._data.move_to_end(key, last=True)
            return True

    def del_key(self, key: str) -> bool:
        """
        Delete a key from store.
        Returns True if deleted, False if not found or expired.
        """
        with self.lock:
            if key not in self._data:
                return False
            rec = self._data[key]
            del self._data[key]
            return not rec.is_expired()

    def exists(self, key: str) -> bool:
        """Check whether key exists and is unexpired."""
        with self.lock:
            if key not in self._data:
                return False
            rec = self._data[key]
            if rec.is_expired():
                del self._data[key]
                return False
            return True

    def expire(self, key: str, seconds: float) -> bool:
        """
        Set a TTL in seconds on an existing unexpired key.
        Returns True if set, False if key does not exist.
        """
        with self.lock:
            if key not in self._data:
                return False
            rec = self._data[key]
            now = time.time()
            if rec.is_expired(now):
                del self._data[key]
                return False
            rec.expires_at = now + seconds
            return True

    def get_version(self, key: str) -> int:
        """Return the current version of the key, or 0 if not found/expired."""
        with self.lock:
            if key not in self._data:
                return 0
            rec = self._data[key]
            if rec.is_expired():
                del self._data[key]
                return 0
            return rec.version

    def get_ttl(self, key: str) -> Optional[float]:
        """Return remaining TTL in seconds, or None if no TTL or key doesn't exist."""
        with self.lock:
            if key not in self._data:
                return None
            rec = self._data[key]
            now = time.time()
            if rec.is_expired(now):
                del self._data[key]
                return None
            if rec.expires_at is None:
                return None
            return max(0.0, rec.expires_at - now)

    def evict_expired(self) -> int:
        """
        Active expiry scan: remove all expired keys.
        Returns the number of keys evicted.
        """
        with self.lock:
            now = time.time()
            expired = [k for k, r in self._data.items() if r.is_expired(now)]
            for k in expired:
                del self._data[k]
            return len(expired)

    def keys(self) -> List[str]:
        """Return list of all current, unexpired keys in LRU order (oldest to newest)."""
        with self.lock:
            now = time.time()
            valid = []
            for k, r in list(self._data.items()):
                if r.is_expired(now):
                    del self._data[k]
                else:
                    valid.append(k)
            return valid

    def size(self) -> int:
        """Return number of valid unexpired keys."""
        return len(self.keys())

    def clear(self) -> None:
        """Clear all keys in store."""
        with self.lock:
            self._data.clear()

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        """Return an unexpired snapshot of records for replication or debugging."""
        with self.lock:
            now = time.time()
            snap = {}
            for k, r in list(self._data.items()):
                if r.is_expired(now):
                    del self._data[k]
                else:
                    snap[k] = {
                        "value": r.value,
                        "version": r.version,
                        "created_at": r.created_at,
                        "updated_at": r.updated_at,
                        "expires_at": r.expires_at,
                    }
            return snap
