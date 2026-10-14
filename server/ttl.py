"""Active TTL expiry sweeper running in a background thread."""

import threading
import time
from typing import Optional
from .store import Store
from .wal import WAL


class TTLSweeper:
    """
    Background worker that actively scans and evicts expired keys from the store.
    Complements passive expiration on read.
    """

    def __init__(
        self,
        store: Store,
        wal: Optional[WAL] = None,
        sweep_interval: float = 0.5,
    ):
        self.store = store
        self.wal = wal
        self.sweep_interval = sweep_interval
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start the background sweeper thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_sweep_loop,
            name="TTLSweeperThread",
            daemon=True,
        )
        self._thread.start()

    def _run_sweep_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                evicted = self.store.evict_expired()
                # If keys were actively evicted and WAL is present, Store's passive and replay
                # handles it naturally; WAL records deletion or allows replay to check exp.
            except Exception:
                pass
            # Wait for next interval or until stopped
            self._stop_event.wait(timeout=self.sweep_interval)

    def stop(self) -> None:
        """Signal background sweeper thread to stop."""
        self._stop_event.set()

    def join(self, timeout: Optional[float] = 2.0) -> None:
        """Wait for background sweeper thread to terminate."""
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
