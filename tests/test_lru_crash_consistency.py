"""Tests that LRU eviction state and key order stay consistent across a crash and WAL replay."""

import os
import socket
import subprocess
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from client.client import MiniRedisClient


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def wait_for_server(port: int, timeout: float = 5.0) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except (ConnectionRefusedError, socket.timeout, OSError):
            time.sleep(0.1)
    return False


class TestLRUCrashConsistency(unittest.TestCase):
    def setUp(self):
        self.port = find_free_port()
        self.wal_file = os.path.abspath(f"test_lru_crash_{self.port}.wal")
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)
        self.capacity = 3
        self.proc = None

    def tearDown(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()
            if self.proc.stdout:
                self.proc.stdout.close()
            if self.proc.stderr:
                self.proc.stderr.close()
        for _ in range(5):
            if os.path.exists(self.wal_file):
                try:
                    os.remove(self.wal_file)
                    break
                except OSError:
                    time.sleep(0.1)

    def start_node(self) -> subprocess.Popen:
        if self.proc:
            if self.proc.stdout:
                self.proc.stdout.close()
            if self.proc.stderr:
                self.proc.stderr.close()
        cmd = [
            sys.executable,
            "-m",
            "server.node",
            "--host",
            "127.0.0.1",
            "--port",
            str(self.port),
            "--capacity",
            str(self.capacity),
            "--wal",
            self.wal_file,
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertTrue(wait_for_server(self.port), f"Server failed to start on port {self.port}")
        return proc

    def test_lru_eviction_persists_across_crash(self):
        # 1. Start node with capacity 3
        self.proc = self.start_node()

        client = MiniRedisClient(host="127.0.0.1", port=self.port)
        with client:
            client.set("k1", "v1")
            client.set("k2", "v2")
            client.set("k3", "v3")

            # Touch k1 by reading it -> updates recency so order is k2, k3, k1
            self.assertEqual(client.get("k1"), "v1")

            # Insert k4 -> should evict k2!
            client.set("k4", "v4")

            # Check pre-crash state
            self.assertIsNone(client.get("k2"))
            self.assertEqual(client.get("k1"), "v1")
            self.assertEqual(client.get("k3"), "v3")
            self.assertEqual(client.get("k4"), "v4")

        # 2. Crash abruptly with kill
        self.proc.kill()
        self.proc.wait()
        time.sleep(0.3)

        # 3. Restart node with same capacity (3) and same WAL
        self.proc = self.start_node()

        # 4. Verify post-crash state: evicted key k2 must remain evicted,
        # and surviving keys (k1, k3, k4) must be present!
        new_client = MiniRedisClient(host="127.0.0.1", port=self.port)
        with new_client:
            self.assertIsNone(new_client.get("k2"), "k2 should remain evicted after crash recovery")
            self.assertEqual(new_client.get("k1"), "v1")
            self.assertEqual(new_client.get("k3"), "v3")
            self.assertEqual(new_client.get("k4"), "v4")

            # Next insert (k5) should evict the oldest key (k1, since it was accessed before k3 and k4)
            new_client.set("k5", "v5")
            self.assertIsNone(new_client.get("k1"), "k1 should be evicted when inserting k5 at capacity")
            self.assertEqual(new_client.get("k3"), "v3")
            self.assertEqual(new_client.get("k4"), "v4")
            self.assertEqual(new_client.get("k5"), "v5")


if __name__ == "__main__":
    unittest.main()
