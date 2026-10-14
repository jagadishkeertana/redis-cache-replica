"""Tests concurrent client access with many real TCP socket connections simultaneously."""

import concurrent.futures
import os
import random
import socket
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from server.node import NodeServer
from client.client import MiniRedisClient


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class TestConcurrentClients(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.port = find_free_port()
        cls.wal_file = f"test_concurrent_{cls.port}.wal"
        cls.node = NodeServer(host="127.0.0.1", port=cls.port, wal_path=cls.wal_file)
        cls.node.start(background=True)
        # Give server time to bind
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.node.stop()
        if os.path.exists(cls.wal_file):
            try:
                os.remove(cls.wal_file)
            except OSError:
                pass

    def test_concurrent_set_get_del(self):
        num_threads = 25
        ops_per_thread = 20

        errors = []

        def worker(thread_id: int):
            try:
                client = MiniRedisClient(host="127.0.0.1", port=self.port, timeout=5.0)
                with client:
                    for i in range(ops_per_thread):
                        key = f"thread_{thread_id}_key_{i}"
                        val = f"val_{thread_id}_{i}"

                        # SET
                        if not client.set(key, val):
                            errors.append(f"SET failed for {key}")

                        # GET
                        got = client.get(key)
                        if got != val:
                            errors.append(f"GET mismatch: expected {val}, got {got}")

                        # EXISTS
                        if not client.exists(key):
                            errors.append(f"EXISTS failed for {key}")

                        # DEL
                        if not client.delete(key):
                            errors.append(f"DEL failed for {key}")

                        # Verify DEL
                        if client.exists(key):
                            errors.append(f"Key {key} still exists after DEL")

            except Exception as e:
                errors.append(f"Thread {thread_id} encountered exception: {e}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, t) for t in range(num_threads)]
            concurrent.futures.wait(futures)

        self.assertEqual(errors, [], f"Errors occurred during concurrent execution: {errors}")

    def test_concurrent_shared_key_increment(self):
        """Many clients updating the same keys simultaneously."""
        shared_keys = ["counter_a", "counter_b", "counter_c"]
        num_threads = 20
        ops_per_thread = 15
        errors = []

        # Initialize shared keys
        init_client = MiniRedisClient(host="127.0.0.1", port=self.port)
        with init_client:
            for k in shared_keys:
                init_client.set(k, "0")

        def worker(thread_id: int):
            try:
                client = MiniRedisClient(host="127.0.0.1", port=self.port, timeout=5.0)
                with client:
                    for _ in range(ops_per_thread):
                        k = random.choice(shared_keys)
                        client.set(k, f"val_{thread_id}")
                        v = client.get(k)
                        if v is None:
                            errors.append(f"Got None for shared key {k}")
            except Exception as e:
                errors.append(f"Worker {thread_id} failed: {e}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, t) for t in range(num_threads)]
            concurrent.futures.wait(futures)

        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
