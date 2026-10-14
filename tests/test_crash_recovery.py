"""Tests crash recovery via subprocess kill -9 and WAL replay."""

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


class TestCrashRecovery(unittest.TestCase):
    def setUp(self):
        self.port = find_free_port()
        self.wal_file = os.path.abspath(f"test_crash_{self.port}.wal")
        if os.path.exists(self.wal_file):
            os.remove(self.wal_file)
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
            "--wal",
            self.wal_file,
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertTrue(wait_for_server(self.port), f"Server failed to start on port {self.port}")
        return proc

    def test_kill_and_wal_replay_recovery(self):
        # 1. Start initial node
        self.proc = self.start_node()

        client = MiniRedisClient(host="127.0.0.1", port=self.port)
        with client:
            # Write batch of keys
            client.set("user:100", "Alice")
            client.set("user:200", "Bob")
            client.set("temp:key", "to_be_deleted")
            client.set("counter", "1")
            client.set("counter", "2")  # version should be 2
            client.delete("temp:key")

            # Verify before crash
            self.assertEqual(client.get("user:100"), "Alice")
            self.assertEqual(client.get("user:200"), "Bob")
            self.assertIsNone(client.get("temp:key"))
            self.assertEqual(client.get("counter"), "2")
            self.assertEqual(client.version("counter"), 2)

        # 2. Simulate kill -9 crash abruptly
        self.proc.kill()
        self.proc.wait()
        time.sleep(0.3)

        # Confirm port is closed
        with self.assertRaises((ConnectionRefusedError, socket.timeout, OSError)):
            with socket.create_connection(("127.0.0.1", self.port), timeout=0.2):
                pass

        # Verify WAL file exists and has content
        self.assertTrue(os.path.exists(self.wal_file))
        self.assertGreater(os.path.getsize(self.wal_file), 0)

        # 3. Restart new node pointing to the same WAL
        self.proc = self.start_node()

        # 4. Verify all data and versions are restored correctly
        new_client = MiniRedisClient(host="127.0.0.1", port=self.port)
        with new_client:
            self.assertEqual(new_client.get("user:100"), "Alice")
            self.assertEqual(new_client.get("user:200"), "Bob")
            self.assertIsNone(new_client.get("temp:key"))
            self.assertEqual(new_client.get("counter"), "2")
            self.assertEqual(new_client.version("counter"), 2)

            # Further mutations after recovery should increment version properly
            new_client.set("counter", "3")
            self.assertEqual(new_client.get("counter"), "3")
            self.assertEqual(new_client.version("counter"), 3)


if __name__ == "__main__":
    unittest.main()
