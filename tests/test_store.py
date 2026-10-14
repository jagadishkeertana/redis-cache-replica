"""Unit tests for Store: LRU eviction, TTL expiration, REPLSET versioning, and core operations."""

import time
import unittest
import sys
import os

# Ensure project root is in python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from server.store import Store
from server.ttl import TTLSweeper


class TestStoreCoreOps(unittest.TestCase):
    def setUp(self):
        self.store = Store()

    def test_set_and_get(self):
        ver1 = self.store.set("user:1", "Alice")
        self.assertEqual(ver1, 1)
        self.assertEqual(self.store.get("user:1"), "Alice")

        ver2 = self.store.set("user:1", "Bob")
        self.assertEqual(ver2, 2)
        self.assertEqual(self.store.get("user:1"), "Bob")

    def test_get_nonexistent(self):
        self.assertIsNone(self.store.get("nonexistent"))

    def test_del_key(self):
        self.store.set("k1", "v1")
        self.assertTrue(self.store.del_key("k1"))
        self.assertFalse(self.store.del_key("k1"))
        self.assertIsNone(self.store.get("k1"))

    def test_exists(self):
        self.assertFalse(self.store.exists("k1"))
        self.store.set("k1", "v1")
        self.assertTrue(self.store.exists("k1"))
        self.store.del_key("k1")
        self.assertFalse(self.store.exists("k1"))

    def test_version_tracking(self):
        self.assertEqual(self.store.get_version("k1"), 0)
        self.store.set("k1", "v1")
        self.assertEqual(self.store.get_version("k1"), 1)
        self.store.set("k1", "v2")
        self.assertEqual(self.store.get_version("k1"), 2)


class TestStoreTTL(unittest.TestCase):
    def setUp(self):
        self.store = Store()

    def test_passive_ttl_expiration(self):
        # Set with 0.1s TTL
        self.store.set("temp", "val", ttl=0.1)
        self.assertEqual(self.store.get("temp"), "val")
        time.sleep(0.15)
        # Should be passively evicted
        self.assertIsNone(self.store.get("temp"))
        self.assertFalse(self.store.exists("temp"))

    def test_expire_command(self):
        self.store.set("session", "abc")
        self.assertTrue(self.store.expire("session", 0.1))
        self.assertEqual(self.store.get("session"), "abc")
        time.sleep(0.15)
        self.assertIsNone(self.store.get("session"))

    def test_active_ttl_sweeper(self):
        sweeper = TTLSweeper(self.store, sweep_interval=0.05)
        sweeper.start()
        try:
            self.store.set("s1", "v1", ttl=0.1)
            self.store.set("s2", "v2", ttl=0.1)
            self.store.set("s3", "v3", ttl=10.0)

            self.assertEqual(self.store.size(), 3)
            time.sleep(0.2)
            # Sweeper should have evicted s1 and s2
            self.assertEqual(self.store.size(), 1)
            self.assertEqual(self.store.keys(), ["s3"])
        finally:
            sweeper.stop()
            sweeper.join()


class TestStoreLRU(unittest.TestCase):
    def test_lru_eviction_at_capacity(self):
        # Store with capacity 3
        store = Store(capacity=3)
        store.set("a", "1")
        store.set("b", "2")
        store.set("c", "3")
        self.assertEqual(store.keys(), ["a", "b", "c"])

        # Insert 4th item -> 'a' should be evicted
        store.set("d", "4")
        self.assertIsNone(store.get("a"))
        self.assertEqual(store.keys(), ["b", "c", "d"])

    def test_lru_access_renewal(self):
        store = Store(capacity=3)
        store.set("a", "1")
        store.set("b", "2")
        store.set("c", "3")

        # Access 'a' so it becomes most recently used
        val = store.get("a")
        self.assertEqual(val, "1")
        # Order should now be b, c, a
        self.assertEqual(store.keys(), ["b", "c", "a"])

        # Insert 'd' -> 'b' should be evicted. Order is now c, a, d
        store.set("d", "4")
        self.assertIsNone(store.get("b"))
        self.assertEqual(store.keys(), ["c", "a", "d"])

        # Access 'a' again -> moves 'a' to the most recent end: order becomes c, d, a
        self.assertEqual(store.get("a"), "1")
        self.assertEqual(store.keys(), ["c", "d", "a"])


class TestStoreReplicationVersioning(unittest.TestCase):
    def setUp(self):
        self.store = Store()

    def test_replset_newer_version_accepted(self):
        ok = self.store.replset("k1", "v1", version=1)
        self.assertTrue(ok)
        self.assertEqual(self.store.get("k1"), "v1")
        self.assertEqual(self.store.get_version("k1"), 1)

        ok2 = self.store.replset("k1", "v2", version=3)
        self.assertTrue(ok2)
        self.assertEqual(self.store.get("k1"), "v2")
        self.assertEqual(self.store.get_version("k1"), 3)

    def test_replset_stale_and_duplicate_version_rejected(self):
        self.store.replset("k1", "v10", version=10)

        # Stale version (version 5 <= 10)
        ok_stale = self.store.replset("k1", "v5", version=5)
        self.assertFalse(ok_stale)
        self.assertEqual(self.store.get("k1"), "v10")  # Untouched

        # Duplicate version (version 10 <= 10)
        ok_dup = self.store.replset("k1", "v10_dup", version=10)
        self.assertFalse(ok_dup)
        self.assertEqual(self.store.get("k1"), "v10")  # Untouched


if __name__ == "__main__":
    unittest.main()
