"""Tests for consistent hashing ring: uniform distribution, reshuffle-on-resize, and replica selection."""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cluster.hash_ring import HashRing


class TestHashRing(unittest.TestCase):
    def setUp(self):
        self.nodes = ["node:6380", "node:6381", "node:6382"]
        self.ring = HashRing(nodes=self.nodes, vnodes=150)
        self.sample_keys = [f"cache_key_{i}" for i in range(3000)]

    def test_uniform_distribution(self):
        """Verify that keys are balanced relatively evenly across all 3 nodes."""
        dist = self.ring.get_distribution(self.sample_keys)
        total = len(self.sample_keys)
        expected_ratio = 1.0 / len(self.nodes)

        for node, count in dist.items():
            ratio = count / total
            # With 150 vnodes, distribution is comfortably within 24% to 42% (ideal is 33.3%)
            self.assertGreater(ratio, 0.24, f"Node {node} got too few keys: {ratio:.2%}")
            self.assertLess(ratio, 0.42, f"Node {node} got too many keys: {ratio:.2%}")

    def test_reshuffle_on_add_node(self):
        """
        When a new node is added:
        1. Only ~1/(N+1) of total keys should move (to the new node).
        2. No keys should migrate between existing nodes (monotonicity).
        """
        orig_mapping = {k: self.ring.get_node(k) for k in self.sample_keys}

        new_node = "node:6383"
        self.ring.add_node(new_node)
        new_mapping = {k: self.ring.get_node(k) for k in self.sample_keys}

        moved_to_new_node = 0
        migrated_between_old_nodes = 0

        for k in self.sample_keys:
            old_n = orig_mapping[k]
            new_n = new_mapping[k]
            if new_n == new_node:
                moved_to_new_node += 1
            elif old_n != new_n:
                migrated_between_old_nodes += 1

        total = len(self.sample_keys)
        migration_ratio = moved_to_new_node / total

        # The new node should take roughly ~25% of keys (1/4)
        self.assertGreater(migration_ratio, 0.16)
        self.assertLess(migration_ratio, 0.35)

        # CRITICAL PROPERTY: 0 keys should migrate between the old nodes!
        self.assertEqual(
            migrated_between_old_nodes,
            0,
            "Keys migrated between existing nodes during scale out!",
        )

    def test_reshuffle_on_remove_node(self):
        """
        When a node is removed:
        1. Only keys belonging to the removed node are reassigned.
        2. Keys belonging to surviving nodes remain completely untouched.
        """
        removed_node = "node:6382"
        orig_mapping = {k: self.ring.get_node(k) for k in self.sample_keys}

        self.ring.remove_node(removed_node)
        new_mapping = {k: self.ring.get_node(k) for k in self.sample_keys}

        unaffected_moved = 0
        for k in self.sample_keys:
            old_n = orig_mapping[k]
            new_n = new_mapping[k]
            if old_n != removed_node and old_n != new_n:
                unaffected_moved += 1

        self.assertEqual(
            unaffected_moved,
            0,
            "Keys belonging to unaffected nodes were reassigned during node removal!",
        )

    def test_get_nodes_primary_and_replica(self):
        """Verify get_nodes returns distinct primary and replica nodes."""
        for i in range(50):
            key = f"user_account_{i}"
            nodes = self.ring.get_nodes(key, count=2)
            self.assertEqual(len(nodes), 2)
            self.assertNotEqual(nodes[0], nodes[1])
            self.assertEqual(nodes[0], self.ring.get_node(key))


if __name__ == "__main__":
    unittest.main()
