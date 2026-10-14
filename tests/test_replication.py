"""Tests primary-replica replication, versioned sync, stale-write protection, and failover."""

import os
import socket
import sys
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from server.node import NodeServer
from cluster.router import RouterServer
from cluster.replication import send_node_command
from client.client import MiniRedisClient


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


class TestReplicationAndFailover(unittest.TestCase):
    def setUp(self):
        self.primary_port = find_free_port()
        self.replica_port = find_free_port()
        self.router_port = find_free_port()

        self.primary_addr = f"127.0.0.1:{self.primary_port}"
        self.replica_addr = f"127.0.0.1:{self.replica_port}"

        self.primary_wal = f"test_repl_p_{self.primary_port}.wal"
        self.replica_wal = f"test_repl_r_{self.replica_port}.wal"

        # 1. Start Primary Node
        self.primary_node = NodeServer(
            host="127.0.0.1",
            port=self.primary_port,
            wal_path=self.primary_wal,
        )
        self.primary_node.start(background=True)

        # 2. Start Replica Node
        self.replica_node = NodeServer(
            host="127.0.0.1",
            port=self.replica_port,
            wal_path=self.replica_wal,
        )
        self.replica_node.start(background=True)

        # 3. Start Router
        self.router = RouterServer(
            host="127.0.0.1",
            port=self.router_port,
            primary_nodes=[self.primary_addr],
            replica_map={self.primary_addr: self.replica_addr},
            node_timeout=0.5,
            health_check_interval=0.2,
        )
        self.router.start(background=True)
        time.sleep(0.3)

    def tearDown(self):
        self.router.stop()
        self.primary_node.stop()
        self.replica_node.stop()

        for f in (self.primary_wal, self.replica_wal):
            if os.path.exists(f):
                try:
                    os.remove(f)
                except OSError:
                    pass

    def test_replication_sync_to_replica(self):
        """Write to Router, verify it is automatically replicated to the replica with version."""
        client = MiniRedisClient(host="127.0.0.1", port=self.router_port)
        with client:
            ok = client.set("greeting", "HelloDistributedWorld")
            self.assertTrue(ok)
            self.assertEqual(client.get("greeting"), "HelloDistributedWorld")
            self.assertEqual(client.version("greeting"), 1)

        # Give socket replication a fraction of a second
        time.sleep(0.1)

        # Query replica directly to verify replication occurred!
        repl_val = send_node_command(self.replica_addr, "GET greeting")
        self.assertEqual(repl_val, "+HelloDistributedWorld")
        repl_ver = send_node_command(self.replica_addr, "VERSION greeting")
        self.assertEqual(repl_ver, ":1")

    def test_version_based_stale_write_protection(self):
        """Directly sending an older or duplicate REPLSET to replica must return +IGNORED."""
        # 1. Write key with version 2
        send_node_command(self.replica_addr, "REPLSET config:theme dark 2")
        self.assertEqual(send_node_command(self.replica_addr, "GET config:theme"), "+dark")

        # 2. Try sending stale write with version 1
        stale_reply = send_node_command(self.replica_addr, "REPLSET config:theme light 1")
        self.assertEqual(stale_reply, "+IGNORED")
        # Ensure value was NOT updated
        self.assertEqual(send_node_command(self.replica_addr, "GET config:theme"), "+dark")

        # 3. Try sending duplicate write with version 2
        dup_reply = send_node_command(self.replica_addr, "REPLSET config:theme blue 2")
        self.assertEqual(dup_reply, "+IGNORED")
        self.assertEqual(send_node_command(self.replica_addr, "GET config:theme"), "+dark")

        # 4. Valid newer write with version 3
        new_reply = send_node_command(self.replica_addr, "REPLSET config:theme light 3")
        self.assertEqual(new_reply, "+OK")
        self.assertEqual(send_node_command(self.replica_addr, "GET config:theme"), "+light")

    def test_primary_failover_to_replica(self):
        """When primary crashes, Router automatically fails over reads and writes to replica."""
        client = MiniRedisClient(host="127.0.0.1", port=self.router_port)
        with client:
            client.set("cluster_state", "active")
            self.assertEqual(client.get("cluster_state"), "active")

        time.sleep(0.1)

        # Kill primary abruptly
        self.primary_node.stop()
        time.sleep(0.2)

        # Client should still read seamlessly from promoted replica through Router!
        with MiniRedisClient(host="127.0.0.1", port=self.router_port) as client2:
            val = client2.get("cluster_state")
            self.assertEqual(val, "active")

            # Writes should now succeed on the promoted replica!
            ok = client2.set("after_failover_key", "survived")
            self.assertTrue(ok)
            self.assertEqual(client2.get("after_failover_key"), "survived")

    def test_node_recovery_and_rejoin(self):
        """Test that downed node can be recovered and rejoined to the cluster."""
        # Simulate primary failure
        self.primary_node.stop()
        self.router.handle_failover(self.primary_addr)
        self.assertIn(self.primary_addr, self.router.replication_mgr.failed_nodes)

        # Restart primary node
        self.primary_node = NodeServer(
            host="127.0.0.1",
            port=self.primary_port,
            wal_path=self.primary_wal,
        )
        self.primary_node.start(background=True)
        time.sleep(0.3)

        # Trigger recovery
        recovered = self.router.recover_node(self.primary_addr)
        self.assertTrue(recovered)
        self.assertNotIn(self.primary_addr, self.router.replication_mgr.failed_nodes)
        self.assertIn(self.primary_addr, self.router.hash_ring.nodes)


if __name__ == "__main__":
    unittest.main()
