"""Primary-replica synchronization, version-based replication, and failover management."""

import logging
import socket
import time
from typing import Dict, List, Optional, Tuple, Set

logger = logging.getLogger(__name__)


def parse_address(addr: str) -> Tuple[str, int]:
    """Parse 'host:port' string into (host, port)."""
    parts = addr.split(":")
    if len(parts) == 2:
        return parts[0], int(parts[1])
    raise ValueError(f"Invalid address format: {addr}")


def send_node_command(node_addr: str, command: str, timeout: float = 1.0) -> Optional[str]:
    """
    Send a raw command line to a Mini-Redis node and return its single-line response.
    Returns None if connection or read fails.
    """
    try:
        host, port = parse_address(node_addr)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect((host, port))
            if not command.endswith("\n"):
                command += "\r\n"
            sock.sendall(command.encode("utf-8"))

            response = ""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response += chunk.decode("utf-8", errors="replace")
                if "\n" in response:
                    break
            return response.strip("\r\n") if response else None
    except Exception as e:
        logger.debug(f"Failed to send command to {node_addr}: {e}")
        return None


def check_node_health(node_addr: str, timeout: float = 0.5) -> bool:
    """Check if node responds with +PONG."""
    reply = send_node_command(node_addr, "PING", timeout=timeout)
    return reply == "+PONG"


class ReplicationManager:
    """
    Coordinates primary-replica replication, version-aware propagation,
    health monitoring, failover, and node recovery/rejoin.
    """

    def __init__(
        self,
        replica_map: Optional[Dict[str, str]] = None,
        health_check_timeout: float = 0.5,
    ):
        """
        :param replica_map: Explicit mapping of primary_addr -> replica_addr.
        """
        # primary -> replica
        self.replica_map: Dict[str, str] = dict(replica_map) if replica_map else {}
        # reverse mapping: replica -> primary
        self.primary_map: Dict[str, str] = {r: p for p, r in self.replica_map.items()}

        self.failed_nodes: Set[str] = set()
        self.promoted_replicas: Dict[str, str] = {}  # original_primary -> promoted_replica
        self.health_check_timeout = health_check_timeout

    def register_pair(self, primary: str, replica: str) -> None:
        """Register a primary-replica pair."""
        self.replica_map[primary] = replica
        self.primary_map[replica] = primary

    def get_replica_for(self, primary: str) -> Optional[str]:
        """Get the active replica node for a given primary."""
        return self.replica_map.get(primary)

    def is_alive(self, node: str) -> bool:
        """Check if node is responsive."""
        return check_node_health(node, timeout=self.health_check_timeout)

    def replicate_set(
        self,
        replica: str,
        key: str,
        value: str,
        version: int,
        ttl: Optional[float] = None,
    ) -> bool:
        """
        Send REPLSET command to replica with version.
        Returns True if replica accepted (+OK), False if +IGNORED or unreachable.
        """
        cmd = f"REPLSET {key} {value} {version}"
        if ttl is not None and ttl > 0:
            cmd += f" {ttl}"
        reply = send_node_command(replica, cmd, timeout=self.health_check_timeout)
        if reply == "+OK":
            return True
        elif reply == "+IGNORED":
            logger.info(f"Replica {replica} ignored stale write for {key} at version {version}")
            return False
        return False

    def replicate_del(self, replica: str, key: str) -> bool:
        """Send DEL command to replica."""
        reply = send_node_command(replica, f"DEL {key}", timeout=self.health_check_timeout)
        return reply in (":1", ":0")

    def replicate_expire(self, replica: str, key: str, seconds: float) -> bool:
        """Send EXPIRE command to replica."""
        reply = send_node_command(replica, f"EXPIRE {key} {seconds}", timeout=self.health_check_timeout)
        return reply == ":1"

    def failover(self, failed_primary: str) -> Optional[str]:
        """
        Mark primary as failed and promote its replica if healthy.
        Returns promoted node address, or None if replica is also unavailable.
        """
        self.failed_nodes.add(failed_primary)
        replica = self.replica_map.get(failed_primary)

        if not replica:
            return None

        if self.is_alive(replica):
            self.promoted_replicas[failed_primary] = replica
            logger.warning(f"[FAILOVER] Promoted replica {replica} for failed primary {failed_primary}")
            return replica
        else:
            self.failed_nodes.add(replica)
            logger.error(f"[FAILOVER] Replica {replica} also down! Shard unavailable.")
            return None

    def recover_node(self, recovered_node: str) -> bool:
        """
        Handle a node recovering and rejoining the cluster.
        Returns True if node was recovered, False otherwise.
        """
        if not self.is_alive(recovered_node):
            return False

        if recovered_node in self.failed_nodes:
            self.failed_nodes.remove(recovered_node)

        # If it was a primary that had a promoted replica:
        if recovered_node in self.promoted_replicas:
            promoted_replica = self.promoted_replicas.pop(recovered_node)
            # The recovered node can now become replica of the promoted node,
            # or reclaim primary status after state catch-up.
            logger.info(f"[RECOVERY] Node {recovered_node} rejoined. Syncing with {promoted_replica}")

        return True
