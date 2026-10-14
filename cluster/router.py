"""Client-facing routing layer with consistent hashing, replication, and failover."""

import argparse
import logging
import socket
import socketserver
import threading
import time
from typing import Dict, List, Optional, Tuple

from .hash_ring import HashRing
from .replication import ReplicationManager, send_node_command, check_node_health

logger = logging.getLogger(__name__)


class RouterTCPHandler(socketserver.BaseRequestHandler):
    """Handles incoming client TCP connections to the cluster router."""

    def handle(self) -> None:
        buffer = ""
        self.request.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        while not self.server.is_stopping:
            try:
                data = self.request.recv(4096)
                if not data:
                    break
                buffer += data.decode("utf-8", errors="replace")

                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip("\r")
                    if not line.strip():
                        continue
                    reply = self.server.router.route_command(line)
                    if not reply.endswith("\r\n"):
                        reply += "\r\n"
                    self.request.sendall(reply.encode("utf-8"))
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                break
            except Exception as e:
                try:
                    self.request.sendall(f"-ERR {str(e)}\r\n".encode("utf-8"))
                except Exception:
                    pass
                break


class ThreadedRouterServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, RequestHandlerClass, router):
        super().__init__(server_address, RequestHandlerClass)
        self.router = router
        self.is_stopping = False


class RouterServer:
    """
    Cluster Router:
    - Consistent hashing ring partitions keyspace across primary nodes.
    - Forwards writes to primary and replicates to replica with versioning.
    - Detects node failures, performs automatic failover to replica.
    - Background health checks for node recovery and cluster rejoin.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        primary_nodes: Optional[List[str]] = None,
        replica_map: Optional[Dict[str, str]] = None,
        vnodes: int = 150,
        health_check_interval: float = 1.0,
        node_timeout: float = 1.0,
    ):
        self.host = host
        self.port = port
        self.node_timeout = node_timeout
        self.health_check_interval = health_check_interval

        self.primaries = list(primary_nodes) if primary_nodes else []
        self.hash_ring = HashRing(nodes=self.primaries, vnodes=vnodes)
        self.replication_mgr = ReplicationManager(replica_map=replica_map, health_check_timeout=node_timeout)

        self._lock = threading.RLock()
        self._running = False
        self._tcp_server: Optional[ThreadedRouterServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._health_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def get_primary_for_key(self, key: str) -> Optional[str]:
        """Use consistent hash ring to find primary node for key."""
        with self._lock:
            return self.hash_ring.get_node(key)

    def handle_failover(self, failed_primary: str) -> Optional[str]:
        """
        Handle primary failure:
        1. Remove failed primary from hash ring.
        2. Promote replica to primary.
        3. Add promoted replica to hash ring.
        """
        with self._lock:
            promoted = self.replication_mgr.failover(failed_primary)
            self.hash_ring.remove_node(failed_primary)
            if promoted:
                self.hash_ring.add_node(promoted)
                logger.warning(f"Re-routed hash ring from {failed_primary} to {promoted}")
            return promoted

    def recover_node(self, node: str) -> bool:
        """
        Attempt to recover and rejoin a downed node.
        """
        with self._lock:
            ok = self.replication_mgr.recover_node(node)
            if ok:
                # If it's a primary that recovered, re-add to ring
                self.hash_ring.add_node(node)
                logger.info(f"Node {node} recovered and re-added to hash ring.")
            return ok

    def _execute_with_failover(self, key: str, command: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Execute command on the primary node responsible for key, with automatic failover.
        Returns (node_used, reply_string).
        """
        primary = self.get_primary_for_key(key)
        if not primary:
            return None, "-ERR no active cluster nodes available"

        reply = send_node_command(primary, command, timeout=self.node_timeout)
        if reply is not None:
            return primary, reply

        # Primary failed! Initiate failover
        promoted = self.handle_failover(primary)
        if not promoted:
            return None, f"-ERR primary {primary} failed and no replica available"

        # Retry on promoted replica
        reply2 = send_node_command(promoted, command, timeout=self.node_timeout)
        if reply2 is not None:
            return promoted, reply2

        return None, f"-ERR node {promoted} also unreachable after failover"

    def route_command(self, line: str) -> str:
        """Parse client command and route appropriately."""
        line = line.strip()
        if not line:
            return "-ERR empty command"

        parts = line.split()
        cmd = parts[0].upper()

        if cmd == "PING":
            return "+PONG"

        elif cmd == "CLUSTER" and len(parts) > 1 and parts[1].upper() == "NODES":
            with self._lock:
                node_list = []
                for p in self.primaries:
                    r = self.replication_mgr.get_replica_for(p)
                    status = "DOWN" if p in self.replication_mgr.failed_nodes else "ONLINE"
                    node_list.append(f"{p} role=primary status={status} replica={r}")
                return "+" + " | ".join(node_list)

        elif cmd in ("SET", "GET", "DEL", "EXISTS", "EXPIRE", "VERSION"):
            if len(parts) < 2:
                return f"-ERR wrong number of arguments for '{cmd}'"
            key = parts[1]

            if cmd == "SET":
                node_used, reply = self._execute_with_failover(key, line)
                if reply and reply.startswith("+OK"):
                    # Asynchronously/synchronously replicate to replica with version
                    ver_reply = send_node_command(node_used, f"VERSION {key}", timeout=self.node_timeout)
                    version = 1
                    if ver_reply and ver_reply.startswith(":"):
                        try:
                            version = int(ver_reply[1:])
                        except ValueError:
                            version = 1

                    replica = self.replication_mgr.get_replica_for(node_used)
                    if replica and replica not in self.replication_mgr.failed_nodes:
                        value = parts[2] if len(parts) > 2 else ""
                        ttl = float(parts[3]) if len(parts) > 3 else None
                        self.replication_mgr.replicate_set(replica, key, value, version, ttl=ttl)

                return reply or "-ERR operation failed"

            elif cmd == "DEL":
                node_used, reply = self._execute_with_failover(key, line)
                if reply in (":1", ":0") and node_used:
                    replica = self.replication_mgr.get_replica_for(node_used)
                    if replica and replica not in self.replication_mgr.failed_nodes:
                        self.replication_mgr.replicate_del(replica, key)
                return reply or "-ERR operation failed"

            elif cmd == "EXPIRE":
                node_used, reply = self._execute_with_failover(key, line)
                if reply == ":1" and node_used:
                    replica = self.replication_mgr.get_replica_for(node_used)
                    if replica and replica not in self.replication_mgr.failed_nodes:
                        try:
                            sec = float(parts[2])
                            self.replication_mgr.replicate_expire(replica, key, sec)
                        except (IndexError, ValueError):
                            pass
                return reply or "-ERR operation failed"

            elif cmd in ("GET", "EXISTS", "VERSION"):
                _, reply = self._execute_with_failover(key, line)
                return reply or "-ERR operation failed"

        elif cmd == "INFO":
            with self._lock:
                return (
                    f"+router_host={self.host} router_port={self.port} "
                    f"active_primaries={len(self.hash_ring.nodes)} "
                    f"failed_nodes={len(self.replication_mgr.failed_nodes)}"
                )

        return f"-ERR unknown command '{cmd}'"

    def _health_check_loop(self) -> None:
        """Background health check: periodically tests failed nodes and initiates rejoin."""
        while not self._stop_event.is_set():
            try:
                failed = list(self.replication_mgr.failed_nodes)
                for node in failed:
                    if check_node_health(node, timeout=self.node_timeout):
                        self.recover_node(node)
            except Exception:
                pass
            self._stop_event.wait(self.health_check_interval)

    def start(self, background: bool = True) -> None:
        """Start the Router server and background health checker."""
        with self._lock:
            if self._running:
                return
            self._tcp_server = ThreadedRouterServer((self.host, self.port), RouterTCPHandler, self)
            self._running = True
            self._stop_event.clear()

            self._health_thread = threading.Thread(
                target=self._health_check_loop,
                name="RouterHealthCheckThread",
                daemon=True,
            )
            self._health_thread.start()

            if background:
                self._server_thread = threading.Thread(
                    target=self._tcp_server.serve_forever,
                    name=f"RouterServer-{self.port}",
                    daemon=True,
                )
                self._server_thread.start()

        if not background:
            try:
                self._tcp_server.serve_forever()
            finally:
                self.stop()

    def stop(self) -> None:
        """Stop Router server and health checker."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            self._stop_event.set()

            if self._tcp_server:
                self._tcp_server.is_stopping = True
                self._tcp_server.shutdown()
                self._tcp_server.server_close()
                self._tcp_server = None

            if self._health_thread and self._health_thread.is_alive():
                self._health_thread.join(timeout=1.0)
                self._health_thread = None

            if self._server_thread and self._server_thread.is_alive():
                self._server_thread.join(timeout=1.0)
                self._server_thread = None


def main():
    parser = argparse.ArgumentParser(description="Mini-Redis Cluster Router")
    parser.add_argument("--host", default="0.0.0.0", help="Router host")
    parser.add_argument("--port", type=int, default=6379, help="Router port")
    parser.add_argument(
        "--primaries",
        nargs="+",
        default=["127.0.0.1:6380", "127.0.0.1:6381", "127.0.0.1:6382"],
        help="Primary node addresses",
    )
    parser.add_argument(
        "--replicas",
        nargs="+",
        default=["127.0.0.1:6383", "127.0.0.1:6384", "127.0.0.1:6385"],
        help="Replica node addresses corresponding to primaries",
    )
    args = parser.parse_args()

    replica_map = {}
    for p, r in zip(args.primaries, args.replicas):
        replica_map[p] = r

    router = RouterServer(
        host=args.host,
        port=args.port,
        primary_nodes=args.primaries,
        replica_map=replica_map,
    )

    print(f"[*] Mini-Redis Router starting on {args.host}:{args.port}")
    print(f"[*] Primaries: {args.primaries}")
    print(f"[*] Replicas: {replica_map}")

    try:
        router.start(background=False)
    except KeyboardInterrupt:
        print("\n[*] Shutting down router...")
        router.stop()


if __name__ == "__main__":
    main()
