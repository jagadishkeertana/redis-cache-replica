"""TCP server for Mini-Redis node, protocol parsing, and WAL integration."""

import argparse
import os
import shlex
import socket
import socketserver
import sys
import threading
import time
from typing import Optional, List

from .store import Store
from .wal import WAL
from .ttl import TTLSweeper


class ProtocolParser:
    """Parses Redis-inspired wire protocol commands and formats replies."""

    @staticmethod
    def parse_command(line: str) -> List[str]:
        """
        Parse command line into tokens.
        Handles quoted arguments cleanly or whitespace splitting.
        """
        line = line.strip()
        if not line:
            return []
        try:
            return shlex.split(line)
        except ValueError:
            return line.split()

    @staticmethod
    def format_status(msg: str) -> str:
        return f"+{msg}\r\n"

    @staticmethod
    def format_bulk(val: Optional[str]) -> str:
        if val is None:
            return "$-1\r\n"
        return f"+{val}\r\n"

    @staticmethod
    def format_integer(num: int) -> str:
        return f":{num}\r\n"

    @staticmethod
    def format_error(msg: str) -> str:
        return f"-ERR {msg}\r\n"


class NodeTCPHandler(socketserver.BaseRequestHandler):
    """Handles incoming client TCP connections."""

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
                    reply = self.server.node.execute_command(line)
                    self.request.sendall(reply.encode("utf-8"))
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError):
                break
            except Exception as e:
                try:
                    self.request.sendall(f"-ERR {str(e)}\r\n".encode("utf-8"))
                except Exception:
                    pass
                break


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, server_address, RequestHandlerClass, node):
        super().__init__(server_address, RequestHandlerClass)
        self.node = node
        self.is_stopping = False


class NodeServer:
    """
    Mini-Redis standalone TCP node backed by Store, WAL, and TTL sweeper.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        wal_path: Optional[str] = None,
        capacity: Optional[int] = None,
        sweep_interval: float = 0.5,
        sync_wal: bool = True,
    ):
        self.host = host
        self.port = port
        self.wal_path = wal_path
        self.capacity = capacity
        self.sweep_interval = sweep_interval
        self.sync_wal = sync_wal

        # Initialize Store
        self.store = Store(capacity=self.capacity)

        # Initialize WAL if path provided
        self.wal: Optional[WAL] = None
        if self.wal_path:
            self.wal = WAL(file_path=self.wal_path, sync_on_write=self.sync_wal)
            # Replay WAL to recover state
            self.wal.replay(self.store)

        # Initialize TTL sweeper
        self.sweeper = TTLSweeper(
            store=self.store,
            wal=self.wal,
            sweep_interval=self.sweep_interval,
        )

        self._tcp_server: Optional[ThreadedTCPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

    def execute_command(self, line: str) -> str:
        """
        Execute command string and return protocol response.
        """
        tokens = ProtocolParser.parse_command(line)
        if not tokens:
            return ProtocolParser.format_error("empty command")

        cmd = tokens[0].upper()

        try:
            if cmd == "PING":
                return ProtocolParser.format_status("PONG")

            elif cmd == "SET":
                # SET <key> <value> [TTL]
                if len(tokens) < 3:
                    return ProtocolParser.format_error("wrong number of arguments for 'SET'")
                key = tokens[1]
                value = tokens[2]
                ttl = None
                if len(tokens) >= 4:
                    try:
                        ttl = float(tokens[3])
                    except ValueError:
                        return ProtocolParser.format_error("invalid TTL value")

                version = self.store.set(key=key, value=value, ttl=ttl)
                if self.wal:
                    self.wal.log_set(key=key, value=value, version=version, ttl=ttl)
                return ProtocolParser.format_status("OK")

            elif cmd == "GET":
                if len(tokens) < 2:
                    return ProtocolParser.format_error("wrong number of arguments for 'GET'")
                key = tokens[1]
                val = self.store.get(key)
                if self.wal and val is not None:
                    # Record touch in WAL so LRU recency persists across crash
                    self.wal.log_touch(key)
                return ProtocolParser.format_bulk(val)

            elif cmd == "DEL":
                if len(tokens) < 2:
                    return ProtocolParser.format_error("wrong number of arguments for 'DEL'")
                key = tokens[1]
                deleted = self.store.del_key(key)
                if deleted and self.wal:
                    self.wal.log_del(key)
                return ProtocolParser.format_integer(1 if deleted else 0)

            elif cmd == "EXISTS":
                if len(tokens) < 2:
                    return ProtocolParser.format_error("wrong number of arguments for 'EXISTS'")
                key = tokens[1]
                exists = self.store.exists(key)
                return ProtocolParser.format_integer(1 if exists else 0)

            elif cmd == "EXPIRE":
                if len(tokens) < 3:
                    return ProtocolParser.format_error("wrong number of arguments for 'EXPIRE'")
                key = tokens[1]
                try:
                    seconds = float(tokens[2])
                except ValueError:
                    return ProtocolParser.format_error("invalid expire time")
                ok = self.store.expire(key, seconds)
                if ok and self.wal:
                    self.wal.log_expire(key, seconds)
                return ProtocolParser.format_integer(1 if ok else 0)

            elif cmd == "VERSION":
                if len(tokens) < 2:
                    return ProtocolParser.format_error("wrong number of arguments for 'VERSION'")
                key = tokens[1]
                ver = self.store.get_version(key)
                return ProtocolParser.format_integer(ver)

            elif cmd == "REPLSET":
                # REPLSET <key> <value> <version> [TTL]
                if len(tokens) < 4:
                    return ProtocolParser.format_error("wrong number of arguments for 'REPLSET'")
                key = tokens[1]
                value = tokens[2]
                try:
                    version = int(tokens[3])
                except ValueError:
                    return ProtocolParser.format_error("invalid version for 'REPLSET'")

                ttl = None
                if len(tokens) >= 5:
                    try:
                        ttl = float(tokens[4])
                    except ValueError:
                        return ProtocolParser.format_error("invalid TTL for 'REPLSET'")

                updated = self.store.replset(key=key, value=value, version=version, ttl=ttl)
                if updated:
                    if self.wal:
                        self.wal.log_replset(key=key, value=value, version=version, ttl=ttl)
                    return ProtocolParser.format_status("OK")
                else:
                    return ProtocolParser.format_status("IGNORED")

            elif cmd == "KEYS":
                # Helper for debugging/monitoring
                keys = self.store.keys()
                return ProtocolParser.format_bulk(" ".join(keys))

            elif cmd == "INFO":
                return ProtocolParser.format_bulk(
                    f"host={self.host} port={self.port} keys={self.store.size()} capacity={self.capacity}"
                )

            elif cmd == "TOUCH":
                if len(tokens) < 2:
                    return ProtocolParser.format_error("wrong number of arguments for 'TOUCH'")
                key = tokens[1]
                ok = self.store.touch(key)
                if ok and self.wal:
                    self.wal.log_touch(key)
                return ProtocolParser.format_integer(1 if ok else 0)

            else:
                return ProtocolParser.format_error(f"unknown command '{tokens[0]}'")

        except Exception as e:
            return ProtocolParser.format_error(str(e))

    def start(self, background: bool = True) -> None:
        """Start the node server and background sweeper."""
        with self._lock:
            if self._running:
                return

            self._tcp_server = ThreadedTCPServer((self.host, self.port), NodeTCPHandler, self)
            self._running = True
            self.sweeper.start()

            if background:
                self._server_thread = threading.Thread(
                    target=self._tcp_server.serve_forever,
                    name=f"NodeServer-{self.port}",
                    daemon=True,
                )
                self._server_thread.start()

        if not background:
            try:
                self._tcp_server.serve_forever()
            finally:
                self.stop()

    def stop(self) -> None:
        """Stop TCP server, sweeper, and flush WAL."""
        with self._lock:
            if not self._running:
                return
            self._running = False

            if self._tcp_server:
                self._tcp_server.is_stopping = True
                self._tcp_server.shutdown()
                self._tcp_server.server_close()
                self._tcp_server = None

            if self.sweeper:
                self.sweeper.stop()
                self.sweeper.join(timeout=1.0)

            if self.wal:
                self.wal.close()

            if self._server_thread and self._server_thread.is_alive():
                self._server_thread.join(timeout=2.0)
                self._server_thread = None

    def is_alive(self) -> bool:
        return self._running


def main():
    parser = argparse.ArgumentParser(description="Mini-Redis Node Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=6380, help="Port to listen on")
    parser.add_argument("--wal", default=None, help="Path to WAL persistence file")
    parser.add_argument("--capacity", type=int, default=None, help="LRU capacity limit")
    parser.add_argument("--ttl-interval", type=float, default=0.5, help="TTL sweeper interval")
    args = parser.parse_args()

    wal_file = args.wal or f"data/node_{args.port}.wal"
    node = NodeServer(
        host=args.host,
        port=args.port,
        wal_path=wal_file,
        capacity=args.capacity,
        sweep_interval=args.ttl_interval,
    )

    print(f"[*] Mini-Redis node starting on {args.host}:{args.port} (WAL: {wal_file}, Capacity: {args.capacity})")
    try:
        node.start(background=False)
    except KeyboardInterrupt:
        print("\n[*] Shutting down node...")
        node.stop()


if __name__ == "__main__":
    main()
