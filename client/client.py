"""Python client library for Mini-Redis."""

import socket
import threading
from typing import Optional, Any


class MiniRedisError(Exception):
    """Base exception for Mini-Redis client errors."""
    pass


class MiniRedisClient:
    """
    Client for communicating with Mini-Redis Router or standalone Node.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 6379, timeout: float = 2.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock: Optional[socket.socket] = None
        self._lock = threading.Lock()
        self._buffer = ""

    def connect(self) -> None:
        """Establish TCP connection to Mini-Redis server."""
        if self._sock is not None:
            return
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((self.host, self.port))
        self._sock = s
        self._buffer = ""

    def close(self) -> None:
        """Close current connection."""
        with self._lock:
            if self._sock:
                try:
                    self._sock.close()
                except Exception:
                    pass
                self._sock = None
                self._buffer = ""

    def __enter__(self) -> "MiniRedisClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _send_command(self, cmd_line: str) -> str:
        with self._lock:
            for attempt in range(2):
                try:
                    if self._sock is None:
                        self.connect()

                    if not cmd_line.endswith("\r\n"):
                        cmd_line = cmd_line.rstrip("\r\n") + "\r\n"

                    self._sock.sendall(cmd_line.encode("utf-8"))

                    # Read response line
                    while "\n" not in self._buffer:
                        chunk = self._sock.recv(4096)
                        if not chunk:
                            raise ConnectionResetError("Connection closed by server")
                        self._buffer += chunk.decode("utf-8", errors="replace")

                    line, self._buffer = self._buffer.split("\n", 1)
                    line = line.strip("\r")
                    return line

                except (ConnectionError, socket.timeout, OSError) as e:
                    self.close()
                    if attempt == 1:
                        raise MiniRedisError(f"Command '{cmd_line.strip()}' failed: {e}")

            raise MiniRedisError("Connection failed")

    def ping(self) -> str:
        """Health check. Returns 'PONG'."""
        reply = self._send_command("PING")
        if reply.startswith("+"):
            return reply[1:]
        raise MiniRedisError(reply)

    def set(self, key: str, value: str, ttl: Optional[float] = None) -> bool:
        """Store or update key-value pair."""
        cmd = f"SET {key} {value}"
        if ttl is not None and ttl > 0:
            cmd += f" {ttl}"
        reply = self._send_command(cmd)
        if reply == "+OK":
            return True
        elif reply.startswith("-ERR"):
            raise MiniRedisError(reply[5:])
        return False

    def get(self, key: str) -> Optional[str]:
        """Retrieve key value. Returns None if not found or expired."""
        reply = self._send_command(f"GET {key}")
        if reply == "$-1":
            return None
        elif reply.startswith("+"):
            return reply[1:]
        elif reply.startswith("-ERR"):
            raise MiniRedisError(reply[5:])
        return reply

    def delete(self, key: str) -> bool:
        """Delete key. Returns True if deleted, False if not found."""
        reply = self._send_command(f"DEL {key}")
        if reply == ":1":
            return True
        elif reply == ":0":
            return False
        elif reply.startswith("-ERR"):
            raise MiniRedisError(reply[5:])
        return False

    def del_key(self, key: str) -> bool:
        """Alias for delete."""
        return self.delete(key)

    def exists(self, key: str) -> bool:
        """Check whether key exists."""
        reply = self._send_command(f"EXISTS {key}")
        if reply == ":1":
            return True
        elif reply == ":0":
            return False
        elif reply.startswith("-ERR"):
            raise MiniRedisError(reply[5:])
        return False

    def expire(self, key: str, seconds: float) -> bool:
        """Set TTL on an existing key."""
        reply = self._send_command(f"EXPIRE {key} {seconds}")
        if reply == ":1":
            return True
        elif reply == ":0":
            return False
        elif reply.startswith("-ERR"):
            raise MiniRedisError(reply[5:])
        return False

    def version(self, key: str) -> int:
        """Get version of key."""
        reply = self._send_command(f"VERSION {key}")
        if reply.startswith(":"):
            try:
                return int(reply[1:])
            except ValueError:
                return 0
        elif reply.startswith("-ERR"):
            raise MiniRedisError(reply[5:])
        return 0
