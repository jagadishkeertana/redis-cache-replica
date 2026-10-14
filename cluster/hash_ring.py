"""Consistent Hashing implementation with virtual nodes."""

import bisect
import hashlib
from typing import List, Dict, Optional, Set


class HashRing:
    """
    Consistent Hash Ring with virtual nodes (vnodes).
    Provides uniform key distribution and minimal reshuffling on node changes.
    """

    def __init__(self, nodes: Optional[List[str]] = None, vnodes: int = 150):
        """
        :param nodes: Initial list of node identifiers (e.g. ['host:port', ...])
        :param vnodes: Number of virtual nodes per physical node for distribution balance
        """
        self.vnodes = vnodes
        self.ring: List[int] = []  # Sorted list of hash integers
        self.ring_map: Dict[int, str] = {}  # hash -> physical node id
        self.nodes: Set[str] = set()

        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, key: str) -> int:
        """MD5-based 32-bit integer hash."""
        return int(hashlib.md5(key.encode("utf-8")).hexdigest()[:8], 16)

    def add_node(self, node: str) -> None:
        """Add a physical node to the hash ring."""
        if node in self.nodes:
            return
        self.nodes.add(node)
        for i in range(self.vnodes):
            vkey = f"{node}#vnode{i}"
            h = self._hash(vkey)
            self.ring_map[h] = node
            bisect.insort(self.ring, h)

    def remove_node(self, node: str) -> None:
        """Remove a physical node from the hash ring."""
        if node not in self.nodes:
            return
        self.nodes.remove(node)
        for i in range(self.vnodes):
            vkey = f"{node}#vnode{i}"
            h = self._hash(vkey)
            if h in self.ring_map:
                del self.ring_map[h]
                idx = bisect.bisect_left(self.ring, h)
                if idx < len(self.ring) and self.ring[idx] == h:
                    self.ring.pop(idx)

    def get_node(self, key: str) -> Optional[str]:
        """
        Determine the primary physical node responsible for a given key.
        Returns None if ring is empty.
        """
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h)
        if idx == len(self.ring):
            idx = 0
        return self.ring_map[self.ring[idx]]

    def get_nodes(self, key: str, count: int = 2) -> List[str]:
        """
        Get up to `count` distinct physical nodes for a key (e.g. primary + replica).
        Walks clockwise around the ring.
        """
        if not self.ring:
            return []
        if len(self.nodes) <= count:
            # Return primary first, then others
            primary = self.get_node(key)
            res = [primary] if primary else []
            for n in self.nodes:
                if n not in res:
                    res.append(n)
            return res[:count]

        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h)
        ring_len = len(self.ring)
        selected: List[str] = []

        for offset in range(ring_len):
            curr_idx = (idx + offset) % ring_len
            node = self.ring_map[self.ring[curr_idx]]
            if node not in selected:
                selected.append(node)
                if len(selected) == count:
                    break
        return selected

    def get_distribution(self, keys: List[str]) -> Dict[str, int]:
        """
        Helper to measure distribution of a set of keys across nodes.
        """
        dist: Dict[str, int] = {node: 0 for node in self.nodes}
        for key in keys:
            node = self.get_node(key)
            if node:
                dist[node] += 1
        return dist
