"""In-memory write buffer: a skip list ordered by key.

Each key keeps only its newest version (seq, op, value). A delete is stored as a
tombstone (op=DELETE) so it can shadow older versions that already live on disk.
"""
import random
import time
from typing import Iterator, Optional, Tuple

PUT, DELETE = 0, 1
_MAX_LEVEL = 18
_P = 0.5


class _Node:
    __slots__ = ("key", "seq", "op", "value", "forward")

    def __init__(self, key, seq, op, value, level):
        self.key = key
        self.seq = seq
        self.op = op
        self.value = value
        self.forward = [None] * level


def _vsize(value: Optional[str]) -> int:
    return 0 if value is None else len(value.encode())


class MemTable:
    def __init__(self, seed: Optional[int] = None):
        self._rng = random.Random(seed)
        self._head = _Node(None, 0, PUT, None, _MAX_LEVEL)
        self._level = 1
        self._count = 0
        self._bytes = 0
        self.created_at = time.monotonic()

    def __len__(self) -> int:
        return self._count

    @property
    def size_bytes(self) -> int:
        return self._bytes

    def age_seconds(self) -> float:
        return time.monotonic() - self.created_at

    def _random_level(self) -> int:
        lvl = 1
        while lvl < _MAX_LEVEL and self._rng.random() < _P:
            lvl += 1
        return lvl

    def put(self, key: str, seq: int, op: int, value: Optional[str]) -> None:
        update = [self._head] * _MAX_LEVEL
        x = self._head
        for i in range(self._level - 1, -1, -1):
            while x.forward[i] is not None and x.forward[i].key < key:
                x = x.forward[i]
            update[i] = x
        nxt = x.forward[0]
        if nxt is not None and nxt.key == key:
            self._bytes += _vsize(value) - _vsize(nxt.value)
            nxt.seq, nxt.op, nxt.value = seq, op, value
            return
        lvl = self._random_level()
        if lvl > self._level:
            self._level = lvl
        node = _Node(key, seq, op, value, lvl)
        for i in range(lvl):
            node.forward[i] = update[i].forward[i]
            update[i].forward[i] = node
        self._count += 1
        self._bytes += len(key.encode()) + _vsize(value) + 16

    def _seek(self, key: Optional[str]) -> Optional[_Node]:
        """First node with node.key >= key."""
        x = self._head
        if key is None:
            return x.forward[0]
        for i in range(self._level - 1, -1, -1):
            while x.forward[i] is not None and x.forward[i].key < key:
                x = x.forward[i]
        return x.forward[0]

    def get(self, key: str) -> Optional[Tuple[int, int, Optional[str]]]:
        node = self._seek(key)
        if node is not None and node.key == key:
            return node.seq, node.op, node.value
        return None

    def items(self, lo: Optional[str] = None, hi: Optional[str] = None) -> Iterator[Tuple[str, int, int, Optional[str]]]:
        """Yield (key, seq, op, value) in key order for lo <= key <= hi."""
        node = self._seek(lo)
        while node is not None and (hi is None or node.key <= hi):
            yield node.key, node.seq, node.op, node.value
            node = node.forward[0]
