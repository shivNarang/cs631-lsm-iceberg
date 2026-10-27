"""Write-ahead log so un-flushed MemTable writes survive a crash.

One JSON record per line: [seq, op, key, value]. After a flush is committed to
Iceberg the log is truncated. On restart, records with seq > the last flushed
seq are replayed into a fresh MemTable (so a crash between "commit" and
"truncate" is harmless).
"""
import json
import os
from typing import Iterator, Optional, Tuple


class WriteAheadLog:
    def __init__(self, path: str, sync: bool = False):
        self.path = path
        self.sync = sync
        self.bytes_written = 0
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._f = open(path, "ab")

    def append(self, seq: int, op: int, key: str, value: Optional[str]) -> None:
        line = (json.dumps([seq, op, key, value], separators=(",", ":")) + "\n").encode()
        self._f.write(line)
        self._f.flush()
        if self.sync:
            os.fsync(self._f.fileno())
        self.bytes_written += len(line)

    def reset(self) -> None:
        self._f.close()
        self._f = open(self.path, "wb")
        if self.sync:
            os.fsync(self._f.fileno())

    def close(self) -> None:
        if not self._f.closed:
            self._f.close()

    @staticmethod
    def replay(path: str) -> Iterator[Tuple[int, int, str, Optional[str]]]:
        if not os.path.exists(path):
            return
        with open(path, "rb") as f:
            for line in f:
                try:
                    seq, op, key, value = json.loads(line)
                except (ValueError, TypeError):
                    break  # torn write at the tail from a crash: stop here
                yield seq, op, key, value
