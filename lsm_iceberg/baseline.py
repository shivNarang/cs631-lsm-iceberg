"""Vanilla Iceberg baseline: same schema, writes appended in micro-batches
(as a streaming job would), reads via Iceberg scan + min/max pruning."""
from typing import Iterator, Optional, Tuple

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import And, EqualTo, GreaterThanOrEqual, LessThanOrEqual

from .lsm_table import KV_SCHEMA
from .memtable import DELETE, PUT
from .run import ARROW_SCHEMA


class VanillaIcebergTable:
    def __init__(self, catalog, identifier: str, batch_size: int = 50_000):
        self.batch_size = batch_size
        ns = identifier.rsplit(".", 1)[0]
        catalog.create_namespace_if_not_exists(ns)
        try:
            self.table = catalog.load_table(identifier)
        except NoSuchTableError:
            self.table = catalog.create_table(identifier, schema=KV_SCHEMA)
        snap = self.table.current_snapshot()
        self.seq = int(snap.summary.additional_properties.get("max-seq", 0)) if snap else 0
        self._buf = []
        self.files_scanned_last = 0

    def put(self, key: str, value: str) -> None:
        self._write(key, PUT, value)

    def delete(self, key: str) -> None:
        self._write(key, DELETE, None)

    def _write(self, key, op, value):
        self.seq += 1
        self._buf.append((key, value, self.seq, op))
        if len(self._buf) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self._buf:
            return
        cols = list(zip(*self._buf))
        t = pa.Table.from_arrays([pa.array(cols[0], pa.string()), pa.array(cols[1], pa.string()),
                                  pa.array(cols[2], pa.int64()), pa.array(cols[3], pa.int32())],
                                 schema=ARROW_SCHEMA)
        self.table.append(t, snapshot_properties={"max-seq": str(self.seq)})
        self._buf = []

    def get(self, key: str) -> Optional[str]:
        scan = self.table.scan(row_filter=EqualTo("key", key))
        self.files_scanned_last = sum(1 for _ in scan.plan_files())
        rows = scan.to_arrow().to_pylist()
        if not rows:
            return None
        best = max(rows, key=lambda r: r["seq"])
        return best["value"] if best["op"] == PUT else None

    def scan(self, lo: str, hi: str) -> Iterator[Tuple[str, str]]:
        scan = self.table.scan(row_filter=And(GreaterThanOrEqual("key", lo), LessThanOrEqual("key", hi)))
        self.files_scanned_last = sum(1 for _ in scan.plan_files())
        latest = {}
        for r in scan.to_arrow().to_pylist():
            cur = latest.get(r["key"])
            if cur is None or r["seq"] > cur["seq"]:
                latest[r["key"]] = r
        for k in sorted(latest):
            if latest[k]["op"] == PUT:
                yield k, latest[k]["value"]
