"""LSM-tree key-value table stored as an Apache Iceberg table.

Write path:  put/delete -> WAL -> MemTable (skip list)
             MemTable full -> flush: sorted Parquet run + Puffin index,
             committed to Iceberg in ONE transaction (add_files + LSM manifest pointer).
Read path:   MemTable -> L0 runs newest-first (key range, bloom filter, fence pointers)
             -> deeper levels. First version found wins; tombstones mean "deleted".

The Iceberg table itself stays a normal table (columns key, value, seq, op), so any
engine (Spark, Trino, PyIceberg) can still read it; the "current" value of a key is
the row with the highest seq.
"""
import copy
import heapq
import json
import os
from typing import Iterator, Optional, Tuple
from urllib.parse import unquote, urlparse

from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.table.statistics import StatisticsFile
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StringType

from .config import LSMConfig
from .memtable import DELETE, PUT, MemTable
from .run import BlockCache, LookupStats, RunMeta, RunReader, write_index, write_run_files
from .wal import WriteAheadLog

PROP_MANIFEST = "lsm.manifest-location"

KV_SCHEMA = Schema(
    NestedField(1, "key", StringType(), required=True),
    NestedField(2, "value", StringType(), required=False),
    NestedField(3, "seq", LongType(), required=True),
    NestedField(4, "op", IntegerType(), required=True),
)


def local_path(location: str) -> str:
    p = urlparse(location)
    return unquote(p.path) if p.scheme in ("file", "") else location


def _atomic_write_json(path: str, obj) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)


class LSMTable:
    def __init__(self, catalog, identifier: str, config: Optional[LSMConfig] = None):
        self.config = config or LSMConfig()
        self.catalog = catalog
        self.identifier = identifier
        self.table = self._load_or_create()
        root = local_path(self.table.location())
        self.data_dir = os.path.join(root, "data", "lsm")
        self.index_dir = os.path.join(root, "metadata", "lsm-index")
        self.manifest_dir = os.path.join(root, "metadata", "lsm")
        for d in (self.data_dir, self.index_dir, self.manifest_dir):
            os.makedirs(d, exist_ok=True)

        self.block_cache = BlockCache(self.config.block_cache_blocks)
        self.stats = LookupStats()
        self.bytes_flushed = 0
        self._load_manifest()

        # Crash recovery: replay WAL records newer than the last flushed seq.
        self.wal_path = os.path.join(root, "wal", "current.wal")
        self.memtable = MemTable()
        self.seq = self.manifest["last-seq"]
        self.recovered = 0
        for seq, op, key, value in WriteAheadLog.replay(self.wal_path):
            if seq > self.manifest["last-seq"]:
                self.memtable.put(key, seq, op, value)
                self.seq = max(self.seq, seq)
                self.recovered += 1
        self.wal = WriteAheadLog(self.wal_path, sync=self.config.wal_sync)

    # ------------------------------------------------------------------ setup
    def _load_or_create(self):
        ns = self.identifier.rsplit(".", 1)[0]
        self.catalog.create_namespace_if_not_exists(ns)
        try:
            return self.catalog.load_table(self.identifier)
        except NoSuchTableError:
            return self.catalog.create_table(
                self.identifier,
                schema=KV_SCHEMA,
                sort_order=SortOrder(SortField(source_id=1, transform=IdentityTransform())),
                properties={"lsm.enabled": "true", "lsm.format-version": "1"},
            )

    def _load_manifest(self) -> None:
        loc = self.table.properties.get(PROP_MANIFEST)
        if loc is None:
            self.manifest = {"format-version": 1, "manifest-version": 0, "last-seq": 0,
                             "next-run-id": 1, "levels": {"0": []}}
        else:
            with open(loc) as f:
                self.manifest = json.load(f)
        self.levels = {}
        for lvl, runs in self.manifest["levels"].items():
            readers = [RunReader.load(RunMeta.from_dict(r), self.config.row_group_size, self.config.bloom_fpr)
                       for r in runs]
            readers.sort(key=lambda r: r.meta.max_seq, reverse=True)  # newest first
            self.levels[int(lvl)] = readers

    # ------------------------------------------------------------ write path
    def put(self, key: str, value: str) -> None:
        if value is None:
            raise ValueError("use delete() to remove a key")
        self._write(key, PUT, value)

    def delete(self, key: str) -> None:
        self._write(key, DELETE, None)

    def _write(self, key: str, op: int, value: Optional[str]) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        self.seq += 1
        self.wal.append(self.seq, op, key, value)
        self.memtable.put(key, self.seq, op, value)
        if self._should_flush():
            self.flush()

    def _should_flush(self) -> bool:
        c = self.config
        mt = self.memtable
        if len(mt) >= c.memtable_max_entries or mt.size_bytes >= c.memtable_max_bytes:
            return True
        return c.flush_interval_s is not None and len(mt) > 0 and mt.age_seconds() >= c.flush_interval_s

    def maybe_flush(self) -> Optional[RunMeta]:
        """Call periodically (e.g. from a timer) so the time-based trigger fires when idle."""
        return self.flush() if self._should_flush() else None

    def flush(self) -> Optional[RunMeta]:
        if len(self.memtable) == 0:
            return None
        run_id = self.manifest["next-run-id"]
        data_path = os.path.join(self.data_dir, f"L0-run-{run_id:06d}.parquet")
        index_path = os.path.join(self.index_dir, f"L0-run-{run_id:06d}.puffin")
        entries = list(self.memtable.items())
        meta, bloom, fences = write_run_files(entries, run_id, 0, data_path, index_path,
                                              self.config.row_group_size, self.config.bloom_fpr)

        new_manifest = copy.deepcopy(self.manifest)
        new_manifest["manifest-version"] += 1
        new_manifest["last-seq"] = self.seq
        new_manifest["next-run-id"] = run_id + 1
        new_manifest["levels"].setdefault("0", []).append(meta.to_dict())
        manifest_path = os.path.join(self.manifest_dir, f"lsm-manifest-{new_manifest['manifest-version']:05d}.json")
        _atomic_write_json(manifest_path, new_manifest)

        # One atomic Iceberg commit: register the data file AND move the LSM manifest pointer.
        try:
            with self.table.transaction() as tx:
                tx.add_files([data_path], snapshot_properties={
                    "lsm.operation": "flush", "lsm.run-id": str(run_id), "lsm.level": "0",
                    "lsm.min-key": meta.min_key, "lsm.max-key": meta.max_key,
                    "lsm.max-seq": str(meta.max_seq)},
                    check_duplicate_files=False)
                tx.set_properties({PROP_MANIFEST: manifest_path})
        except Exception:
            for p in (data_path, manifest_path):
                if os.path.exists(p):
                    os.remove(p)
            raise

        # Attach the run's index to the new snapshot as an Iceberg statistics (Puffin) file.
        snap = self.table.current_snapshot()
        size, footer, _blobs = write_index(meta, bloom, fences, snap.snapshot_id, snap.sequence_number)
        # blob_metadata is left empty: PyIceberg 0.10 only accepts its two built-in blob
        # types in table metadata. Our custom blobs are fully described in the Puffin
        # file's own footer, which is what RunReader reads.
        with self.table.update_statistics() as upd:
            upd.set_statistics(StatisticsFile(
                snapshot_id=snap.snapshot_id, statistics_path=index_path,
                file_size_in_bytes=size, file_footer_size_in_bytes=footer, blob_metadata=[]))

        self.manifest = new_manifest
        self.levels.setdefault(0, []).insert(0, RunReader(meta, bloom, fences))
        self.bytes_flushed += meta.file_size
        self.memtable = MemTable()
        self.wal.reset()
        return meta

    # ------------------------------------------------------------- read path
    def get(self, key: str, trace: Optional[list] = None) -> Optional[str]:
        self.stats.gets += 1
        hit = self.memtable.get(key)
        if hit is not None:
            self.stats.memtable_hits += 1
            if trace is not None:
                trace.append(f"memtable: found seq={hit[0]}")
            return hit[2] if hit[1] == PUT else None
        if trace is not None:
            trace.append("memtable: miss")
        for lvl in sorted(self.levels):
            # L0 runs overlap -> check all, newest first. Deeper levels will be
            # non-overlapping after compaction, so range pruning leaves at most one.
            for run in self.levels[lvl]:
                res = run.get(key, self.block_cache, self.stats, trace)
                if res is not None:
                    seq, op, value = res
                    if op == DELETE and trace is not None:
                        trace.append("-> tombstone: key is deleted")
                    return value if op == PUT else None
        if trace is not None:
            trace.append("-> not found anywhere")
        return None

    def scan(self, lo: Optional[str] = None, hi: Optional[str] = None) -> Iterator[Tuple[str, str]]:
        """Yield live (key, value) pairs with lo <= key <= hi, in key order."""
        sources = [((k, -s, o, v) for k, s, o, v in self.memtable.items(lo, hi))]
        for lvl in sorted(self.levels):
            for run in self.levels[lvl]:
                sources.append(run.iter_range(lo, hi, self.block_cache))
        last = None
        for key, _neg_seq, op, value in heapq.merge(*sources):
            if key == last:
                continue  # older version of a key we already resolved
            last = key
            if op == PUT:
                yield key, value

    # ------------------------------------------------------------------ misc
    def describe(self) -> dict:
        return {
            "table": self.identifier,
            "memtable_entries": len(self.memtable),
            "memtable_bytes": self.memtable.size_bytes,
            "last_seq": self.seq,
            "levels": {lvl: [r.meta.run_id for r in runs] for lvl, runs in self.levels.items()},
            "runs": sum(len(r) for r in self.levels.values()),
            "snapshots": len(self.table.metadata.snapshots),
            "lookup_stats": self.stats.as_dict(),
        }

    def close(self) -> None:
        self.wal.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
