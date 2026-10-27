"""A sorted run (SSTable): one key-sorted Parquet file registered in Iceberg,
plus a Puffin index file holding a bloom filter and fence pointers.

Fence pointers = the first key of every Parquet row group. A point lookup
binary-searches them to read exactly one row group ("block") of the file.
"""
import json
import os
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from dataclasses import asdict, dataclass
from typing import Iterator, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from .bloom import BloomFilter
from .memtable import DELETE
from .puffin import Blob, read_puffin, write_puffin

KEY_FIELD_ID = 1
BLOOM_BLOB = "cs631-lsm-bloom-filter-v1"
FENCE_BLOB = "cs631-lsm-fence-pointers-v1"

ARROW_SCHEMA = pa.schema([
    pa.field("key", pa.string(), nullable=False),
    pa.field("value", pa.string(), nullable=True),
    pa.field("seq", pa.int64(), nullable=False),
    pa.field("op", pa.int32(), nullable=False),
])


@dataclass
class RunMeta:
    run_id: int
    level: int
    data_file: str
    index_file: str
    min_key: str
    max_key: str
    min_seq: int
    max_seq: int
    record_count: int
    tombstones: int
    row_groups: int
    file_size: int

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "RunMeta":
        return cls(**d)


class LookupStats:
    def __init__(self):
        self.reset()

    def reset(self):
        self.gets = 0
        self.memtable_hits = 0
        self.runs_checked = 0
        self.range_pruned = 0
        self.bloom_pruned = 0
        self.bloom_false_positives = 0
        self.blocks_read = 0
        self.block_cache_hits = 0

    def as_dict(self) -> dict:
        return dict(self.__dict__)


class BlockCache:
    """LRU cache of decoded row groups, keyed by (run_id, row_group)."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._d = OrderedDict()

    def get(self, k):
        v = self._d.get(k)
        if v is not None:
            self._d.move_to_end(k)
        return v

    def put(self, k, v):
        if self.capacity <= 0:
            return
        self._d[k] = v
        self._d.move_to_end(k)
        while len(self._d) > self.capacity:
            self._d.popitem(last=False)

    def evict_run(self, run_id: int):
        for k in [k for k in self._d if k[0] == run_id]:
            del self._d[k]


def write_run_files(entries: List[Tuple[str, int, int, Optional[str]]], run_id: int, level: int,
                    data_path: str, index_path: str, row_group_size: int, bloom_fpr: float):
    """Write sorted entries to Parquet and build (but don't yet write) the index.

    Returns (RunMeta, bloom, fences)."""
    keys = [e[0] for e in entries]
    table = pa.Table.from_arrays(
        [pa.array(keys, pa.string()),
         pa.array([e[3] for e in entries], pa.string()),
         pa.array([e[1] for e in entries], pa.int64()),
         pa.array([e[2] for e in entries], pa.int32())],
        schema=ARROW_SCHEMA,
    )
    pq.write_table(table, data_path, row_group_size=row_group_size, compression="zstd")
    bloom, fences = build_index(keys, row_group_size, bloom_fpr)
    seqs = [e[1] for e in entries]
    meta = RunMeta(
        run_id=run_id, level=level, data_file=data_path, index_file=index_path,
        min_key=keys[0], max_key=keys[-1], min_seq=min(seqs), max_seq=max(seqs),
        record_count=len(entries), tombstones=sum(1 for e in entries if e[2] == DELETE),
        row_groups=len(fences), file_size=os.path.getsize(data_path),
    )
    return meta, bloom, fences


def build_index(keys: List[str], row_group_size: int, bloom_fpr: float):
    bloom = BloomFilter.for_capacity(len(keys), bloom_fpr)
    for k in keys:
        bloom.add(k)
    fences = keys[::row_group_size]
    return bloom, fences


def write_index(meta: RunMeta, bloom: BloomFilter, fences: List[str], snapshot_id: int, sequence_number: int):
    props = {"run-id": str(meta.run_id), "level": str(meta.level),
             "min-key": meta.min_key, "max-key": meta.max_key}
    blobs = [
        Blob(BLOOM_BLOB, [KEY_FIELD_ID], bloom.to_bytes(), snapshot_id, sequence_number,
             {**props, "num-hashes": str(bloom.num_hashes), "num-bits": str(bloom.num_bits)}),
        Blob(FENCE_BLOB, [KEY_FIELD_ID], json.dumps(fences).encode(), snapshot_id, sequence_number, props),
    ]
    return write_puffin(meta.index_file, blobs)


class RunReader:
    def __init__(self, meta: RunMeta, bloom: BloomFilter, fences: List[str]):
        self.meta = meta
        self.bloom = bloom
        self.fences = fences
        self._pf = None

    @classmethod
    def load(cls, meta: RunMeta, row_group_size: int, bloom_fpr: float) -> "RunReader":
        """Load the run's Puffin index; rebuild it from the Parquet file if it is missing."""
        try:
            blobs = read_puffin(meta.index_file)
            bloom = BloomFilter.from_bytes(blobs[BLOOM_BLOB][1])
            fences = json.loads(blobs[FENCE_BLOB][1])
        except (FileNotFoundError, KeyError, ValueError):
            pf = pq.ParquetFile(meta.data_file)
            keys = []
            fences = []
            for i in range(pf.num_row_groups):
                rg = pf.read_row_group(i, columns=["key"]).column("key").to_pylist()
                fences.append(rg[0])
                keys.extend(rg)
            bloom = BloomFilter.for_capacity(len(keys), bloom_fpr)
            for k in keys:
                bloom.add(k)
            write_index(meta, bloom, fences, -1, -1)
        return cls(meta, bloom, fences)

    def _parquet(self) -> pq.ParquetFile:
        if self._pf is None:
            self._pf = pq.ParquetFile(self.meta.data_file)
        return self._pf

    def _block(self, rg: int, cache: BlockCache, stats: Optional[LookupStats]):
        ck = (self.meta.run_id, rg)
        blk = cache.get(ck)
        if blk is not None:
            if stats:
                stats.block_cache_hits += 1
            return blk
        t = self._parquet().read_row_group(rg)
        blk = (t.column("key").to_pylist(), t.column("seq").to_pylist(),
               t.column("op").to_pylist(), t.column("value").to_pylist())
        cache.put(ck, blk)
        if stats:
            stats.blocks_read += 1
        return blk

    def get(self, key: str, cache: BlockCache, stats: LookupStats, trace: Optional[list] = None):
        m = self.meta
        name = f"run {m.run_id} (L{m.level})"
        stats.runs_checked += 1
        if key < m.min_key or key > m.max_key:
            stats.range_pruned += 1
            if trace is not None:
                trace.append(f"{name}: key outside [{m.min_key}, {m.max_key}] -> skip")
            return None
        if not self.bloom.might_contain(key):
            stats.bloom_pruned += 1
            if trace is not None:
                trace.append(f"{name}: bloom filter says NO -> skip")
            return None
        rg = bisect_right(self.fences, key) - 1
        keys, seqs, ops, vals = self._block(rg, cache, stats)
        i = bisect_left(keys, key)
        if i < len(keys) and keys[i] == key:
            if trace is not None:
                trace.append(f"{name}: bloom MAYBE, fence pointer -> row group {rg}, found seq={seqs[i]}")
            return seqs[i], ops[i], vals[i]
        stats.bloom_false_positives += 1
        if trace is not None:
            trace.append(f"{name}: bloom MAYBE, fence pointer -> row group {rg}, not there (false positive)")
        return None

    def iter_range(self, lo: Optional[str], hi: Optional[str], cache: BlockCache) -> Iterator[tuple]:
        """Yield (key, -seq, op, value) for lo <= key <= hi, in key order."""
        m = self.meta
        if (lo is not None and lo > m.max_key) or (hi is not None and hi < m.min_key):
            return
        start = 0 if lo is None else max(0, bisect_right(self.fences, lo) - 1)
        end = len(self.fences) - 1 if hi is None else bisect_right(self.fences, hi) - 1
        for rg in range(start, end + 1):
            keys, seqs, ops, vals = self._block(rg, cache, None)
            i = 0 if lo is None else bisect_left(keys, lo)
            while i < len(keys):
                if hi is not None and keys[i] > hi:
                    return
                yield keys[i], -seqs[i], ops[i], vals[i]
                i += 1
