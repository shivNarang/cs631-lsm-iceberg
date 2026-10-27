from dataclasses import dataclass
from typing import Optional


@dataclass
class LSMConfig:
    # Flush triggers: whichever is hit first.
    memtable_max_entries: int = 50_000
    memtable_max_bytes: int = 8 * 1024 * 1024
    flush_interval_s: Optional[float] = None  # time-based flush; None disables it

    # On-disk run (SSTable) layout.
    row_group_size: int = 1024  # rows per Parquet row group = one "block"
    bloom_fpr: float = 0.01     # target false-positive rate of per-run bloom filters

    # Read path.
    block_cache_blocks: int = 256  # decoded row groups kept in memory (LRU)

    # Durability.
    wal_sync: bool = False  # fsync every WAL append (slow, but survives power loss)
