# LSM Tree Indexing in Apache Iceberg (Java)

Team: Shiv Narang (23b1075), Kaushal Vijayvergiya (23b0901), Kritin Gupta (23b0948)

A key-value table whose writes go through an LSM tree (WAL → MemTable → sorted runs). Every
flushed run is an ordinary Apache Iceberg data file, and its Puffin index (bloom filter + fence
pointers) is registered on the run's snapshot as an Iceberg statistics file. Point lookups can
therefore skip files that Iceberg's min/max statistics cannot.

Built on the reference Apache Iceberg Java library (1.10), with Parquet 1.16 and a local Hadoop
filesystem catalog. Requires Java 17+ and Maven (`brew install maven`).

## Run

```bash
./run.sh test                      # 10 JUnit tests
./run.sh demo                      # checkpoint walkthrough (write path, traced lookups, scan, crash recovery)
./run.sh inspect demo.kv warehouse/demo 3
                                   # Iceberg metadata tree: snapshots → manifests → data files → Puffin blobs
./run.sh bench                     # LSM vs vanilla Iceberg, 20k keys + 40k updates
./run.sh bench --cache-blocks 0    # same, with the block cache off
./run.sh bench --keys 100000 --updates 400000 --batch 25000 --cache-blocks 0   # larger run
```

Results are written to `results/*.json`; tables live under `warehouse/`.

## Code (`src/main/java/cs631/lsm`)

| Class | Role |
|---|---|
| `LSMTable` | Write path, flush + atomic Iceberg commit, read path, range scans, crash recovery |
| `MemTable` | Skip list (`ConcurrentSkipListMap`), newest version per key, tombstones for deletes |
| `WriteAheadLog` | Length + CRC32 framed records, replay stops at a torn or corrupt tail |
| `BloomFilter` | Double hashing; serialized into a Puffin blob |
| `SortedRun` | Writes key-sorted Parquet run + Puffin index; point lookup reads exactly one row group |
| `MergingIterator` | K-way merge over MemTable and runs: newest version wins, tombstones dropped |
| `BlockCache` | LRU cache of decoded row groups |
| `LSMManifest`, `RunMeta` | Versioned LSM level manifest (JSON), pointed to by table property `lsm.manifest-location` |
| `VanillaIcebergTable` | Baseline: micro-batch appends, reads via Iceberg scan with min/max pruning |
| `tools/Demo`, `tools/InspectMetadata`, `tools/Benchmark` | Checkpoint demo, metadata inspector, benchmark |

## One flush, step by step

1. MemTable entries (already sorted) → `L0-run-NNNNNN.parquet` through Iceberg's Parquet writer.
2. New LSM manifest `metadata/lsm/lsm-manifest-NNNNN.json`, which adds the run to level 0.
3. **One Iceberg transaction:** `newAppend()` adds the data file (snapshot summary tagged with
   `lsm.operation=flush`, run id, level and key range) **and** `updateProperties()` moves
   `lsm.manifest-location` to the new manifest. Either both happen or neither does.
4. Puffin file `metadata/lsm-index/L0-run-NNNNNN.puffin` (blobs `cs631-lsm-bloom-filter-v1`,
   `cs631-lsm-fence-pointers-v1`) registered on the new snapshot via `updateStatistics()`.
5. WAL truncated. If the process crashes before this, replay skips records with `seq <= lastSeq`.

## Not done yet

* Compaction (all runs stay in L0, so the run count grows with every flush)
* Spark integration, full YCSB/TPC-H evaluation, concurrent writers
