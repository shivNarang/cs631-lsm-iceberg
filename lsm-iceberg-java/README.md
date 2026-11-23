# LSM Tree Indexing in Apache Iceberg (Java)

Team: Shiv Narang (23b1075), Kaushal Vijayvergiya (23b0901), Kritin Gupta (23b0948)

A key-value table whose writes go through an LSM tree (WAL → MemTable → sorted runs → leveled
merges). Every run is an ordinary Apache Iceberg data file in **Avro**, and its index lives in
Iceberg's **Puffin** format: a bloom filter, an exact **key index** (key → block, row) and a block
directory. Point lookups read one block from one file; plain Iceberg has to open every file.
Spark reads the same table through the standard Iceberg runtime.

Built on the reference Apache Iceberg Java library (1.10, unmodified), Avro 1.12, Parquet 1.16
and a local Hadoop filesystem catalog. Requires Java 17+ and Maven (`brew install maven`).

## Run

```bash
./run.sh test                      # 19 JUnit tests
./run.sh demo                      # write path, traced lookups, range scan, merge, crash recovery
./run.sh inspect demo.kv warehouse/demo 3
                                   # Iceberg metadata tree: snapshots → manifests → data files → Puffin blobs
./run.sh bench                     # quick comparison: LSM vs Iceberg append / merge-on-read / copy-on-write (~2 min)
./run.sh eval                      # full evaluation, 7 tables, 100k keys + 400k updates (~15 min) → results/evaluation.json
./run.sh usecase                   # customer-profile store fed by a CDC stream (builds warehouse/usecase)
./run.sh spark                     # Spark SQL on that table: metadata, analytics, pruning, time travel
```

`./run.sh spark` downloads Spark 3.5 the first time (about 300 MB) and runs it in local mode.

## Code (`src/main/java/cs631/lsm`)

| Class | Role |
|---|---|
| `LSMTable` | Write path, flush, leveled merges (compaction), read path, range scans, crash recovery |
| `MemTable` | Skip list (`ConcurrentSkipListMap`), newest version per key, tombstones for deletes |
| `WriteAheadLog` | Length + CRC32 framed records, replay stops at a torn or corrupt tail |
| `RunFiles` | Writes a run as Avro (blocks we end ourselves, so we know each block's byte offset) or Parquet; reads exactly one block |
| `SortedRun` | One run + its Puffin index; point lookup via key index (or fence pointers), range iterator |
| `KeyIndex` | Open-addressing hash table key → (block, row), 12 bytes per slot, stored as a Puffin blob |
| `BloomFilter` | Double hashing, 1% false positives; Puffin blob |
| `MergingIterator` | K-way merge: newest version per key; used by scans and by merges |
| `BlockCache` | LRU cache of decoded blocks |
| `LSMManifest`, `RunMeta` | Versioned LSM level manifest (JSON), pointed to by table property `lsm.manifest-location` |
| `VanillaIcebergTable`, `IcebergMorTable`, `IcebergCowTable` | Baselines: Iceberg append, merge-on-read (equality deletes), copy-on-write |
| `tools/*` | Demo, metadata inspector, benchmark/evaluation, CDC use case |
| `spark/` | Separate Maven module: Spark SQL job over the use-case table |

## One flush

1. MemTable entries (already sorted) → `L0-run-NNNNNN.avro`. Blocks of ~64 KB; we record the byte
   offset where each block starts.
2. New LSM manifest `metadata/lsm/lsm-manifest-NNNNN.json` adds the run to level 0.
3. **One Iceberg transaction:** `newAppend()` adds the data file (snapshot summary tagged
   `lsm.operation=flush`) **and** `updateProperties()` moves `lsm.manifest-location`.
4. Puffin file with three blobs (`cs631-lsm-bloom-filter-v1`, `cs631-lsm-key-index-v1`,
   `cs631-lsm-block-directory-v1`), registered on the snapshot with `updateStatistics()`.
5. WAL truncated. If the process crashes before this, replay skips records with `seq <= lastSeq`.

## One merge (compaction)

Leveled, like RocksDB: L0 runs overlap; every deeper level holds sorted runs with disjoint key
ranges, and each level may hold 10× more entries than the one above it.

1. Trigger: L0 has 4 runs, or a level is over its size limit (then one run moves down, round-robin).
2. K-way merge of the input runs and the overlapping runs of the next level. Only the newest
   version of each key is kept; delete markers are dropped when no deeper level can hold the key.
3. Output split into runs of `targetRunEntries`, each with its own index.
4. **One Iceberg transaction:** `newRewrite()` removes the input files and adds the outputs
   (operation `replace`, summary `lsm.operation=compaction`) and moves the manifest pointer.
   Old files stay readable through earlier snapshots (time travel) until snapshots are expired.

## Limitations

* One writer at a time; merges run inline after a flush (no background thread).
* Local filesystem only in the tests (the Avro reader goes through Iceberg's FileIO, so object stores should work but were not tried).
* Snapshot expiry is not wired in. Iceberg would delete the Puffin files of expired snapshots; our tables rebuild a missing index on open, which is correct but slow.
* Spark reads the table but does not use our Puffin index yet; it benefits from merged levels through ordinary min/max pruning.
