package cs631.lsm;

/** Tunables for the LSM tree. Fields are public so tests/benchmarks can tweak them directly. */
public class LSMConfig {
  /** Data file format of the sorted runs. */
  public enum Format { AVRO, PARQUET }

  /**
   * How a point lookup finds its row inside a run. HASH: the Puffin key index maps the key
   * straight to (block, row). FENCE: binary search over the first key of each block (checkpoint design).
   */
  public enum LookupIndex { HASH, FENCE }

  // ---- write buffer
  /** Flush triggers: whichever is hit first. */
  public int memtableMaxEntries = 50_000;
  public long memtableMaxBytes = 8L * 1024 * 1024;
  /** Time-based flush in seconds; <= 0 disables it. */
  public double flushIntervalSeconds = 0;
  /** fsync every WAL append (survives power loss, much slower). */
  public boolean walSync = false;

  // ---- run files
  public Format fileFormat = Format.AVRO;
  /** Avro block codec: "null" (uncompressed), "deflate", "snappy" or "zstandard". */
  public String avroCodec = "null";
  /** Parquet column codec. */
  public String parquetCodec = "zstd";
  /** Target block size: one Avro block or one Parquet row group. A point lookup reads one block. */
  public long blockBytes = 64 * 1024;

  // ---- index
  public LookupIndex lookupIndex = LookupIndex.HASH;
  /** Target false-positive rate of the per-run bloom filter. */
  public double bloomFpr = 0.01;
  /** Decoded blocks kept in memory (LRU). 0 disables the cache. */
  public int blockCacheBlocks = 256;

  // ---- compaction (leveled)
  /** Merge automatically after a flush when a level is over its limit. */
  public boolean autoCompact = true;
  /** Merge L0 into L1 when L0 has this many runs. */
  public int l0CompactionTrigger = 4;
  /** Size limit of L1 in entries; 0 = l0CompactionTrigger * memtableMaxEntries. Level n+1 holds sizeRatio times more. */
  public long level1MaxEntries = 0;
  public int levelSizeRatio = 10;
  /** Entries per output run of a merge; 0 = memtableMaxEntries. */
  public long targetRunEntries = 0;
  /** Number of levels including L0. Data never moves below the last one. */
  public int maxLevels = 5;

  public LSMConfig memtableMaxEntries(int n) { this.memtableMaxEntries = n; return this; }
  public LSMConfig blockBytes(long n) { this.blockBytes = n; return this; }
  /** Old name of {@link #blockBytes(long)}. */
  public LSMConfig rowGroupBytes(long n) { return blockBytes(n); }
  public LSMConfig blockCacheBlocks(int n) { this.blockCacheBlocks = n; return this; }
  public LSMConfig flushIntervalSeconds(double s) { this.flushIntervalSeconds = s; return this; }
  public LSMConfig fileFormat(Format f) { this.fileFormat = f; return this; }
  public LSMConfig lookupIndex(LookupIndex i) { this.lookupIndex = i; return this; }
  public LSMConfig autoCompact(boolean b) { this.autoCompact = b; return this; }
  public LSMConfig l0CompactionTrigger(int n) { this.l0CompactionTrigger = n; return this; }
  public LSMConfig levelSizeRatio(int n) { this.levelSizeRatio = n; return this; }

  long level1Limit() {
    return level1MaxEntries > 0 ? level1MaxEntries : (long) l0CompactionTrigger * memtableMaxEntries;
  }

  long runTargetEntries() {
    return targetRunEntries > 0 ? targetRunEntries : memtableMaxEntries;
  }

  /** Entry limit of level n (n >= 1). */
  long levelLimit(int level) {
    long lim = level1Limit();
    for (int i = 1; i < level; i++) lim *= levelSizeRatio;
    return lim;
  }
}
