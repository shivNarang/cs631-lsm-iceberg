package cs631.lsm;

/** Tunables for the LSM tree. Fields are public so tests/benchmarks can tweak them directly. */
public class LSMConfig {
  /** Flush triggers: whichever is hit first. */
  public int memtableMaxEntries = 50_000;
  public long memtableMaxBytes = 8L * 1024 * 1024;
  /** Time-based flush in seconds; <= 0 disables it. */
  public double flushIntervalSeconds = 0;

  /** Target Parquet row-group size. One row group = one "block" read by a point lookup. */
  public long rowGroupBytes = 64 * 1024;
  /** Target false-positive rate of the per-run bloom filter. */
  public double bloomFpr = 0.01;

  /** Decoded row groups kept in memory (LRU). 0 disables the cache. */
  public int blockCacheBlocks = 256;

  /** fsync every WAL append (survives power loss, much slower). */
  public boolean walSync = false;

  public LSMConfig memtableMaxEntries(int n) { this.memtableMaxEntries = n; return this; }
  public LSMConfig rowGroupBytes(long n) { this.rowGroupBytes = n; return this; }
  public LSMConfig blockCacheBlocks(int n) { this.blockCacheBlocks = n; return this; }
  public LSMConfig flushIntervalSeconds(double s) { this.flushIntervalSeconds = s; return this; }
}
