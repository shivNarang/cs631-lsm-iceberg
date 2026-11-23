package cs631.lsm;

import java.util.LinkedHashMap;
import java.util.Map;

/** LRU cache of decoded blocks (an Avro block or a Parquet row group), keyed by (runId, block). */
public class BlockCache {
  /** A decoded block: parallel arrays sorted by key. */
  public record Block(String[] keys, long[] seqs, byte[] ops, String[] values) {}

  private record Key(long runId, int rowGroup) {}

  private final int capacity;
  private final LinkedHashMap<Key, Block> map;

  public BlockCache(int capacity) {
    this.capacity = capacity;
    this.map = new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<Key, Block> e) {
        return size() > BlockCache.this.capacity;
      }
    };
  }

  public Block get(long runId, int rowGroup) {
    return capacity <= 0 ? null : map.get(new Key(runId, rowGroup));
  }

  public void put(long runId, int rowGroup, Block b) {
    if (capacity > 0) map.put(new Key(runId, rowGroup), b);
  }

  public void evictRun(long runId) {
    map.keySet().removeIf(k -> k.runId() == runId);
  }
}
