package cs631.lsm;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory write buffer: a skip list ordered by key. Each key keeps only its newest version.
 * Deletes are stored as tombstones so they can shadow older versions already on disk.
 */
public class MemTable {
  private final ConcurrentSkipListMap<String, Entry> map = new ConcurrentSkipListMap<>();
  private long bytes = 0;
  private final long createdNanos = System.nanoTime();

  public void put(String key, long seq, byte op, String value) {
    Entry old = map.put(key, new Entry(seq, op, value));
    if (old == null) {
      bytes += utf8Len(key) + utf8Len(value) + 16;
    } else {
      bytes += utf8Len(value) - utf8Len(old.value());
    }
  }

  public Entry get(String key) {
    return map.get(key);
  }

  public int size() {
    return map.size();
  }

  public long sizeBytes() {
    return bytes;
  }

  public double ageSeconds() {
    return (System.nanoTime() - createdNanos) / 1e9;
  }

  /** Entries with lo <= key <= hi (null = unbounded), in key order. */
  public NavigableMap<String, Entry> range(String lo, String hi) {
    if (lo == null && hi == null) return map;
    if (lo == null) return map.headMap(hi, true);
    if (hi == null) return map.tailMap(lo, true);
    if (lo.compareTo(hi) > 0) return new ConcurrentSkipListMap<>();
    return map.subMap(lo, true, hi, true);
  }

  public Iterable<Map.Entry<String, Entry>> entries() {
    return map.entrySet();
  }

  private static int utf8Len(String s) {
    return s == null ? 0 : s.getBytes(StandardCharsets.UTF_8).length;
  }
}
