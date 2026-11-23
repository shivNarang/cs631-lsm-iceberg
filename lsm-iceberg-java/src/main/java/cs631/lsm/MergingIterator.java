package cs631.lsm;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * K-way merge of sorted sources (MemTable + runs) for range scans: live (key, value) pairs in key
 * order. Older versions are skipped and tombstones are dropped.
 */
public class MergingIterator implements Iterator<Map.Entry<String, String>> {
  private final Newest versions;
  private Map.Entry<String, String> next;

  public MergingIterator(List<Iterator<KeyValue>> sources) {
    this.versions = new Newest(sources);
    this.next = advance();
  }

  private Map.Entry<String, String> advance() {
    while (versions.hasNext()) {
      KeyValue kv = versions.next();
      if (kv.op() == Entry.PUT) return Map.entry(kv.key(), kv.value());
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    return next != null;
  }

  @Override
  public Map.Entry<String, String> next() {
    if (next == null) throw new NoSuchElementException();
    Map.Entry<String, String> r = next;
    next = advance();
    return r;
  }

  /**
   * The newest version of every key across the sources, tombstones included, in key order. A
   * min-heap ordered by (key asc, seq desc) means the first version seen for a key is the newest.
   * Used by range scans and by merges (compaction). {@link #skipped()} counts older versions dropped.
   */
  public static final class Newest implements Iterator<KeyValue> {
    private record Head(KeyValue kv, Iterator<KeyValue> src) {}

    private static final Comparator<Head> ORDER =
        Comparator.<Head, String>comparing(h -> h.kv().key()).thenComparing(h -> -h.kv().seq());

    private final PriorityQueue<Head> heap = new PriorityQueue<>(ORDER);
    private long skipped;

    public Newest(List<Iterator<KeyValue>> sources) {
      for (Iterator<KeyValue> s : sources) push(s);
    }

    private void push(Iterator<KeyValue> s) {
      if (s.hasNext()) heap.add(new Head(s.next(), s));
    }

    @Override
    public boolean hasNext() {
      return !heap.isEmpty();
    }

    @Override
    public KeyValue next() {
      if (heap.isEmpty()) throw new NoSuchElementException();
      Head h = heap.poll();
      push(h.src());
      KeyValue newest = h.kv();
      while (!heap.isEmpty() && heap.peek().kv().key().equals(newest.key())) {
        Head old = heap.poll();
        push(old.src());
        skipped++;
      }
      return newest;
    }

    public long skipped() {
      return skipped;
    }
  }
}
