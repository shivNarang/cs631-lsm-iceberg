package cs631.lsm;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * K-way merge of sorted sources (MemTable + runs). Ordered by (key asc, seq desc), so the first
 * version seen for a key is the newest; older versions are skipped and tombstones are dropped.
 */
public class MergingIterator implements Iterator<Map.Entry<String, String>> {
  private record Head(KeyValue kv, Iterator<KeyValue> src) {}

  private static final Comparator<Head> ORDER =
      Comparator.<Head, String>comparing(h -> h.kv().key()).thenComparing(h -> -h.kv().seq());

  private final PriorityQueue<Head> heap = new PriorityQueue<>(ORDER);
  private Map.Entry<String, String> next;

  public MergingIterator(List<Iterator<KeyValue>> sources) {
    for (Iterator<KeyValue> s : sources) push(s);
    next = advance();
  }

  private void push(Iterator<KeyValue> s) {
    if (s.hasNext()) heap.add(new Head(s.next(), s));
  }

  private Map.Entry<String, String> advance() {
    while (!heap.isEmpty()) {
      Head h = heap.poll();
      push(h.src());
      KeyValue newest = h.kv();
      // drop older versions of the same key
      while (!heap.isEmpty() && heap.peek().kv().key().equals(newest.key())) {
        Head old = heap.poll();
        push(old.src());
      }
      if (newest.op() == Entry.PUT) return Map.entry(newest.key(), newest.value());
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
}
