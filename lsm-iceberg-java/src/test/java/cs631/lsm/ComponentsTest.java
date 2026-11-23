package cs631.lsm;

import static org.junit.jupiter.api.Assertions.*;

import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ComponentsTest {
  @TempDir Path tmp;

  @Test
  void memtableKeepsKeysSortedAndNewestVersion() {
    MemTable mt = new MemTable();
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 2000; i++) keys.add(String.format("k%05d", i));
    Collections.shuffle(keys, new Random(2));
    long s = 0;
    for (String k : keys) mt.put(k, ++s, Entry.PUT, "v" + k);
    mt.put("k00010", 9999, Entry.DELETE, null);
    assertEquals(2000, mt.size());
    List<String> out = new ArrayList<>(mt.range(null, null).keySet());
    Collections.sort(keys);
    assertEquals(keys, out);
    assertEquals(new Entry(9999, Entry.DELETE, null), mt.get("k00010"));
    assertNull(mt.get("nope"));
    assertEquals(List.of("k00100", "k00101", "k00102", "k00103"), new ArrayList<>(mt.range("k00100", "k00103").keySet()));
  }

  @Test
  void bloomFilterHasNoFalseNegativesAndLowFpr() {
    BloomFilter bf = BloomFilter.forCapacity(10_000, 0.01);
    for (int i = 0; i < 10_000; i++) bf.add("in" + i);
    for (int i = 0; i < 10_000; i++) assertTrue(bf.mightContain("in" + i));
    int fp = 0;
    for (int i = 0; i < 10_000; i++) if (bf.mightContain("out" + i)) fp++;
    assertTrue(fp / 10_000.0 < 0.03, "fpr " + fp / 10_000.0);
    BloomFilter copy = BloomFilter.fromBytes(bf.toBytes());
    assertTrue(copy.mightContain("in42"));
    assertEquals(bf.numBits(), copy.numBits());
  }

  @Test
  void walReplayStopsAtTornTail() throws Exception {
    Path p = tmp.resolve("w/x.wal");
    try (WriteAheadLog w = new WriteAheadLog(p, false)) {
      w.append(1, Entry.PUT, "a", "1");
      w.append(2, Entry.DELETE, "a", null);
    }
    try (FileOutputStream f = new FileOutputStream(p.toFile(), true)) {
      f.write(new byte[] {0, 0, 0, 40, 1, 2}); // crash in the middle of a record
    }
    assertEquals(List.of(new KeyValue("a", 1, Entry.PUT, "1"), new KeyValue("a", 2, Entry.DELETE, null)),
        WriteAheadLog.replay(p));
  }

  @Test
  void keyIndexFindsEveryKeyAndRejectsMostAbsentOnes() {
    int n = 20_000;
    String[] keys = new String[n];
    int[] blockOf = new int[n];
    int[] rowOf = new int[n];
    for (int i = 0; i < n; i++) {
      keys[i] = String.format("user%08d", i * 3);
      blockOf[i] = i / 100;
      rowOf[i] = i % 100;
    }
    KeyIndex idx = KeyIndex.fromBytes(KeyIndex.build(keys, blockOf, rowOf).toBytes());
    for (int i = 0; i < n; i++) {
      int want = i;
      assertTrue(idx.find(keys[i], (b, r) -> b == blockOf[want] && r == rowOf[want]), keys[i]);
    }
    int falseHits = 0;
    for (int i = 0; i < n; i++) if (idx.mayContain(String.format("user%08dx", i))) falseHits++;
    assertTrue(falseHits < 5, "fingerprint collisions: " + falseHits);
  }
}
