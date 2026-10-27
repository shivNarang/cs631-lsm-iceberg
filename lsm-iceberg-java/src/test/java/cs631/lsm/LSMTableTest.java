package cs631.lsm;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LSMTableTest {
  @TempDir Path tmp;
  Path warehouse;
  LSMConfig cfg;

  @BeforeEach
  void setUp() {
    warehouse = tmp.resolve("ware house"); // path with a space on purpose
    cfg = new LSMConfig().memtableMaxEntries(300).rowGroupBytes(2048);
  }

  LSMTable open() {
    return new LSMTable(Catalogs.local(warehouse), "t.kv", cfg);
  }

  static Map<String, String> randomWorkload(LSMTable t, int n, int keyspace, long seed) {
    Random rng = new Random(seed);
    Map<String, String> model = new HashMap<>();
    for (int i = 0; i < n; i++) {
      String k = String.format("user%06d", rng.nextInt(keyspace));
      if (rng.nextDouble() < 0.15) {
        t.delete(k);
        model.remove(k);
      } else {
        t.put(k, "v" + i);
        model.put(k, "v" + i);
      }
    }
    return model;
  }

  /** What any Iceberg engine sees after resolving "latest seq wins". */
  static Map<String, String> icebergResolvedView(Table table) throws Exception {
    Map<String, Record> latest = new HashMap<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      for (Record r : rows) {
        String k = r.getField("key").toString();
        Record cur = latest.get(k);
        if (cur == null || (long) r.getField("seq") > (long) cur.getField("seq")) latest.put(k, r.copy());
      }
    }
    Map<String, String> out = new HashMap<>();
    latest.forEach((k, r) -> {
      if ((int) r.getField("op") == Entry.PUT) out.put(k, r.getField("value").toString());
    });
    return out;
  }

  @Test
  void matchesModelAcrossManyFlushes() {
    try (LSMTable t = open()) {
      Map<String, String> model = randomWorkload(t, 3000, 800, 7);
      assertTrue(t.levels().get(0).size() >= 5, "runs: " + t.runCount());
      for (int i = 0; i < 800; i++) {
        String k = String.format("user%06d", i);
        assertEquals(model.get(k), t.get(k), k);
      }
      assertEquals(new TreeMap<>(model), new TreeMap<>(t.scanToMap(null, null)));
      Map<String, String> expected = new TreeMap<>();
      model.forEach((k, v) -> {
        if (k.compareTo("user000100") >= 0 && k.compareTo("user000200") <= 0) expected.put(k, v);
      });
      assertEquals(expected, new TreeMap<>(t.scanToMap("user000100", "user000200")));
      assertEquals(List.copyOf(expected.keySet()), List.copyOf(t.scanToMap("user000100", "user000200").keySet()),
          "scan must be in key order");
    }
  }

  @Test
  void eachFlushIsAnIcebergSnapshotWithAPuffinIndex() {
    try (LSMTable t = open()) {
      randomWorkload(t, 1500, 800, 7);
      int runs = t.runCount();
      List<Snapshot> snaps = new java.util.ArrayList<>();
      t.table().snapshots().forEach(snaps::add);
      assertEquals(runs, snaps.size());
      assertEquals("flush", snaps.get(snaps.size() - 1).summary().get("lsm.operation"));
      List<StatisticsFile> stats = t.table().statisticsFiles();
      assertEquals(runs, stats.size());
      for (StatisticsFile s : stats) {
        assertTrue(Files.exists(Catalogs.localPath(s.path())), s.path());
        assertEquals(2, s.blobMetadata().size());
        assertEquals(SortedRun.BLOOM_BLOB, s.blobMetadata().get(0).type());
      }
    }
  }

  @Test
  void icebergReadersSeeSameData() throws Exception {
    try (LSMTable t = open()) {
      Map<String, String> model = randomWorkload(t, 3000, 800, 7);
      t.flush();
      assertEquals(model, icebergResolvedView(t.table()));
    }
  }

  @Test
  void crashRecoveryFromWal() {
    LSMTable t = open();
    Map<String, String> model = randomWorkload(t, 1000, 800, 7);
    int unflushed = t.memtable().size();
    assertTrue(unflushed > 0);
    // simulate a crash: no flush(), no close()
    try (LSMTable t2 = open()) {
      assertTrue(t2.recovered() >= unflushed);
      assertEquals(unflushed, t2.memtable().size());
      assertEquals(new TreeMap<>(model), new TreeMap<>(t2.scanToMap(null, null)));
      t2.put("after", "restart");
      assertEquals("restart", t2.get("after"));
      assertTrue(t2.lastSeq() > t.lastSeq());
    }
  }

  @Test
  void missingIndexIsRebuilt() throws Exception {
    Map<String, String> model;
    String indexFile;
    try (LSMTable t = open()) {
      model = randomWorkload(t, 1000, 800, 7);
      t.flush();
      indexFile = t.levels().get(0).get(0).meta().indexFile();
    }
    Files.delete(Catalogs.localPath(indexFile));
    try (LSMTable t2 = open()) {
      assertEquals(new TreeMap<>(model), new TreeMap<>(t2.scanToMap(null, null)));
      assertTrue(Files.exists(Catalogs.localPath(indexFile)));
    }
  }

  @Test
  void bloomFiltersAndKeyRangesPruneRuns() {
    try (LSMTable t = open()) {
      for (int i = 0; i < 3000; i++) t.put(String.format("key%06d", (i * 7919) % 100000), "x");
      t.flush();
      t.stats().reset();
      for (int i = 0; i < 500; i++) {
        t.get("absent" + i); // outside every run's key range
        t.get(String.format("key%06dx", i * 2 + 1)); // inside ranges, never written
      }
      LookupStats s = t.stats();
      assertTrue(s.rangePruned + s.bloomPruned > 0.9 * s.runsChecked, s.toString());
    }
  }

  @Test
  void vanillaBaselineGivesSameAnswers() {
    Catalog cat = Catalogs.local(warehouse);
    try (LSMTable t = open()) {
      VanillaIcebergTable v = new VanillaIcebergTable(cat, "t.vanilla", 300);
      Random rng = new Random(3);
      for (int i = 0; i < 2000; i++) {
        String k = String.format("user%06d", rng.nextInt(500));
        if (rng.nextDouble() < 0.1) {
          t.delete(k);
          v.delete(k);
        } else {
          t.put(k, "v" + i);
          v.put(k, "v" + i);
        }
      }
      t.flush();
      v.flush();
      for (int i = 0; i < 500; i += 7) {
        String k = String.format("user%06d", i);
        assertEquals(t.get(k), v.get(k), k);
      }
      assertEquals(new TreeMap<>(t.scanToMap("user000050", "user000090")), v.scan("user000050", "user000090"));
    }
  }
}
