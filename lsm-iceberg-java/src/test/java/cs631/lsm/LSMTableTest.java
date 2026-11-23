package cs631.lsm;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LSMTableTest {
  @TempDir Path tmp;
  Path warehouse;
  LSMConfig cfg;

  @BeforeEach
  void setUp() {
    warehouse = tmp.resolve("ware house"); // path with a space on purpose
    cfg = new LSMConfig().memtableMaxEntries(300).blockBytes(2048);
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

  static void assertMatches(Map<String, String> model, LSMTable t, int keyspace) {
    for (int i = 0; i < keyspace; i++) {
      String k = String.format("user%06d", i);
      assertEquals(model.get(k), t.get(k), k);
    }
    assertEquals(new TreeMap<>(model), new TreeMap<>(t.scanToMap(null, null)));
    Map<String, String> expected = new TreeMap<>();
    model.forEach((k, v) -> {
      if (k.compareTo("user000100") >= 0 && k.compareTo("user000200") <= 0) expected.put(k, v);
    });
    Map<String, String> got = t.scanToMap("user000100", "user000200");
    assertEquals(expected, new TreeMap<>(got));
    assertEquals(List.copyOf(expected.keySet()), List.copyOf(got.keySet()), "scan must be in key order");
  }

  /** Runs of every level below L0 must be sorted by key and must not overlap. */
  static void assertLevelsDontOverlap(LSMTable t) {
    t.levels().forEach((lvl, runs) -> {
      if (lvl == 0) return;
      for (int i = 1; i < runs.size(); i++) {
        assertTrue(runs.get(i - 1).meta().maxKey().compareTo(runs.get(i).meta().minKey()) < 0,
            "L" + lvl + " runs overlap: " + runs.get(i - 1).meta() + " / " + runs.get(i).meta());
      }
    });
  }

  @ParameterizedTest(name = "{0} + {1} index, compaction={2}")
  @CsvSource({"AVRO,HASH,true", "AVRO,HASH,false", "PARQUET,FENCE,true", "PARQUET,HASH,true", "AVRO,FENCE,false"})
  void matchesModelForEveryFormatAndIndex(LSMConfig.Format format, LSMConfig.LookupIndex index, boolean compaction) {
    cfg.fileFormat(format).lookupIndex(index).autoCompact(compaction);
    try (LSMTable t = open()) {
      Map<String, String> model = randomWorkload(t, 4000, 800, 7);
      if (compaction) {
        assertFalse(t.levels().getOrDefault(1, List.of()).isEmpty(), t.levelSummary());
        assertTrue(t.levels().get(0).size() < cfg.l0CompactionTrigger, t.levelSummary());
      } else {
        assertTrue(t.levels().get(0).size() >= 10, t.levelSummary());
      }
      assertLevelsDontOverlap(t);
      assertMatches(model, t, 800);
      for (SortedRun r : t.levels().values().stream().flatMap(List::stream).toList()) {
        assertEquals(format.name(), r.meta().format());
        assertTrue(r.meta().dataFile().endsWith(RunFiles.extension(format)));
      }
    }
  }

  @Test
  void eachFlushIsAnIcebergSnapshotWithAPuffinIndex() {
    cfg.autoCompact(false);
    try (LSMTable t = open()) {
      randomWorkload(t, 1500, 800, 7);
      int runs = t.runCount();
      List<Snapshot> snaps = new ArrayList<>();
      t.table().snapshots().forEach(snaps::add);
      assertEquals(runs, snaps.size());
      assertEquals("flush", snaps.get(snaps.size() - 1).summary().get("lsm.operation"));
      List<StatisticsFile> stats = t.table().statisticsFiles();
      assertEquals(runs, stats.size());
      for (StatisticsFile s : stats) {
        assertTrue(Files.exists(Catalogs.localPath(s.path())), s.path());
        assertEquals(List.of(SortedRun.BLOOM_BLOB, SortedRun.KEY_INDEX_BLOB, SortedRun.BLOCK_DIR_BLOB),
            s.blobMetadata().stream().map(b -> b.type()).toList());
      }
    }
  }

  @Test
  void compactionKeepsNewestVersionsAndDropsTombstones() throws Exception {
    cfg.autoCompact(false);
    try (LSMTable t = open()) {
      Map<String, String> model = randomWorkload(t, 3000, 800, 11);
      t.flush();
      int before = t.runCount();
      long rowsBefore = t.levels().values().stream().flatMap(List::stream).mapToLong(r -> r.meta().recordCount()).sum();
      List<LSMTable.Compaction> done = t.compact(true);
      assertEquals(1, done.size());
      LSMTable.Compaction c = done.get(0);
      assertEquals(before, c.inputRuns().size());
      assertTrue(c.versionsDropped() > 0 && c.tombstonesDropped() > 0, c.toString());
      assertEquals(rowsBefore, c.entriesOut() + c.versionsDropped() + c.tombstonesDropped());

      assertTrue(t.levels().get(0).isEmpty());
      assertLevelsDontOverlap(t);
      assertEquals(model.size(), t.levelEntries(1), "one row per live key, no tombstones at the bottom");
      assertMatches(model, t, 800);

      Snapshot s = t.table().currentSnapshot();
      assertEquals("replace", s.operation());
      assertEquals("compaction", s.summary().get("lsm.operation"));
      assertEquals(model, icebergResolvedView(t.table()));
      long liveFiles = 0, liveRows = 0;
      try (CloseableIterable<FileScanTask> tasks = t.table().newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          liveFiles++;
          liveRows += task.file().recordCount();
          assertEquals(FileFormat.AVRO, task.file().format());
        }
      }
      assertEquals(t.runCount(), liveFiles);
      assertEquals(model.size(), liveRows);
    }
  }

  @Test
  void leveledCompactionCascadesAndSurvivesRestart() {
    cfg.memtableMaxEntries(100).l0CompactionTrigger(2).levelSizeRatio(3);
    cfg.level1MaxEntries = 300;
    cfg.maxLevels = 4;
    Map<String, String> model;
    try (LSMTable t = open()) {
      model = randomWorkload(t, 6000, 2000, 5);
      assertTrue(t.levels().keySet().stream().anyMatch(l -> l >= 2 && !t.levels().get(l).isEmpty()), t.levelSummary());
      assertLevelsDontOverlap(t);
      assertMatches(model, t, 2000);
      t.flush();
    }
    try (LSMTable t2 = open()) { // after a restart, merges must find the Iceberg DataFiles of old runs
      assertMatches(model, t2, 2000);
      Random rng = new Random(9);
      for (int i = 0; i < 3000; i++) {
        String k = String.format("user%06d", rng.nextInt(2000));
        t2.put(k, "w" + i);
        model.put(k, "w" + i);
      }
      assertTrue(t2.writeStats().compactions > 0);
      assertLevelsDontOverlap(t2);
      assertMatches(model, t2, 2000);
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
  void compactedLevelsLetPlainIcebergPruneFiles() throws Exception {
    try (LSMTable t = open()) {
      randomWorkload(t, 4000, 800, 3);
      t.flush();
      t.compact(true);
      assertTrue(t.runCount() >= 2, t.levelSummary());
      // Our Avro files carry key bounds, and merged runs don't overlap, so a normal Iceberg scan
      // (what Spark does) plans at most one file for a key.
      try (CloseableIterable<FileScanTask> tasks = t.table().newScan().filter(Expressions.equal("key", "user000123")).planFiles()) {
        int n = 0;
        for (FileScanTask ignored : tasks) n++;
        assertEquals(1, n);
      }
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

  @ParameterizedTest
  @CsvSource({"AVRO", "PARQUET"})
  void missingIndexIsRebuilt(LSMConfig.Format format) throws Exception {
    cfg.fileFormat(format);
    Map<String, String> model;
    List<RunMeta> runs;
    try (LSMTable t = open()) {
      model = randomWorkload(t, 2000, 800, 7);
      t.flush();
      runs = t.levels().values().stream().flatMap(List::stream).map(SortedRun::meta).toList();
    }
    for (RunMeta m : runs) Files.deleteIfExists(Catalogs.localPath(m.indexFile()));
    try (LSMTable t2 = open()) {
      assertMatches(model, t2, 800);
      for (RunMeta m : runs) assertTrue(Files.exists(Catalogs.localPath(SortedRun.rebuiltIndexPath(m))));
    }
  }

  @Test
  void keyIndexAvoidsBlockReadsForAbsentKeys() {
    cfg.autoCompact(false);
    try (LSMTable t = open()) {
      for (int i = 0; i < 3000; i++) t.put(String.format("key%06d", (i * 7919) % 100000), "x");
      t.flush();
      t.stats().reset();
      for (int i = 0; i < 500; i++) {
        t.get("absent" + i); // outside every run's key range
        t.get(String.format("key%06dx", i * 2 + 1)); // inside ranges, never written
      }
      LookupStats s = t.stats();
      assertEquals(s.runsChecked, s.rangePruned + s.bloomPruned + s.indexPruned, s.toString());
      assertEquals(0, s.blocksRead, "absent keys must not read any block: " + s);
    }
  }

  @Test
  void icebergBaselinesGiveSameAnswers() {
    Catalog cat = Catalogs.local(warehouse);
    try (LSMTable t = open()) {
      List<KvStore> baselines = List.of(new VanillaIcebergTable(cat, "t.append", 300),
          new IcebergMorTable(cat, "t.mor", 300), new IcebergCowTable(cat, "t.cow", 300, 200));
      Random rng = new Random(3);
      for (int i = 0; i < 2000; i++) {
        String k = String.format("user%06d", rng.nextInt(500));
        boolean del = rng.nextDouble() < 0.1;
        if (del) t.delete(k); else t.put(k, "v" + i);
        for (KvStore b : baselines) {
          if (del) b.delete(k); else b.put(k, "v" + i);
        }
      }
      t.flush();
      for (KvStore b : baselines) {
        b.flush();
        for (int i = 0; i < 500; i += 7) {
          String k = String.format("user%06d", i);
          assertEquals(t.get(k), b.get(k), b.getClass().getSimpleName() + " " + k);
        }
        assertEquals(new TreeMap<>(t.scanToMap("user000050", "user000090")), b.scan("user000050", "user000090"),
            b.getClass().getSimpleName());
      }
      assertTrue(baselines.get(2).dataFiles() >= 2, "copy-on-write keeps several sorted files");
    }
  }
}
