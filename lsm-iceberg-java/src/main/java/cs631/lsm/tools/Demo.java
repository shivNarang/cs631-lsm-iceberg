package cs631.lsm.tools;

import cs631.lsm.Catalogs;
import cs631.lsm.LSMConfig;
import cs631.lsm.LSMTable;
import cs631.lsm.RunMeta;
import cs631.lsm.SortedRun;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Snapshot;

/** Demo: write path, read path with a trace of every decision, range scan, merge, crash recovery. */
public class Demo {
  static void step(String title) {
    System.out.println("\n" + "=".repeat(72) + "\n" + title + "\n" + "=".repeat(72));
  }

  static void showGet(LSMTable t, String key) {
    List<String> trace = new ArrayList<>();
    String v = t.get(key, trace);
    System.out.println("get(\"" + key + "\") = " + (v == null ? "null" : "\"" + v + "\""));
    trace.forEach(line -> System.out.println("    " + line));
  }

  static void showRuns(LSMTable t) {
    t.levels().values().stream().flatMap(List::stream).map(SortedRun::meta)
        .sorted(Comparator.comparingInt(RunMeta::level).thenComparing(RunMeta::minKey))
        .forEach(m -> System.out.printf("  L%d run %d: %s, %,d rows, keys [%s .. %s], %d blocks, %,d bytes%n",
            m.level(), m.runId(), m.format(), m.recordCount(), m.minKey(), m.maxKey(), m.blocks(), m.fileSize()));
  }

  public static void main(String[] args) throws Exception {
    Path warehouse = Path.of(args.length > 0 ? args[0] : "warehouse/demo").toAbsolutePath();
    Tools.deleteRecursively(warehouse);
    // Small buffer so the demo makes several runs. Merges are triggered by hand in step 5.
    LSMConfig cfg = new LSMConfig().memtableMaxEntries(1000).blockBytes(4 * 1024).autoCompact(false);
    LSMTable t = new LSMTable(Catalogs.local(warehouse), "demo.kv", cfg);

    step("1. WRITE PATH: 2,500 puts -> WAL -> MemTable; flush every 1,000 keys to an Avro run");
    for (int i = 0; i < 2500; i++) t.put(String.format("user%05d", (i * 37) % 2500), "profile-v1-" + i);
    showRuns(t);
    System.out.println("  MemTable still holds " + t.memtable().size() + " un-flushed entries (protected by the WAL)");
    System.out.println("  Iceberg snapshots: " + Tools.count(t.table().snapshots())
        + ", Puffin index files: " + t.table().statisticsFiles().size()
        + " (each: bloom filter + key index + block directory)");
    System.out.println("  Note: every run's key range covers almost the whole key space, so Iceberg's");
    System.out.println("  min/max statistics alone cannot skip any of them for a point lookup.");

    step("2. UPDATES + DELETES (newer versions shadow older ones)");
    t.put("user00042", "profile-v2-UPDATED");
    t.delete("user00007");
    t.flush();
    t.put("user00099", "profile-v3-in-memtable");
    System.out.println("  put user00042 (flushed), delete user00007 (flushed tombstone), put user00099 (memtable only)");
    System.out.println("  " + t.levelSummary());

    step("3. READ PATH: point lookups with a trace of every decision");
    showGet(t, "user00099");   // memtable hit
    showGet(t, "user00042");   // newest run
    showGet(t, "user00007");   // tombstone
    showGet(t, "user01234");   // older run; bloom filter skips the newer one
    showGet(t, "user99999");   // outside every run's key range
    showGet(t, "user00042a");  // inside the ranges, bloom filters say no
    System.out.println("\nLookup stats for the 6 lookups above: " + t.stats());

    step("4. RANGE SCAN: k-way merge over MemTable + all runs");
    t.scan("user00040", "user00045").forEachRemaining(e -> System.out.println("  " + e.getKey() + " -> " + e.getValue()));

    step("5. MERGE (compaction): L0 runs -> sorted, non-overlapping L1 runs");
    System.out.println("  before: " + t.levelSummary());
    for (LSMTable.Compaction c : t.compact(true)) {
      System.out.printf("  merged L%d -> L%d: runs %s in, runs %s out%n", c.fromLevel(), c.toLevel(), c.inputRuns(), c.outputRuns());
      System.out.printf("  %,d entries in, %,d out: %,d older versions and %,d tombstones dropped (%.2f s)%n",
          c.entriesIn(), c.entriesOut(), c.versionsDropped(), c.tombstonesDropped(), c.seconds());
    }
    System.out.println("  after:  " + t.levelSummary());
    showRuns(t);
    Snapshot s = t.table().currentSnapshot();
    System.out.println("  Iceberg snapshot " + s.snapshotId() + ": operation=" + s.operation() + ", lsm.operation="
        + s.summary().get("lsm.operation") + ", files removed=" + s.summary().get("deleted-data-files")
        + ", added=" + s.summary().get("added-data-files"));
    System.out.println("\n  Lookups after the merge: at most one run per level can hold a key.");
    showGet(t, "user01234");
    showGet(t, "user00007");   // the tombstone and the old version are gone

    step("6. CRASH RECOVERY: write 3 keys, 'crash' without flushing, reopen");
    for (String k : List.of("crash-a", "crash-b", "crash-c")) t.put(k, "survives");
    // no flush(), no close(): just open the table again
    LSMTable t2 = new LSMTable(Catalogs.local(warehouse), "demo.kv", cfg);
    System.out.println("  replayed " + t2.recovered() + " WAL records; get(\"crash-b\") = \"" + t2.get("crash-b") + "\"");

    step("7. STATE");
    t2.describe().forEach((k, v) -> {
      if (!k.equals("lookupStats") && !k.equals("writeStats")) System.out.println("  " + k + ": " + v);
    });
    t2.close();
    System.out.println("\nInspect the Iceberg metadata tree with:\n  ./run.sh inspect demo.kv \"" + warehouse + "\"");
  }
}
