package cs631.lsm;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.util.Pair;

/**
 * LSM-tree key-value table stored as an Apache Iceberg table.
 *
 * <pre>
 * Write path:  put/delete -> WAL -> MemTable (skip list)
 *              MemTable full -> flush: one key-sorted Avro run at L0, committed to Iceberg in ONE
 *              transaction (append the data file + move the LSM manifest pointer), then its Puffin
 *              index (bloom filter, key index, block directory) attached to that snapshot.
 * Merge:       L0 has too many runs -> merge them with the overlapping L1 runs into new L1 runs
 *              (sorted, non-overlapping, newest version of each key only). A level over its size
 *              limit pushes one run down the same way. Each merge is one Iceberg rewrite commit.
 * Read path:   MemTable -> L0 runs newest first -> at most one run per deeper level.
 *              In a run: key range, bloom filter, key index -> one block.
 * </pre>
 *
 * The Iceberg table stays an ordinary table (key, value, seq, op), so Spark/Trino can still read
 * it: the current value of a key is the row with the highest seq.
 */
public class LSMTable implements AutoCloseable {
  public static final String PROP_MANIFEST = "lsm.manifest-location";
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final BlockCache NO_CACHE = new BlockCache(0);

  private final LSMConfig cfg;
  private final Table table;
  private final FileIO io;
  private final Configuration conf = new Configuration();
  private final String dataDir;
  private final String indexDir;
  private final String manifestDir;

  private LSMManifest manifest;
  private final TreeMap<Integer, List<SortedRun>> levels = new TreeMap<>();
  private final BlockCache cache;
  private final LookupStats stats = new LookupStats();
  private final WriteStats writeStats = new WriteStats();
  private Map<String, DataFile> dataFilesByPath; // filled lazily after a restart
  private MemTable memtable = new MemTable();
  private final WriteAheadLog wal;
  private long seq;
  private int recovered;

  /** What one merge did (printed by the demo). */
  public record Compaction(int fromLevel, int toLevel, List<Long> inputRuns, List<Long> outputRuns,
                           long entriesIn, long entriesOut, long versionsDropped, long tombstonesDropped,
                           long snapshotId, double seconds) {}

  public LSMTable(Catalog catalog, String identifier, LSMConfig cfg) {
    this.cfg = cfg;
    TableIdentifier id = TableIdentifier.parse(identifier);
    this.table = catalog.tableExists(id)
        ? catalog.loadTable(id)
        : catalog.buildTable(id, KvSchema.SCHEMA)
            .withSortOrder(KvSchema.SORT_BY_KEY)
            .withProperties(Map.of("lsm.enabled", "true", "lsm.format-version", "2", "format-version", "2",
                "write.format.default", cfg.fileFormat.name().toLowerCase()))
            .create();
    this.io = table.io();
    String root = table.location();
    this.dataDir = root + "/data/lsm";
    this.indexDir = root + "/metadata/lsm-index";
    this.manifestDir = root + "/metadata/lsm";
    this.cache = new BlockCache(cfg.blockCacheBlocks);
    loadManifest();

    // Crash recovery: replay WAL records newer than the last flushed seq.
    Path walPath = Catalogs.localPath(root).resolve("wal").resolve("current.wal");
    this.seq = manifest.lastSeq;
    for (KeyValue kv : WriteAheadLog.replay(walPath)) {
      if (kv.seq() > manifest.lastSeq) {
        memtable.put(kv.key(), kv.seq(), kv.op(), kv.value());
        seq = Math.max(seq, kv.seq());
        recovered++;
      }
    }
    this.wal = new WriteAheadLog(walPath, cfg.walSync);
  }

  // ------------------------------------------------------------------ setup

  private void loadManifest() {
    String loc = table.properties().get(PROP_MANIFEST);
    if (loc == null) {
      manifest = new LSMManifest();
      manifest.levels.put(0, new ArrayList<>());
    } else {
      try (InputStream in = io.newInputFile(loc).newStream()) {
        manifest = JSON.readValue(in, LSMManifest.class);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    levels.clear();
    Map<String, List<Pair<BlobMetadata, ByteBuffer>>> puffinCache = new HashMap<>();
    manifest.levels.forEach((lvl, runs) -> {
      List<SortedRun> readers = new ArrayList<>();
      for (RunMeta m : runs) readers.add(SortedRun.load(io, conf, m, cfg, puffinCache));
      levels.put(lvl, readers);
      sortLevel(lvl);
    });
  }

  /** L0: newest first (runs overlap). Deeper levels: by key (runs don't overlap). */
  private void sortLevel(int lvl) {
    List<SortedRun> runs = levels.get(lvl);
    if (runs == null) return;
    if (lvl == 0) runs.sort((a, b) -> Long.compare(b.meta().maxSeq(), a.meta().maxSeq()));
    else runs.sort(Comparator.comparing(r -> r.meta().minKey()));
  }

  // ------------------------------------------------------------- write path

  public void put(String key, String value) {
    if (value == null) throw new IllegalArgumentException("use delete() to remove a key");
    write(key, Entry.PUT, value);
  }

  public void delete(String key) {
    write(key, Entry.DELETE, null);
  }

  private void write(String key, byte op, String value) {
    if (key == null || key.isEmpty()) throw new IllegalArgumentException("key must be a non-empty string");
    seq++;
    wal.append(seq, op, key, value);
    memtable.put(key, seq, op, value);
    writeStats.writes++;
    writeStats.userBytes += key.length() + (value == null ? 0 : value.length());
    if (shouldFlush()) flush();
  }

  private boolean shouldFlush() {
    if (memtable.size() >= cfg.memtableMaxEntries || memtable.sizeBytes() >= cfg.memtableMaxBytes) return true;
    return cfg.flushIntervalSeconds > 0 && memtable.size() > 0 && memtable.ageSeconds() >= cfg.flushIntervalSeconds;
  }

  /** Call periodically (e.g. from a timer) so the time-based trigger also fires when idle. */
  public RunMeta maybeFlush() {
    return shouldFlush() ? flush() : null;
  }

  private String runName(int level, long runId) {
    return String.format("L%d-run-%06d", level, runId);
  }

  public RunMeta flush() {
    if (memtable.size() == 0) return null;
    long runId = manifest.nextRunId;
    String name = runName(0, runId);
    String dataPath = dataDir + "/" + name + RunFiles.extension(cfg.fileFormat);
    String indexPath = indexDir + "/" + name + ".puffin";
    List<KeyValue> rows = new ArrayList<>(memtable.size());
    for (Map.Entry<String, Entry> e : memtable.entries()) {
      Entry v = e.getValue();
      rows.add(new KeyValue(e.getKey(), v.seq(), v.op(), v.value()));
    }
    SortedRun run = SortedRun.write(io, conf, rows, runId, 0, dataPath, indexPath, cfg, table.sortOrder());
    RunMeta meta = run.meta();

    LSMManifest next = manifest.copy();
    next.manifestVersion++;
    next.lastSeq = seq;
    next.nextRunId = runId + 1;
    next.levels.computeIfAbsent(0, k -> new ArrayList<>()).add(meta);
    String manifestPath = writeManifest(next);

    // One atomic Iceberg commit: register the data file AND move the LSM manifest pointer.
    try {
      Transaction tx = table.newTransaction();
      tx.newAppend()
          .appendFile(run.dataFile())
          .set("lsm.operation", "flush")
          .set("lsm.run-id", Long.toString(runId))
          .set("lsm.level", "0")
          .set("lsm.min-key", meta.minKey())
          .set("lsm.max-key", meta.maxKey())
          .set("lsm.max-seq", Long.toString(meta.maxSeq()))
          .commit();
      tx.updateProperties().set(PROP_MANIFEST, manifestPath).commit();
      tx.commitTransaction();
    } catch (RuntimeException e) {
      io.deleteFile(dataPath);
      io.deleteFile(manifestPath);
      throw e;
    }
    attachIndex(indexPath, List.of(run));

    manifest = next;
    levels.computeIfAbsent(0, k -> new ArrayList<>()).add(0, run);
    memtable = new MemTable();
    wal.reset();
    writeStats.flushes++;
    writeStats.flushBytes += meta.fileSize();
    writeStats.flushRows += meta.recordCount();
    if (cfg.autoCompact) compact(false);
    return meta;
  }

  private String writeManifest(LSMManifest m) {
    String path = String.format("%s/lsm-manifest-%05d.json", manifestDir, m.manifestVersion);
    try (OutputStream out = io.newOutputFile(path).createOrOverwrite()) {
      JSON.writerWithDefaultPrettyPrinter().writeValue(out, m);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return path;
  }

  /** Attach the runs' index to the current snapshot as an Iceberg statistics (Puffin) file. */
  private void attachIndex(String indexPath, List<SortedRun> runs) {
    if (runs.isEmpty()) return;
    Snapshot snap = table.currentSnapshot();
    StatisticsFile st = SortedRun.writeIndexFile(io, indexPath, runs, snap.snapshotId(), snap.sequenceNumber());
    table.updateStatistics().setStatistics(st).commit();
  }

  // ------------------------------------------------------------- compaction

  /**
   * Run merges until every level is within its limit. With force=true, L0 is merged into L1
   * even if it is below its trigger. Returns what each merge did.
   */
  public List<Compaction> compact(boolean force) {
    List<Compaction> done = new ArrayList<>();
    boolean forceL0 = force;
    while (true) {
      List<SortedRun> l0 = levels.getOrDefault(0, List.of());
      if (!l0.isEmpty() && (forceL0 || l0.size() >= cfg.l0CompactionTrigger)) {
        done.add(merge(0, new ArrayList<>(l0)));
        forceL0 = false;
        continue;
      }
      Integer over = null;
      for (int lvl : levels.keySet()) {
        if (lvl >= 1 && lvl < cfg.maxLevels - 1 && levelEntries(lvl) > cfg.levelLimit(lvl)) {
          over = lvl;
          break;
        }
      }
      if (over == null) return done;
      done.add(merge(over, List.of(pickRun(over))));
    }
  }

  /** Round-robin over the key space: the first run after the level's cursor. */
  private SortedRun pickRun(int lvl) {
    List<SortedRun> runs = levels.get(lvl);
    String cursor = manifest.compactCursor.get(lvl);
    for (SortedRun r : runs) if (cursor == null || r.meta().minKey().compareTo(cursor) > 0) return r;
    return runs.get(0);
  }

  public long levelEntries(int lvl) {
    return levels.getOrDefault(lvl, List.of()).stream().mapToLong(r -> r.meta().recordCount()).sum();
  }

  private static boolean overlaps(RunMeta m, String lo, String hi) {
    return m.maxKey().compareTo(lo) >= 0 && m.minKey().compareTo(hi) <= 0;
  }

  /** Merge the given runs of level `from` with the overlapping runs of level from+1. */
  private Compaction merge(int from, List<SortedRun> inputs) {
    long t0 = System.nanoTime();
    int to = from + 1;
    String lo = inputs.stream().map(r -> r.meta().minKey()).min(String::compareTo).orElseThrow();
    String hi = inputs.stream().map(r -> r.meta().maxKey()).max(String::compareTo).orElseThrow();
    List<SortedRun> lower = levels.getOrDefault(to, List.of()).stream().filter(r -> overlaps(r.meta(), lo, hi)).toList();
    List<SortedRun> all = new ArrayList<>(inputs);
    all.addAll(lower);
    // A delete marker can be dropped only if no deeper level might still hold an older version.
    boolean bottom = levels.tailMap(to, false).values().stream().flatMap(List::stream).noneMatch(r -> overlaps(r.meta(), lo, hi));

    List<Iterator<KeyValue>> sources = new ArrayList<>();
    for (SortedRun r : all) sources.add(r.range(null, null, NO_CACHE));
    MergingIterator.Newest merged = new MergingIterator.Newest(sources);

    long entriesIn = all.stream().mapToLong(r -> r.meta().recordCount()).sum();
    long tombstonesDropped = 0, entriesOut = 0;
    long runId = manifest.nextRunId;
    List<SortedRun> outputs = new ArrayList<>();
    String indexPath = String.format("%s/merge-%05d.puffin", indexDir, manifest.manifestVersion + 1);
    List<KeyValue> chunk = new ArrayList<>();
    while (merged.hasNext()) {
      KeyValue kv = merged.next();
      if (bottom && kv.op() == Entry.DELETE) {
        tombstonesDropped++;
        continue;
      }
      chunk.add(kv);
      if (chunk.size() >= cfg.runTargetEntries()) {
        outputs.add(writeRun(chunk, runId++, to, indexPath));
        entriesOut += chunk.size();
        chunk = new ArrayList<>();
      }
    }
    if (!chunk.isEmpty()) {
      outputs.add(writeRun(chunk, runId++, to, indexPath));
      entriesOut += chunk.size();
    }

    Set<Long> inputIds = all.stream().map(r -> r.meta().runId()).collect(Collectors.toSet());
    LSMManifest next = manifest.copy();
    next.manifestVersion++;
    next.nextRunId = runId;
    next.levels.computeIfAbsent(from, k -> new ArrayList<>()).removeIf(m -> inputIds.contains(m.runId()));
    List<RunMeta> toLevel = next.levels.computeIfAbsent(to, k -> new ArrayList<>());
    toLevel.removeIf(m -> inputIds.contains(m.runId()));
    outputs.forEach(o -> toLevel.add(o.meta()));
    if (from >= 1) next.compactCursor.put(from, inputs.get(inputs.size() - 1).meta().maxKey());
    String manifestPath = writeManifest(next);

    // One atomic Iceberg commit: replace the input files with the merged ones AND move the manifest pointer.
    try {
      Transaction tx = table.newTransaction();
      RewriteFiles rw = tx.newRewrite();
      for (SortedRun r : all) rw.deleteFile(dataFileOf(r));
      for (SortedRun r : outputs) rw.addFile(r.dataFile());
      rw.set("lsm.operation", "compaction")
          .set("lsm.from-level", Integer.toString(from))
          .set("lsm.to-level", Integer.toString(to))
          .set("lsm.input-runs", all.stream().map(r -> Long.toString(r.meta().runId())).collect(Collectors.joining(",")))
          .set("lsm.output-runs", outputs.stream().map(r -> Long.toString(r.meta().runId())).collect(Collectors.joining(",")))
          .set("lsm.versions-dropped", Long.toString(merged.skipped()))
          .set("lsm.tombstones-dropped", Long.toString(tombstonesDropped))
          .commit();
      tx.updateProperties().set(PROP_MANIFEST, manifestPath).commit();
      tx.commitTransaction();
    } catch (RuntimeException e) {
      outputs.forEach(o -> io.deleteFile(o.meta().dataFile()));
      io.deleteFile(manifestPath);
      throw e;
    }
    attachIndex(indexPath, outputs);
    long snapshotId = table.currentSnapshot().snapshotId();

    manifest = next;
    for (SortedRun r : all) {
      levels.get(r.meta().level()).remove(r);
      cache.evictRun(r.meta().runId());
      r.close();
      if (dataFilesByPath != null) dataFilesByPath.remove(r.meta().dataFile());
    }
    levels.computeIfAbsent(to, k -> new ArrayList<>()).addAll(outputs);
    sortLevel(to);
    if (levels.get(from).isEmpty() && from > 0) levels.remove(from);

    double secs = (System.nanoTime() - t0) / 1e9;
    writeStats.compactions++;
    writeStats.compactionBytesRead += all.stream().mapToLong(r -> r.meta().fileSize()).sum();
    writeStats.compactionBytesWritten += outputs.stream().mapToLong(r -> r.meta().fileSize()).sum();
    writeStats.compactionRowsWritten += entriesOut;
    writeStats.versionsDropped += merged.skipped();
    writeStats.tombstonesDropped += tombstonesDropped;
    writeStats.compactionNanos += System.nanoTime() - t0;
    return new Compaction(from, to, all.stream().map(r -> r.meta().runId()).toList(),
        outputs.stream().map(r -> r.meta().runId()).toList(), entriesIn, entriesOut, merged.skipped(),
        tombstonesDropped, snapshotId, secs);
  }

  private SortedRun writeRun(List<KeyValue> rows, long runId, int level, String indexPath) {
    String dataPath = dataDir + "/" + runName(level, runId) + RunFiles.extension(cfg.fileFormat);
    return SortedRun.write(io, conf, rows, runId, level, dataPath, indexPath, cfg, table.sortOrder());
  }

  /** The Iceberg DataFile of a run (known after writing it; looked up in the table after a restart). */
  private DataFile dataFileOf(SortedRun r) {
    if (r.dataFile() != null) return r.dataFile();
    if (dataFilesByPath == null) {
      dataFilesByPath = new HashMap<>();
      try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
        for (FileScanTask t : tasks) dataFilesByPath.put(t.file().location(), t.file());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    DataFile df = dataFilesByPath.get(r.meta().dataFile());
    if (df == null) throw new IllegalStateException("data file of run " + r.meta().runId() + " is not in the table");
    r.dataFile(df);
    return df;
  }

  // -------------------------------------------------------------- read path

  public String get(String key) {
    return get(key, null);
  }

  /** Point lookup. If trace is non-null, every decision is appended to it (used by the demo). */
  public String get(String key, List<String> trace) {
    stats.gets++;
    Entry hit = memtable.get(key);
    if (hit != null) {
      stats.memtableHits++;
      if (trace != null) trace.add("memtable: found seq=" + hit.seq() + (hit.isDelete() ? " (tombstone: deleted)" : ""));
      return hit.isDelete() ? null : hit.value();
    }
    if (trace != null) trace.add("memtable: miss");
    KeyValue kv = null;
    for (Map.Entry<Integer, List<SortedRun>> e : levels.entrySet()) {
      List<SortedRun> runs = e.getValue();
      if (runs.isEmpty()) continue;
      if (e.getKey() == 0) {
        for (SortedRun run : runs) { // L0 runs overlap: check all, newest first
          kv = run.get(key, cache, stats, trace);
          if (kv != null) break;
        }
      } else {
        // Deeper levels don't overlap: only the run whose key range covers the key can have it.
        SortedRun run = coveringRun(runs, key);
        stats.runsChecked += runs.size() - (run == null ? 0 : 1);
        stats.rangePruned += runs.size() - (run == null ? 0 : 1);
        if (run == null) {
          if (trace != null) trace.add("L" + e.getKey() + " (" + runs.size() + " runs): no run's key range covers the key -> skip");
        } else {
          kv = run.get(key, cache, stats, trace);
        }
      }
      if (kv != null) break;
    }
    if (kv == null) {
      if (trace != null) trace.add("-> not found anywhere");
      return null;
    }
    if (kv.op() == Entry.DELETE && trace != null) trace.add("-> tombstone: key is deleted");
    return kv.op() == Entry.PUT ? kv.value() : null;
  }

  /** Runs of a deeper level are sorted by key and don't overlap: binary search for the one covering key. */
  private static SortedRun coveringRun(List<SortedRun> runs, String key) {
    int lo = 0, hi = runs.size() - 1, best = -1;
    while (lo <= hi) {
      int mid = (lo + hi) >>> 1;
      if (runs.get(mid).meta().minKey().compareTo(key) <= 0) {
        best = mid;
        lo = mid + 1;
      } else {
        hi = mid - 1;
      }
    }
    if (best < 0) return null;
    SortedRun r = runs.get(best);
    return r.meta().maxKey().compareTo(key) >= 0 ? r : null;
  }

  /** Live (key, value) pairs with lo <= key <= hi (null = unbounded), in key order. */
  public Iterator<Map.Entry<String, String>> scan(String lo, String hi) {
    List<Iterator<KeyValue>> sources = new ArrayList<>();
    Iterator<Map.Entry<String, Entry>> mt = memtable.range(lo, hi).entrySet().iterator();
    sources.add(new Iterator<>() {
      public boolean hasNext() { return mt.hasNext(); }
      public KeyValue next() {
        Map.Entry<String, Entry> e = mt.next();
        return new KeyValue(e.getKey(), e.getValue().seq(), e.getValue().op(), e.getValue().value());
      }
    });
    for (List<SortedRun> runs : levels.values()) {
      for (SortedRun run : runs) sources.add(run.range(lo, hi, cache)); // range() returns empty for non-overlapping runs
    }
    return new MergingIterator(sources);
  }

  public Map<String, String> scanToMap(String lo, String hi) {
    Map<String, String> out = new LinkedHashMap<>();
    scan(lo, hi).forEachRemaining(e -> out.put(e.getKey(), e.getValue()));
    return out;
  }

  // ------------------------------------------------------------------- misc

  public Table table() { return table; }
  public LookupStats stats() { return stats; }
  public WriteStats writeStats() { return writeStats; }
  public MemTable memtable() { return memtable; }
  public int recovered() { return recovered; }
  public long lastSeq() { return seq; }
  public Map<Integer, List<SortedRun>> levels() { return levels; }
  public LSMConfig config() { return cfg; }

  public int runCount() {
    return levels.values().stream().mapToInt(List::size).sum();
  }

  /** e.g. "L0: 2 runs (2,000 entries) | L1: 3 runs (5,400 entries)". */
  public String levelSummary() {
    List<String> parts = new ArrayList<>();
    levels.forEach((l, runs) -> {
      if (!runs.isEmpty() || l == 0) parts.add(String.format("L%d: %d runs (%,d entries)", l, runs.size(), levelEntries(l)));
    });
    return String.join(" | ", parts);
  }

  public long indexBytes() {
    return levels.values().stream().flatMap(List::stream).mapToLong(SortedRun::indexBytes).sum();
  }

  public Map<String, Object> describe() {
    Map<String, Object> d = new LinkedHashMap<>();
    d.put("table", table.name());
    d.put("fileFormat", cfg.fileFormat);
    d.put("lookupIndex", cfg.lookupIndex);
    d.put("memtableEntries", memtable.size());
    d.put("lastSeq", seq);
    Map<Integer, List<Long>> lv = new TreeMap<>();
    levels.forEach((l, runs) -> lv.put(l, runs.stream().map(r -> r.meta().runId()).toList()));
    d.put("levels (run ids)", lv);
    d.put("snapshots", table.snapshots() instanceof java.util.Collection<?> c ? c.size() : -1);
    d.put("puffinIndexFiles", table.statisticsFiles().size());
    d.put("writeStats", writeStats.asMap());
    d.put("lookupStats", stats.asMap());
    return d;
  }

  /** Run ids of all live runs (for tests). */
  Set<Long> liveRunIds() {
    Set<Long> s = new HashSet<>();
    levels.values().forEach(rs -> rs.forEach(r -> s.add(r.meta().runId())));
    return s;
  }

  @Override
  public void close() {
    wal.close();
    levels.values().forEach(rs -> rs.forEach(SortedRun::close));
  }
}
