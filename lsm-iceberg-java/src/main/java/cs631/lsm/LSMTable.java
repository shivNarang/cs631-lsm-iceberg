package cs631.lsm;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;

/**
 * LSM-tree key-value table stored as an Apache Iceberg table.
 *
 * <pre>
 * Write path:  put/delete -> WAL -> MemTable (skip list)
 *              MemTable full -> flush: key-sorted Parquet run, committed to Iceberg in ONE
 *              transaction (append the data file + move the LSM manifest pointer), then a
 *              Puffin index (bloom filter + fence pointers) attached to that snapshot.
 * Read path:   MemTable -> L0 runs newest first (key range, bloom filter, fence pointers)
 *              -> deeper levels. The first version found wins; a tombstone means deleted.
 * </pre>
 *
 * The Iceberg table stays an ordinary table (key, value, seq, op), so Spark/Trino can still read
 * it: the current value of a key is the row with the highest seq.
 */
public class LSMTable implements AutoCloseable {
  public static final String PROP_MANIFEST = "lsm.manifest-location";
  private static final ObjectMapper JSON = new ObjectMapper();

  private final LSMConfig cfg;
  private final Table table;
  private final FileIO io;
  private final Configuration conf = new Configuration();
  private final String dataDir;
  private final String indexDir;
  private final String manifestDir;
  private final Path walPath;

  private LSMManifest manifest;
  private final TreeMap<Integer, List<SortedRun>> levels = new TreeMap<>();
  private final BlockCache cache;
  private final LookupStats stats = new LookupStats();
  private MemTable memtable = new MemTable();
  private WriteAheadLog wal;
  private long seq;
  private int recovered;

  public LSMTable(Catalog catalog, String identifier, LSMConfig cfg) {
    this.cfg = cfg;
    TableIdentifier id = TableIdentifier.parse(identifier);
    this.table = catalog.tableExists(id)
        ? catalog.loadTable(id)
        : catalog.buildTable(id, KvSchema.SCHEMA)
            .withSortOrder(KvSchema.SORT_BY_KEY)
            .withProperties(Map.of("lsm.enabled", "true", "lsm.format-version", "1", "format-version", "2"))
            .create();
    this.io = table.io();
    String root = table.location();
    this.dataDir = root + "/data/lsm";
    this.indexDir = root + "/metadata/lsm-index";
    this.manifestDir = root + "/metadata/lsm";
    this.cache = new BlockCache(cfg.blockCacheBlocks);
    loadManifest();

    // Crash recovery: replay WAL records newer than the last flushed seq.
    this.walPath = Catalogs.localPath(root).resolve("wal").resolve("current.wal");
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
    manifest.levels.forEach((lvl, runs) -> {
      List<SortedRun> readers = new ArrayList<>();
      for (RunMeta m : runs) readers.add(SortedRun.load(io, conf, m, cfg));
      readers.sort((a, b) -> Long.compare(b.meta().maxSeq(), a.meta().maxSeq())); // newest first
      levels.put(lvl, readers);
    });
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

  public RunMeta flush() {
    if (memtable.size() == 0) return null;
    long runId = manifest.nextRunId;
    String name = String.format("L0-run-%06d", runId);
    String dataPath = dataDir + "/" + name + ".parquet";
    String indexPath = indexDir + "/" + name + ".puffin";
    List<KeyValue> rows = new ArrayList<>(memtable.size());
    for (Map.Entry<String, Entry> e : memtable.entries()) {
      Entry v = e.getValue();
      rows.add(new KeyValue(e.getKey(), v.seq(), v.op(), v.value()));
    }
    SortedRun.Written w = SortedRun.write(io, conf, rows, runId, 0, dataPath, indexPath, cfg, table.sortOrder());
    RunMeta meta = w.meta();

    LSMManifest next = manifest.copy();
    next.manifestVersion++;
    next.lastSeq = seq;
    next.nextRunId = runId + 1;
    next.levels.computeIfAbsent(0, k -> new ArrayList<>()).add(meta);
    String manifestPath = String.format("%s/lsm-manifest-%05d.json", manifestDir, next.manifestVersion);
    try (OutputStream out = io.newOutputFile(manifestPath).createOrOverwrite()) {
      JSON.writerWithDefaultPrettyPrinter().writeValue(out, next);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // One atomic Iceberg commit: register the data file AND move the LSM manifest pointer.
    try {
      Transaction tx = table.newTransaction();
      tx.newAppend()
          .appendFile(w.dataFile())
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

    // Attach the run's index to the new snapshot as an Iceberg statistics (Puffin) file.
    Snapshot snap = table.currentSnapshot();
    StatisticsFile stats = SortedRun.writeIndex(io, meta, w.bloom(), w.fences(), snap.snapshotId(), snap.sequenceNumber());
    table.updateStatistics().setStatistics(stats).commit();

    manifest = next;
    levels.computeIfAbsent(0, k -> new ArrayList<>()).add(0, new SortedRun(meta, w.bloom(), w.fences(), conf));
    memtable = new MemTable();
    wal.reset();
    return meta;
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
    // L0 runs overlap -> check all, newest first. Deeper levels will be non-overlapping after
    // compaction, so range pruning leaves at most one candidate per level.
    for (List<SortedRun> runs : levels.values()) {
      for (SortedRun run : runs) {
        KeyValue kv = run.get(key, cache, stats, trace);
        if (kv != null) {
          if (kv.op() == Entry.DELETE && trace != null) trace.add("-> tombstone: key is deleted");
          return kv.op() == Entry.PUT ? kv.value() : null;
        }
      }
    }
    if (trace != null) trace.add("-> not found anywhere");
    return null;
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
      for (SortedRun run : runs) sources.add(run.range(lo, hi, cache));
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
  public MemTable memtable() { return memtable; }
  public int recovered() { return recovered; }
  public long lastSeq() { return seq; }
  public Map<Integer, List<SortedRun>> levels() { return levels; }

  public int runCount() {
    return levels.values().stream().mapToInt(List::size).sum();
  }

  public Map<String, Object> describe() {
    Map<String, Object> d = new LinkedHashMap<>();
    d.put("table", table.name());
    d.put("memtableEntries", memtable.size());
    d.put("memtableBytes", memtable.sizeBytes());
    d.put("lastSeq", seq);
    Map<Integer, List<Long>> lv = new TreeMap<>();
    levels.forEach((l, runs) -> lv.put(l, runs.stream().map(r -> r.meta().runId()).toList()));
    d.put("levels (run ids, newest first)", lv);
    d.put("snapshots", table.snapshots() instanceof java.util.Collection<?> c ? c.size() : -1);
    d.put("puffinIndexFiles", table.statisticsFiles().size());
    d.put("lookupStats", stats.asMap());
    return d;
  }

  @Override
  public void close() {
    wal.close();
    levels.values().forEach(rs -> rs.forEach(SortedRun::close));
  }
}
