package cs631.lsm.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import cs631.lsm.Catalogs;
import cs631.lsm.IcebergCowTable;
import cs631.lsm.IcebergMorTable;
import cs631.lsm.KvStore;
import cs631.lsm.LSMConfig;
import cs631.lsm.LSMTable;
import cs631.lsm.LookupStats;
import cs631.lsm.VanillaIcebergTable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import org.apache.iceberg.catalog.Catalog;

/**
 * Evaluation: the LSM table (and variants) against three ways of keeping a key-value table in
 * plain Iceberg.
 *
 * <pre>
 * Tables      lsm            Avro runs, Puffin key index, leveled merges   (our design)
 *             lsm-parquet    same, but Parquet runs                        (effect of Avro)
 *             lsm-fence      same, but fence-pointer lookups              (effect of the key index)
 *             lsm-nocompact  same, but no merges                          (effect of compaction)
 *             append         Iceberg: append every micro-batch, resolve newest version on read
 *             mor            Iceberg merge-on-read: data file + equality-delete file per batch
 *             cow            Iceberg copy-on-write: rewrite the touched sorted files per batch
 * Phases      1. load N keys in random order + M skewed updates/deletes (write speed, bytes
 *                written, and lookup latency sampled as files pile up)
 *             2. point lookups (80% existing keys, hot keys more likely; 20% keys never written)
 *             3. range scans of `scan-width` keys
 *             4. YCSB workloads on the loaded table, zipfian keys:
 *                A 50% read / 50% update, B 95/5, C 100% read, E 95% short scans / 5% inserts
 * </pre>
 *
 * Every table writes a file every `batch` writes. Slow tables stop a phase after `time-limit`
 * seconds and report what they did in that time.
 *
 * Usage: ./run.sh bench [--keys 20000] [--updates 40000] [--batch 5000] [--lookups 300] [--scans 30]
 *          [--ycsb-ops 2000] [--time-limit 5] [--cache-blocks 0] [--stores lsm,append,mor,cow] [--out benchmark]
 *        ./run.sh eval   (all seven tables, 100k keys + 400k updates; about 15 minutes)
 */
public class Benchmark {
  record Op(boolean delete, String key, String value) {}

  static String key(int i) {
    return String.format("user%08d", i);
  }

  static List<Op> loadWorkload(int keys, int updates, long seed) {
    Random rng = new Random(seed);
    List<Integer> order = new ArrayList<>();
    for (int i = 0; i < keys; i++) order.add(i);
    Collections.shuffle(order, rng); // streaming arrivals are not key-ordered
    List<Op> ops = new ArrayList<>(keys + updates);
    for (int i : order) ops.add(new Op(false, key(i), "load-" + i));
    for (int j = 0; j < updates; j++) {
      int k = (int) (keys * Math.pow(rng.nextDouble(), 2)); // skew: low ids are hot
      ops.add(rng.nextDouble() < 0.1 ? new Op(true, key(k), null) : new Op(false, key(k), "upd-" + j));
    }
    return ops;
  }

  /** YCSB's scrambled zipfian generator (theta 0.99): a few keys are very hot, spread over the key space. */
  static final class Zipf {
    private final int n;
    private final double theta = 0.99, alpha, zetan, eta;
    private final Random rng;

    Zipf(int n, long seed) {
      this.n = n;
      this.rng = new Random(seed);
      double z = 0;
      for (int i = 1; i <= n; i++) z += 1 / Math.pow(i, theta);
      this.zetan = z;
      double zeta2 = 1 + 1 / Math.pow(2, theta);
      this.alpha = 1 / (1 - theta);
      this.eta = (1 - Math.pow(2.0 / n, 1 - theta)) / (1 - zeta2 / zetan);
    }

    int next() {
      double u = rng.nextDouble(), uz = u * zetan;
      long rank = uz < 1 ? 0 : uz < 1 + Math.pow(0.5, theta) ? 1 : (long) (n * Math.pow(eta * u - eta + 1, alpha));
      long h = 0xcbf29ce484222325L;
      for (int i = 0; i < 8; i++) {
        h ^= (rank >>> (8 * i)) & 0xff;
        h *= 0x100000001b3L;
      }
      return (int) Math.floorMod(h, (long) n);
    }
  }

  /** Our LSM table seen through the benchmark interface. Files read = blocks read (one block = one file). */
  static final class LsmStore implements KvStore {
    final LSMTable t;
    private double lastFiles;

    LsmStore(LSMTable t) {
      this.t = t;
    }

    public void put(String k, String v) { t.put(k, v); }
    public void delete(String k) { t.delete(k); }
    public void flush() { t.flush(); }

    public String get(String k) {
      LookupStats s = t.stats();
      long before = s.blocksRead + s.blockCacheHits;
      String v = t.get(k);
      lastFiles = s.blocksRead + s.blockCacheHits - before;
      return v;
    }

    public Map<String, String> scan(String lo, String hi) { return t.scanToMap(lo, hi); }
    public long bytesWritten() { return t.writeStats().flushBytes + t.writeStats().compactionBytesWritten; }
    public long rowsWritten() { return t.writeStats().flushRows + t.writeStats().compactionRowsWritten; }
    public double filesReadLastGet() { return lastFiles; }
    public long dataFiles() { return t.runCount(); }
  }

  static double pct(double[] xs, int n, double p) {
    if (n == 0) return Double.NaN;
    double[] s = Arrays.copyOf(xs, n);
    Arrays.sort(s);
    return s[Math.min(n - 1, (int) (p / 100 * n))];
  }

  static double round(double x, int d) {
    if (Double.isNaN(x)) return x;
    double f = Math.pow(10, d);
    return Math.round(x * f) / f;
  }

  public static void main(String[] args) throws Exception {
    Map<String, String> a = new LinkedHashMap<>();
    a.put("keys", "20000");
    a.put("updates", "40000");
    a.put("batch", "5000");
    a.put("lookups", "300");
    a.put("scans", "30");
    a.put("scan-width", "100");
    a.put("ycsb-ops", "2000");
    a.put("time-limit", "5");
    a.put("samples", "8");
    a.put("seed", "42");
    a.put("cache-blocks", "0");
    a.put("stores", "lsm,append,mor,cow");
    a.put("out", "benchmark");
    for (int i = 0; i + 1 < args.length; i += 2) a.put(args[i].replaceFirst("^--", ""), args[i + 1]);
    int keys = Integer.parseInt(a.get("keys")), updates = Integer.parseInt(a.get("updates"));
    int batch = Integer.parseInt(a.get("batch")), nLookups = Integer.parseInt(a.get("lookups"));
    int nScans = Integer.parseInt(a.get("scans")), width = Integer.parseInt(a.get("scan-width"));
    int ycsbOps = Integer.parseInt(a.get("ycsb-ops")), samples = Integer.parseInt(a.get("samples"));
    long seed = Long.parseLong(a.get("seed"));
    double limit = Double.parseDouble(a.get("time-limit"));
    int cacheBlocks = Integer.parseInt(a.get("cache-blocks"));
    List<String> names = List.of(a.get("stores").split(","));

    Path wh = Path.of("warehouse/bench").toAbsolutePath();
    Tools.deleteRecursively(wh);
    Catalog cat = Catalogs.local(wh);
    Map<String, KvStore> stores = new LinkedHashMap<>();
    List<LSMTable> lsmTables = new ArrayList<>();
    for (String name : names) {
      LSMConfig cfg = new LSMConfig().memtableMaxEntries(batch).blockCacheBlocks(cacheBlocks);
      cfg.memtableMaxBytes = Long.MAX_VALUE; // flush by entry count, like the baselines
      KvStore s = switch (name) {
        case "lsm" -> new LsmStore(new LSMTable(cat, "bench.lsm", cfg));
        case "lsm-parquet" -> new LsmStore(new LSMTable(cat, "bench.lsm_parquet", cfg.fileFormat(LSMConfig.Format.PARQUET)));
        case "lsm-fence" -> new LsmStore(new LSMTable(cat, "bench.lsm_fence", cfg.lookupIndex(LSMConfig.LookupIndex.FENCE)));
        case "lsm-nocompact" -> new LsmStore(new LSMTable(cat, "bench.lsm_nocompact", cfg.autoCompact(false)));
        case "append" -> new VanillaIcebergTable(cat, "bench.append", batch);
        case "mor" -> new IcebergMorTable(cat, "bench.mor", batch);
        case "cow" -> new IcebergCowTable(cat, "bench.cow", batch, batch);
        default -> throw new IllegalArgumentException("unknown store " + name);
      };
      if (s instanceof LsmStore ls) lsmTables.add(ls.t);
      stores.put(name, s);
    }

    List<Op> load = loadWorkload(keys, updates, seed);
    long userBytes = 0;
    for (Op op : load) userBytes += op.key().length() + (op.value() == null ? 0 : op.value().length());

    Random lr = new Random(seed + 1);
    List<String> lookupKeys = new ArrayList<>();
    for (int i = 0; i < nLookups; i++) {
      lookupKeys.add(lr.nextDouble() < 0.8 ? key((int) (keys * Math.pow(lr.nextDouble(), 2))) : key(lr.nextInt(keys)) + "x");
    }
    List<String> probe = lookupKeys.subList(0, Math.min(20, lookupKeys.size()));

    Map<String, Map<String, Object>> res = new LinkedHashMap<>();
    Map<String, List<String>> answers = new LinkedHashMap<>();
    for (var e : stores.entrySet()) {
      String name = e.getKey();
      KvStore s = e.getValue();
      Map<String, Object> r = new LinkedHashMap<>();
      res.put(name, r);
      System.out.printf("%n== %s%n", name);

      // ---- phase 1: load + updates, sampling lookup latency as files pile up
      List<Map<String, Object>> curve = new ArrayList<>();
      long writeNanos = 0;
      int step = Math.max(1, load.size() / samples);
      for (int i = 0; i < load.size(); i++) {
        long t0 = System.nanoTime();
        Op op = load.get(i);
        if (op.delete()) s.delete(op.key()); else s.put(op.key(), op.value());
        writeNanos += System.nanoTime() - t0;
        if ((i + 1) % step == 0 || i == load.size() - 1) {
          double tot = 0;
          long st = System.nanoTime();
          int done = 0;
          for (String k : probe) {
            long g0 = System.nanoTime();
            s.get(k);
            tot += (System.nanoTime() - g0) / 1e6;
            done++;
            if ((System.nanoTime() - st) / 1e9 > limit / 4) break;
          }
          Map<String, Object> pt = new LinkedHashMap<>();
          pt.put("writes", i + 1);
          pt.put("data_files", s.dataFiles());
          pt.put("get_mean_ms", round(tot / done, 3));
          curve.add(pt);
        }
      }
      long t0 = System.nanoTime();
      s.flush();
      writeNanos += System.nanoTime() - t0;
      r.put("write_ops_per_s", Math.round(load.size() / (writeNanos / 1e9)));
      r.put("bytes_written", s.bytesWritten());
      r.put("write_amplification", round((double) s.bytesWritten() / userBytes, 2));
      r.put("rows_written", s.rowsWritten());
      r.put("row_write_amplification", round((double) s.rowsWritten() / load.size(), 2));
      r.put("data_files", s.dataFiles());
      Path loc = Catalogs.localPath(tableOf(s).location());
      r.put("table_bytes_live", liveBytes(s));
      r.put("table_dir_bytes", Tools.dirBytes(loc));
      System.out.printf("  load: %,d ops/s, %d files%n", r.get("write_ops_per_s"), s.dataFiles());

      // ---- phase 2: point lookups
      for (int i = 0; i < Math.min(30, nLookups); i++) s.get(lookupKeys.get(i)); // JIT warm-up
      if (s instanceof LsmStore ls) ls.t.stats().reset();
      double[] lat = new double[nLookups];
      double files = 0;
      List<String> ans = new ArrayList<>();
      long ps = System.nanoTime();
      int n = 0;
      for (; n < nLookups && (System.nanoTime() - ps) / 1e9 < limit; n++) {
        long g0 = System.nanoTime();
        ans.add(s.get(lookupKeys.get(n)));
        lat[n] = (System.nanoTime() - g0) / 1e6;
        files += s.filesReadLastGet();
      }
      answers.put(name, ans);
      r.put("lookups_done", n);
      r.put("get_p50_ms", round(pct(lat, n, 50), 3));
      r.put("get_p95_ms", round(pct(lat, n, 95), 3));
      r.put("files_read_per_get", round(files / Math.max(1, n), 2));
      if (s instanceof LsmStore ls) {
        LookupStats st = ls.t.stats();
        r.put("runs_skipped_pct", round(100.0 * (st.rangePruned + st.bloomPruned + st.indexPruned) / Math.max(1, st.runsChecked), 1));
        r.put("bloom_false_positive_block_reads", st.bloomFalsePositives);
        r.put("index_bytes", ls.t.indexBytes());
        r.put("levels", ls.t.levelSummary());
        r.put("write_stats", ls.t.writeStats().asMap());
      }
      System.out.printf("  get: p50 %.3f ms over %d lookups, %.2f files/get%n", r.get("get_p50_ms"), n, r.get("files_read_per_get"));

      // ---- phase 3: range scans
      Random sr = new Random(seed + 2);
      double[] slat = new double[nScans];
      ps = System.nanoTime();
      int m = 0;
      for (; m < nScans && (System.nanoTime() - ps) / 1e9 < limit; m++) {
        int lo = sr.nextInt(keys - width);
        long g0 = System.nanoTime();
        s.scan(key(lo), key(lo + width - 1));
        slat[m] = (System.nanoTime() - g0) / 1e6;
      }
      r.put("scan_p50_ms", round(pct(slat, m, 50), 2));

      // ---- phase 4: YCSB
      Map<String, Object> ycsb = new LinkedHashMap<>();
      int inserted = 0;
      for (String w : List.of("A", "B", "C", "E")) {
        Zipf z = new Zipf(keys, seed + w.charAt(0));
        Random wr = new Random(seed + 10 + w.charAt(0));
        long w0 = System.nanoTime();
        int ops = 0;
        for (; ops < ycsbOps && (System.nanoTime() - w0) / 1e9 < limit; ops++) {
          double u = wr.nextDouble();
          switch (w) {
            case "A" -> { if (u < 0.5) s.get(key(z.next())); else s.put(key(z.next()), "ycsbA-" + ops); }
            case "B" -> { if (u < 0.95) s.get(key(z.next())); else s.put(key(z.next()), "ycsbB-" + ops); }
            case "C" -> s.get(key(z.next()));
            default -> {
              if (u < 0.95) {
                int lo = z.next();
                s.scan(key(lo), key(lo + 1 + wr.nextInt(100)));
              } else {
                s.put(key(keys + inserted++), "ycsbE-" + ops);
              }
            }
          }
        }
        s.flush(); // buffered writes must reach the table; that cost belongs to the workload
        ycsb.put(w, Math.round(ops / ((System.nanoTime() - w0) / 1e9)));
      }
      r.put("ycsb_ops_per_s", ycsb);
      r.put("curve", curve);
      System.out.printf("  ycsb ops/s: %s%n", ycsb);
    }

    // correctness: every table must give the first table's answers
    List<String> ref = answers.values().iterator().next();
    for (var e : answers.entrySet()) {
      int differ = 0;
      for (int i = 0; i < e.getValue().size(); i++) if (!Objects.equals(ref.get(i), e.getValue().get(i))) differ++;
      res.get(e.getKey()).put("answers_differ", differ + " / " + e.getValue().size());
    }
    for (LSMTable t : lsmTables) t.close();

    System.out.printf("%nWorkload: %,d keys + %,d updates/deletes, a file every %,d writes, block cache %d%n", keys, updates, batch, cacheBlocks);
    StringBuilder head = new StringBuilder(String.format("%-22s", "metric"));
    for (String name : stores.keySet()) head.append(String.format("%15s", name));
    System.out.println(head);
    for (String metric : List.of("write_ops_per_s", "write_amplification", "row_write_amplification", "data_files", "get_p50_ms", "get_p95_ms",
        "files_read_per_get", "scan_p50_ms", "lookups_done", "answers_differ")) {
      StringBuilder line = new StringBuilder(String.format("%-22s", metric));
      for (String name : stores.keySet()) line.append(String.format("%15s", res.get(name).get(metric)));
      System.out.println(line);
    }
    for (String w : List.of("A", "B", "C", "E")) {
      StringBuilder line = new StringBuilder(String.format("%-22s", "ycsb " + w + " ops/s"));
      for (String name : stores.keySet()) line.append(String.format("%15s", ((Map<?, ?>) res.get(name).get("ycsb_ops_per_s")).get(w)));
      System.out.println(line);
    }

    Map<String, Object> out = new LinkedHashMap<>();
    out.put("config", a);
    out.put("user_bytes", userBytes);
    out.putAll(res);
    Path dir = Path.of("results");
    Files.createDirectories(dir);
    Path f = dir.resolve(a.get("out") + ".json");
    Files.writeString(f, new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(out), StandardCharsets.UTF_8);
    System.out.println("saved " + f);
  }

  private static org.apache.iceberg.Table tableOf(KvStore s) {
    if (s instanceof LsmStore ls) return ls.t.table();
    if (s instanceof VanillaIcebergTable v) return v.table();
    if (s instanceof IcebergMorTable v) return v.table();
    return ((IcebergCowTable) s).table();
  }

  /** Bytes of the data and delete files in the current snapshot. */
  private static long liveBytes(KvStore s) {
    try (var tasks = tableOf(s).newScan().planFiles()) {
      long b = 0;
      java.util.Set<String> seen = new java.util.HashSet<>();
      for (var t : tasks) {
        if (seen.add(t.file().location())) b += t.file().fileSizeInBytes();
        for (var d : t.deletes()) if (seen.add(d.location())) b += d.fileSizeInBytes();
      }
      return b;
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
