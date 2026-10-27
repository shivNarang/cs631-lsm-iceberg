package cs631.lsm.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import cs631.lsm.Catalogs;
import cs631.lsm.LSMConfig;
import cs631.lsm.LSMTable;
import cs631.lsm.LookupStats;
import cs631.lsm.VanillaIcebergTable;
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
 * Preliminary benchmark: LSM-on-Iceberg vs vanilla Iceberg (micro-batch appends).
 * YCSB-style: load N keys in random order, then skewed updates/deletes, then point lookups
 * (80% existing keys, 20% never written) and short range scans. Both tables write a file every
 * `batch` writes.
 *
 * Usage: ./run.sh bench [--keys 20000] [--updates 40000] [--batch 5000] [--lookups 500]
 *                        [--scans 50] [--cache-blocks 256]
 */
public class Benchmark {
  interface Store {
    void put(String k, String v);
    void delete(String k);
    void flush();
    String get(String k);
    int scan(String lo, String hi);
  }

  record Op(boolean delete, String key, String value) {}

  static List<Op> workload(int keys, int updates, long seed) {
    Random rng = new Random(seed);
    List<Integer> order = new ArrayList<>();
    for (int i = 0; i < keys; i++) order.add(i);
    Collections.shuffle(order, rng); // streaming arrivals are not key-ordered
    List<Op> ops = new ArrayList<>(keys + updates);
    for (int i : order) ops.add(new Op(false, String.format("user%08d", i), "load-" + i));
    for (int j = 0; j < updates; j++) {
      int k = (int) (keys * Math.pow(rng.nextDouble(), 2)); // skew: low ids are hot
      ops.add(rng.nextDouble() < 0.1 ? new Op(true, String.format("user%08d", k), null)
          : new Op(false, String.format("user%08d", k), "upd-" + j));
    }
    return ops;
  }

  static double pct(double[] xs, double p) {
    double[] s = xs.clone();
    Arrays.sort(s);
    return s[Math.min(s.length - 1, (int) (p / 100 * s.length))];
  }

  static double round(double x, int d) {
    double f = Math.pow(10, d);
    return Math.round(x * f) / f;
  }

  public static void main(String[] args) throws Exception {
    Map<String, Integer> a = new LinkedHashMap<>(Map.of("keys", 20_000, "updates", 40_000, "batch", 5_000,
        "lookups", 500, "scans", 50, "scan-width", 100, "seed", 42, "cache-blocks", 256, "warmup", 200));
    for (int i = 0; i + 1 < args.length; i += 2) a.put(args[i].replaceFirst("^--", ""), Integer.parseInt(args[i + 1]));

    Path wh = Path.of("warehouse/bench").toAbsolutePath();
    Tools.deleteRecursively(wh);
    Catalog cat = Catalogs.local(wh);
    LSMConfig cfg = new LSMConfig().memtableMaxEntries(a.get("batch")).blockCacheBlocks(a.get("cache-blocks"));
    LSMTable lsm = new LSMTable(cat, "bench.lsm_kv", cfg);
    VanillaIcebergTable van = new VanillaIcebergTable(cat, "bench.vanilla_kv", a.get("batch"));

    Map<String, Store> stores = new LinkedHashMap<>();
    stores.put("lsm", new Store() {
      public void put(String k, String v) { lsm.put(k, v); }
      public void delete(String k) { lsm.delete(k); }
      public void flush() { lsm.flush(); }
      public String get(String k) { return lsm.get(k); }
      public int scan(String lo, String hi) { return lsm.scanToMap(lo, hi).size(); }
    });
    stores.put("vanilla", new Store() {
      public void put(String k, String v) { van.put(k, v); }
      public void delete(String k) { van.delete(k); }
      public void flush() { van.flush(); }
      public String get(String k) { return van.get(k); }
      public int scan(String lo, String hi) { return van.scan(lo, hi).size(); }
    });

    List<Op> ops = workload(a.get("keys"), a.get("updates"), a.get("seed"));
    Map<String, Map<String, Object>> res = new LinkedHashMap<>();
    for (var e : stores.entrySet()) {
      Store s = e.getValue();
      long t0 = System.nanoTime();
      for (Op op : ops) {
        if (op.delete()) s.delete(op.key()); else s.put(op.key(), op.value());
      }
      s.flush();
      double secs = (System.nanoTime() - t0) / 1e9;
      Map<String, Object> r = new LinkedHashMap<>();
      r.put("write_ops_per_s", Math.round(ops.size() / secs));
      res.put(e.getKey(), r);
    }
    res.get("lsm").put("data_files", lsm.runCount());
    res.get("vanilla").put("data_files", liveFiles(van));
    res.get("lsm").put("table_bytes", Tools.dirBytes(Catalogs.localPath(lsm.table().location())));
    res.get("vanilla").put("table_bytes", Tools.dirBytes(Catalogs.localPath(van.table().location())));

    // point lookups
    Random rng = new Random(a.get("seed") + 1);
    int n = a.get("lookups");
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      keys.add(rng.nextDouble() < 0.8 ? String.format("user%08d", (int) (a.get("keys") * Math.pow(rng.nextDouble(), 2)))
          : String.format("user%08dx", rng.nextInt(a.get("keys"))));
    }
    Map<String, List<String>> answers = new LinkedHashMap<>();
    double vanillaFiles = 0;
    for (var e : stores.entrySet()) {
      Store s = e.getValue();
      for (int i = 0; i < Math.min(a.get("warmup"), n); i++) s.get(keys.get(i)); // JIT warm-up
      if (e.getKey().equals("lsm")) lsm.stats().reset();
      double[] lat = new double[n];
      List<String> ans = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        long t0 = System.nanoTime();
        ans.add(s.get(keys.get(i)));
        lat[i] = (System.nanoTime() - t0) / 1e6;
        if (e.getKey().equals("vanilla")) vanillaFiles += van.filesScannedLast();
      }
      answers.put(e.getKey(), ans);
      Map<String, Object> r = res.get(e.getKey());
      r.put("get_p50_ms", round(pct(lat, 50), 3));
      r.put("get_p95_ms", round(pct(lat, 95), 3));
    }
    LookupStats st = lsm.stats();
    res.get("lsm").put("files_read_per_get", round((double) (st.blocksRead + st.blockCacheHits) / n, 2));
    res.get("lsm").put("runs_skipped_pct", round(100.0 * (st.rangePruned + st.bloomPruned) / Math.max(1, st.runsChecked), 1));
    res.get("vanilla").put("files_read_per_get", round(vanillaFiles / n, 2));
    int differ = 0;
    for (int i = 0; i < n; i++) if (!Objects.equals(answers.get("lsm").get(i), answers.get("vanilla").get(i))) differ++;

    // range scans
    for (var e : stores.entrySet()) {
      Random r2 = new Random(a.get("seed") + 2);
      double[] lat = new double[a.get("scans")];
      for (int i = 0; i < lat.length; i++) {
        int lo = r2.nextInt(a.get("keys") - a.get("scan-width"));
        long t0 = System.nanoTime();
        e.getValue().scan(String.format("user%08d", lo), String.format("user%08d", lo + a.get("scan-width")));
        lat[i] = (System.nanoTime() - t0) / 1e6;
      }
      res.get(e.getKey()).put("scan_p50_ms", round(pct(lat, 50), 2));
    }
    lsm.close();

    System.out.printf("%nWorkload: %,d keys loaded + %,d updates/deletes, a file every %,d writes, block cache = %d blocks%n",
        a.get("keys"), a.get("updates"), a.get("batch"), a.get("cache-blocks"));
    System.out.printf("%-26s%16s%18s%n", "metric", "LSM-Iceberg", "Vanilla Iceberg");
    for (String m : List.of("write_ops_per_s", "data_files", "table_bytes", "get_p50_ms", "get_p95_ms",
        "files_read_per_get", "scan_p50_ms")) {
      System.out.printf("%-26s%16s%18s%n", m, res.get("lsm").get(m), res.get("vanilla").get(m));
    }
    System.out.printf("%-26s%16s%n", "runs skipped (range/bloom)", res.get("lsm").get("runs_skipped_pct") + "%");
    System.out.println("answers that differ between the two tables: " + differ + " / " + n);

    Map<String, Object> out = new LinkedHashMap<>();
    out.put("config", a);
    out.putAll(res);
    out.put("answers_differ", differ);
    Path dir = Path.of("results");
    Files.createDirectories(dir);
    Path f = dir.resolve(a.get("cache-blocks") > 0 ? "benchmark.json" : "benchmark_nocache.json");
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(f.toFile(), out);
    System.out.println("saved " + f);
  }

  private static long liveFiles(VanillaIcebergTable van) {
    try (var tasks = van.table().newScan().planFiles()) {
      return Tools.count(tasks);
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
