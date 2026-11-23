package cs631.lsm.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import cs631.lsm.Catalogs;
import cs631.lsm.LSMConfig;
import cs631.lsm.LSMTable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Use case: a customer-profile store fed by a change stream (CDC) from an online shop.
 *
 * <p>The shop's database emits an event whenever a customer signs up, changes plan, moves city,
 * spends money or closes the account. We keep the current profile of every customer in one
 * LSM-Iceberg table. Two kinds of users read the same table:
 *
 * <ul>
 *   <li>the support app looks customers up by id and needs answers in milliseconds (our LSM read path);
 *   <li>analysts run Spark SQL over all customers (revenue by city and plan), and can time-travel
 *       to earlier snapshots. See {@code ./run.sh spark}.
 * </ul>
 *
 * Value format: {@code city|plan|total_spend}. Writes a small JSON file with the snapshot ids the
 * Spark job compares (before and after the merge).
 *
 * <p>Usage: ./run.sh usecase [customers] [events]
 */
public class UseCase {
  static final String[] CITIES = {"Mumbai", "Delhi", "Bengaluru", "Chennai", "Kolkata", "Pune", "Hyderabad", "Ahmedabad"};
  static final String[] PLANS = {"free", "silver", "gold"};

  public static void main(String[] args) throws Exception {
    int customers = args.length > 0 ? Integer.parseInt(args[0]) : 50_000;
    int events = args.length > 1 ? Integer.parseInt(args[1]) : 200_000;
    Path warehouse = Path.of("warehouse/usecase").toAbsolutePath();
    Tools.deleteRecursively(warehouse);
    // Merges are switched off during ingest so the Spark job can compare "many overlapping L0
    // runs" with "after the merge". In normal use autoCompact stays on.
    // Block cache off, so every lookup really reads its block from the Avro file.
    LSMConfig cfg = new LSMConfig().memtableMaxEntries(10_000).autoCompact(false).blockCacheBlocks(0);
    Random rng = new Random(7);
    Map<String, Object> info = new LinkedHashMap<>();

    try (LSMTable t = new LSMTable(Catalogs.local(warehouse), "shop.customers", cfg)) {
      System.out.printf("Ingesting %,d sign-ups and %,d change events (CDC stream) ...%n", customers, events);
      String[] city = new String[customers];
      String[] plan = new String[customers];
      long[] spend = new long[customers];
      boolean[] closed = new boolean[customers];
      long t0 = System.nanoTime();
      for (int c = 0; c < customers; c++) {
        city[c] = CITIES[rng.nextInt(CITIES.length)];
        plan[c] = "free";
        t.put(id(c), city[c] + "|free|0");
      }
      for (int e = 0; e < events; e++) {
        int c = (int) (customers * Math.pow(rng.nextDouble(), 1.5)); // active customers change more often
        double u = rng.nextDouble();
        if (closed[c]) {
          closed[c] = false; // re-opens the account
          plan[c] = "free";
          spend[c] = 0;
        } else if (u < 0.02) {
          closed[c] = true;
          t.delete(id(c));
          continue;
        } else if (u < 0.12) {
          plan[c] = PLANS[rng.nextInt(PLANS.length)];
        } else if (u < 0.17) {
          city[c] = CITIES[rng.nextInt(CITIES.length)];
        } else {
          spend[c] += 100 + rng.nextInt(5000);
        }
        t.put(id(c), city[c] + "|" + plan[c] + "|" + spend[c]);
      }
      t.flush();
      double secs = (System.nanoTime() - t0) / 1e9;
      System.out.printf("  %,d writes in %.1f s (%,.0f writes/s)%n", customers + events, secs, (customers + events) / secs);
      System.out.println("  " + t.levelSummary());
      long before = t.table().currentSnapshot().snapshotId();
      info.put("snapshot_before_merge", before);
      info.put("files_before_merge", t.runCount());

      info.put("writes_per_s", Math.round((customers + events) / secs));
      System.out.println("\nSupport app: 2,000 lookups by customer id (block cache off)");
      info.put("lookup_ms_before_merge", lookups(t, customers, rng));
      int pb = planned(t.table(), id(12345));
      info.put("iceberg_files_planned_before_merge", pb);
      System.out.println("  files a plain Iceberg scan (e.g. Spark) plans for one customer: " + pb);

      System.out.println("\nMerging (compaction) ...");
      List<LSMTable.Compaction> merges = t.compact(true);
      info.put("merge_seconds", merges.stream().mapToDouble(LSMTable.Compaction::seconds).sum());
      info.put("versions_dropped", merges.stream().mapToLong(LSMTable.Compaction::versionsDropped).sum());
      info.put("tombstones_dropped", merges.stream().mapToLong(LSMTable.Compaction::tombstonesDropped).sum());
      for (LSMTable.Compaction c : merges) {
        System.out.printf("  L%d -> L%d: %d runs in, %d runs out, %,d old versions and %,d deleted customers dropped (%.2f s)%n",
            c.fromLevel(), c.toLevel(), c.inputRuns().size(), c.outputRuns().size(), c.versionsDropped(), c.tombstonesDropped(), c.seconds());
      }
      System.out.println("  " + t.levelSummary());
      info.put("snapshot_after_merge", t.table().currentSnapshot().snapshotId());
      info.put("files_after_merge", t.runCount());

      System.out.println("\nSupport app after the merge:");
      info.put("lookup_ms_after_merge", lookups(t, customers, rng));
      int pa = planned(t.table(), id(12345));
      info.put("iceberg_files_planned_after_merge", pa);
      System.out.println("  files a plain Iceberg scan (e.g. Spark) plans for one customer: " + pa);
      System.out.println("  customer " + id(12345) + " = " + t.get(id(12345)));
      info.put("example_customer", id(12345));
    }
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(warehouse.resolve("usecase.json").toFile(), info);
    java.nio.file.Files.createDirectories(Path.of("results"));
    new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of("results/usecase.json").toFile(), info);
    System.out.println("\nTable ready for Spark: ./run.sh spark");
  }

  static String id(int c) {
    return String.format("cust%08d", c);
  }

  static Map<String, Double> lookups(LSMTable t, int customers, Random rng) {
    double[] lat = new double[2000];
    for (int i = 0; i < 200; i++) t.get(id(rng.nextInt(customers))); // warm-up
    for (int i = 0; i < lat.length; i++) {
      long g0 = System.nanoTime();
      t.get(id(rng.nextInt(customers)));
      lat[i] = (System.nanoTime() - g0) / 1e6;
    }
    Arrays.sort(lat);
    double p50 = lat[lat.length / 2], p95 = lat[(int) (lat.length * 0.95)];
    System.out.printf("  p50 %.3f ms, p95 %.3f ms%n", p50, p95);
    return Map.of("p50", Math.round(p50 * 1000) / 1000.0, "p95", Math.round(p95 * 1000) / 1000.0);
  }

  static int planned(Table table, String key) throws IOException {
    List<FileScanTask> tasks = new ArrayList<>();
    try (CloseableIterable<FileScanTask> it = table.newScan().filter(Expressions.equal("key", key)).planFiles()) {
      it.forEach(tasks::add);
    }
    return tasks.size();
  }
}
