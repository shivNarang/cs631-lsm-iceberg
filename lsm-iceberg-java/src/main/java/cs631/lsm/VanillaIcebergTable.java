package cs631.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Vanilla Iceberg baseline: same schema, writes appended in micro-batches as a streaming job
 * would (rows in arrival order), reads through a normal Iceberg scan with min/max pruning.
 */
public class VanillaIcebergTable {
  private final Table table;
  private final int batchSize;
  private final List<KeyValue> buf = new ArrayList<>();
  private long seq;
  private int filesScannedLast;

  public VanillaIcebergTable(Catalog catalog, String identifier, int batchSize) {
    TableIdentifier id = TableIdentifier.parse(identifier);
    this.table = catalog.tableExists(id) ? catalog.loadTable(id)
        : catalog.buildTable(id, KvSchema.SCHEMA).withProperty("format-version", "2").create();
    this.batchSize = batchSize;
    Snapshot s = table.currentSnapshot();
    this.seq = s == null ? 0 : Long.parseLong(s.summary().getOrDefault("max-seq", "0"));
  }

  public void put(String key, String value) { write(key, Entry.PUT, value); }

  public void delete(String key) { write(key, Entry.DELETE, null); }

  private void write(String key, byte op, String value) {
    buf.add(new KeyValue(key, ++seq, op, value));
    if (buf.size() >= batchSize) flush();
  }

  public void flush() {
    if (buf.isEmpty()) return;
    String path = table.locationProvider().newDataLocation("batch-" + UUID.randomUUID() + ".parquet");
    var df = KvSchema.writeParquet(table.io().newOutputFile(path), buf, 128L * 1024 * 1024, null);
    table.newAppend().appendFile(df).set("max-seq", Long.toString(seq)).commit();
    buf.clear();
  }

  private List<Record> read(Expression filter) {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
      int n = 0;
      for (FileScanTask ignored : tasks) n++;
      filesScannedLast = n;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    List<Record> out = new ArrayList<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).where(filter).build()) {
      rows.forEach(r -> out.add(r.copy()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out;
  }

  public String get(String key) {
    Record best = null;
    for (Record r : read(Expressions.equal("key", key))) {
      if (!key.equals(r.getField("key").toString())) continue; // keep only exact matches
      if (best == null || (long) r.getField("seq") > (long) best.getField("seq")) best = r;
    }
    if (best == null || (int) best.getField("op") != Entry.PUT) return null;
    return best.getField("value").toString();
  }

  public Map<String, String> scan(String lo, String hi) {
    Map<String, Record> latest = new HashMap<>();
    for (Record r : read(Expressions.and(Expressions.greaterThanOrEqual("key", lo), Expressions.lessThanOrEqual("key", hi)))) {
      String k = r.getField("key").toString();
      if (k.compareTo(lo) < 0 || k.compareTo(hi) > 0) continue;
      Record cur = latest.get(k);
      if (cur == null || (long) r.getField("seq") > (long) cur.getField("seq")) latest.put(k, r);
    }
    Map<String, String> out = new TreeMap<>();
    latest.forEach((k, r) -> {
      if ((int) r.getField("op") == Entry.PUT) out.put(k, r.getField("value").toString());
    });
    return out;
  }

  public int filesScannedLast() { return filesScannedLast; }

  public Table table() { return table; }
}
