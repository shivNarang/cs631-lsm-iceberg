package cs631.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Shared part of the plain-Iceberg baselines: same schema as the LSM table, writes buffered in
 * micro-batches of `batchSize` (as a streaming job would commit them), reads through a normal
 * Iceberg scan, which skips files with min/max statistics and applies delete files.
 */
abstract class IcebergBaseline implements KvStore {
  protected final Table table;
  protected final int batchSize;
  /** Newest buffered version of each key in the current batch, in arrival order. */
  protected final LinkedHashMap<String, KeyValue> buffer = new LinkedHashMap<>();
  protected long seq;
  protected long bytesWritten;
  protected long rowsWritten;
  private double filesLast;

  IcebergBaseline(Catalog catalog, String identifier, int batchSize, Map<String, String> props) {
    TableIdentifier id = TableIdentifier.parse(identifier);
    this.table = catalog.tableExists(id) ? catalog.loadTable(id)
        : catalog.buildTable(id, KvSchema.SCHEMA).withProperty("format-version", "2").withProperties(props).create();
    this.batchSize = batchSize;
  }

  @Override
  public void put(String key, String value) {
    write(key, Entry.PUT, value);
  }

  @Override
  public void delete(String key) {
    write(key, Entry.DELETE, null);
  }

  private void write(String key, byte op, String value) {
    buffer.remove(key);
    buffer.put(key, new KeyValue(key, ++seq, op, value));
    if (buffer.size() >= batchSize) flush();
  }

  @Override
  public void flush() {
    if (buffer.isEmpty()) return;
    commitBatch(new ArrayList<>(buffer.values()));
    buffer.clear();
  }

  /** Write one micro-batch (newest version of each key, arrival order) to the table. */
  protected abstract void commitBatch(List<KeyValue> batch);

  /** Rows matching the filter as Iceberg returns them (delete files already applied). */
  protected List<Record> read(Expression filter) {
    double files = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().filter(filter).planFiles()) {
      for (FileScanTask t : tasks) files += 1 + t.deletes().size();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    filesLast = files;
    List<Record> out = new ArrayList<>();
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).where(filter).build()) {
      rows.forEach(r -> out.add(r.copy()));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out;
  }

  /** Keep, for each key, the row with the highest seq; return the live values in key order. */
  protected static Map<String, String> resolve(List<Record> rows, String lo, String hi) {
    Map<String, Record> latest = new java.util.TreeMap<>();
    for (Record r : rows) {
      String k = r.getField("key").toString();
      if ((lo != null && k.compareTo(lo) < 0) || (hi != null && k.compareTo(hi) > 0)) continue;
      Record cur = latest.get(k);
      if (cur == null || (long) r.getField("seq") > (long) cur.getField("seq")) latest.put(k, r);
    }
    Map<String, String> out = new java.util.TreeMap<>();
    latest.forEach((k, r) -> {
      if ((int) r.getField("op") == Entry.PUT) out.put(k, r.getField("value").toString());
    });
    return out;
  }

  @Override
  public String get(String key) {
    return resolve(read(Expressions.equal("key", key)), key, key).get(key);
  }

  @Override
  public Map<String, String> scan(String lo, String hi) {
    return resolve(read(Expressions.and(Expressions.greaterThanOrEqual("key", lo), Expressions.lessThanOrEqual("key", hi))), lo, hi);
  }

  @Override
  public long bytesWritten() {
    return bytesWritten;
  }

  @Override
  public long rowsWritten() {
    return rowsWritten;
  }

  @Override
  public double filesReadLastGet() {
    return filesLast;
  }

  @Override
  public long dataFiles() {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      long n = 0;
      for (FileScanTask ignored : tasks) n++;
      return n;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Table table() {
    return table;
  }
}
