package cs631.lsm;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Catalog;

/**
 * Baseline 1, "append": every micro-batch is appended as a new Parquet file (rows in arrival
 * order, all versions kept, seq/op columns like the LSM table). Readers resolve the newest version.
 * This is the cheapest way to ingest updates into Iceberg, and the slowest to read.
 */
public class VanillaIcebergTable extends IcebergBaseline {
  public VanillaIcebergTable(Catalog catalog, String identifier, int batchSize) {
    super(catalog, identifier, batchSize, Map.of());
    Snapshot s = table.currentSnapshot();
    this.seq = s == null ? 0 : Long.parseLong(s.summary().getOrDefault("max-seq", "0"));
  }

  @Override
  protected void commitBatch(List<KeyValue> batch) {
    String path = table.locationProvider().newDataLocation("batch-" + UUID.randomUUID() + ".parquet");
    DataFile df = KvSchema.writeParquet(table.io().newOutputFile(path), batch, 128L * 1024 * 1024, null);
    bytesWritten += df.fileSizeInBytes();
    rowsWritten += batch.size();
    table.newAppend().appendFile(df).set("max-seq", Long.toString(seq)).commit();
  }
}
