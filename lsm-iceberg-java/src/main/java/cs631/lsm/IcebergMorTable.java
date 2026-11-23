package cs631.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedFiles;

/**
 * Baseline 2, Iceberg merge-on-read upserts (what Flink's upsert mode does): each micro-batch
 * commits one data file with the new rows plus one equality-delete file on {@code key} that hides
 * every older row of those keys. Readers apply the delete files while scanning.
 */
public class IcebergMorTable extends IcebergBaseline {
  private static final Schema KEY_ONLY = KvSchema.SCHEMA.select("key");
  private final GenericAppenderFactory factory;

  public IcebergMorTable(Catalog catalog, String identifier, int batchSize) {
    super(catalog, identifier, batchSize, Map.of("write.delete.mode", "merge-on-read", "write.update.mode", "merge-on-read"));
    this.factory = new GenericAppenderFactory(KvSchema.SCHEMA, table.spec(), new int[] {KvSchema.KEY_FIELD_ID}, KEY_ONLY, null);
  }

  @Override
  protected void commitBatch(List<KeyValue> batch) {
    List<KeyValue> puts = new ArrayList<>();
    for (KeyValue kv : batch) if (kv.op() == Entry.PUT) puts.add(kv);
    RowDelta delta = table.newRowDelta();
    if (!puts.isEmpty()) {
      String path = table.locationProvider().newDataLocation("mor-data-" + UUID.randomUUID() + ".parquet");
      DataFile df = KvSchema.writeParquet(table.io().newOutputFile(path), puts, 128L * 1024 * 1024, null);
      bytesWritten += df.fileSizeInBytes();
      rowsWritten += puts.size();
      delta.addRows(df);
    }
    // Deletes in the same commit only hide rows of older commits, so the new rows stay visible.
    String delPath = table.locationProvider().newDataLocation("mor-eq-delete-" + UUID.randomUUID() + ".parquet");
    EqualityDeleteWriter<Record> w = factory.newEqDeleteWriter(
        EncryptedFiles.plainAsEncryptedOutput(table.io().newOutputFile(delPath)), FileFormat.PARQUET, null);
    GenericRecord rec = GenericRecord.create(KEY_ONLY);
    try (w) {
      for (KeyValue kv : batch) {
        Record r = rec.copy();
        r.set(0, kv.key());
        w.write(r);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    DeleteFile del = w.toDeleteFile();
    bytesWritten += del.fileSizeInBytes();
    rowsWritten += batch.size();
    delta.addDeletes(del).commit();
  }
}
