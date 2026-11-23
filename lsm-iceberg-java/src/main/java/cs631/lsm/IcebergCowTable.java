package cs631.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * Baseline 3, Iceberg copy-on-write: the table is kept as key-sorted, non-overlapping Parquet
 * files of up to `fileRows` rows. Each micro-batch rewrites every file whose key range it touches
 * (one Iceberg rewrite commit). Reads are fast (min/max pruning finds one file) but every batch of
 * random updates rewrites most of the table.
 */
public class IcebergCowTable extends IcebergBaseline {
  private final int fileRows;

  public IcebergCowTable(Catalog catalog, String identifier, int batchSize, int fileRows) {
    super(catalog, identifier, batchSize, Map.of("write.delete.mode", "copy-on-write", "write.update.mode", "copy-on-write"));
    this.fileRows = fileRows;
  }

  private record Live(DataFile file, String lo) {}

  @Override
  protected void commitBatch(List<KeyValue> batch) {
    // Current files sorted by their lowest key; file i owns keys in [lo_i, lo_{i+1}).
    List<Live> files = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      for (FileScanTask t : tasks) {
        String lo = Conversions.fromByteBuffer(Types.StringType.get(), t.file().lowerBounds().get(KvSchema.KEY_FIELD_ID)).toString();
        files.add(new Live(t.file(), lo));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    files.sort((a, b) -> a.lo().compareTo(b.lo()));
    boolean[] touched = new boolean[files.size()];
    for (KeyValue kv : batch) {
      int owner = 0;
      for (int i = 1; i < files.size() && files.get(i).lo().compareTo(kv.key()) <= 0; i++) owner = i;
      if (!files.isEmpty()) touched[owner] = true;
    }
    TreeMap<String, KeyValue> rows = new TreeMap<>();
    List<DataFile> replaced = new ArrayList<>();
    for (int i = 0; i < files.size(); i++) {
      if (!touched[i]) continue;
      replaced.add(files.get(i).file());
      try (CloseableIterable<Record> it = Parquet.read(table.io().newInputFile(files.get(i).file().location()))
          .project(KvSchema.SCHEMA)
          .createReaderFunc(fs -> GenericParquetReaders.buildReader(KvSchema.SCHEMA, fs))
          .build()) {
        for (Record r : it) {
          String k = r.getField("key").toString();
          rows.put(k, new KeyValue(k, (long) r.getField("seq"), Entry.PUT, r.getField("value").toString()));
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    for (KeyValue kv : batch) {
      if (kv.op() == Entry.DELETE) rows.remove(kv.key());
      else rows.put(kv.key(), kv);
    }
    List<DataFile> added = new ArrayList<>();
    List<KeyValue> chunk = new ArrayList<>();
    for (KeyValue kv : rows.values()) {
      chunk.add(kv);
      if (chunk.size() >= fileRows) {
        added.add(writeSorted(chunk));
        chunk = new ArrayList<>();
      }
    }
    if (!chunk.isEmpty()) added.add(writeSorted(chunk));
    if (replaced.isEmpty()) {
      var append = table.newAppend();
      added.forEach(append::appendFile);
      append.commit();
    } else {
      // An upsert batch changes the data, so it is an Iceberg "overwrite" (a "replace" must keep the rows).
      OverwriteFiles ow = table.newOverwrite();
      replaced.forEach(ow::deleteFile);
      added.forEach(ow::addFile);
      ow.commit();
    }
  }

  private DataFile writeSorted(List<KeyValue> rows) {
    String path = table.locationProvider().newDataLocation("cow-" + UUID.randomUUID() + ".parquet");
    DataFile df = KvSchema.writeParquet(table.io().newOutputFile(path), rows, 128L * 1024 * 1024, KvSchema.SORT_BY_KEY);
    bytesWritten += df.fileSizeInBytes();
    rowsWritten += rows.size();
    return df;
  }
}
