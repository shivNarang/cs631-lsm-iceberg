package cs631.lsm;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

/** The Iceberg schema shared by the LSM table and the vanilla baseline, and a Parquet file writer. */
public final class KvSchema {
  private KvSchema() {}

  public static final int KEY_FIELD_ID = 1;

  /** op: 0 = put, 1 = delete. The current value of a key is its row with the highest seq. */
  public static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "key", Types.StringType.get()),
      Types.NestedField.optional(2, "value", Types.StringType.get()),
      Types.NestedField.required(3, "seq", Types.LongType.get()),
      Types.NestedField.required(4, "op", Types.IntegerType.get()));

  public static final SortOrder SORT_BY_KEY = SortOrder.builderFor(SCHEMA).asc("key").build();

  /** Write rows (already in the desired order) to one Parquet file and describe it as an Iceberg DataFile. */
  public static DataFile writeParquet(OutputFile out, List<KeyValue> rows, long rowGroupBytes, SortOrder sortOrder) {
    GenericRecord template = GenericRecord.create(SCHEMA);
    FileAppender<Record> appender;
    try {
      appender = Parquet.write(out)
          .schema(SCHEMA)
          .createWriterFunc(GenericParquetWriter::create)
          .setAll(Map.of(
              TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(rowGroupBytes),
              TableProperties.PARQUET_COMPRESSION, "zstd"))
          .overwrite()
          .build();
      for (KeyValue kv : rows) {
        Record r = template.copy();
        r.set(0, kv.key());
        r.set(1, kv.value());
        r.set(2, kv.seq());
        r.set(3, (int) kv.op());
        appender.add(r);
      }
      appender.close();
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
    DataFiles.Builder b = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(out.location())
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(appender.length())
        .withMetrics(appender.metrics())
        .withSplitOffsets(appender.splitOffsets());
    if (sortOrder != null) b.withSortOrder(sortOrder);
    return b.build();
  }
}
