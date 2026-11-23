package cs631.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

/**
 * Format-specific part of a sorted run: write key-sorted rows as one Avro or Parquet data file
 * and read back exactly one block.
 *
 * <p>Avro: rows are written in blocks we close ourselves (DataFileWriter.sync()), so we know the
 * byte offset where every block starts. A lookup seeks to that offset and decodes one block. Avro
 * is row-oriented, so a block is decoded in one pass with no per-column pages to assemble.
 *
 * <p>Parquet: one block = one row group; the "offset" is the row-group number.
 */
public final class RunFiles {
  private RunFiles() {}

  /** Where the blocks of a written file are: blockOffsets[i] and the number of rows in block i. */
  public record Layout(DataFile dataFile, long[] blockOffsets, int[] blockRows) {}

  public interface Reader extends AutoCloseable {
    BlockCache.Block readBlock(int block);

    @Override
    void close();
  }

  static final Schema AVRO_SCHEMA = AvroSchemaUtil.convert(KvSchema.SCHEMA, "kv");

  public static String extension(LSMConfig.Format f) {
    return f == LSMConfig.Format.AVRO ? ".avro" : ".parquet";
  }

  public static Layout write(FileIO io, Configuration conf, String path, List<KeyValue> rows, LSMConfig cfg,
                             SortOrder sortOrder, LSMConfig.Format format) {
    return format == LSMConfig.Format.AVRO
        ? writeAvro(io, path, rows, cfg.blockBytes, cfg.avroCodec, sortOrder)
        : writeParquet(io, conf, path, rows, cfg, sortOrder);
  }

  public static Reader open(FileIO io, Configuration conf, RunMeta meta, long[] offsets, int[] rows) {
    return "PARQUET".equals(meta.format()) ? new ParquetReader(conf, meta.dataFile()) : new AvroReader(io, meta.dataFile(), offsets, rows);
  }

  // ------------------------------------------------------------------ Avro

  static Layout writeAvro(FileIO io, String path, List<KeyValue> rows, long blockBytes, String codec, SortOrder sortOrder) {
    List<Long> offsets = new ArrayList<>();
    List<Integer> counts = new ArrayList<>();
    long tombstones = 0, minSeq = Long.MAX_VALUE, maxSeq = Long.MIN_VALUE;
    int minOp = Integer.MAX_VALUE, maxOp = Integer.MIN_VALUE;
    try (PositionOutputStream os = io.newOutputFile(path).createOrOverwrite();
         DataFileWriter<GenericData.Record> w = new DataFileWriter<>(new GenericDatumWriter<GenericData.Record>(AVRO_SCHEMA))) {
      w.setCodec(CodecFactory.fromString(codec));
      // We end blocks ourselves; the interval is only a safety cap (Avro preallocates a buffer this big).
      w.setSyncInterval((int) Math.min(64L << 20, Math.max(4096, blockBytes * 4)));
      w.create(AVRO_SCHEMA, os);
      GenericData.Record rec = new GenericData.Record(AVRO_SCHEMA);
      int inBlock = 0;
      long blockSize = 0;
      for (KeyValue kv : rows) {
        if (inBlock == 0) offsets.add(w.sync()); // ends the previous block; returns where the next one starts
        rec.put(0, kv.key());
        rec.put(1, kv.value());
        rec.put(2, kv.seq());
        rec.put(3, (int) kv.op());
        w.append(rec);
        inBlock++;
        blockSize += kv.key().length() + (kv.value() == null ? 1 : kv.value().length() + 1) + 12;
        if (kv.op() == Entry.DELETE) tombstones++;
        minSeq = Math.min(minSeq, kv.seq());
        maxSeq = Math.max(maxSeq, kv.seq());
        minOp = Math.min(minOp, kv.op());
        maxOp = Math.max(maxOp, kv.op());
        if (blockSize >= blockBytes) {
          counts.add(inBlock);
          inBlock = 0;
          blockSize = 0;
        }
      }
      if (inBlock > 0) counts.add(inBlock);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    long n = rows.size();
    // Iceberg's own Avro writer records only row counts. We add key/seq/op bounds so any engine
    // (Spark, Trino) can still skip this file with min/max pruning.
    Map<Integer, Long> valueCounts = Map.of(1, n, 2, n, 3, n, 4, n);
    Map<Integer, Long> nullCounts = Map.of(1, 0L, 2, tombstones, 3, 0L, 4, 0L);
    Map<Integer, java.nio.ByteBuffer> lower = new HashMap<>();
    Map<Integer, java.nio.ByteBuffer> upper = new HashMap<>();
    if (n > 0) {
      lower.put(1, Conversions.toByteBuffer(Types.StringType.get(), rows.get(0).key()));
      upper.put(1, Conversions.toByteBuffer(Types.StringType.get(), rows.get(rows.size() - 1).key()));
      lower.put(3, Conversions.toByteBuffer(Types.LongType.get(), minSeq));
      upper.put(3, Conversions.toByteBuffer(Types.LongType.get(), maxSeq));
      lower.put(4, Conversions.toByteBuffer(Types.IntegerType.get(), minOp));
      upper.put(4, Conversions.toByteBuffer(Types.IntegerType.get(), maxOp));
    }
    long[] off = offsets.stream().mapToLong(Long::longValue).toArray();
    DataFiles.Builder b = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(path)
        .withFormat(FileFormat.AVRO)
        .withFileSizeInBytes(io.newInputFile(path).getLength())
        .withMetrics(new Metrics(n, new HashMap<>(), valueCounts, nullCounts, new HashMap<>(), lower, upper));
    // No split offsets: Avro readers start a split at the first sync marker AFTER the offset, so
    // block start offsets would make Iceberg skip the block that starts there.
    if (sortOrder != null) b.withSortOrder(sortOrder);
    return new Layout(b.build(), off, counts.stream().mapToInt(Integer::intValue).toArray());
  }

  /** Avro's SeekableInput on top of an Iceberg InputFile (works for local files and object stores). */
  private static final class IcebergSeekableInput implements SeekableInput {
    private final SeekableInputStream in;
    private final long length;

    IcebergSeekableInput(InputFile f) {
      this.length = f.getLength();
      this.in = f.newStream();
    }

    @Override public void seek(long p) throws IOException { in.seek(p); }
    @Override public long tell() throws IOException { return in.getPos(); }
    @Override public long length() { return length; }
    @Override public int read(byte[] b, int off, int len) throws IOException { return in.read(b, off, len); }
    @Override public void close() throws IOException { in.close(); }
  }

  static final class AvroReader implements Reader {
    private final DataFileReader<GenericRecord> r;
    private final long[] offsets;
    private final int[] rows;
    private GenericRecord reuse;

    AvroReader(FileIO io, String path, long[] offsets, int[] rows) {
      this.offsets = offsets;
      this.rows = rows;
      try {
        this.r = new DataFileReader<>(new IcebergSeekableInput(io.newInputFile(path)), new GenericDatumReader<>(AVRO_SCHEMA));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public synchronized BlockCache.Block readBlock(int block) {
      try {
        r.seek(offsets[block]);
        int n = rows[block];
        String[] keys = new String[n];
        long[] seqs = new long[n];
        byte[] ops = new byte[n];
        String[] values = new String[n];
        for (int i = 0; i < n; i++) {
          reuse = r.next(reuse);
          keys[i] = reuse.get(0).toString();
          Object v = reuse.get(1);
          values[i] = v == null ? null : v.toString();
          seqs[i] = (Long) reuse.get(2);
          ops[i] = (byte) (int) (Integer) reuse.get(3);
        }
        return new BlockCache.Block(keys, seqs, ops, values);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      try {
        r.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  /** Read a whole Avro run block by block (used to rebuild a lost index). */
  static List<BlockCache.Block> readAllAvroBlocks(FileIO io, String path, List<Long> offsetsOut) {
    List<BlockCache.Block> out = new ArrayList<>();
    List<KeyValue> cur = new ArrayList<>();
    long curStart = -1;
    try (DataFileReader<GenericRecord> r =
             new DataFileReader<>(new IcebergSeekableInput(io.newInputFile(path)), new GenericDatumReader<>(AVRO_SCHEMA))) {
      GenericRecord rec = null;
      while (r.hasNext()) {
        long start = r.previousSync(); // read before next(): Avro moves it as soon as a block's last row is read
        rec = r.next(rec);
        if (start != curStart) {
          if (!cur.isEmpty()) out.add(toBlock(cur));
          cur = new ArrayList<>();
          curStart = start;
          offsetsOut.add(start);
        }
        Object v = rec.get(1);
        cur.add(new KeyValue(rec.get(0).toString(), (Long) rec.get(2), (byte) (int) (Integer) rec.get(3),
            v == null ? null : v.toString()));
      }
      if (!cur.isEmpty()) out.add(toBlock(cur));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out;
  }

  private static BlockCache.Block toBlock(List<KeyValue> rows) {
    int n = rows.size();
    String[] keys = new String[n];
    long[] seqs = new long[n];
    byte[] ops = new byte[n];
    String[] values = new String[n];
    for (int i = 0; i < n; i++) {
      KeyValue kv = rows.get(i);
      keys[i] = kv.key();
      seqs[i] = kv.seq();
      ops[i] = kv.op();
      values[i] = kv.value();
    }
    return new BlockCache.Block(keys, seqs, ops, values);
  }

  // --------------------------------------------------------------- Parquet

  static Layout writeParquet(FileIO io, Configuration conf, String path, List<KeyValue> rows, LSMConfig cfg, SortOrder sortOrder) {
    DataFile df = KvSchema.writeParquet(io.newOutputFile(path), rows, cfg.blockBytes, cfg.parquetCodec, sortOrder);
    try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path), conf))) {
      List<BlockMetaData> blocks = r.getFooter().getBlocks();
      long[] off = new long[blocks.size()];
      int[] counts = new int[blocks.size()];
      for (int i = 0; i < off.length; i++) {
        off[i] = i;
        counts[i] = (int) blocks.get(i).getRowCount();
      }
      return new Layout(df, off, counts);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static final class ParquetReader implements Reader {
    private final ParquetFileReader reader;
    private final MessageType schema;

    ParquetReader(Configuration conf, String path) {
      try {
        reader = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path), conf));
        schema = reader.getFooter().getFileMetaData().getSchema();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    int rowGroups() {
      return reader.getRowGroups().size();
    }

    @Override
    public synchronized BlockCache.Block readBlock(int rowGroup) {
      try {
        PageReadStore pages = reader.readRowGroup(rowGroup);
        int n = (int) pages.getRowCount();
        MessageColumnIO cio = new ColumnIOFactory().getColumnIO(schema);
        RecordReader<Group> rr = cio.getRecordReader(pages, new GroupRecordConverter(schema));
        String[] keys = new String[n];
        long[] seqs = new long[n];
        byte[] ops = new byte[n];
        String[] values = new String[n];
        for (int i = 0; i < n; i++) {
          Group g = rr.read();
          keys[i] = g.getString("key", 0);
          values[i] = g.getFieldRepetitionCount("value") == 0 ? null : g.getString("value", 0);
          seqs[i] = g.getLong("seq", 0);
          ops[i] = (byte) g.getInteger("op", 0);
        }
        return new BlockCache.Block(keys, seqs, ops, values);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      try {
        reader.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  /** Read a whole Parquet run row group by row group (used to rebuild a lost index). */
  static List<BlockCache.Block> readAllParquetBlocks(Configuration conf, String path) {
    try (ParquetReader r = new ParquetReader(conf, path)) {
      List<BlockCache.Block> out = new ArrayList<>();
      for (int i = 0; i < r.rowGroups(); i++) out.add(r.readBlock(i));
      return out;
    }
  }
}
