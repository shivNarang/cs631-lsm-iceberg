package cs631.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.util.Pair;
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
 * A sorted run (SSTable): one key-sorted Parquet file registered in Iceberg, plus a Puffin index
 * file with two blobs: a bloom filter over the keys, and fence pointers (the first key of every
 * Parquet row group). A point lookup binary-searches the fence pointers and reads exactly one row
 * group.
 */
public class SortedRun implements AutoCloseable {
  public static final String BLOOM_BLOB = "cs631-lsm-bloom-filter-v1";
  public static final String FENCE_BLOB = "cs631-lsm-fence-pointers-v1";

  /** Everything produced by writing a run's data file (the index is written after the commit). */
  public record Written(RunMeta meta, DataFile dataFile, BloomFilter bloom, String[] fences) {}

  private final RunMeta meta;
  private final BloomFilter bloom;
  private final String[] fences;
  private final Configuration conf;
  private ParquetFileReader reader;
  private MessageType parquetSchema;

  public SortedRun(RunMeta meta, BloomFilter bloom, String[] fences, Configuration conf) {
    this.meta = meta;
    this.bloom = bloom;
    this.fences = fences;
    this.conf = conf;
  }

  public RunMeta meta() {
    return meta;
  }

  public BloomFilter bloom() {
    return bloom;
  }

  public String[] fences() {
    return fences;
  }

  // ---------------------------------------------------------------- writing

  public static Written write(FileIO io, Configuration conf, List<KeyValue> sortedRows, long runId, int level,
                              String dataPath, String indexPath, LSMConfig cfg, SortOrder sortOrder) {
    DataFile df = KvSchema.writeParquet(io.newOutputFile(dataPath), sortedRows, cfg.rowGroupBytes, sortOrder);
    String[] keys = new String[sortedRows.size()];
    long minSeq = Long.MAX_VALUE, maxSeq = Long.MIN_VALUE, tombstones = 0;
    for (int i = 0; i < keys.length; i++) {
      KeyValue kv = sortedRows.get(i);
      keys[i] = kv.key();
      minSeq = Math.min(minSeq, kv.seq());
      maxSeq = Math.max(maxSeq, kv.seq());
      if (kv.op() == Entry.DELETE) tombstones++;
    }
    BloomFilter bloom = BloomFilter.forCapacity(keys.length, cfg.bloomFpr);
    for (String k : keys) bloom.add(k);
    // Fence pointers: row-group sizes come from the Parquet footer, first keys from our sorted input.
    long[] rowCounts = rowGroupRowCounts(dataPath, conf);
    String[] fences = new String[rowCounts.length];
    long start = 0;
    for (int i = 0; i < rowCounts.length; i++) {
      fences[i] = keys[(int) start];
      start += rowCounts[i];
    }
    RunMeta meta = new RunMeta(runId, level, dataPath, indexPath, keys[0], keys[keys.length - 1], minSeq, maxSeq,
        keys.length, tombstones, fences.length, df.fileSizeInBytes());
    return new Written(meta, df, bloom, fences);
  }

  private static long[] rowGroupRowCounts(String path, Configuration conf) {
    try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(path), conf))) {
      return r.getFooter().getBlocks().stream().mapToLong(BlockMetaData::getRowCount).toArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Write the Puffin index and return it as an Iceberg statistics file for the given snapshot. */
  public static StatisticsFile writeIndex(FileIO io, RunMeta meta, BloomFilter bloom, String[] fences,
                                          long snapshotId, long sequenceNumber) {
    Map<String, String> props = Map.of(
        "run-id", Long.toString(meta.runId()), "level", Integer.toString(meta.level()),
        "min-key", meta.minKey(), "max-key", meta.maxKey());
    List<Integer> fields = List.of(KvSchema.KEY_FIELD_ID);
    try {
      io.deleteFile(meta.indexFile()); // Puffin writers never overwrite (e.g. when rebuilding a corrupt index)
    } catch (RuntimeException ignored) {
      // not there
    }
    PuffinWriter w = Puffin.write(io.newOutputFile(meta.indexFile())).createdBy("cs631-lsm-iceberg").build();
    try (w) {
      w.add(new Blob(BLOOM_BLOB, fields, snapshotId, sequenceNumber, bloom.toBytes(), null, props));
      w.add(new Blob(FENCE_BLOB, fields, snapshotId, sequenceNumber, encodeFences(fences), null, props));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    List<org.apache.iceberg.BlobMetadata> blobs = new ArrayList<>();
    for (BlobMetadata bm : w.writtenBlobsMetadata()) blobs.add(GenericBlobMetadata.from(bm));
    return new GenericStatisticsFile(snapshotId, meta.indexFile(), w.fileSize(), w.footerSize(), blobs);
  }

  // ---------------------------------------------------------------- loading

  /** Load the run's Puffin index; if it is missing or unreadable, rebuild it from the Parquet file. */
  public static SortedRun load(FileIO io, Configuration conf, RunMeta meta, LSMConfig cfg) {
    try (PuffinReader r = Puffin.read(io.newInputFile(meta.indexFile())).build()) {
      BloomFilter bloom = null;
      String[] fences = null;
      for (Pair<BlobMetadata, ByteBuffer> p : r.readAll(r.fileMetadata().blobs())) {
        if (p.first().type().equals(BLOOM_BLOB)) bloom = BloomFilter.fromBytes(p.second());
        if (p.first().type().equals(FENCE_BLOB)) fences = decodeFences(p.second());
      }
      if (bloom != null && fences != null) return new SortedRun(meta, bloom, fences, conf);
    } catch (RuntimeException | IOException e) {
      // fall through to rebuild
    }
    SortedRun tmp = new SortedRun(meta, null, null, conf);
    List<String> keys = new ArrayList<>();
    List<String> fences = new ArrayList<>();
    for (int rg = 0; rg < tmp.rowGroupCount(); rg++) {
      BlockCache.Block b = tmp.readBlock(rg);
      fences.add(b.keys()[0]);
      keys.addAll(Arrays.asList(b.keys()));
    }
    tmp.close();
    BloomFilter bloom = BloomFilter.forCapacity(keys.size(), cfg.bloomFpr);
    keys.forEach(bloom::add);
    String[] f = fences.toArray(new String[0]);
    writeIndex(io, meta, bloom, f, -1, -1);
    return new SortedRun(meta, bloom, f, conf);
  }

  private static ByteBuffer encodeFences(String[] fences) {
    List<byte[]> enc = new ArrayList<>();
    int size = 4;
    for (String f : fences) {
      byte[] b = f.getBytes(StandardCharsets.UTF_8);
      enc.add(b);
      size += 4 + b.length;
    }
    ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(fences.length);
    for (byte[] b : enc) buf.putInt(b.length).put(b);
    buf.flip();
    return buf;
  }

  private static String[] decodeFences(ByteBuffer in) {
    ByteBuffer buf = in.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    String[] out = new String[buf.getInt()];
    for (int i = 0; i < out.length; i++) {
      byte[] b = new byte[buf.getInt()];
      buf.get(b);
      out[i] = new String(b, StandardCharsets.UTF_8);
    }
    return out;
  }

  // ---------------------------------------------------------------- reading

  private ParquetFileReader reader() {
    if (reader == null) {
      try {
        reader = ParquetFileReader.open(HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(meta.dataFile()), conf));
        parquetSchema = reader.getFooter().getFileMetaData().getSchema();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return reader;
  }

  int rowGroupCount() {
    return reader().getRowGroups().size();
  }

  /** Decode exactly one Parquet row group. */
  BlockCache.Block readBlock(int rowGroup) {
    try {
      ParquetFileReader r = reader();
      PageReadStore pages = r.readRowGroup(rowGroup);
      int n = (int) pages.getRowCount();
      MessageColumnIO io = new ColumnIOFactory().getColumnIO(parquetSchema);
      RecordReader<Group> rr = io.getRecordReader(pages, new GroupRecordConverter(parquetSchema));
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

  private BlockCache.Block block(int rg, BlockCache cache, LookupStats stats) {
    BlockCache.Block b = cache.get(meta.runId(), rg);
    if (b != null) {
      if (stats != null) stats.blockCacheHits++;
      return b;
    }
    b = readBlock(rg);
    cache.put(meta.runId(), rg, b);
    if (stats != null) stats.blocksRead++;
    return b;
  }

  /** Index of the last fence <= key (the row group that would hold key), or -1. */
  private int floorFence(String key) {
    int i = Arrays.binarySearch(fences, key);
    return i >= 0 ? i : -i - 2;
  }

  /** Point lookup in this run. Returns the stored version, or null if the run doesn't have the key. */
  public KeyValue get(String key, BlockCache cache, LookupStats stats, List<String> trace) {
    String name = "run " + meta.runId() + " (L" + meta.level() + ")";
    stats.runsChecked++;
    if (key.compareTo(meta.minKey()) < 0 || key.compareTo(meta.maxKey()) > 0) {
      stats.rangePruned++;
      if (trace != null) trace.add(name + ": key outside [" + meta.minKey() + ", " + meta.maxKey() + "] -> skip");
      return null;
    }
    if (!bloom.mightContain(key)) {
      stats.bloomPruned++;
      if (trace != null) trace.add(name + ": bloom filter says NO -> skip");
      return null;
    }
    int rg = floorFence(key);
    BlockCache.Block b = block(rg, cache, stats);
    int i = Arrays.binarySearch(b.keys(), key);
    if (i >= 0) {
      if (trace != null) trace.add(name + ": bloom MAYBE, fence pointer -> row group " + rg + ", found seq=" + b.seqs()[i]);
      return new KeyValue(key, b.seqs()[i], b.ops()[i], b.values()[i]);
    }
    stats.bloomFalsePositives++;
    if (trace != null) trace.add(name + ": bloom MAYBE, fence pointer -> row group " + rg + ", not there (false positive)");
    return null;
  }

  /** Rows with lo <= key <= hi (null = unbounded), in key order, reading only the row groups needed. */
  public Iterator<KeyValue> range(String lo, String hi, BlockCache cache) {
    if ((lo != null && lo.compareTo(meta.maxKey()) > 0) || (hi != null && hi.compareTo(meta.minKey()) < 0)) {
      return java.util.Collections.emptyIterator();
    }
    final int startRg = lo == null ? 0 : Math.max(0, floorFence(lo));
    final int endRg = hi == null ? fences.length - 1 : floorFence(hi);
    return new Iterator<>() {
      int rg = startRg;
      BlockCache.Block b = null;
      int i = 0;
      KeyValue next = advance();

      private KeyValue advance() {
        while (rg <= endRg) {
          if (b == null) {
            b = block(rg, cache, null);
            i = 0;
            if (lo != null && rg == startRg) {
              int p = Arrays.binarySearch(b.keys(), lo);
              i = p >= 0 ? p : -p - 1;
            }
          }
          if (i < b.keys().length) {
            if (hi != null && b.keys()[i].compareTo(hi) > 0) {
              rg = endRg + 1;
              return null;
            }
            KeyValue kv = new KeyValue(b.keys()[i], b.seqs()[i], b.ops()[i], b.values()[i]);
            i++;
            return kv;
          }
          rg++;
          b = null;
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public KeyValue next() {
        if (next == null) throw new NoSuchElementException();
        KeyValue r = next;
        next = advance();
        return r;
      }
    };
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      reader = null;
    }
  }
}
