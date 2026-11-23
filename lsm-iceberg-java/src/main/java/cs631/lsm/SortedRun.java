package cs631.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

/**
 * A sorted run (SSTable): one key-sorted Avro (or Parquet) data file registered in Iceberg, plus
 * its index in a Puffin file. The Puffin file holds three blobs per run:
 *
 * <ul>
 *   <li>bloom filter: "definitely not here" without touching anything else;
 *   <li>key index: hash table key -> (block, row), so a point lookup jumps straight to the row;
 *   <li>block directory: first key, byte offset and row count of every block, used by range
 *       scans to find where to start, and by the FENCE lookup mode.
 * </ul>
 *
 * One Puffin file can hold the blobs of several runs (a merge writes many runs in one commit);
 * each blob carries the property {@code run-id}.
 */
public class SortedRun implements AutoCloseable {
  public static final String BLOOM_BLOB = "cs631-lsm-bloom-filter-v1";
  public static final String KEY_INDEX_BLOB = "cs631-lsm-key-index-v1";
  public static final String BLOCK_DIR_BLOB = "cs631-lsm-block-directory-v1";

  private final RunMeta meta;
  private final BloomFilter bloom;
  private final KeyIndex keyIndex;
  private final String[] firstKeys;
  private final long[] offsets;
  private final int[] rows;
  private final FileIO io;
  private final Configuration conf;
  private final LSMConfig cfg;
  private DataFile dataFile; // known after write; looked up from the table after a restart
  private RunFiles.Reader reader;

  private SortedRun(RunMeta meta, BloomFilter bloom, KeyIndex keyIndex, String[] firstKeys, long[] offsets, int[] rows,
                    FileIO io, Configuration conf, LSMConfig cfg) {
    this.meta = meta;
    this.bloom = bloom;
    this.keyIndex = keyIndex;
    this.firstKeys = firstKeys;
    this.offsets = offsets;
    this.rows = rows;
    this.io = io;
    this.conf = conf;
    this.cfg = cfg;
  }

  public RunMeta meta() { return meta; }
  public BloomFilter bloom() { return bloom; }
  public KeyIndex keyIndex() { return keyIndex; }
  public String[] firstKeys() { return firstKeys; }
  public long[] blockOffsets() { return offsets; }
  public DataFile dataFile() { return dataFile; }
  void dataFile(DataFile df) { this.dataFile = df; }

  // ---------------------------------------------------------------- writing

  /** Write key-sorted rows as a new run (data file only; the Puffin index is written after the commit). */
  public static SortedRun write(FileIO io, Configuration conf, List<KeyValue> sortedRows, long runId, int level,
                                String dataPath, String indexPath, LSMConfig cfg, SortOrder sortOrder) {
    RunFiles.Layout layout = RunFiles.write(io, conf, dataPath, sortedRows, cfg, sortOrder, cfg.fileFormat);
    long minSeq = Long.MAX_VALUE, maxSeq = Long.MIN_VALUE, tombstones = 0;
    for (KeyValue kv : sortedRows) {
      minSeq = Math.min(minSeq, kv.seq());
      maxSeq = Math.max(maxSeq, kv.seq());
      if (kv.op() == Entry.DELETE) tombstones++;
    }
    RunMeta meta = new RunMeta(runId, level, cfg.fileFormat.name(), dataPath, indexPath,
        sortedRows.get(0).key(), sortedRows.get(sortedRows.size() - 1).key(), minSeq, maxSeq,
        sortedRows.size(), tombstones, layout.blockRows().length, layout.dataFile().fileSizeInBytes());
    String[] keys = new String[sortedRows.size()];
    for (int i = 0; i < keys.length; i++) keys[i] = sortedRows.get(i).key();
    SortedRun run = fromKeys(meta, keys, layout.blockOffsets(), layout.blockRows(), io, conf, cfg);
    run.dataFile = layout.dataFile();
    return run;
  }

  /** Build the in-memory index of a run from its sorted keys and block layout. */
  private static SortedRun fromKeys(RunMeta meta, String[] keys, long[] offsets, int[] rows, FileIO io,
                                    Configuration conf, LSMConfig cfg) {
    BloomFilter bloom = BloomFilter.forCapacity(keys.length, cfg.bloomFpr);
    int[] blockOf = new int[keys.length];
    int[] rowOf = new int[keys.length];
    String[] first = new String[rows.length];
    int pos = 0;
    for (int b = 0; b < rows.length; b++) {
      first[b] = keys[pos];
      for (int r = 0; r < rows[b]; r++, pos++) {
        blockOf[pos] = b;
        rowOf[pos] = r;
      }
    }
    for (String k : keys) bloom.add(k);
    return new SortedRun(meta, bloom, KeyIndex.build(keys, blockOf, rowOf), first, offsets, rows, io, conf, cfg);
  }

  /**
   * Write one Puffin file holding the index blobs of the given runs and describe it as an Iceberg
   * statistics file for the given snapshot.
   */
  public static StatisticsFile writeIndexFile(FileIO io, String path, List<SortedRun> runs, long snapshotId, long sequenceNumber) {
    try {
      io.deleteFile(path); // Puffin writers never overwrite (e.g. when rebuilding an index)
    } catch (RuntimeException ignored) {
      // not there
    }
    List<Integer> fields = List.of(KvSchema.KEY_FIELD_ID);
    PuffinWriter w = Puffin.write(io.newOutputFile(path)).createdBy("cs631-lsm-iceberg").build();
    try (w) {
      for (SortedRun r : runs) {
        RunMeta m = r.meta;
        Map<String, String> props = Map.of(
            "run-id", Long.toString(m.runId()), "level", Integer.toString(m.level()),
            "min-key", m.minKey(), "max-key", m.maxKey(), "data-file", m.dataFile());
        w.add(new Blob(BLOOM_BLOB, fields, snapshotId, sequenceNumber, r.bloom.toBytes(), null, props));
        w.add(new Blob(KEY_INDEX_BLOB, fields, snapshotId, sequenceNumber, r.keyIndex.toBytes(), null, props));
        w.add(new Blob(BLOCK_DIR_BLOB, fields, snapshotId, sequenceNumber, encodeBlockDirectory(r), null, props));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    List<org.apache.iceberg.BlobMetadata> blobs = new ArrayList<>();
    for (BlobMetadata bm : w.writtenBlobsMetadata()) blobs.add(GenericBlobMetadata.from(bm));
    return new GenericStatisticsFile(snapshotId, path, w.fileSize(), w.footerSize(), blobs);
  }

  private static ByteBuffer encodeBlockDirectory(SortedRun r) {
    List<byte[]> enc = new ArrayList<>();
    int size = 4;
    for (String f : r.firstKeys) {
      byte[] b = f.getBytes(StandardCharsets.UTF_8);
      enc.add(b);
      size += 8 + 4 + 4 + b.length;
    }
    ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(r.firstKeys.length);
    for (int i = 0; i < r.firstKeys.length; i++) {
      buf.putLong(r.offsets[i]).putInt(r.rows[i]).putInt(enc.get(i).length).put(enc.get(i));
    }
    buf.flip();
    return buf;
  }

  // ---------------------------------------------------------------- loading

  /** Blobs of a Puffin file, read once per file while opening a table. */
  public static List<Pair<BlobMetadata, ByteBuffer>> readPuffin(FileIO io, String path) {
    try (PuffinReader r = Puffin.read(io.newInputFile(path)).build()) {
      List<Pair<BlobMetadata, ByteBuffer>> out = new ArrayList<>();
      for (Pair<BlobMetadata, ByteBuffer> p : r.readAll(r.fileMetadata().blobs())) {
        ByteBuffer copy = ByteBuffer.allocate(p.second().remaining());
        copy.put(p.second().duplicate()).flip();
        out.add(Pair.of(p.first(), copy));
      }
      return out;
    } catch (IOException | RuntimeException e) {
      return null;
    }
  }

  /** Where a run's index goes if it has to be rebuilt (its original Puffin file may be shared). */
  static String rebuiltIndexPath(RunMeta meta) {
    String dir = meta.indexFile().substring(0, meta.indexFile().lastIndexOf('/'));
    return String.format("%s/run-%06d-rebuilt.puffin", dir, meta.runId());
  }

  /** Load the run's index from Puffin; if it is missing or unreadable, rebuild it from the data file. */
  public static SortedRun load(FileIO io, Configuration conf, RunMeta meta, LSMConfig cfg,
                               Map<String, List<Pair<BlobMetadata, ByteBuffer>>> puffinCache) {
    for (String path : List.of(meta.indexFile(), rebuiltIndexPath(meta))) {
      List<Pair<BlobMetadata, ByteBuffer>> blobs = puffinCache.computeIfAbsent(path, p -> readPuffin(io, p));
      SortedRun r = blobs == null ? null : fromBlobs(meta, blobs, io, conf, cfg);
      if (r != null) return r;
    }
    return rebuild(io, conf, meta, cfg);
  }

  private static SortedRun fromBlobs(RunMeta meta, List<Pair<BlobMetadata, ByteBuffer>> blobs, FileIO io,
                                     Configuration conf, LSMConfig cfg) {
    String id = Long.toString(meta.runId());
    BloomFilter bloom = null;
    KeyIndex index = null;
    ByteBuffer dir = null;
    for (Pair<BlobMetadata, ByteBuffer> p : blobs) {
      if (!id.equals(p.first().properties().get("run-id"))) continue;
      switch (p.first().type()) {
        case BLOOM_BLOB -> bloom = BloomFilter.fromBytes(p.second());
        case KEY_INDEX_BLOB -> index = KeyIndex.fromBytes(p.second());
        case BLOCK_DIR_BLOB -> dir = p.second();
        default -> { }
      }
    }
    if (bloom == null || index == null || dir == null) return null;
    ByteBuffer buf = dir.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    int n = buf.getInt();
    String[] first = new String[n];
    long[] off = new long[n];
    int[] rows = new int[n];
    for (int i = 0; i < n; i++) {
      off[i] = buf.getLong();
      rows[i] = buf.getInt();
      byte[] b = new byte[buf.getInt()];
      buf.get(b);
      first[i] = new String(b, StandardCharsets.UTF_8);
    }
    return new SortedRun(meta, bloom, index, first, off, rows, io, conf, cfg);
  }

  private static SortedRun rebuild(FileIO io, Configuration conf, RunMeta meta, LSMConfig cfg) {
    List<Long> off = new ArrayList<>();
    List<BlockCache.Block> blocks;
    if ("PARQUET".equals(meta.format())) {
      blocks = RunFiles.readAllParquetBlocks(conf, meta.dataFile());
      for (int i = 0; i < blocks.size(); i++) off.add((long) i);
    } else {
      blocks = RunFiles.readAllAvroBlocks(io, meta.dataFile(), off);
    }
    List<String> keys = new ArrayList<>();
    int[] rows = new int[blocks.size()];
    for (int i = 0; i < rows.length; i++) {
      rows[i] = blocks.get(i).keys().length;
      keys.addAll(Arrays.asList(blocks.get(i).keys()));
    }
    SortedRun r = fromKeys(meta, keys.toArray(new String[0]), off.stream().mapToLong(Long::longValue).toArray(), rows, io, conf, cfg);
    writeIndexFile(io, rebuiltIndexPath(meta), List.of(r), -1, -1);
    return r;
  }

  // ---------------------------------------------------------------- reading

  private RunFiles.Reader reader() {
    if (reader == null) reader = RunFiles.open(io, conf, meta, offsets, rows);
    return reader;
  }

  private BlockCache.Block block(int b, BlockCache cache, LookupStats stats) {
    BlockCache.Block blk = cache.get(meta.runId(), b);
    if (blk != null) {
      if (stats != null) stats.blockCacheHits++;
      return blk;
    }
    blk = reader().readBlock(b);
    cache.put(meta.runId(), b, blk);
    if (stats != null) stats.blocksRead++;
    return blk;
  }

  /** Index of the last block whose first key <= key, or -1. */
  private int floorBlock(String key) {
    int i = Arrays.binarySearch(firstKeys, key);
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
    if (cfg.lookupIndex == LSMConfig.LookupIndex.HASH) {
      KeyValue[] hit = new KeyValue[1];
      int[] at = new int[2];
      keyIndex.find(key, (b, row) -> {
        BlockCache.Block blk = block(b, cache, stats);
        if (row < blk.keys().length && blk.keys()[row].equals(key)) {
          hit[0] = new KeyValue(key, blk.seqs()[row], blk.ops()[row], blk.values()[row]);
          at[0] = b;
          at[1] = row;
          return true;
        }
        return false;
      });
      if (hit[0] != null) {
        if (trace != null) trace.add(name + ": bloom MAYBE -> key index says block " + at[0] + ", row " + at[1]
            + " -> found seq=" + hit[0].seq());
        return hit[0];
      }
      stats.indexPruned++;
      if (trace != null) trace.add(name + ": bloom MAYBE -> key index says NOT here -> skip (no block read)");
      return null;
    }
    int b = floorBlock(key);
    BlockCache.Block blk = block(b, cache, stats);
    int i = Arrays.binarySearch(blk.keys(), key);
    if (i >= 0) {
      if (trace != null) trace.add(name + ": bloom MAYBE -> fence pointers say block " + b + " -> found seq=" + blk.seqs()[i]);
      return new KeyValue(key, blk.seqs()[i], blk.ops()[i], blk.values()[i]);
    }
    stats.bloomFalsePositives++;
    if (trace != null) trace.add(name + ": bloom MAYBE -> fence pointers say block " + b + " -> not there (false positive)");
    return null;
  }

  /** Rows with lo <= key <= hi (null = unbounded), in key order, reading only the blocks needed. */
  public Iterator<KeyValue> range(String lo, String hi, BlockCache cache) {
    if ((lo != null && lo.compareTo(meta.maxKey()) > 0) || (hi != null && hi.compareTo(meta.minKey()) < 0)) {
      return Collections.emptyIterator();
    }
    final int startBlock = lo == null ? 0 : Math.max(0, floorBlock(lo));
    final int endBlock = hi == null ? firstKeys.length - 1 : floorBlock(hi);
    return new Iterator<>() {
      int b = startBlock;
      BlockCache.Block blk = null;
      int i = 0;
      KeyValue next = advance();

      private KeyValue advance() {
        while (b <= endBlock) {
          if (blk == null) {
            blk = block(b, cache, null);
            i = 0;
            if (lo != null && b == startBlock) {
              int p = Arrays.binarySearch(blk.keys(), lo);
              i = p >= 0 ? p : -p - 1;
            }
          }
          if (i < blk.keys().length) {
            if (hi != null && blk.keys()[i].compareTo(hi) > 0) {
              b = endBlock + 1;
              return null;
            }
            KeyValue kv = new KeyValue(blk.keys()[i], blk.seqs()[i], blk.ops()[i], blk.values()[i]);
            i++;
            return kv;
          }
          b++;
          blk = null;
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

  /** Approximate memory used by this run's index (bloom filter + key index + block directory). */
  public long indexBytes() {
    long dir = 0;
    for (String f : firstKeys) dir += 16 + f.length();
    return bloom.numBits() / 8 + 12L * keyIndex.slots() + dir;
  }

  @Override
  public void close() {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }
}
