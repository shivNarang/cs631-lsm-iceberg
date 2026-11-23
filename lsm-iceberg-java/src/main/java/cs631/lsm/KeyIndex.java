package cs631.lsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Exact per-run key index stored as a Puffin blob: an open-addressing hash table that maps each
 * key to the block that holds it and its row inside that block. A lookup hashes the key once and
 * goes straight to (block, row), so there is no binary search and a key the run doesn't hold
 * costs no block read (unlike a bloom false positive).
 *
 * <p>Each slot stores a 32-bit fingerprint of the key, the block number and the row number (12
 * bytes). Two keys can share a fingerprint, so the caller confirms the key after reading the row.
 */
public final class KeyIndex {
  private static final int EMPTY = -1;
  private final int mask;
  private final int[] fingerprints;
  private final int[] blocks; // EMPTY marks a free slot
  private final int[] rows;
  private final int size;

  private KeyIndex(int slots, int size) {
    this.mask = slots - 1;
    this.fingerprints = new int[slots];
    this.blocks = new int[slots];
    this.rows = new int[slots];
    this.size = size;
    java.util.Arrays.fill(blocks, EMPTY);
  }

  /** keys[i] is in block blockOf[i] at row rowOf[i]. */
  public static KeyIndex build(String[] keys, int[] blockOf, int[] rowOf) {
    int slots = Integer.highestOneBit(Math.max(4, (int) (keys.length / 0.7)) - 1) << 1;
    KeyIndex idx = new KeyIndex(slots, keys.length);
    for (int i = 0; i < keys.length; i++) {
      long h = hash(keys[i]);
      int s = (int) (h >>> 32) & idx.mask;
      while (idx.blocks[s] != EMPTY) s = (s + 1) & idx.mask;
      idx.fingerprints[s] = (int) h;
      idx.blocks[s] = blockOf[i];
      idx.rows[s] = rowOf[i];
    }
    return idx;
  }

  private static long hash(String key) {
    return BloomFilter.hash64(key.getBytes(StandardCharsets.UTF_8), 0x51_7C_C1_B7_27_22_0A_95L);
  }

  /**
   * Visit every (block, row) whose fingerprint matches the key until the visitor returns true
   * (found). Returns false if none matched, meaning the key is not in this run.
   */
  public boolean find(String key, Visitor v) {
    long h = hash(key);
    int fp = (int) h;
    for (int s = (int) (h >>> 32) & mask; blocks[s] != EMPTY; s = (s + 1) & mask) {
      if (fingerprints[s] == fp && v.visit(blocks[s], rows[s])) return true;
    }
    return false;
  }

  /** Convenience for tests: true if some slot matches the key's fingerprint. */
  public boolean mayContain(String key) {
    return find(key, (b, r) -> true);
  }

  public interface Visitor {
    boolean visit(int block, int row);
  }

  public int size() {
    return size;
  }

  public int slots() {
    return blocks.length;
  }

  public ByteBuffer toBytes() {
    ByteBuffer buf = ByteBuffer.allocate(8 + 12 * blocks.length).order(ByteOrder.LITTLE_ENDIAN);
    buf.putInt(blocks.length).putInt(size);
    for (int i = 0; i < blocks.length; i++) buf.putInt(fingerprints[i]).putInt(blocks[i]).putInt(rows[i]);
    buf.flip();
    return buf;
  }

  public static KeyIndex fromBytes(ByteBuffer in) {
    ByteBuffer buf = in.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    int slots = buf.getInt();
    KeyIndex idx = new KeyIndex(slots, buf.getInt());
    for (int i = 0; i < slots; i++) {
      idx.fingerprints[i] = buf.getInt();
      idx.blocks[i] = buf.getInt();
      idx.rows[i] = buf.getInt();
    }
    return idx;
  }
}
