package cs631.lsm;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/** Bloom filter (Kirsch–Mitzenmacher double hashing) used to skip runs that can't contain a key. */
public class BloomFilter {
  private final long numBits;
  private final int numHashes;
  private final long[] words;

  public BloomFilter(long numBits, int numHashes) {
    this.numBits = Math.max(64, numBits);
    this.numHashes = Math.max(1, numHashes);
    this.words = new long[(int) ((this.numBits + 63) / 64)];
  }

  private BloomFilter(long numBits, int numHashes, long[] words) {
    this.numBits = numBits;
    this.numHashes = numHashes;
    this.words = words;
  }

  public static BloomFilter forCapacity(long n, double fpr) {
    n = Math.max(1, n);
    long m = (long) Math.ceil(-n * Math.log(fpr) / (Math.log(2) * Math.log(2)));
    int k = (int) Math.max(1, Math.round((double) m / n * Math.log(2)));
    return new BloomFilter(m, k);
  }

  public void add(String key) {
    byte[] b = key.getBytes(StandardCharsets.UTF_8);
    long h1 = hash64(b, 0x9E3779B97F4A7C15L);
    long h2 = hash64(b, 0xC2B2AE3D27D4EB4FL) | 1;
    for (int i = 0; i < numHashes; i++) {
      long pos = Math.floorMod(h1 + i * h2, numBits);
      words[(int) (pos >>> 6)] |= 1L << (pos & 63);
    }
  }

  public boolean mightContain(String key) {
    byte[] b = key.getBytes(StandardCharsets.UTF_8);
    long h1 = hash64(b, 0x9E3779B97F4A7C15L);
    long h2 = hash64(b, 0xC2B2AE3D27D4EB4FL) | 1;
    for (int i = 0; i < numHashes; i++) {
      long pos = Math.floorMod(h1 + i * h2, numBits);
      if ((words[(int) (pos >>> 6)] & (1L << (pos & 63))) == 0) return false;
    }
    return true;
  }

  public long numBits() {
    return numBits;
  }

  public int numHashes() {
    return numHashes;
  }

  /** FNV-1a over the bytes, finished with the MurmurHash3 64-bit mixer. */
  static long hash64(byte[] data, long seed) {
    long h = seed ^ (data.length * 0x100000001B3L);
    for (byte x : data) {
      h ^= (x & 0xff);
      h *= 0x100000001B3L;
    }
    h ^= h >>> 33;
    h *= 0xff51afd7ed558ccdL;
    h ^= h >>> 33;
    h *= 0xc4ceb9fe1a85ec53L;
    h ^= h >>> 33;
    return h;
  }

  public ByteBuffer toBytes() {
    ByteBuffer buf = ByteBuffer.allocate(12 + 8 * words.length).order(ByteOrder.LITTLE_ENDIAN);
    buf.putLong(numBits).putInt(numHashes);
    for (long w : words) buf.putLong(w);
    buf.flip();
    return buf;
  }

  public static BloomFilter fromBytes(ByteBuffer in) {
    ByteBuffer buf = in.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    long m = buf.getLong();
    int k = buf.getInt();
    long[] words = new long[(int) ((m + 63) / 64)];
    for (int i = 0; i < words.length; i++) words[i] = buf.getLong();
    return new BloomFilter(m, k, words);
  }
}
