package cs631.lsm;

import java.util.Map;

/** What the benchmark needs from every key-value table it compares. */
public interface KvStore {
  void put(String key, String value);

  void delete(String key);

  /** Make every buffered write durable in the table. */
  void flush();

  String get(String key);

  Map<String, String> scan(String lo, String hi);

  /** Bytes of data (and delete) files written so far. */
  long bytesWritten();

  /** Rows written to data (and delete) files so far, counting rewrites. */
  long rowsWritten();

  /** Files the last get() had to open. */
  double filesReadLastGet();

  /** Live data files in the current snapshot. */
  long dataFiles();
}
