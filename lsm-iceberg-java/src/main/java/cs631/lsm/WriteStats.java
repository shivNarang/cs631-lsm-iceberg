package cs631.lsm;

import java.util.LinkedHashMap;
import java.util.Map;

/** Counters for the write side: flushes, merges and how many bytes they wrote. */
public class WriteStats {
  /** Bytes of keys and values the user wrote (the "logical" data). */
  public long userBytes;
  public long writes;
  public long flushes;
  public long flushBytes;
  public long flushRows;
  public long compactions;
  public long compactionBytesRead;
  public long compactionBytesWritten;
  public long compactionRowsWritten;
  /** Older versions of a key removed by merges. */
  public long versionsDropped;
  /** Delete markers removed by merges that reached the bottom of the tree. */
  public long tombstonesDropped;
  public long compactionNanos;

  /** Data-file bytes written per byte of user data (WAL not counted). */
  public double writeAmplification() {
    return userBytes == 0 ? 0 : (double) (flushBytes + compactionBytesWritten) / userBytes;
  }

  public Map<String, Object> asMap() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("writes", writes);
    m.put("userBytes", userBytes);
    m.put("flushes", flushes);
    m.put("flushBytes", flushBytes);
    m.put("flushRows", flushRows);
    m.put("compactions", compactions);
    m.put("compactionBytesRead", compactionBytesRead);
    m.put("compactionBytesWritten", compactionBytesWritten);
    m.put("compactionRowsWritten", compactionRowsWritten);
    m.put("versionsDropped", versionsDropped);
    m.put("tombstonesDropped", tombstonesDropped);
    m.put("compactionSeconds", Math.round(compactionNanos / 1e7) / 100.0);
    m.put("writeAmplification", Math.round(writeAmplification() * 100) / 100.0);
    return m;
  }

  @Override
  public String toString() {
    return asMap().toString();
  }
}
