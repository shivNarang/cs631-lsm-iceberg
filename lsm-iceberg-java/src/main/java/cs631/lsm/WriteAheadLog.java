package cs631.lsm;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Write-ahead log so un-flushed MemTable writes survive a crash.
 *
 * <p>Record format: [int payloadLength][int crc32(payload)][payload], payload =
 * [long seq][byte op][int keyLen][key utf8][int valueLen or -1][value utf8]. A record with a bad
 * length or checksum (a torn write from a crash) ends replay. The log is truncated after each flush
 * is committed to Iceberg; replay skips records with seq <= the last flushed seq, so a crash
 * between "commit" and "truncate" is harmless.
 */
public class WriteAheadLog implements AutoCloseable {
  private final Path path;
  private final boolean sync;
  private FileOutputStream file;
  private DataOutputStream out;
  private long bytesWritten = 0;

  public WriteAheadLog(Path path, boolean sync) {
    this.path = path;
    this.sync = sync;
    try {
      Files.createDirectories(path.getParent());
      open(true);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void open(boolean append) throws IOException {
    file = new FileOutputStream(path.toFile(), append);
    out = new DataOutputStream(new BufferedOutputStream(file, 64 * 1024));
  }

  public void append(long seq, byte op, String key, String value) {
    try {
      ByteArrayOutputStream buf = new ByteArrayOutputStream(32 + key.length() + (value == null ? 0 : value.length()));
      DataOutputStream p = new DataOutputStream(buf);
      p.writeLong(seq);
      p.writeByte(op);
      byte[] k = key.getBytes(StandardCharsets.UTF_8);
      p.writeInt(k.length);
      p.write(k);
      if (value == null) {
        p.writeInt(-1);
      } else {
        byte[] v = value.getBytes(StandardCharsets.UTF_8);
        p.writeInt(v.length);
        p.write(v);
      }
      byte[] payload = buf.toByteArray();
      CRC32 crc = new CRC32();
      crc.update(payload);
      out.writeInt(payload.length);
      out.writeInt((int) crc.getValue());
      out.write(payload);
      out.flush(); // hand to the OS: survives a process crash
      if (sync) file.getFD().sync(); // survives power loss
      bytesWritten += 8 + payload.length;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Called after a flush is committed: start an empty log. */
  public void reset() {
    try {
      out.close();
      open(false);
      if (sync) file.getFD().sync();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public long bytesWritten() {
    return bytesWritten;
  }

  @Override
  public void close() {
    try {
      out.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static List<KeyValue> replay(Path path) {
    List<KeyValue> out = new ArrayList<>();
    if (!Files.exists(path)) return out;
    try (DataInputStream in = new DataInputStream(new java.io.BufferedInputStream(new FileInputStream(path.toFile())))) {
      while (true) {
        int len;
        int crc;
        byte[] payload;
        try {
          len = in.readInt();
          crc = in.readInt();
          if (len < 0 || len > (1 << 28)) break;
          payload = in.readNBytes(len);
        } catch (EOFException e) {
          break;
        }
        if (payload.length != len) break; // torn tail
        CRC32 c = new CRC32();
        c.update(payload);
        if ((int) c.getValue() != crc) break; // corrupt tail
        DataInputStream p = new DataInputStream(new java.io.ByteArrayInputStream(payload));
        long seq = p.readLong();
        byte op = p.readByte();
        String key = new String(p.readNBytes(p.readInt()), StandardCharsets.UTF_8);
        int vl = p.readInt();
        String value = vl < 0 ? null : new String(p.readNBytes(vl), StandardCharsets.UTF_8);
        out.add(new KeyValue(key, seq, op, value));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out;
  }
}
