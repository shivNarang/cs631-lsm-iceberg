package cs631.lsm;

import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

/** A local Iceberg catalog: tables and their metadata live under a warehouse directory. */
public final class Catalogs {
  private Catalogs() {}

  public static Catalog local(Path warehouse) {
    warehouse.toFile().mkdirs();
    return new HadoopCatalog(new Configuration(), warehouse.toAbsolutePath().toString());
  }

  /** Turn an Iceberg location ("file:/a/b", "file:///a/b" or "/a/b") into a local path. */
  public static Path localPath(String location) {
    String p = location;
    if (p.startsWith("file:")) {
      p = p.substring("file:".length());
      while (p.startsWith("//")) p = p.substring(1);
    }
    return Path.of(p);
  }
}
