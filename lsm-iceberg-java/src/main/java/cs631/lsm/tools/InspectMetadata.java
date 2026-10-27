package cs631.lsm.tools;

import cs631.lsm.Catalogs;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * Phase 1 tool: walk an Iceberg table's metadata tree.
 * table metadata -> snapshots -> manifest list -> manifests -> data files (+ key min/max)
 *                              -> statistics (Puffin) files and their blobs
 * Usage: ./run.sh inspect namespace.table [warehouse] [max-snapshots]
 */
public class InspectMetadata {
  static String shorten(String s, int n) {
    return s.length() <= n ? s : "..." + s.substring(s.length() - (n - 3));
  }

  static String file(String path) {
    return path.substring(path.lastIndexOf('/') + 1);
  }

  public static void main(String[] args) throws Exception {
    String ident = args.length > 0 ? args[0] : "demo.kv";
    Path warehouse = Path.of(args.length > 1 ? args[1] : "warehouse/demo").toAbsolutePath();
    int maxSnaps = args.length > 2 ? Integer.parseInt(args[2]) : 3;
    Table t = Catalogs.local(warehouse).loadTable(TableIdentifier.parse(ident));

    System.out.println("TABLE " + ident);
    System.out.println("  location   : " + shorten(t.location(), 80));
    System.out.println("  schema     : " + t.schema().asStruct());
    System.out.println("  sort order : " + t.sortOrder());
    System.out.println("  properties :");
    new TreeMap<>(t.properties()).forEach((k, v) -> System.out.println("     " + k + " = " + shorten(v, 70)));

    Map<Long, StatisticsFile> stats = new HashMap<>();
    for (StatisticsFile s : t.statisticsFiles()) stats.put(s.snapshotId(), s);
    List<Snapshot> snaps = new java.util.ArrayList<>();
    t.snapshots().forEach(snaps::add);
    System.out.printf("%n  %d snapshots (showing last %d):%n", snaps.size(), Math.min(maxSnaps, snaps.size()));
    for (Snapshot s : snaps.subList(Math.max(0, snaps.size() - maxSnaps), snaps.size())) {
      Map<String, String> lsm = new TreeMap<>();
      s.summary().forEach((k, v) -> {
        if (k.startsWith("lsm.")) lsm.put(k, v);
      });
      System.out.printf("%n  SNAPSHOT %d  seq#%d  op=%s  %s%n", s.snapshotId(), s.sequenceNumber(), s.operation(), lsm);
      System.out.println("    manifest list: " + file(s.manifestListLocation()));
      for (ManifestFile m : s.allManifests(t.io())) {
        System.out.println("    MANIFEST " + file(m.path()) + "  (added files: " + m.addedFilesCount() + ")");
        try (ManifestReader<DataFile> r = ManifestFiles.read(m, t.io())) {
          for (DataFile df : r) {
            String lo = Conversions.fromByteBuffer(Types.StringType.get(), df.lowerBounds().get(1)).toString();
            String hi = Conversions.fromByteBuffer(Types.StringType.get(), df.upperBounds().get(1)).toString();
            System.out.printf("      DATA FILE %-24s rows=%-6d key min/max=[%s, %s]%n", file(df.location()), df.recordCount(), lo, hi);
          }
        }
      }
      StatisticsFile st = stats.get(s.snapshotId());
      if (st != null) {
        System.out.println("    PUFFIN INDEX " + file(st.path()) + " (" + st.fileSizeInBytes() + " bytes)");
        st.blobMetadata().forEach(b -> System.out.println("      blob " + b.type() + " on field ids " + b.fields()));
      }
    }
  }
}
