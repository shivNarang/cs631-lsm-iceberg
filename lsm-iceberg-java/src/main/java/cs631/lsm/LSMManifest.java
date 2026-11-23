package cs631.lsm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The LSM "level manifest": which runs exist on which level, plus the last flushed sequence
 * number. Stored as a versioned JSON file; the Iceberg table property {@code
 * lsm.manifest-location} points at the current version (same pattern Iceberg uses for its own
 * metadata.json), and that property is moved in the same commit that adds or replaces data files.
 */
public class LSMManifest {
  public int formatVersion = 2;
  public long manifestVersion = 0;
  public long lastSeq = 0;
  public long nextRunId = 1;
  public Map<Integer, List<RunMeta>> levels = new TreeMap<>();
  /** Per level: the max key of the last run merged down, so merges walk the key space round-robin. */
  public Map<Integer, String> compactCursor = new TreeMap<>();

  public LSMManifest copy() {
    LSMManifest m = new LSMManifest();
    m.formatVersion = formatVersion;
    m.manifestVersion = manifestVersion;
    m.lastSeq = lastSeq;
    m.nextRunId = nextRunId;
    levels.forEach((k, v) -> m.levels.put(k, new ArrayList<>(v)));
    m.compactCursor.putAll(compactCursor);
    return m;
  }
}
