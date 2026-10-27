package cs631.lsm;

import java.util.LinkedHashMap;
import java.util.Map;

/** Counters describing what point lookups did. */
public class LookupStats {
  public long gets;
  public long memtableHits;
  public long runsChecked;
  public long rangePruned;
  public long bloomPruned;
  public long bloomFalsePositives;
  public long blocksRead;
  public long blockCacheHits;

  public void reset() {
    gets = memtableHits = runsChecked = rangePruned = bloomPruned = bloomFalsePositives = blocksRead = blockCacheHits = 0;
  }

  public Map<String, Long> asMap() {
    Map<String, Long> m = new LinkedHashMap<>();
    m.put("gets", gets);
    m.put("memtableHits", memtableHits);
    m.put("runsChecked", runsChecked);
    m.put("rangePruned", rangePruned);
    m.put("bloomPruned", bloomPruned);
    m.put("bloomFalsePositives", bloomFalsePositives);
    m.put("blocksRead", blocksRead);
    m.put("blockCacheHits", blockCacheHits);
    return m;
  }

  @Override
  public String toString() {
    return asMap().toString();
  }
}
