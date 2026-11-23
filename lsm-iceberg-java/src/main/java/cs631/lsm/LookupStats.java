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
  /** bloom filter said maybe, but the key index said the key isn't in the run (no block read). */
  public long indexPruned;
  public long bloomFalsePositives;
  public long blocksRead;
  public long blockCacheHits;

  public void reset() {
    gets = memtableHits = runsChecked = rangePruned = bloomPruned = indexPruned = bloomFalsePositives = blocksRead = blockCacheHits = 0;
  }

  public Map<String, Long> asMap() {
    Map<String, Long> m = new LinkedHashMap<>();
    m.put("gets", gets);
    m.put("memtableHits", memtableHits);
    m.put("runsChecked", runsChecked);
    m.put("rangePruned", rangePruned);
    m.put("bloomPruned", bloomPruned);
    m.put("indexPruned", indexPruned);
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
