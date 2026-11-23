package cs631.lsm;

/** Metadata of one sorted run, as recorded in the LSM level manifest. */
public record RunMeta(
    long runId,
    int level,
    String format,
    String dataFile,
    String indexFile,
    String minKey,
    String maxKey,
    long minSeq,
    long maxSeq,
    long recordCount,
    long tombstones,
    int blocks,
    long fileSize) {}
