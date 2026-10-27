package cs631.lsm;

/** One versioned record as it flows through range-scan merging. */
public record KeyValue(String key, long seq, byte op, String value) {}
