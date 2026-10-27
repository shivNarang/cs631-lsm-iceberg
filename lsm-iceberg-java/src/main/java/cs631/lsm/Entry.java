package cs631.lsm;

/** The newest version of a key: its sequence number, operation and value (null for deletes). */
public record Entry(long seq, byte op, String value) {
  public static final byte PUT = 0;
  public static final byte DELETE = 1;

  public boolean isDelete() {
    return op == DELETE;
  }
}
