package org.apache.hadoop.hbase.mapreduce.loadtest;

/**
 * Generates bulk puts for random keys which have already been successfully
 * processed by something else. This is typically used for overwriting keys
 * which were previously inserted by another generator, though this is not a
 * requirement.
 */
public class PutReGenerator extends PutGenerator {

  /**
   * Default constructor.
   *
   * @param columnFamily
   * @param keysWritten the source of keys to write and sink of keys written
   * @param bulkInsert if true, operations will each insert multiple key-values
   */
  public PutReGenerator(byte[] columnFamily, KeyCounter keysWritten,
      boolean bulkInsert) {
    super(columnFamily, keysWritten, 0, bulkInsert);
  }

  /**
   * Override superclass behavior to get a random key instead of the next
   * sequential key.
   */
  protected long getNextKey() throws KeyCounter.NoKeysException {
    return keysWritten.getRandomKey();
  }
}
