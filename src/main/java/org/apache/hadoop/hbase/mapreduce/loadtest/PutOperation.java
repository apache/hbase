package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * An operation that inserts one or more key-values.
 */
public class PutOperation extends Operation {

  private final Put put;
  private final long key;
  private boolean success;
  private final KeyCounter writtenKeys;
  private final double profilingFraction;
  private Random random;

  // Multi-part operations need to coordinate between themselves so that only
  // one of the parts makes certain updates. Related parts share an atomic
  // counter, allowing them to determine which part was last to complete.
  private final AtomicInteger partsRemaining;
  private boolean wasLast;

  /**
   * Create a single-part (bulk insert) put operation.
   *
   * @param key the load-tester key space key being inserted
   * @param put
   * @param writtenKeys the sink of keys to be updated after execution
   */
  public PutOperation(long key, Put put, KeyCounter writtenKeys, 
      double profilingFraction) {
    this(key, put, writtenKeys, null, profilingFraction);
  }

  /**
   * Create a multi-part (non-bulk insert) put operation.
   *
   * @param key
   * @param put
   * @param partsRemaining shared counter of parts remaining to execute
   * @param writtenKeys the sink of keys to be updated after execution
   */
  public PutOperation(long key, Put put, KeyCounter writtenKeys,
      AtomicInteger partsRemaining, double profilingFraction) {
    this.put = put;
    this.key = key;
    this.writtenKeys = writtenKeys;
    this.success = false;
    this.profilingFraction = profilingFraction;
    this.partsRemaining = partsRemaining;
    this.wasLast = false;
    this.random = new Random();
  }

  public Operation.Type getType() {
    return (partsRemaining == null)
        ? Operation.Type.BULK_PUT
        : Operation.Type.PUT;
  }

  public void perform(HTable table) throws IOException {
    table.setProfiling(random.nextDouble() < profilingFraction);
    table.put(put);
    success = true;
    if (partsRemaining != null) {
      // If this was not a bulk insert, determine if it was the last part.
      wasLast = (partsRemaining.decrementAndGet() == 0);
    }
  }

  /**
   * Mark the key as completed so it can be used in get requests.
   */
  public void postAction() {
    if (partsRemaining == null || wasLast) {
      // Only mark the key complete if it was a bulk insert or the last part of
      // several related inserts for the same key.
      writtenKeys.markKey(key, success);
    }
  }

  public long getNumKeys() {
    // Return 1 only if this was the only (bulk) insert or if it was the last of
    // several related inserts for the same key.
    return (partsRemaining == null || wasLast) ? 1 : 0;
  }

  public long getNumColumns() {
    return put.size();
  }
}
