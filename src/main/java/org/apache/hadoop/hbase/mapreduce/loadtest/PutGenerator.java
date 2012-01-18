package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.Put;

/**
 * Generates put operations, either bulk inserts or single key-value inserts.
 * Operations are generated for consecutive increasing keys within the load
 * tester key space, starting from a specified key.
 */
public class PutGenerator implements OperationGenerator {

  protected final AtomicLong nextKey;
  protected final byte[] columnFamily;
  protected final KeyCounter keysWritten;

  // If queue is null, then bulk inserts will be generated, otherwise, inserts
  // of single KeyValues will be generated.
  private final LinkedBlockingQueue<PutOperation> queue;

  /**
   * Default constructor.
   *
   * @param columnFamily
   * @param keysWritten the sink to be populated with written keys
   * @param startKey the first key to insert
   * @param bulkInsert if true, operations will each insert multiple key-values
   */
  public PutGenerator(byte[] columnFamily, KeyCounter keysWritten,
      long startKey, boolean bulkInsert) {
    nextKey = new AtomicLong(startKey);
    this.columnFamily = columnFamily;
    this.keysWritten = keysWritten;
    if (bulkInsert) {
      this.queue = null;
    } else {
      this.queue = new LinkedBlockingQueue<PutOperation>();
    }
  }

  public Operation nextOperation(DataGenerator dataGenerator) {
    // Loop until we get a put operation or an error returns null.
    while (true) {
      // Check if there are queued operations which were parts of previous puts.
      if (queue != null) {
        PutOperation head = queue.poll();
        if (head != null) {
          return head;
        }
      }

      try {
        long key = getNextKey();
        PutOperation put;
        if (queue != null) {
          put = getPut(dataGenerator, key);
        } else {
          put = getBulkPut(dataGenerator, key);
        }
        if (put != null) {
          return put;
        }
        // The next key was not supposed to be written, so try again.
      } catch (KeyCounter.NoKeysException e) {
        // There were no keys to be written, do not try again.
        return null;
      }
    }
  }

  protected long getNextKey() throws KeyCounter.NoKeysException {
    return nextKey.getAndIncrement();
  }

  protected PutOperation getBulkPut(DataGenerator dataGenerator, long key) {
    Put put = dataGenerator.constructBulkPut(key, columnFamily);
    if (put != null) {
      return new PutOperation(key, put, keysWritten);
    } else {
      // Key was defined to be skipped, mark it as complete so it can be read.
      keysWritten.markKey(key, true);
      return null;
    }
  }

  protected PutOperation getPut(DataGenerator dataGenerator, long key) {
    List<Put> puts = dataGenerator.constructPuts(key, columnFamily);
    if (puts != null) {
      AtomicInteger remainingParts = new AtomicInteger(puts.size());
      for (int i = 1; i < puts.size(); i++) {
        queue.offer(new PutOperation(key, puts.get(i), keysWritten,
            remainingParts));
      }
      return new PutOperation(key, puts.get(0), keysWritten, remainingParts);
    } else {
      keysWritten.markKey(key, true);
      return null;
    }
  }
}
