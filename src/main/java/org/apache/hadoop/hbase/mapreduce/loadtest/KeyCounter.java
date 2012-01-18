package org.apache.hadoop.hbase.mapreduce.loadtest;

import java.io.Serializable;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A class for tracking successful or failed keys. For keys in the space of
 * longs, and are traversed in a linear sequence of all consecutive keys,
 * instances of this class track which keys have been successfully processed or
 * not and can return a random successful key.
 *
 * It is assumed that the number of keys which succeed is orders of magnitude
 * greater than the number of keys which fail. The memory required by instances
 * of this class is constant in the number of successful keys (assuming a bound
 * on the duration for which a key can be in process) and linear in the number
 * of failed keys.
 */
public class KeyCounter implements Serializable {

  private static final long serialVersionUID = 8011474084218253286L;

  // The number of recent successful keys which should be buffered for returning
  // random successful keys if the standard approach fails.
  private final static int NUM_RECENT_KEYS = 10000;

  // The first key that is expected to be counted. All internal variables in the
  // key space are stored after subtracting startKey from them, so that the
  // internal key space always starts from 0 and is less likely to overflow.
  private final long startKey;

  // Store all keys that have failed.
  private final ConcurrentHashMap<Long,Long> failedKeySet;

  // Store some number of recently successful row keys so that there is a bound
  // on the time it takes to get a random successful key. The map key is in the
  // range [0, NUM_RECENT_KEYS) and the value is a row key that was recently
  // successful.
  private final ConcurrentHashMap<Long,Long> recentSuccessfulKeys;

  // Used for inserting and overwriting recently successful keys in order of
  // recentness.
  private final AtomicLong numSuccessfulKeys;

  // The greatest completed key contiguous to the range of completed keys
  // starting with the startKey.
  private final AtomicLong lastContiguousKey;

  // A queue of keys which have been completed but are not contiguous to the
  // completed range of keys starting with the startKey.
  private final PriorityQueue<Long> keysCompleted;

  // Used to randomly choose a completed successful key.
  private final Random random;

  /**
   * Thrown if there are no completed successful keys.
   */
  public class NoKeysException extends Exception {
    private static final long serialVersionUID = -7069323512531319455L;
  }

  /**
   * Create a new KeyCounter which starts counting keys from startKey.
   *
   * @param startKey the first key expected to be counted
   */
  public KeyCounter(long startKey) {
    this.startKey = startKey;
    numSuccessfulKeys = new AtomicLong(0);
    lastContiguousKey = new AtomicLong(-1);
    keysCompleted = new PriorityQueue<Long>();
    failedKeySet = new ConcurrentHashMap<Long,Long>();
    recentSuccessfulKeys = new ConcurrentHashMap<Long, Long>(
        (int)(1.1 * NUM_RECENT_KEYS), 0.75f, 100);
    random = new Random();
  }

  /**
   * Get a random key which was marked as successful.
   *
   * @return a random, successful key
   * @throws NoKeysException if there have been no successful keys
   */
  public long getRandomKey() throws NoKeysException {
    long last = lastContiguousKey.get();
    if (last == -1) {
      throw new NoKeysException();
    }
    long key = random.nextInt() << 32 | random.nextInt();
    if (last == 0) {
      key = 0;
    } else {
      key = Math.abs(key % last);
    }
    if (failedKeySet.containsKey(key)) {
      if (recentSuccessfulKeys.size() > 0) {
        key = recentSuccessfulKeys.get(
            Math.abs(key % recentSuccessfulKeys.size()));
      } else {
        throw new NoKeysException();
      }
    }
    // Translate to the external key space.
    return key + startKey;
  }

  /**
   * Mark a specified key as processed and either successful or failed.
   *
   * @param key the key which has been processed
   * @param success true if the processing succeeded
   */
  public void markKey(long key, boolean success) {
    // Translate to the internal key space.
    key -= startKey;

    if (success) {
      recentSuccessfulKeys.put(
          numSuccessfulKeys.getAndIncrement() % NUM_RECENT_KEYS, key);
    } else {
      failedKeySet.put(key, key);
    }

    synchronized (keysCompleted) {
      if (key > lastContiguousKey.get() + 1) {
        keysCompleted.offer(key);
      } else if (key == lastContiguousKey.get() + 1) {
        long last = key;
        Long next;
        while ((next = keysCompleted.peek()) != null &&
            next.longValue() == last + 1) {
          keysCompleted.poll();
          last++;
        }
        lastContiguousKey.set(last);
      } else {
        // This key should already be represented in this counter.
        return;
      }
    }
  }
}
