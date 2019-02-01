/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates multiple threads that write key/values into the */
public abstract class MultiThreadedWriterBase extends MultiThreadedAction {
  private static final Logger LOG = LoggerFactory.getLogger(MultiThreadedWriterBase.class);

  /**
   * A temporary place to keep track of inserted/updated keys. This is written to by
   * all writers and is drained on a separate thread that populates
   * {@link #wroteUpToKey}, the maximum key in the contiguous range of keys
   * being inserted/updated. This queue is supposed to stay small.
   */
  protected BlockingQueue<Long> wroteKeys;

  /**
   * This is the current key to be inserted/updated by any thread. Each thread does an
   * atomic get and increment operation and inserts the current value.
   */
  protected AtomicLong nextKeyToWrite = new AtomicLong();

  /**
   * The highest key in the contiguous range of keys .
   */
  protected AtomicLong wroteUpToKey = new AtomicLong();

  /** The sorted set of keys NOT inserted/updated by the writers */
  protected Set<Long> failedKeySet = new ConcurrentSkipListSet<>();

  /**
   * The total size of the temporary inserted/updated key set that have not yet lined
   * up in a our contiguous sequence starting from startKey. Supposed to stay
   * small.
   */
  protected AtomicLong wroteKeyQueueSize = new AtomicLong();

  /** Enable this if used in conjunction with a concurrent reader. */
  protected boolean trackWroteKeys;

  public MultiThreadedWriterBase(LoadTestDataGenerator dataGen, Configuration conf,
      TableName tableName, String actionLetter) throws IOException {
    super(dataGen, conf, tableName, actionLetter);
    this.wroteKeys = createWriteKeysQueue(conf);
  }

  protected BlockingQueue<Long> createWriteKeysQueue(Configuration conf) {
    return new ArrayBlockingQueue<>(10000);
  }

  @Override
  public void start(long startKey, long endKey, int numThreads) throws IOException {
    super.start(startKey, endKey, numThreads);
    nextKeyToWrite.set(startKey);
    wroteUpToKey.set(startKey - 1);

    if (trackWroteKeys) {
      new Thread(new WroteKeysTracker(),
          "MultiThreadedWriterBase-WroteKeysTracker-" + System.currentTimeMillis()).start();
      numThreadsWorking.incrementAndGet();
    }
  }

  protected String getRegionDebugInfoSafe(Table table, byte[] rowKey) {
    HRegionLocation cached = null, real = null;
    try (RegionLocator locator = connection.getRegionLocator(tableName)) {
      cached = locator.getRegionLocation(rowKey, false);
      real = locator.getRegionLocation(rowKey, true);
    } catch (Throwable t) {
      // Cannot obtain region information for another catch block - too bad!
    }
    String result = "no information can be obtained";
    if (cached != null) {
      result = "cached: " + cached.toString();
    }
    if (real != null && real.getServerName() != null) {
      if (cached != null && cached.getServerName() != null && real.equals(cached)) {
        result += "; cache is up to date";
      } else {
        result = (cached != null) ? (result + "; ") : "";
        result += "real: " + real.toString();
      }
    }
    return result;
  }

  /**
   * A thread that keeps track of the highest key in the contiguous range of
   * inserted/updated keys.
   */
  private class WroteKeysTracker implements Runnable {

    @Override
    public void run() {
      Thread.currentThread().setName(getClass().getSimpleName());
      try {
        long expectedKey = startKey;
        Queue<Long> sortedKeys = new PriorityQueue<>();
        while (expectedKey < endKey) {
          // Block until a new element is available.
          Long k;
          try {
            k = wroteKeys.poll(1, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            LOG.info("Inserted key tracker thread interrupted", e);
            break;
          }
          if (k == null) {
            continue;
          }
          if (k == expectedKey) {
            // Skip the "sorted key" queue and consume this key.
            wroteUpToKey.set(k);
            ++expectedKey;
          } else {
            sortedKeys.add(k);
          }

          // See if we have a sequence of contiguous keys lined up.
          while (!sortedKeys.isEmpty()
              && ((k = sortedKeys.peek()) == expectedKey)) {
            sortedKeys.poll();
            wroteUpToKey.set(k);
            ++expectedKey;
          }

          wroteKeyQueueSize.set(wroteKeys.size() + sortedKeys.size());
        }
      } catch (Exception ex) {
        LOG.error("Error in inserted/updaed key tracker", ex);
      } finally {
        numThreadsWorking.decrementAndGet();
      }
    }
  }

  public int getNumWriteFailures() {
    return failedKeySet.size();
  }

  /**
   * The max key until which all keys have been inserted/updated (successfully or not).
   * @return the last key that we have inserted/updated all keys up to (inclusive)
   */
  public long wroteUpToKey() {
    return wroteUpToKey.get();
  }

  public boolean failedToWriteKey(long k) {
    return failedKeySet.contains(k);
  }

  @Override
  protected String progressInfo() {
    StringBuilder sb = new StringBuilder();
    appendToStatus(sb, "wroteUpTo", wroteUpToKey.get());
    appendToStatus(sb, "wroteQSize", wroteKeyQueueSize.get());
    return sb.toString();
  }

  /**
   * Used for a joint write/read workload. Enables tracking the last inserted/updated
   * key, which requires a blocking queue and a consumer thread.
   * @param enable whether to enable tracking the last inserted/updated key
   */
  public void setTrackWroteKeys(boolean enable) {
    trackWroteKeys = enable;
  }
}
