/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/** Creates multiple threads that write key/values into the */
public class MultiThreadedWriter extends MultiThreadedAction {
  private static final Log LOG = LogFactory.getLog(MultiThreadedWriter.class);

  private long minColumnsPerKey = 1;
  private long maxColumnsPerKey = 10;
  private Set<HBaseWriterThread> writers = new HashSet<HBaseWriterThread>();

  private boolean isMultiPut = false;

  /**
   * A temporary place to keep track of inserted keys. This is written to by
   * all writers and is drained on a separate thread that populates
   * {@link #insertedUpToKey}, the maximum key in the contiguous range of keys
   * being inserted. This queue is supposed to stay small.
   */
  private BlockingQueue<Long> insertedKeys =
      new ArrayBlockingQueue<Long>(10000);

  /**
   * This is the current key to be inserted by any thread. Each thread does an
   * atomic get and increment operation and inserts the current value.
   */
  private AtomicLong nextKeyToInsert = new AtomicLong();

  /**
   * The highest key in the contiguous range of keys .
   */
  private AtomicLong insertedUpToKey = new AtomicLong();

  /** The sorted set of keys NOT inserted by the writers */
  private Set<Long> failedKeySet = new ConcurrentSkipListSet<Long>();

  /**
   * The total size of the temporary inserted key set that have not yet lined
   * up in a our contiguous sequence starting from startKey. Supposed to stay
   * small.
   */
  private AtomicLong insertedKeyQueueSize = new AtomicLong();

  /** Enable this if used in conjunction with a concurrent reader. */
  private boolean trackInsertedKeys;

  public MultiThreadedWriter(Configuration conf, byte[] tableName,
      byte[] columnFamily) {
    super(conf, tableName, columnFamily, "W");
  }

  /** Use multi-puts vs. separate puts for every column in a row */
  public void setMultiPut(boolean isMultiPut) {
    this.isMultiPut = isMultiPut;
  }

  public void setColumnsPerKey(long minColumnsPerKey, long maxColumnsPerKey) {
    this.minColumnsPerKey = minColumnsPerKey;
    this.maxColumnsPerKey = maxColumnsPerKey;
  }

  @Override
  public void start(long startKey, long endKey, int numThreads)
      throws IOException {
    super.start(startKey, endKey, numThreads);

    if (verbose) {
      LOG.debug("Inserting keys [" + startKey + ", " + endKey + ")");
    }

    nextKeyToInsert.set(startKey);
    insertedUpToKey.set(startKey - 1);

    for (int i = 0; i < numThreads; ++i) {
      HBaseWriterThread writer = new HBaseWriterThread(i);
      writers.add(writer);
    }

    if (trackInsertedKeys) {
      new Thread(new InsertedKeysTracker()).start();
      numThreadsWorking.incrementAndGet();
    }

    startThreads(writers);
  }

  public static byte[] longToByteArrayKey(long rowKey) {
    return LoadTestKVGenerator.md5PrefixedKey(rowKey).getBytes();
  }

  private class HBaseWriterThread extends Thread {
    private final HTable table;

    private final Random random = new Random();
    private final LoadTestKVGenerator dataGenerator = new LoadTestKVGenerator(
        minDataSize, maxDataSize);

    public HBaseWriterThread(int writerId) throws IOException {
      setName(getClass().getSimpleName() + "_" + writerId);
      table = new HTable(conf, tableName);
    }

    public void run() {
      try {
        long rowKey;
        while ((rowKey = nextKeyToInsert.getAndIncrement()) < endKey) {
          long numColumns = minColumnsPerKey + Math.abs(random.nextLong())
              % (maxColumnsPerKey - minColumnsPerKey);
          numKeys.addAndGet(1);
          if (isMultiPut) {
            multiPutInsertKey(rowKey, 0, numColumns);
          } else {
            for (long col = 0; col < numColumns; ++col) {
              insert(rowKey, col);
            }
          }
          if (trackInsertedKeys) {
            insertedKeys.add(rowKey);
          }
        }
      } finally {
        try {
          table.close();
        } catch (IOException e) {
          LOG.error("Error closing table", e);
        }
        numThreadsWorking.decrementAndGet();
      }
    }

    public void insert(long rowKey, long col) {
      Put put = new Put(longToByteArrayKey(rowKey));
      String colAsStr = String.valueOf(col);
      put.add(columnFamily, Bytes.toBytes(colAsStr),
          dataGenerator.generateRandomSizeValue(rowKey, colAsStr));
      try {
        long start = System.currentTimeMillis();
        table.put(put);
        numCols.addAndGet(1);
        totalOpTimeMs.addAndGet(System.currentTimeMillis() - start);
      } catch (IOException e) {
        failedKeySet.add(rowKey);
        LOG.error("Failed to insert: " + rowKey);
        e.printStackTrace();
      }
    }

    public void multiPutInsertKey(long rowKey, long startCol, long endCol) {
      if (verbose) {
        LOG.debug("Preparing put for key = " + rowKey + ", cols = ["
            + startCol + ", " + endCol + ")");
      }

      if (startCol >= endCol) {
        return;
      }

      Put put = new Put(LoadTestKVGenerator.md5PrefixedKey(
          rowKey).getBytes());
      byte[] columnQualifier;
      byte[] value;
      for (long i = startCol; i < endCol; ++i) {
        String qualStr = String.valueOf(i);
        columnQualifier = qualStr.getBytes();
        value = dataGenerator.generateRandomSizeValue(rowKey, qualStr);
        put.add(columnFamily, columnQualifier, value);
      }

      try {
        long start = System.currentTimeMillis();
        table.put(put);
        numCols.addAndGet(endCol - startCol);
        totalOpTimeMs.addAndGet(
            System.currentTimeMillis() - start);
      } catch (IOException e) {
        failedKeySet.add(rowKey);
        e.printStackTrace();
      }
    }
  }

  /**
   * A thread that keeps track of the highest key in the contiguous range of
   * inserted keys.
   */
  private class InsertedKeysTracker implements Runnable {

    @Override
    public void run() {
      Thread.currentThread().setName(getClass().getSimpleName());
      try {
        long expectedKey = startKey;
        Queue<Long> sortedKeys = new PriorityQueue<Long>();
        while (expectedKey < endKey) {
          // Block until a new element is available.
          Long k;
          try {
            k = insertedKeys.poll(1, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            LOG.info("Inserted key tracker thread interrupted", e);
            break;
          }
          if (k == null) {
            continue;
          }
          if (k == expectedKey) {
            // Skip the "sorted key" queue and consume this key.
            insertedUpToKey.set(k);
            ++expectedKey;
          } else {
            sortedKeys.add(k);
          }

          // See if we have a sequence of contiguous keys lined up.
          while (!sortedKeys.isEmpty()
              && ((k = sortedKeys.peek()) == expectedKey)) {
            sortedKeys.poll();
            insertedUpToKey.set(k);
            ++expectedKey;
          }

          insertedKeyQueueSize.set(insertedKeys.size() + sortedKeys.size());
        }
      } catch (Exception ex) {
        LOG.error("Error in inserted key tracker", ex);
      } finally {
        numThreadsWorking.decrementAndGet();
      }
    }

  }

  @Override
  public void waitForFinish() {
    super.waitForFinish();
    System.out.println("Failed to write keys: " + failedKeySet.size());
    for (Long key : failedKeySet) {
       System.out.println("Failed to write key: " + key);
    }
  }

  public int getNumWriteFailures() {
    return failedKeySet.size();
  }

  /**
   * The max key until which all keys have been inserted (successfully or not).
   * @return the last key that we have inserted all keys up to (inclusive)
   */
  public long insertedUpToKey() {
    return insertedUpToKey.get();
  }

  public boolean failedToWriteKey(long k) {
    return failedKeySet.contains(k);
  }

  @Override
  protected String progressInfo() {
    StringBuilder sb = new StringBuilder();
    appendToStatus(sb, "insertedUpTo", insertedUpToKey.get());
    appendToStatus(sb, "insertedQSize", insertedKeyQueueSize.get());
    return sb.toString();
  }

  /**
   * Used for a joint write/read workload. Enables tracking the last inserted
   * key, which requires a blocking queue and a consumer thread.
   * @param enable whether to enable tracking the last inserted key
   */
  void setTrackInsertedKeys(boolean enable) {
    trackInsertedKeys = enable;
  }

}
