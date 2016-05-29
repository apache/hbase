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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.test.LoadTestDataGenerator;

/** Creates multiple threads that read and verify previously written data */
public class MultiThreadedReader extends MultiThreadedAction
{
  private static final Log LOG = LogFactory.getLog(MultiThreadedReader.class);

  protected Set<HBaseReaderThread> readers = new HashSet<HBaseReaderThread>();
  private final double verifyPercent;
  protected volatile boolean aborted;

  protected MultiThreadedWriterBase writer = null;

  /**
   * The number of keys verified in a sequence. This will never be larger than
   * the total number of keys in the range. The reader might also verify
   * random keys when it catches up with the writer.
   */
  private final AtomicLong numUniqueKeysVerified = new AtomicLong();

  /**
   * Default maximum number of read errors to tolerate before shutting down all
   * readers.
   */
  public static final int DEFAULT_MAX_ERRORS = 10;

  /**
   * Default "window" size between the last key written by the writer and the
   * key that we attempt to read. The lower this number, the stricter our
   * testing is. If this is zero, we always attempt to read the highest key
   * in the contiguous sequence of keys written by the writers.
   */
  public static final int DEFAULT_KEY_WINDOW = 0;

  /**
   * Default batch size for multigets
   */
  public static final int DEFAULT_BATCH_SIZE = 1; //translates to simple GET (no multi GET)

  protected AtomicLong numKeysVerified = new AtomicLong(0);
  protected AtomicLong numReadErrors = new AtomicLong(0);
  protected AtomicLong numReadFailures = new AtomicLong(0);
  protected AtomicLong nullResult = new AtomicLong(0);
  private int maxErrors = DEFAULT_MAX_ERRORS;
  private int keyWindow = DEFAULT_KEY_WINDOW;
  private int batchSize = DEFAULT_BATCH_SIZE;
  private int regionReplicaId = -1; // particular region replica id to do reads against if set

  public MultiThreadedReader(LoadTestDataGenerator dataGen, Configuration conf,
      TableName tableName, double verifyPercent) throws IOException {
    super(dataGen, conf, tableName, "R");
    this.verifyPercent = verifyPercent;
  }

  public void linkToWriter(MultiThreadedWriterBase writer) {
    this.writer = writer;
    writer.setTrackWroteKeys(true);
  }

  public void setMaxErrors(int maxErrors) {
    this.maxErrors = maxErrors;
  }

  public void setKeyWindow(int keyWindow) {
    this.keyWindow = keyWindow;
  }

  public void setMultiGetBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public void setRegionReplicaId(int regionReplicaId) {
    this.regionReplicaId = regionReplicaId;
  }

  @Override
  public void start(long startKey, long endKey, int numThreads) throws IOException {
    super.start(startKey, endKey, numThreads);
    if (verbose) {
      LOG.debug("Reading keys [" + startKey + ", " + endKey + ")");
    }

    addReaderThreads(numThreads);
    startThreads(readers);
  }

  protected void addReaderThreads(int numThreads) throws IOException {
    for (int i = 0; i < numThreads; ++i) {
      HBaseReaderThread reader = createReaderThread(i);
      readers.add(reader);
    }
  }

  protected HBaseReaderThread createReaderThread(int readerId) throws IOException {
    HBaseReaderThread reader = new HBaseReaderThread(readerId);
    Threads.setLoggingUncaughtExceptionHandler(reader);
    return reader;
  }

  public class HBaseReaderThread extends Thread {
    protected final int readerId;
    protected final Table table;

    /** The "current" key being read. Increases from startKey to endKey. */
    private long curKey;

    /** Time when the thread started */
    protected long startTimeMs;

    /** If we are ahead of the writer and reading a random key. */
    private boolean readingRandomKey;

    private boolean printExceptionTrace = true;

    /**
     * @param readerId only the keys with this remainder from division by
     *          {@link #numThreads} will be read by this thread
     */
    public HBaseReaderThread(int readerId) throws IOException {
      this.readerId = readerId;
      table = createTable();
      setName(getClass().getSimpleName() + "_" + readerId);
    }

    protected Table createTable() throws IOException {
      return connection.getTable(tableName);
    }

    @Override
    public void run() {
      try {
        runReader();
      } finally {
        closeTable();
        numThreadsWorking.decrementAndGet();
      }
    }

    protected void closeTable() {
      try {
        if (table != null) {
          table.close();
        }
      } catch (IOException e) {
        LOG.error("Error closing table", e);
      }
    }

    private void runReader() {
      if (verbose) {
        LOG.info("Started thread #" + readerId + " for reads...");
      }

      startTimeMs = System.currentTimeMillis();
      curKey = startKey;
      long [] keysForThisReader = new long[batchSize];
      while (curKey < endKey && !aborted) {
        int readingRandomKeyStartIndex = -1;
        int numKeys = 0;
        // if multiGet, loop until we have the number of keys equal to the batch size
        do {
          long k = getNextKeyToRead();
          if (k < startKey || k >= endKey) {
            numReadErrors.incrementAndGet();
            throw new AssertionError("Load tester logic error: proposed key " +
                "to read " + k + " is out of range (startKey=" + startKey +
                ", endKey=" + endKey + ")");
          }
          if (k % numThreads != readerId ||
              writer != null && writer.failedToWriteKey(k)) {
            // Skip keys that this thread should not read, as well as the keys
            // that we know the writer failed to write.
            continue;
          }
          keysForThisReader[numKeys] = k;
          if (readingRandomKey && readingRandomKeyStartIndex == -1) {
            //store the first index of a random read
            readingRandomKeyStartIndex = numKeys;
          }
          numKeys++;
        } while (numKeys < batchSize && curKey < endKey && !aborted);

        if (numKeys > 0) { //meaning there is some key to read
          readKey(keysForThisReader);
          // We have verified some unique key(s).
          numUniqueKeysVerified.getAndAdd(readingRandomKeyStartIndex == -1 ?
              numKeys : readingRandomKeyStartIndex);
        }
      }
    }

    /**
     * Should only be used for the concurrent writer/reader workload. The
     * maximum key we are allowed to read, subject to the "key window"
     * constraint.
     */
    private long maxKeyWeCanRead() {
      long insertedUpToKey = writer.wroteUpToKey();
      if (insertedUpToKey >= endKey - 1) {
        // The writer has finished writing our range, so we can read any
        // key in the range.
        return endKey - 1;
      }
      return Math.min(endKey - 1, writer.wroteUpToKey() - keyWindow);
    }

    protected long getNextKeyToRead() {
      readingRandomKey = false;
      if (writer == null || curKey <= maxKeyWeCanRead()) {
        return curKey++;
      }

      // We caught up with the writer. See if we can read any keys at all.
      long maxKeyToRead;
      while ((maxKeyToRead = maxKeyWeCanRead()) < startKey) {
        // The writer has not written sufficient keys for us to be able to read
        // anything at all. Sleep a bit. This should only happen in the
        // beginning of a load test run.
        Threads.sleepWithoutInterrupt(50);
      }

      if (curKey <= maxKeyToRead) {
        // The writer wrote some keys, and we are now allowed to read our
        // current key.
        return curKey++;
      }

      // startKey <= maxKeyToRead <= curKey - 1. Read one of the previous keys.
      // Don't increment the current key -- we still have to try reading it
      // later. Set a flag to make sure that we don't count this key towards
      // the set of unique keys we have verified.
      readingRandomKey = true;
      return startKey + Math.abs(RandomUtils.nextLong())
          % (maxKeyToRead - startKey + 1);
    }

    private Get[] readKey(long[] keysToRead) {
      Get [] gets = new Get[keysToRead.length];
      int i = 0;
      for (long keyToRead : keysToRead) {
        try {
          gets[i] = createGet(keyToRead);
          if (keysToRead.length == 1) {
            queryKey(gets[i], RandomUtils.nextInt(100) < verifyPercent, keyToRead);
          }
          i++;
        } catch (IOException e) {
          numReadFailures.addAndGet(1);
          LOG.debug("[" + readerId + "] FAILED read, key = " + (keyToRead + "")
              + ", time from start: "
              + (System.currentTimeMillis() - startTimeMs) + " ms");
          if (printExceptionTrace) {
            LOG.warn(e);
            printExceptionTrace = false;
          }
        }
      }
      if (keysToRead.length > 1) {
        try {
          queryKey(gets, RandomUtils.nextInt(100) < verifyPercent, keysToRead);
        } catch (IOException e) {
          numReadFailures.addAndGet(gets.length);
          for (long keyToRead : keysToRead) {
            LOG.debug("[" + readerId + "] FAILED read, key = " + (keyToRead + "")
                + ", time from start: "
                + (System.currentTimeMillis() - startTimeMs) + " ms");
          }
          if (printExceptionTrace) {
            LOG.warn(e);
            printExceptionTrace = false;
          }
        }
      }
      return gets;
    }

    protected Get createGet(long keyToRead) throws IOException {
      Get get = new Get(dataGenerator.getDeterministicUniqueKey(keyToRead));
      String cfsString = "";
      byte[][] columnFamilies = dataGenerator.getColumnFamilies();
      for (byte[] cf : columnFamilies) {
        get.addFamily(cf);
        if (verbose) {
          if (cfsString.length() > 0) {
            cfsString += ", ";
          }
          cfsString += "[" + Bytes.toStringBinary(cf) + "]";
        }
      }
      get = dataGenerator.beforeGet(keyToRead, get);
      if (regionReplicaId > 0) {
        get.setReplicaId(regionReplicaId);
        get.setConsistency(Consistency.TIMELINE);
      }
      if (verbose) {
        LOG.info("[" + readerId + "] " + "Querying key " + keyToRead + ", cfs " + cfsString);
      }
      return get;
    }

    public void queryKey(Get[] gets, boolean verify, long[] keysToRead) throws IOException {
      // read the data
      long start = System.nanoTime();
      // Uses multi/batch gets
      Result[] results = table.get(Arrays.asList(gets));
      long end = System.nanoTime();
      verifyResultsAndUpdateMetrics(verify, gets, end - start, results, table, false);
    }

    public void queryKey(Get get, boolean verify, long keyToRead) throws IOException {
      // read the data

      long start = System.nanoTime();
      // Uses simple get
      Result result = table.get(get);
      long end = System.nanoTime();
      verifyResultsAndUpdateMetrics(verify, get, end - start, result, table, false);
    }

    protected void verifyResultsAndUpdateMetrics(boolean verify, Get[] gets, long elapsedNano,
        Result[] results, Table table, boolean isNullExpected)
        throws IOException {
      totalOpTimeMs.addAndGet(elapsedNano / 1000000);
      numKeys.addAndGet(gets.length);
      int i = 0;
      for (Result result : results) {
        verifyResultsAndUpdateMetricsOnAPerGetBasis(verify, gets[i++], result, table,
            isNullExpected);
      }
    }

    protected void verifyResultsAndUpdateMetrics(boolean verify, Get get, long elapsedNano,
        Result result, Table table, boolean isNullExpected)
        throws IOException {
      verifyResultsAndUpdateMetrics(verify, new Get[]{get}, elapsedNano,
          new Result[]{result}, table, isNullExpected);
    }

    private void verifyResultsAndUpdateMetricsOnAPerGetBasis(boolean verify, Get get,
        Result result, Table table, boolean isNullExpected) throws IOException {
      if (!result.isEmpty()) {
        if (verify) {
          numKeysVerified.incrementAndGet();
        }
      } else {
        HRegionLocation hloc = ((ClusterConnection) connection).getRegionLocation(tableName,
          get.getRow(), false);
        String rowKey = Bytes.toString(get.getRow());
        LOG.info("Key = " + rowKey + ", Region location: " + hloc);
        if(isNullExpected) {
          nullResult.incrementAndGet();
          LOG.debug("Null result obtained for the key ="+rowKey);
          return;
        }
      }
      boolean isOk = verifyResultAgainstDataGenerator(result, verify, false);
      long numErrorsAfterThis = 0;
      if (isOk) {
        long cols = 0;
        // Count the columns for reporting purposes.
        for (byte[] cf : result.getMap().keySet()) {
          cols += result.getFamilyMap(cf).size();
        }
        numCols.addAndGet(cols);
      } else {
        if (writer != null) {
          LOG.error("At the time of failure, writer wrote " + writer.numKeys.get() + " keys");
        }
        numErrorsAfterThis = numReadErrors.incrementAndGet();
      }

      if (numErrorsAfterThis > maxErrors) {
        LOG.error("Aborting readers -- found more than " + maxErrors + " errors");
        aborted = true;
      }
    }
  }

  public long getNumReadFailures() {
    return numReadFailures.get();
  }

  public long getNumReadErrors() {
    return numReadErrors.get();
  }

  public long getNumKeysVerified() {
    return numKeysVerified.get();
  }

  public long getNumUniqueKeysVerified() {
    return numUniqueKeysVerified.get();
  }

  public long getNullResultsCount() {
    return nullResult.get();
  }

  @Override
  protected String progressInfo() {
    StringBuilder sb = new StringBuilder();
    appendToStatus(sb, "verified", numKeysVerified.get());
    appendToStatus(sb, "READ FAILURES", numReadFailures.get());
    appendToStatus(sb, "READ ERRORS", numReadErrors.get());
    appendToStatus(sb, "NULL RESULT", nullResult.get());
    return sb.toString();
  }
}
