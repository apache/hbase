/*
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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterate over an HBase table data, return (ImmutableBytesWritable, Result)
 * pairs.
 */
@InterfaceAudience.Public
public class TableRecordReaderImpl {
  public static final String LOG_PER_ROW_COUNT
      = "hbase.mapreduce.log.scanner.rowcount";

  private static final Logger LOG = LoggerFactory.getLogger(TableRecordReaderImpl.class);

  // HBASE_COUNTER_GROUP_NAME is the name of mapreduce counter group for HBase
  @InterfaceAudience.Private
  static final String HBASE_COUNTER_GROUP_NAME = "HBaseCounters";

  private ResultScanner scanner = null;
  private Scan scan = null;
  private Scan currentScan = null;
  private Table htable = null;
  private byte[] lastSuccessfulRow = null;
  private ImmutableBytesWritable key = null;
  private Result value = null;
  private TaskAttemptContext context = null;
  private long numRestarts = 0;
  private long numStale = 0;
  private long timestamp;
  private int rowcount;
  private boolean logScannerActivity = false;
  private int logPerRowCount = 100;

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow  The first row to start at.
   * @throws IOException When restarting fails.
   */
  public void restart(byte[] firstRow) throws IOException {
    // Update counter metrics based on current scan before reinitializing it
    if (currentScan != null) {
      updateCounters();
    }
    currentScan = new Scan(scan);
    currentScan.withStartRow(firstRow);
    currentScan.setScanMetricsEnabled(true);
    if (this.scanner != null) {
      if (logScannerActivity) {
        LOG.info("Closing the previously opened scanner object.");
      }
      this.scanner.close();
    }
    this.scanner = this.htable.getScanner(currentScan);
    if (logScannerActivity) {
      LOG.info("Current scan=" + currentScan.toString());
      timestamp = System.currentTimeMillis();
      rowcount = 0;
    }
  }

  /**
   * In new mapreduce APIs, TaskAttemptContext has two getCounter methods
   * Check if getCounter(String, String) method is available.
   * @return The getCounter method or null if not available.
   * @deprecated since 2.4.0 and 2.3.2, will be removed in 4.0.0
   */
  @Deprecated
  protected static Method retrieveGetCounterWithStringsParams(TaskAttemptContext context)
    throws IOException {
    Method m = null;
    try {
      m = context.getClass().getMethod("getCounter",
        new Class [] {String.class, String.class});
    } catch (SecurityException e) {
      throw new IOException("Failed test for getCounter", e);
    } catch (NoSuchMethodException e) {
      // Ignore
    }
    return m;
  }

  /**
   * Sets the HBase table.
   *
   * @param htable  The {@link org.apache.hadoop.hbase.HTableDescriptor} to scan.
   */
  public void setHTable(Table htable) {
    Configuration conf = htable.getConfiguration();
    logScannerActivity = conf.getBoolean(
      "hbase.client.log.scanner.activity" /*ScannerCallable.LOG_SCANNER_ACTIVITY*/, false);
    logPerRowCount = conf.getInt(LOG_PER_ROW_COUNT, 100);
    this.htable = htable;
  }

  /**
   * Sets the scan defining the actual details like columns etc.
   *
   * @param scan  The scan to set.
   */
  public void setScan(Scan scan) {
    this.scan = scan;
  }

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   */
  public void initialize(InputSplit inputsplit,
      TaskAttemptContext context) throws IOException,
      InterruptedException {
    if (context != null) {
      this.context = context;
    }
    restart(scan.getStartRow());
  }

  /**
   * Closes the split.
   *
   *
   */
  public void close() {
    if (this.scanner != null) {
      this.scanner.close();
    }
    try {
      this.htable.close();
    } catch (IOException ioe) {
      LOG.warn("Error closing table", ioe);
    }
  }

  /**
   * Returns the current key.
   *
   * @return The current key.
   * @throws InterruptedException When the job is aborted.
   */
  public ImmutableBytesWritable getCurrentKey() throws IOException,
      InterruptedException {
    return key;
  }

  /**
   * Returns the current value.
   *
   * @return The current value.
   * @throws IOException When the value is faulty.
   * @throws InterruptedException When the job is aborted.
   */
  public Result getCurrentValue() throws IOException, InterruptedException {
    return value;
  }


  /**
   * Positions the record reader to the next record.
   *
   * @return <code>true</code> if there was another record.
   * @throws IOException When reading the record failed.
   * @throws InterruptedException When the job was aborted.
   */
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (key == null) {
      key = new ImmutableBytesWritable();
    }
    if (value == null) {
      value = new Result();
    }
    try {
      try {
        value = this.scanner.next();
        if (value != null && value.isStale()) {
          numStale++;
        }
        if (logScannerActivity) {
          rowcount ++;
          if (rowcount >= logPerRowCount) {
            long now = System.currentTimeMillis();
            LOG.info("Mapper took {}ms to process {} rows", (now - timestamp), rowcount);
            timestamp = now;
            rowcount = 0;
          }
        }
      } catch (IOException e) {
        // do not retry if the exception tells us not to do so
        if (e instanceof DoNotRetryIOException) {
          updateCounters();
          throw e;
        }
        // try to handle all other IOExceptions by restarting
        // the scanner, if the second call fails, it will be rethrown
        LOG.info("recovered from " + StringUtils.stringifyException(e));
        if (lastSuccessfulRow == null) {
          LOG.warn("We are restarting the first next() invocation," +
              " if your mapper has restarted a few other times like this" +
              " then you should consider killing this job and investigate" +
              " why it's taking so long.");
        }
        if (lastSuccessfulRow == null) {
          restart(scan.getStartRow());
        } else {
          restart(lastSuccessfulRow);
          scanner.next();    // skip presumed already mapped row
        }
        value = scanner.next();
        if (value != null && value.isStale()) {
          numStale++;
        }
        numRestarts++;
      }

      if (value != null && value.size() > 0) {
        key.set(value.getRow());
        lastSuccessfulRow = key.get();
        return true;
      }

      // Need handle cursor result
      if (value != null && value.isCursor()) {
        key.set(value.getCursor().getRow());
        lastSuccessfulRow = key.get();
        return true;
      }

      updateCounters();
      return false;
    } catch (IOException ioe) {
      updateCounters();
      if (logScannerActivity) {
        long now = System.currentTimeMillis();
        LOG.info("Mapper took {}ms to process {} rows", (now - timestamp), rowcount);
        LOG.info(ioe.toString(), ioe);
        String lastRow = lastSuccessfulRow == null ?
          "null" : Bytes.toStringBinary(lastSuccessfulRow);
        LOG.info("lastSuccessfulRow=" + lastRow);
      }
      throw ioe;
    }
  }

  /**
   * If hbase runs on new version of mapreduce, RecordReader has access to
   * counters thus can update counters based on scanMetrics.
   * If hbase runs on old version of mapreduce, it won't be able to get
   * access to counters and TableRecorderReader can't update counter values.
   */
  private void updateCounters() {
    ScanMetrics scanMetrics = scanner.getScanMetrics();
    if (scanMetrics == null) {
      return;
    }

    updateCounters(scanMetrics, numRestarts, context, numStale);
  }

  /**
   * @deprecated since 2.4.0 and 2.3.2, will be removed in 4.0.0
   *   Use {@link #updateCounters(ScanMetrics, long, TaskAttemptContext, long)} instead.
   */
  @Deprecated
  protected static void updateCounters(ScanMetrics scanMetrics, long numScannerRestarts,
      Method getCounter, TaskAttemptContext context, long numStale) {
    updateCounters(scanMetrics, numScannerRestarts, context, numStale);
  }

  protected static void updateCounters(ScanMetrics scanMetrics, long numScannerRestarts,
      TaskAttemptContext context, long numStale) {
    // we can get access to counters only if hbase uses new mapreduce APIs
    if (context == null) {
      return;
    }

      for (Map.Entry<String, Long> entry : scanMetrics.getMetricsMap().entrySet()) {
        Counter counter = context.getCounter(HBASE_COUNTER_GROUP_NAME, entry.getKey());
        if (counter != null) {
          counter.increment(entry.getValue());
        }
      }
      if (numScannerRestarts != 0L) {
        Counter counter = context.getCounter(HBASE_COUNTER_GROUP_NAME, "NUM_SCANNER_RESTARTS");
        if (counter != null) {
          counter.increment(numScannerRestarts);
        }
      }
      if (numStale != 0L) {
        Counter counter = context.getCounter(HBASE_COUNTER_GROUP_NAME, "NUM_SCAN_RESULTS_STALE");
        if (counter != null) {
          counter.increment(numStale);
        }
      }
  }

  /**
   * The current progress of the record reader through its data.
   *
   * @return A number between 0.0 and 1.0, the fraction of the data read.
   */
  public float getProgress() {
    // Depends on the total number of tuples
    return 0;
  }

}
