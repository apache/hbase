/*
 *
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
package org.apache.hadoop.hbase.mapred;

import static org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl.LOG_PER_ROW_COUNT;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iterate over an HBase table data, return (Text, RowResult) pairs
 */
@InterfaceAudience.Public
public class TableRecordReaderImpl {
  private static final Logger LOG = LoggerFactory.getLogger(TableRecordReaderImpl.class);

  private byte [] startRow;
  private byte [] endRow;
  private byte [] lastSuccessfulRow;
  private Filter trrRowFilter;
  private ResultScanner scanner;
  private Table htable;
  private byte [][] trrInputColumns;
  private long timestamp;
  private int rowcount;
  private boolean logScannerActivity = false;
  private int logPerRowCount = 100;

  /**
   * Restart from survivable exceptions by creating a new scanner.
   */
  public void restart(byte[] firstRow) throws IOException {
    Scan currentScan;
    if ((endRow != null) && (endRow.length > 0)) {
      if (trrRowFilter != null) {
        Scan scan = new Scan(firstRow, endRow);
        TableInputFormat.addColumns(scan, trrInputColumns);
        scan.setFilter(trrRowFilter);
        scan.setCacheBlocks(false);
        this.scanner = this.htable.getScanner(scan);
        currentScan = scan;
      } else {
        LOG.debug("TIFB.restart, firstRow: " +
            Bytes.toStringBinary(firstRow) + ", endRow: " +
            Bytes.toStringBinary(endRow));
        Scan scan = new Scan(firstRow, endRow);
        TableInputFormat.addColumns(scan, trrInputColumns);
        this.scanner = this.htable.getScanner(scan);
        currentScan = scan;
      }
    } else {
      LOG.debug("TIFB.restart, firstRow: " +
          Bytes.toStringBinary(firstRow) + ", no endRow");

      Scan scan = new Scan(firstRow);
      TableInputFormat.addColumns(scan, trrInputColumns);
      scan.setFilter(trrRowFilter);
      this.scanner = this.htable.getScanner(scan);
      currentScan = scan;
    }
    if (logScannerActivity) {
      LOG.info("Current scan=" + currentScan.toString());
      timestamp = System.currentTimeMillis();
      rowcount = 0;
    }
  }

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   */
  public void init() throws IOException {
    restart(startRow);
  }

  byte[] getStartRow() {
    return this.startRow;
  }
  /**
   * @param htable the {@link org.apache.hadoop.hbase.HTableDescriptor} to scan.
   */
  public void setHTable(Table htable) {
    Configuration conf = htable.getConfiguration();
    logScannerActivity = conf.getBoolean(
      "hbase.client.log.scanner.activity" /*ScannerCallable.LOG_SCANNER_ACTIVITY*/, false);
    logPerRowCount = conf.getInt(LOG_PER_ROW_COUNT, 100);
    this.htable = htable;
  }

  /**
   * @param inputColumns the columns to be placed in {@link Result}.
   */
  public void setInputColumns(final byte [][] inputColumns) {
    this.trrInputColumns = inputColumns;
  }

  /**
   * @param startRow the first row in the split
   */
  public void setStartRow(final byte [] startRow) {
    this.startRow = startRow;
  }

  /**
   *
   * @param endRow the last row in the split
   */
  public void setEndRow(final byte [] endRow) {
    this.endRow = endRow;
  }

  /**
   * @param rowFilter the {@link Filter} to be used.
   */
  public void setRowFilter(Filter rowFilter) {
    this.trrRowFilter = rowFilter;
  }

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
   * @return ImmutableBytesWritable
   *
   * @see org.apache.hadoop.mapred.RecordReader#createKey()
   */
  public ImmutableBytesWritable createKey() {
    return new ImmutableBytesWritable();
  }

  /**
   * @return RowResult
   *
   * @see org.apache.hadoop.mapred.RecordReader#createValue()
   */
  public Result createValue() {
    return new Result();
  }

  public long getPos() {
    // This should be the ordinal tuple in the range;
    // not clear how to calculate...
    return 0;
  }

  public float getProgress() {
    // Depends on the total number of tuples and getPos
    return 0;
  }

  /**
   * @param key HStoreKey as input key.
   * @param value MapWritable as input value
   * @return true if there was more data
   */
  public boolean next(ImmutableBytesWritable key, Result value) throws IOException {
    Result result;
    try {
      try {
        result = this.scanner.next();
        if (logScannerActivity) {
          rowcount ++;
          if (rowcount >= logPerRowCount) {
            long now = System.currentTimeMillis();
            LOG.info("Mapper took " + (now-timestamp)
              + "ms to process " + rowcount + " rows");
            timestamp = now;
            rowcount = 0;
          }
        }
      } catch (IOException e) {
        // do not retry if the exception tells us not to do so
        if (e instanceof DoNotRetryIOException) {
          throw e;
        }
        // try to handle all other IOExceptions by restarting
        // the scanner, if the second call fails, it will be rethrown
        LOG.debug("recovered from " + StringUtils.stringifyException(e));
        if (lastSuccessfulRow == null) {
          LOG.warn("We are restarting the first next() invocation," +
              " if your mapper has restarted a few other times like this" +
              " then you should consider killing this job and investigate" +
              " why it's taking so long.");
        }
        if (lastSuccessfulRow == null) {
          restart(startRow);
        } else {
          restart(lastSuccessfulRow);
          this.scanner.next();    // skip presumed already mapped row
        }
        result = this.scanner.next();
      }

      if (result != null && result.size() > 0) {
        key.set(result.getRow());
        lastSuccessfulRow = key.get();
        value.copyFrom(result);
        return true;
      }
      return false;
    } catch (IOException ioe) {
      if (logScannerActivity) {
        long now = System.currentTimeMillis();
        LOG.info("Mapper took " + (now-timestamp)
          + "ms to process " + rowcount + " rows");
        LOG.info(ioe.toString(), ioe);
        String lastRow = lastSuccessfulRow == null ?
          "null" : Bytes.toStringBinary(lastSuccessfulRow);
        LOG.info("lastSuccessfulRow=" + lastRow);
      }
      throw ioe;
    }
  }
}
