/**
 * Copyright 2010 The Apache Software Foundation
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerTimeoutException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;


/**
 * Iterate over an HBase table data, return (Text, RowResult) pairs
 */
@Deprecated
public class TableRecordReaderImpl {
  static final Log LOG = LogFactory.getLog(TableRecordReaderImpl.class);

  private Scan scan = null;
  protected byte [] startRow;
  protected byte [] endRow;
  protected byte [] lastRow = null;
  private Filter trrRowFilter;
  private ResultScanner scanner;
  private HTable htable;
  private byte [][] trrInputColumns;
  private JobConf conf;

  private int timeoutRetryNum = 3;
  private int timeoutRetrySleepBaseMs = 5000;
  private int conseqTimeoutErrorNum = 0;

  /**
   * Restart from survivable exceptions by creating a new scanner.
   *
   * @param firstRow
   * @throws IOException
   */
  public void restart(byte[] firstRow) throws IOException {
    Scan scan = new Scan(this.scan);
    scan.setStartRow(firstRow);
    boolean useClientLocalScan = conf.getBoolean(
        org.apache.hadoop.hbase.mapreduce.TableInputFormat.USE_CLIENT_LOCAL_SCANNER,
        org.apache.hadoop.hbase.mapreduce.TableInputFormat.DEFAULT_USE_CLIENT_LOCAL_SCANNER);
    boolean useSnapshot = conf.getBoolean(
        org.apache.hadoop.hbase.mapreduce.TableInputFormat.CLIENT_LOCAL_SCANNER_USE_SNAPSHOT,
        org.apache.hadoop.hbase.mapreduce.TableInputFormat.DEFAULT_CLIENT_LOCAL_SCANNER_USE_SNAPSHOT);
    if ((endRow != null) && (endRow.length > 0)) {
      scan.setStopRow(endRow);
      if (trrRowFilter != null) {
        scan.addColumns(trrInputColumns);
        scan.setFilter(trrRowFilter);
        scan.setCacheBlocks(false);
        if (useClientLocalScan) {
          this.scanner = this.htable.getLocalScanner(scan, !useSnapshot);
        } else {
          this.scanner = this.htable.getScanner(scan);
        }
      } else {
        LOG.debug("TIFB.restart, firstRow: " +
            Bytes.toStringBinary(firstRow) + ", endRow: " +
            Bytes.toStringBinary(endRow));
        scan.addColumns(trrInputColumns);
        if (useClientLocalScan) {
          this.scanner = this.htable.getLocalScanner(scan, !useSnapshot);
        } else {
          this.scanner = this.htable.getScanner(scan);
        }
      }
    } else {
      LOG.debug("TIFB.restart, firstRow: " +
          Bytes.toStringBinary(firstRow) + ", no endRow");

      scan.addColumns(trrInputColumns);
//      scan.setFilter(trrRowFilter);
      if (useClientLocalScan) {
        this.scanner = this.htable.getLocalScanner(scan);
      } else {
        this.scanner = this.htable.getScanner(scan);
      }
    }
  }

  /**
   * Build the scanner. Not done in constructor to allow for extension.
   *
   * @throws IOException
   */
  public void init(JobConf conf) throws IOException {
    this.conf = conf;
    restart(startRow);
  }

  byte[] getStartRow() {
    return this.startRow;
  }
  /**
   * @param htable the {@link HTable} to scan.
   */
  public void setHTable(HTable htable) {
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

  public byte[] getEndRow() {
    return endRow;
  }

  /**
   * @param rowFilter the {@link Filter} to be used.
   */
  public void setRowFilter(Filter rowFilter) {
    this.trrRowFilter = rowFilter;
  }

  public void close() {
    this.scanner.close();
  }

  public void setTimeoutRetryNumber(int retryNum) {
    this.timeoutRetryNum = retryNum;
  }

  public void setTimeoutRetrySleepBaseMs(int sleepMs) {
    this.timeoutRetrySleepBaseMs = sleepMs;
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
   * @throws IOException
   */
  public boolean next(ImmutableBytesWritable key, Result value)
  throws IOException {
    Result result = null;

    // retry if limited number of timeout errors are encountered
    boolean shouldTryAgain = false;
    do {
      try {
        result = this.scanner.next();

        shouldTryAgain = false;
        conseqTimeoutErrorNum = 0;
      } catch (UnknownScannerException e) {
        shouldTryAgain = false;

        LOG.debug("recovered from " + StringUtils.stringifyException(e));
        if (lastRow == null) {
          restart(startRow);
        } else {
          restart(lastRow);
          // skip presumed already mapped row
          this.scanner.next();
        }
        result = this.scanner.next();
      } catch (ScannerTimeoutException e) {
        LOG.debug("Scanner time out:" + StringUtils.stringifyException(e));

        if (++conseqTimeoutErrorNum <= timeoutRetryNum) {
          shouldTryAgain = true;

          int sleepMs = timeoutRetrySleepBaseMs * (1 << conseqTimeoutErrorNum);
          LOG.debug("sleep "  + sleepMs + "ms in " + conseqTimeoutErrorNum + "-th retry");
          try {
            Thread.sleep(sleepMs);
          } catch (InterruptedException ie) {
            LOG.debug("Sleep interrupted - " + StringUtils.stringifyException(e));
          }

          if (lastRow == null) {
            restart(startRow);
          } else {
            restart(lastRow);
            // skip presumed already mapped row
            this.scanner.next();
          }

          // no need to assign result, it will be assigned in next retry
        } else {
          shouldTryAgain = false;

          LOG.debug("max timeout retry count:" + timeoutRetryNum + " reached, scan failed.");
          result = null;
        }
      }
    } while (shouldTryAgain);

    if (result != null && result.size() > 0) {
      key.set(result.getRow());
      lastRow = key.get();
      Writables.copyWritable(result, value);
      return true;
    }
    return false;
  }

  public void setScan(Scan scan) {
    this.scan = scan;
  }
}
