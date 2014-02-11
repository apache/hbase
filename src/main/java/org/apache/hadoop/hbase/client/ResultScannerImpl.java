/**
 * Copyright 2013 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * This abstract class was designed in order to share code across ClientScanner
 * and ClientLocalScanner and unify the common code.
 */
public abstract class ResultScannerImpl implements ResultScanner {
  protected static final Log CLIENT_LOG =
      LogFactory.getLog(ResultScannerImpl.class);
  protected Scan scan;
  protected HRegionInfo currentRegion = null;
  protected final int caching;
  protected final LinkedList<Result> cache = new LinkedList<Result>();
  protected long lastNextCallTimeStamp;
  protected boolean closed = false;
  protected final HTable htable;

  protected ResultScannerImpl(final Scan scan, final HTable htable) {
    this.htable = htable;
    this.scan = scan;
    this.lastNextCallTimeStamp = EnvironmentEdgeManager.currentTimeMillis();

    // Use the caching from the Scan.
    // If not set, use the default cache setting for this table.
    if (this.scan.getCaching() > 0) {
      this.caching = this.scan.getCaching();
    } else {
      this.caching = htable.scannerCaching;
    }
    if (CLIENT_LOG.isDebugEnabled()) {
      CLIENT_LOG.debug("Creating scanner over "
          + Bytes.toString(htable.getTableName())
          + " starting at key '" + Bytes.toStringBinary(scan.getStartRow()) + "'");
    }
  }

  protected void initialize() throws IOException {
    nextScanner(this.caching, false);
  }

  private boolean checkScanStopRow(final byte [] endKey) {
    if (this.scan.getStopRow().length > 0) {
      // there is a stop row, check to see if we are past it.
      byte [] stopRow = scan.getStopRow();
      int cmp = Bytes.compareTo(stopRow, 0, stopRow.length,
        endKey, 0, endKey.length);
      if (cmp <= 0) {
        // stopRow <= endKey (endKey is equals to or larger than stopRow)
        // This is a stop.
        return true;
      }
    }
    return false; //unlikely.
  }

  protected boolean nextScanner(int nbRows, final boolean done)
  throws IOException{
    cleanUpPreviousScanners();
 // Where to start the next scanner
    byte [] localStartKey;

    // if we're at end of table, close and return false to stop iterating
    if (this.currentRegion != null) {
      byte [] endKey = this.currentRegion.getEndKey();
      if (endKey == null ||
          Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
          checkScanStopRow(endKey) ||
          done) {
        close();
        if (CLIENT_LOG.isDebugEnabled()) {
          CLIENT_LOG.debug("Finished with scanning at " + this.currentRegion);
        }
        return false;
      }
      localStartKey = endKey;
      if (CLIENT_LOG.isDebugEnabled()) {
        CLIENT_LOG.debug("Finished with region " + this.currentRegion);
      }
    } else {
      localStartKey = this.scan.getStartRow();
    }

    if (CLIENT_LOG.isDebugEnabled()) {
      CLIENT_LOG.debug("Advancing internal scanner to startKey at '" +
        Bytes.toStringBinary(localStartKey) + "'");
    }
    return doRealOpenScanners(localStartKey, nbRows);
  }

  /**
   * This function is intended clean up the previous scanners before opening
   * a new scanner.
   * @throws IOException
   */
  protected abstract void cleanUpPreviousScanners() throws IOException;

  /**
   * Opens the scanners once the start key and current region is identified
   */
  protected abstract boolean doRealOpenScanners(byte[] localStartKey,
      int nbRows) throws IOException;

  @Override
  public Result next() throws IOException {
    // If the scanner is closed but there is some rows left in the cache,
    // it will first empty it before returning null
    if (cache.size() == 0 && this.closed) {
      return null;
    }
    if (cache.size() == 0) {
      cacheNextResults();
    }
    if (cache.size() > 0) {
      return cache.poll();
    }
    return null;
  }

  protected long getTimestamp() {
    return lastNextCallTimeStamp;
  }

  /**
   * Fetches the next 'caching' number of results to cache.
   */
  protected abstract void cacheNextResults() throws IOException;

  /**
   * Gets the next nbRows. Internally calls next iteratively.
   */
  @Override
  public Result [] next(int nbRows) throws IOException {
    // Collect values to be returned here
    ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
    for(int i = 0; i < nbRows; i++) {
      Result next = next();
      if (next != null) {
        resultSets.add(next);
      } else {
        break;
      }
    }
    return resultSets.toArray(new Result[resultSets.size()]);
  }

  @Override
  public void close() {
    closeCurrentScanner();
    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  protected abstract void closeCurrentScanner();

  /**
   * Iterator has been taken from the ClientScanner code. Mostly same code.
   * Removing redundant comments.
   */
  @Override
  public Iterator<Result> iterator() {
    return new Iterator<Result>() {
      Result next = null;
      public boolean hasNext() {
        if (next == null) {
          try {
            // Since ResultScannerImpl contains this anonymous
            // Iterator<Result> class inside it, we can access the methods of
            // ResultScannerImpl by this way.
            next = ResultScannerImpl.this.next();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }
      public Result next() {
        if (!hasNext()) {
          return null;
        }
        Result temp = next;
        next = null;
        return temp;
      }
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
}
