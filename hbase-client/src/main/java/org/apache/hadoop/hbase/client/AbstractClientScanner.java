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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

/**
 * Helper class for custom client scanners.
 */
@InterfaceAudience.Private
public abstract class AbstractClientScanner implements ResultScanner {
  protected ScanMetrics scanMetrics;

  /**
   * Check and initialize if application wants to collect scan metrics
   */
  protected void initScanMetrics(Scan scan) {
    // check if application wants to collect scan metrics
    if (scan.isScanMetricsEnabled()) {
      scanMetrics = new ScanMetrics();
    }
  }

  /**
   * Used internally accumulating metrics on scan. To
   * enable collection of metrics on a Scanner, call {@link Scan#setScanMetricsEnabled(boolean)}.
   * These metrics are cleared at key transition points. Metrics are accumulated in the
   * {@link Scan} object itself.
   * @see Scan#getScanMetrics()
   * @return Returns the running {@link ScanMetrics} instance or null if scan metrics not enabled.
   */
  public ScanMetrics getScanMetrics() {
    return scanMetrics;
  }

  /**
   * Get <param>nbRows</param> rows.
   * How many RPCs are made is determined by the {@link Scan#setCaching(int)}
   * setting (or hbase.client.scanner.caching in hbase-site.xml).
   * @param nbRows number of rows to return
   * @return Between zero and <param>nbRows</param> RowResults.  Scan is done
   * if returned array is of zero-length (We never return null).
   * @throws IOException
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
  public Iterator<Result> iterator() {
    return new Iterator<Result>() {
      // The next RowResult, possibly pre-read
      Result next = null;

      // return true if there is another item pending, false if there isn't.
      // this method is where the actual advancing takes place, but you need
      // to call next() to consume it. hasNext() will only advance if there
      // isn't a pending next().
      @Override
      public boolean hasNext() {
        if (next == null) {
          try {
            next = AbstractClientScanner.this.next();
            return next != null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
        return true;
      }

      // get the pending next item and advance the iterator. returns null if
      // there is no next item.
      @Override
      public Result next() {
        // since hasNext() does the real advancing, we call this to determine
        // if there is a next before proceeding.
        if (!hasNext()) {
          return null;
        }

        // if we get to here, then hasNext() has given us an item to return.
        // we want to return the item and then null out the next pointer, so
        // we use a temporary variable.
        Result temp = next;
        next = null;
        return temp;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }
  /**
   * Allow the client to renew the scanner's lease on the server.
   * @return true if the lease was successfully renewed, false otherwise.
   */
  // Note that this method should be on ResultScanner, but that is marked stable.
  // Callers have to cast their instance of ResultScanner to AbstractClientScanner to use this.
  public abstract boolean renewLease();
}
