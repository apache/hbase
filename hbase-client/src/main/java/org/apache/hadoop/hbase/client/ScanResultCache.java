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
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Used to separate the row constructing logic.
 * <p>
 * After we add heartbeat support for scan, RS may return partial result even if allowPartial is
 * false and batch is 0. With this interface, the implementation now looks like:
 * <ol>
 * <li>Get results from ScanResponse proto.</li>
 * <li>Pass them to ScanResultCache and get something back.</li>
 * <li>If we actually get something back, then pass it to ScanConsumer.</li>
 * </ol>
 */
@InterfaceAudience.Private
public abstract class ScanResultCache {

  static final Result[] EMPTY_RESULT_ARRAY = new Result[0];
  int numberOfCompleteRows;
  long resultSize = 0;
  int count = 0;
  Result lastResult = null;
  List<Result> cache;

  ScanResultCache(List<Result> cache) {
    this.cache = cache;
  }

  /**
   * Process the results from the server and load it to cache.
   * @param results the results of a scan next. Must not be null.
   * @param isHeartbeatMessage indicate whether the results is gotten from a heartbeat response.
   */
  abstract void loadResultsToCache(Result[] results, boolean isHeartbeatMessage)
      throws IOException;

  /**
   * Clear the cached result if any. Called when scan error and we will start from a start of a row
   * again.
   */
  void clear() {
    resetCount();
    resetResultSize();
    lastResult = null;
  }

  /**
   * Return the number of complete rows. Used to implement limited scan.
   */
  int numberOfCompleteRows() {
    return numberOfCompleteRows;
  }

  /**
   * Add result array received from server to cache
   * @param resultsToAddToCache The array of Results returned from the server
   * @param start start index to cache from Results array
   * @param end last index to cache from Results array
   */
  void addResultArrayToCache(Result[] resultsToAddToCache, int start, int end) {
    if (resultsToAddToCache != null) {
      for (int r = start; r < end; r++) {
        checkUpdateNumberOfCompleteRowsAndCache(resultsToAddToCache[r]);
      }
    }
  }

  /**
   * Check and update number of complete rows and add result to cache
   * @param rs Result to cache from Results array or constructed from partial results
   */
  abstract void checkUpdateNumberOfCompleteRowsAndCache(Result rs);

  /**
   * Add the result received from server or result constructed from partials to cache
   * @param rs Result to cache from Results array or constructed from partial results
   */
  void addResultToCache(Result rs) {
    cache.add(rs);
    for (Cell cell : rs.rawCells()) {
      resultSize += CellUtil.estimatedHeapSizeOf(cell);
    }
    count++;
    lastResult = rs;
  }

  long getResultSize() {
    return resultSize;
  }

  int getCount() {
    return count;
  }

  void resetResultSize() {
    resultSize = 0;
  }

  void resetCount() {
    count = 0;
  }

  Result getLastResult() {
    return lastResult;
  }
}
