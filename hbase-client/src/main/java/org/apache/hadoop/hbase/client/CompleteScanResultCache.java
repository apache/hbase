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
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A scan result cache that only returns complete result.
 */
@InterfaceAudience.Private
class CompleteScanResultCache extends ScanResultCache {

  private final List<Result> partialResults = new ArrayList<>();

  public CompleteScanResultCache(List<Result> cache) {
    super(cache);
  }

  private Result combine() throws IOException {
    Result result = Result.createCompleteResult(partialResults);
    partialResults.clear();
    return result;
  }

  private void prependCombinedAndCache(Result[] results, int length) throws IOException {
    if (length == 0) {
      checkUpdateNumberOfCompleteRowsAndCache(combine());
      return;
    }
    // the last part of a partial result may not be marked as partial so here we need to check if
    // there is a row change.
    int start = 0;
    if (Bytes.equals(partialResults.get(0).getRow(), results[0].getRow())) {
      partialResults.add(results[0]);
      start = 1;
    }
    checkUpdateNumberOfCompleteRowsAndCache(combine());
    addResultArrayToCache(results, start, length);
  }

  @Override
  public void loadResultsToCache(Result[] results, boolean isHeartbeatMessage) throws IOException {
    // If no results were returned it indicates that either we have the all the partial results
    // necessary to construct the complete result or the server had to send a heartbeat message
    // to the client to keep the client-server connection alive
    if (results.length == 0) {
      // If this response was an empty heartbeat message, then we have not exhausted the region
      // and thus there may be more partials server side that still need to be added to the partial
      // list before we form the complete Result
      if (!partialResults.isEmpty() && !isHeartbeatMessage) {
        checkUpdateNumberOfCompleteRowsAndCache(combine());
      }
      return;
    }
    // In every RPC response there should be at most a single partial result. Furthermore, if
    // there is a partial result, it is guaranteed to be in the last position of the array.
    Result last = results[results.length - 1];
    if (last.mayHaveMoreCellsInRow()) {
      if (partialResults.isEmpty()) {
        partialResults.add(last);
        addResultArrayToCache(results, 0, results.length - 1);
        return;
      }
      // We have only one result and it is partial
      if (results.length == 1) {
        // check if there is a row change
        if (Bytes.equals(partialResults.get(0).getRow(), last.getRow())) {
          partialResults.add(last);
          return;
        }
        Result completeResult = combine();
        partialResults.add(last);
        checkUpdateNumberOfCompleteRowsAndCache(completeResult);
        return;
      }
      // We have some complete results
      prependCombinedAndCache(results, results.length - 1);
      partialResults.add(last);
      return;
    }
    if (!partialResults.isEmpty()) {
      prependCombinedAndCache(results, results.length);
      return;
    }
    addResultArrayToCache(results, 0, results.length);
  }

  @Override
  protected void checkUpdateNumberOfCompleteRowsAndCache(Result rs) {
    numberOfCompleteRows++;
    addResultToCache(rs);
  }

  @Override
  public void clear() {
    partialResults.clear();
    super.clear();
  }
}
