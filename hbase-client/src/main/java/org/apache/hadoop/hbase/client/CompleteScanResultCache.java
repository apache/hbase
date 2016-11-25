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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A scan result cache that only returns complete result.
 */
@InterfaceAudience.Private
class CompleteScanResultCache implements ScanResultCache {

  private final List<Result> partialResults = new ArrayList<>();

  private Result combine() throws IOException {
    Result result = Result.createCompleteResult(partialResults);
    partialResults.clear();
    return result;
  }

  private Result[] prependCombined(Result[] results, int length) throws IOException {
    if (length == 0) {
      return new Result[] { combine() };
    }
    // the last part of a partial result may not be marked as partial so here we need to check if
    // there is a row change.
    int start;
    if (Bytes.equals(partialResults.get(0).getRow(), results[0].getRow())) {
      partialResults.add(results[0]);
      start = 1;
      length--;
    } else {
      start = 0;
    }
    Result[] prependResults = new Result[length + 1];
    prependResults[0] = combine();
    System.arraycopy(results, start, prependResults, 1, length);
    return prependResults;
  }

  @Override
  public Result[] addAndGet(Result[] results, boolean isHeartbeatMessage) throws IOException {
    // If no results were returned it indicates that either we have the all the partial results
    // necessary to construct the complete result or the server had to send a heartbeat message
    // to the client to keep the client-server connection alive
    if (results.length == 0) {
      // If this response was an empty heartbeat message, then we have not exhausted the region
      // and thus there may be more partials server side that still need to be added to the partial
      // list before we form the complete Result
      if (!partialResults.isEmpty() && !isHeartbeatMessage) {
        return new Result[] { combine() };
      }
      return EMPTY_RESULT_ARRAY;
    }
    // In every RPC response there should be at most a single partial result. Furthermore, if
    // there is a partial result, it is guaranteed to be in the last position of the array.
    Result last = results[results.length - 1];
    if (last.isPartial()) {
      if (partialResults.isEmpty()) {
        partialResults.add(last);
        return Arrays.copyOf(results, results.length - 1);
      }
      // We have only one result and it is partial
      if (results.length == 1) {
        // check if there is a row change
        if (Bytes.equals(partialResults.get(0).getRow(), last.getRow())) {
          partialResults.add(last);
          return EMPTY_RESULT_ARRAY;
        }
        Result completeResult = combine();
        partialResults.add(last);
        return new Result[] { completeResult };
      }
      // We have some complete results
      Result[] resultsToReturn = prependCombined(results, results.length - 1);
      partialResults.add(last);
      return resultsToReturn;
    }
    if (!partialResults.isEmpty()) {
      return prependCombined(results, results.length);
    }
    return results;
  }

  @Override
  public void clear() {
    partialResults.clear();
  }
}
