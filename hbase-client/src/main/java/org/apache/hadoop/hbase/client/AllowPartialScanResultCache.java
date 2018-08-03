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

import static org.apache.hadoop.hbase.client.ConnectionUtils.filterCells;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A ScanResultCache that may return partial result.
 * <p>
 * As we can only scan from the starting of a row when error, so here we also implement the logic
 * that skips the cells that have already been returned.
 */
@InterfaceAudience.Private
class AllowPartialScanResultCache extends ScanResultCache {

  // used to filter out the cells that already returned to user as we always start from the
  // beginning of a row when retry.
  private Cell lastCell;

  private boolean lastResultPartial;

  public AllowPartialScanResultCache(List<Result> cache) {
    super(cache);
  }

  private void recordLastResult(Result result) {
    lastCell = result.rawCells()[result.rawCells().length - 1];
    lastResultPartial = result.mayHaveMoreCellsInRow();
  }

  @Override
  public void loadResultsToCache(Result[] results, boolean isHeartbeatMessage) throws IOException {
    if (results.length == 0) {
      if (!isHeartbeatMessage && lastResultPartial) {
        // An empty non heartbeat result indicate that there must be a row change. So if the
        // lastResultPartial is true then we need to increase numberOfCompleteRows.
        numberOfCompleteRows++;
      }
      return;
    }
    int i;
    for (i = 0; i < results.length; i++) {
      Result r = filterCells(results[i], lastCell);
      if (r != null) {
        results[i] = r;
        break;
      }
    }
    if (i == results.length) {
      return;
    }
    if (lastResultPartial && !CellUtil.matchingRow(lastCell, results[0].getRow())) {
      // there is a row change, so increase numberOfCompleteRows
      numberOfCompleteRows++;
    }
    recordLastResult(results[results.length - 1]);
    addResultArrayToCache(results, i, results.length);
  }

  @Override
  protected void checkUpdateNumberOfCompleteRowsAndCache(Result rs) {
    if (!rs.mayHaveMoreCellsInRow()) {
      numberOfCompleteRows++;
    }
    addResultToCache(rs);
  }

  @Override
  public void clear() {
    // we do not cache anything
    super.clear();
  }
}
