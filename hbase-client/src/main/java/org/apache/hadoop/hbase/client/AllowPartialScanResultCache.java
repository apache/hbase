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
import java.util.Arrays;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A ScanResultCache that may return partial result.
 * <p>
 * As we can only scan from the starting of a row when error, so here we also implement the logic
 * that skips the cells that have already been returned.
 */
@InterfaceAudience.Private
class AllowPartialScanResultCache implements ScanResultCache {

  // used to filter out the cells that already returned to user as we always start from the
  // beginning of a row when retry.
  private Cell lastCell;

  private Result filterCells(Result result) {
    if (lastCell == null) {
      return result;
    }

    // not the same row
    if (!CellUtil.matchingRow(lastCell, result.getRow(), 0, result.getRow().length)) {
      return result;
    }
    Cell[] rawCells = result.rawCells();
    int index = Arrays.binarySearch(rawCells, lastCell, CellComparator::compareWithoutRow);
    if (index < 0) {
      index = -index - 1;
    } else {
      index++;
    }
    if (index == 0) {
      return result;
    }
    if (index == rawCells.length) {
      return null;
    }
    return Result.create(Arrays.copyOfRange(rawCells, index, rawCells.length), null,
      result.isStale(), true);
  }

  private void updateLastCell(Result result) {
    lastCell = result.isPartial() ? result.rawCells()[result.rawCells().length - 1] : null;
  }

  @Override
  public Result[] addAndGet(Result[] results, boolean isHeartbeatMessage) throws IOException {
    if (results.length == 0) {
      return EMPTY_RESULT_ARRAY;
    }
    Result first = filterCells(results[0]);
    if (results.length == 1) {
      if (first == null) {
        // do not update last cell if we filter out all cells
        return EMPTY_RESULT_ARRAY;
      }
      updateLastCell(results[0]);
      results[0] = first;
      return results;
    }
    updateLastCell(results[results.length - 1]);
    if (first == null) {
      return Arrays.copyOfRange(results, 1, results.length);
    }
    results[0] = first;
    return results;
  }

  @Override
  public void clear() {
    // we do not cache anything
  }
}
