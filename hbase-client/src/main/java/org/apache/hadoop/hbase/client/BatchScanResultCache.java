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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A scan result cache for batched scan, i.e,
 * {@code scan.getBatch() > 0 && !scan.getAllowPartialResults()}.
 * <p>
 * If user setBatch(5) and rpc returns 3+5+5+5+3 cells, we should return 5+5+5+5+1 to user. setBatch
 * doesn't mean setAllowPartialResult(true).
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class BatchScanResultCache implements ScanResultCache {

  private final int batch;

  // used to filter out the cells that already returned to user as we always start from the
  // beginning of a row when retry.
  private Cell lastCell;

  private boolean lastResultPartial;

  private final Deque<Result> partialResults = new ArrayDeque<>();

  private int numCellsOfPartialResults;

  private int numberOfCompleteRows;

  public BatchScanResultCache(int batch) {
    this.batch = batch;
  }

  private void recordLastResult(Result result) {
    lastCell = result.rawCells()[result.rawCells().length - 1];
    lastResultPartial = result.mayHaveMoreCellsInRow();
  }

  private Result createCompletedResult() throws IOException {
    numberOfCompleteRows++;
    Result result = Result.createCompleteResult(partialResults);
    partialResults.clear();
    numCellsOfPartialResults = 0;
    return result;
  }

  // Add new result to the partial list and return a batched Result if caching size exceed batching
  // limit. As the RS will also respect the scan.getBatch, we can make sure that we will get only
  // one Result back at most(or null, which means we do not have enough cells).
  private Result regroupResults(Result result) {
    partialResults.addLast(result);
    numCellsOfPartialResults += result.size();
    if (numCellsOfPartialResults < batch) {
      return null;
    }
    Cell[] cells = new Cell[batch];
    int cellCount = 0;
    boolean stale = false;
    for (;;) {
      Result r = partialResults.pollFirst();
      stale = stale || r.isStale();
      int newCellCount = cellCount + r.size();
      if (newCellCount > batch) {
        // We have more cells than expected, so split the current result
        int len = batch - cellCount;
        System.arraycopy(r.rawCells(), 0, cells, cellCount, len);
        Cell[] remainingCells = new Cell[r.size() - len];
        System.arraycopy(r.rawCells(), len, remainingCells, 0, r.size() - len);
        partialResults.addFirst(
          Result.create(remainingCells, r.getExists(), r.isStale(), r.mayHaveMoreCellsInRow()));
        break;
      }
      System.arraycopy(r.rawCells(), 0, cells, cellCount, r.size());
      if (newCellCount == batch) {
        break;
      }
      cellCount = newCellCount;
    }
    numCellsOfPartialResults -= batch;
    return Result.create(cells, null, stale,
      result.mayHaveMoreCellsInRow() || !partialResults.isEmpty());
  }

  @Override
  public Result[] addAndGet(Result[] results, boolean isHeartbeatMessage) throws IOException {
    if (results.length == 0) {
      if (!isHeartbeatMessage) {
        if (!partialResults.isEmpty()) {
          return new Result[] { createCompletedResult() };
        }
        if (lastResultPartial) {
          // An empty non heartbeat result indicate that there must be a row change. So if the
          // lastResultPartial is true then we need to increase numberOfCompleteRows.
          numberOfCompleteRows++;
        }
      }
      return EMPTY_RESULT_ARRAY;
    }
    List<Result> regroupedResults = new ArrayList<>();
    for (Result result : results) {
      result = filterCells(result, lastCell);
      if (result == null) {
        continue;
      }
      if (!partialResults.isEmpty()) {
        if (!Bytes.equals(partialResults.peek().getRow(), result.getRow())) {
          // there is a row change
          regroupedResults.add(createCompletedResult());
        }
      } else if (lastResultPartial && !CellUtil.matchingRows(lastCell, result.getRow())) {
        // As for batched scan we may return partial results to user if we reach the batch limit, so
        // here we need to use lastCell to determine if there is row change and increase
        // numberOfCompleteRows.
        numberOfCompleteRows++;
      }
      // check if we have a row change
      if (!partialResults.isEmpty() &&
          !Bytes.equals(partialResults.peek().getRow(), result.getRow())) {
        regroupedResults.add(createCompletedResult());
      }
      Result regroupedResult = regroupResults(result);
      if (regroupedResult != null) {
        if (!regroupedResult.mayHaveMoreCellsInRow()) {
          numberOfCompleteRows++;
        }
        regroupedResults.add(regroupedResult);
        // only update last cell when we actually return it to user.
        recordLastResult(regroupedResult);
      }
      if (!result.mayHaveMoreCellsInRow() && !partialResults.isEmpty()) {
        // We are done for this row
        regroupedResults.add(createCompletedResult());
      }
    }
    return regroupedResults.toArray(new Result[0]);
  }

  @Override
  public void clear() {
    partialResults.clear();
    numCellsOfPartialResults = 0;
  }

  @Override
  public int numberOfCompleteRows() {
    return numberOfCompleteRows;
  }
}
