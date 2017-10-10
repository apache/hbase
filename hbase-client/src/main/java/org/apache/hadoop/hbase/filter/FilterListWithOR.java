/**
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

package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * FilterListWithOR represents an ordered list of filters which will be evaluated with an OR
 * operator.
 */
@InterfaceAudience.Private
public class FilterListWithOR extends FilterListBase {

  /**
   * Save previous return code and previous cell for every filter in filter list. For MUST_PASS_ONE,
   * we use the previous return code to decide whether we should pass current cell encountered to
   * the filter. For MUST_PASS_ALL, the two list are meaningless.
   */
  private List<ReturnCode> prevFilterRCList = null;
  private List<Cell> prevCellList = null;

  public FilterListWithOR(List<Filter> filters) {
    super(filters);
    prevFilterRCList = new ArrayList<>(Collections.nCopies(filters.size(), null));
    prevCellList = new ArrayList<>(Collections.nCopies(filters.size(), null));
  }

  @Override
  public void addFilterLists(List<Filter> filters) {
    if (checkAndGetReversed(filters, isReversed()) != isReversed()) {
      throw new IllegalArgumentException("Filters in the list must have the same reversed flag");
    }
    this.filters.addAll(filters);
    this.prevFilterRCList.addAll(Collections.nCopies(filters.size(), null));
    this.prevCellList.addAll(Collections.nCopies(filters.size(), null));
  }

  @Override
  protected String formatLogFilters(List<Filter> logFilters) {
    return String.format("FilterList OR (%d/%d): %s", logFilters.size(), this.size(),
      logFilters.toString());
  }

  /**
   * For MUST_PASS_ONE, we cannot make sure that when filter-A in filter list return NEXT_COL then
   * the next cell passing to filterList will be the first cell in next column, because if filter-B
   * in filter list return SKIP, then the filter list will return SKIP. In this case, we should pass
   * the cell following the previous cell, and it's possible that the next cell has the same column
   * as the previous cell even if filter-A has NEXT_COL returned for the previous cell. So we should
   * save the previous cell and the return code list when checking previous cell for every filter in
   * filter list, and verify if currentCell fit the previous return code, if fit then pass the
   * currentCell to the corresponding filter. (HBASE-17678)
   */
  private boolean shouldPassCurrentCellToFilter(Cell prevCell, Cell currentCell, int filterIdx)
      throws IOException {
    ReturnCode prevCode = this.prevFilterRCList.get(filterIdx);
    if (prevCell == null || prevCode == null) {
      return true;
    }
    switch (prevCode) {
    case INCLUDE:
    case SKIP:
      return true;
    case SEEK_NEXT_USING_HINT:
      Cell nextHintCell = getNextCellHint(prevCell);
      return nextHintCell == null || this.compareCell(currentCell, nextHintCell) >= 0;
    case NEXT_COL:
    case INCLUDE_AND_NEXT_COL:
      return !CellUtil.matchingRowColumn(prevCell, currentCell);
    case NEXT_ROW:
    case INCLUDE_AND_SEEK_NEXT_ROW:
      return !CellUtil.matchingRows(prevCell, currentCell);
    default:
      throw new IllegalStateException("Received code is not valid.");
    }
  }

  /**
   * FilterList with MUST_PASS_ONE choose the minimal forward step among sub-filter in filter list.
   * Let's call it: The Minimal Step Rule. So if filter-A in filter list return INCLUDE and filter-B
   * in filter list return INCLUDE_AND_NEXT_COL, then the filter list should return INCLUDE. For
   * SEEK_NEXT_USING_HINT, it's more special, because we do not know how far it will forward, so we
   * use SKIP by default.<br/>
   * <br/>
   * The jump step will be:
   *
   * <pre>
   * INCLUDE &lt; SKIP &lt; INCLUDE_AND_NEXT_COL &lt; NEXT_COL &lt; INCLUDE_AND_SEEK_NEXT_ROW &lt; NEXT_ROW &lt; SEEK_NEXT_USING_HINT
   * </pre>
   *
   * Here, we have the following map to describe The Minimal Step Rule. if current return code (for
   * previous sub-filters in filter list) is <strong>ReturnCode</strong>, and current filter returns
   * <strong>localRC</strong>, then we should return map[ReturnCode][localRC] for the merged result,
   * according to The Minimal Step Rule.<br/>
   *
   * <pre>
   * LocalCode\ReturnCode       INCLUDE INCLUDE_AND_NEXT_COL     INCLUDE_AND_SEEK_NEXT_ROW  SKIP      NEXT_COL              NEXT_ROW                  SEEK_NEXT_USING_HINT
   * INCLUDE                    INCLUDE INCLUDE                  INCLUDE                    INCLUDE   INCLUDE               INCLUDE                   INCLUDE
   * INCLUDE_AND_NEXT_COL       INCLUDE INCLUDE_AND_NEXT_COL     INCLUDE_AND_NEXT_COL       INCLUDE   INCLUDE_AND_NEXT_COL  INCLUDE_AND_NEXT_COL      INCLUDE
   * INCLUDE_AND_SEEK_NEXT_ROW  INCLUDE INCLUDE_AND_NEXT_COL     INCLUDE_AND_SEEK_NEXT_ROW  INCLUDE   INCLUDE_AND_NEXT_COL  INCLUDE_AND_SEEK_NEXT_ROW INCLUDE
   * SKIP                       INCLUDE INCLUDE                  INCLUDE                    SKIP      SKIP                  SKIP                      SKIP
   * NEXT_COL                   INCLUDE INCLUDE_AND_NEXT_COL     INCLUDE_AND_NEXT_COL       SKIP      NEXT_COL              NEXT_COL                  SKIP
   * NEXT_ROW                   INCLUDE INCLUDE_AND_NEXT_COL     INCLUDE_AND_SEEK_NEXT_ROW  SKIP      NEXT_COL              NEXT_ROW                  SKIP
   * SEEK_NEXT_USING_HINT       INCLUDE INCLUDE                  INCLUDE                    SKIP      SKIP                  SKIP                      SEEK_NEXT_USING_HINT
   * </pre>
   *
   * @param rc Return code which is calculated by previous sub-filter(s) in filter list.
   * @param localRC Return code of the current sub-filter in filter list.
   * @return Return code which is merged by the return code of previous sub-filter(s) and the return
   *         code of current sub-filter.
   */
  private ReturnCode mergeReturnCode(ReturnCode rc, ReturnCode localRC) {
    if (rc == null) return localRC;
    switch (localRC) {
    case INCLUDE:
      return ReturnCode.INCLUDE;
    case INCLUDE_AND_NEXT_COL:
      if (isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.SKIP,
        ReturnCode.SEEK_NEXT_USING_HINT)) {
        return ReturnCode.INCLUDE;
      }
      if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_NEXT_COL, ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW,
        ReturnCode.NEXT_COL, ReturnCode.NEXT_ROW)) {
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
      break;
    case INCLUDE_AND_SEEK_NEXT_ROW:
      if (isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.SKIP,
        ReturnCode.SEEK_NEXT_USING_HINT)) {
        return ReturnCode.INCLUDE;
      }
      if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_NEXT_COL, ReturnCode.NEXT_COL)) {
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
      if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW, ReturnCode.NEXT_ROW)) {
        return ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW;
      }
      break;
    case SKIP:
      if (isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL,
        ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
        return ReturnCode.INCLUDE;
      }
      if (isInReturnCodes(rc, ReturnCode.SKIP, ReturnCode.NEXT_COL, ReturnCode.NEXT_ROW,
        ReturnCode.SEEK_NEXT_USING_HINT)) {
        return ReturnCode.SKIP;
      }
      break;
    case NEXT_COL:
      if (isInReturnCodes(rc, ReturnCode.INCLUDE)) {
        return ReturnCode.INCLUDE;
      }
      if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_NEXT_COL,
        ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
      if (isInReturnCodes(rc, ReturnCode.SKIP, ReturnCode.SEEK_NEXT_USING_HINT)) {
        return ReturnCode.SKIP;
      }
      break;
    case NEXT_ROW:
      if (isInReturnCodes(rc, ReturnCode.INCLUDE)) {
        return ReturnCode.INCLUDE;
      }
      if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_NEXT_COL)) {
        return ReturnCode.INCLUDE_AND_NEXT_COL;
      }
      if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
        return ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW;
      }
      if (isInReturnCodes(rc, ReturnCode.SKIP, ReturnCode.SEEK_NEXT_USING_HINT)) {
        return ReturnCode.SKIP;
      }
      if (isInReturnCodes(rc, ReturnCode.NEXT_COL)) {
        return ReturnCode.NEXT_COL;
      }
      if (isInReturnCodes(rc, ReturnCode.NEXT_ROW)) {
        return ReturnCode.NEXT_ROW;
      }
      break;
    case SEEK_NEXT_USING_HINT:
      if (isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL,
        ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
        return ReturnCode.INCLUDE;
      }
      if (isInReturnCodes(rc, ReturnCode.SKIP, ReturnCode.NEXT_COL, ReturnCode.NEXT_ROW)) {
        return ReturnCode.SKIP;
      }
      if (isInReturnCodes(rc, ReturnCode.SEEK_NEXT_USING_HINT)) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }
      break;
    }
    throw new IllegalStateException(
        "Received code is not valid. rc: " + rc + ", localRC: " + localRC);
  }

  private void updatePrevFilterRCList(int index, ReturnCode currentRC) {
    prevFilterRCList.set(index, currentRC);
  }

  private void updatePrevCellList(int index, Cell currentCell, ReturnCode currentRC) {
    if (currentCell == null || currentRC == ReturnCode.INCLUDE || currentRC == ReturnCode.SKIP) {
      // If previous return code is INCLUDE or SKIP, we should always pass the next cell to the
      // corresponding sub-filter(need not test shouldPassCurrentCellToFilter() method), So we
      // need not save current cell to prevCellList for saving heap memory.
      prevCellList.set(index, null);
    } else {
      prevCellList.set(index, KeyValueUtil.toNewKeyCell(currentCell));
    }
  }

  private ReturnCode filterKeyValueWithMustPassOne(Cell c) throws IOException {
    ReturnCode rc = null;
    boolean everyFilterReturnHint = true;
    Cell transformed = c;
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);

      Cell prevCell = this.prevCellList.get(i);
      if (filter.filterAllRemaining() || !shouldPassCurrentCellToFilter(prevCell, c, i)) {
        everyFilterReturnHint = false;
        continue;
      }

      ReturnCode localRC = filter.filterKeyValue(c);

      // Update previous return code and previous cell for filter[i].
      updatePrevFilterRCList(i, localRC);
      updatePrevCellList(i, c, localRC);

      if (localRC != ReturnCode.SEEK_NEXT_USING_HINT) {
        everyFilterReturnHint = false;
      }

      rc = mergeReturnCode(rc, localRC);

      // For INCLUDE* case, we need to update the transformed cell.
      if (isInReturnCodes(localRC, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL,
        ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
        transformed = filter.transformCell(transformed);
      }
    }

    this.transformedCell = transformed;
    if (everyFilterReturnHint) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    } else if (rc == null) {
      // Each sub-filter in filter list got true for filterAllRemaining().
      return ReturnCode.SKIP;
    } else {
      return rc;
    }
  }

  @Override
  public ReturnCode filterKeyValue(Cell c) throws IOException {
    if (isEmpty()) {
      return ReturnCode.INCLUDE;
    }
    this.referenceCell = c;
    return filterKeyValueWithMustPassOne(c);
  }

  @Override
  public void reset() throws IOException {
    for (int i = 0, n = filters.size(); i < n; i++) {
      filters.get(i).reset();
      prevFilterRCList.set(i, null);
      prevCellList.set(i, null);
    }
  }

  @Override
  public boolean filterRowKey(byte[] rowKey, int offset, int length) throws IOException {
    if (isEmpty()) {
      return super.filterRowKey(rowKey, offset, length);
    }
    boolean retVal = true;
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (!filter.filterAllRemaining() && !filter.filterRowKey(rowKey, offset, length)) {
        retVal = false;
      }
    }
    return retVal;
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) throws IOException {
    if (isEmpty()) {
      return super.filterRowKey(firstRowCell);
    }
    boolean retVal = true;
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (!filter.filterAllRemaining() && !filter.filterRowKey(firstRowCell)) {
        retVal = false;
      }
    }
    return retVal;
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    if (isEmpty()) {
      return super.filterAllRemaining();
    }
    for (int i = 0, n = filters.size(); i < n; i++) {
      if (!filters.get(i).filterAllRemaining()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean filterRow() throws IOException {
    if (isEmpty()) {
      return super.filterRow();
    }
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (!filter.filterRow()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Cell getNextCellHint(Cell currentCell) throws IOException {
    if (isEmpty()) {
      return super.getNextCellHint(currentCell);
    }
    Cell minKeyHint = null;
    // If any condition can pass, we need to keep the min hint
    for (int i = 0, n = filters.size(); i < n; i++) {
      if (filters.get(i).filterAllRemaining()) {
        continue;
      }
      Cell curKeyHint = filters.get(i).getNextCellHint(currentCell);
      if (curKeyHint == null) {
        // If we ever don't have a hint and this is must-pass-one, then no hint
        return null;
      }
      // If this is the first hint we find, set it
      if (minKeyHint == null) {
        minKeyHint = curKeyHint;
        continue;
      }
      if (this.compareCell(minKeyHint, curKeyHint) > 0) {
        minKeyHint = curKeyHint;
      }
    }
    return minKeyHint;
  }
}
