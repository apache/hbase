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
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
    subFiltersIncludedCell = new ArrayList<>(Collections.nCopies(filters.size(), false));
  }

  @Override
  public void addFilterLists(List<Filter> filters) {
    if (checkAndGetReversed(filters, isReversed()) != isReversed()) {
      throw new IllegalArgumentException("Filters in the list must have the same reversed flag");
    }
    this.filters.addAll(filters);
    this.subFiltersIncludedCell.addAll(Collections.nCopies(filters.size(), false));
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
   * currentCell to the corresponding filter. (HBASE-17678) <br>
   * Note that: In StoreScanner level, NEXT_ROW will skip to the next row in current family, and in
   * RegionScanner level, NEXT_ROW will skip to the next row in current family and switch to the
   * next family for RegionScanner, INCLUDE_AND_NEXT_ROW is the same. so we should pass current cell
   * to the filter, if row mismatch or row match but column family mismatch. (HBASE-18368)
   * @see org.apache.hadoop.hbase.filter.Filter.ReturnCode
   * @param subFilter which sub-filter to calculate the return code by using previous cell and
   *          previous return code.
   * @param prevCell the previous cell passed to given sub-filter.
   * @param currentCell the current cell which will pass to given sub-filter.
   * @param prevCode the previous return code for given sub-filter.
   * @return return code calculated by using previous cell and previous return code. null means can
   *         not decide which return code should return, so we will pass the currentCell to
   *         subFilter for getting currentCell's return code, and it won't impact the sub-filter's
   *         internal states.
   */
  private ReturnCode calculateReturnCodeByPrevCellAndRC(Filter subFilter, Cell currentCell,
      Cell prevCell, ReturnCode prevCode) throws IOException {
    if (prevCell == null || prevCode == null) {
      return null;
    }
    switch (prevCode) {
    case INCLUDE:
    case SKIP:
        return null;
    case SEEK_NEXT_USING_HINT:
        Cell nextHintCell = subFilter.getNextCellHint(prevCell);
        return nextHintCell != null && compareCell(currentCell, nextHintCell) < 0
          ? ReturnCode.SEEK_NEXT_USING_HINT : null;
    case NEXT_COL:
    case INCLUDE_AND_NEXT_COL:
        // Once row changed, reset() will clear prevCells, so we need not to compare their rows
        // because rows are the same here.
        return CellUtil.matchingColumn(prevCell, currentCell) ? ReturnCode.NEXT_COL : null;
    case NEXT_ROW:
    case INCLUDE_AND_SEEK_NEXT_ROW:
        // As described above, rows are definitely the same, so we only compare the family.
        return CellUtil.matchingFamily(prevCell, currentCell) ? ReturnCode.NEXT_ROW : null;
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
      if (isInReturnCodes(rc, ReturnCode.NEXT_COL, ReturnCode.NEXT_ROW)) {
        return ReturnCode.NEXT_COL;
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
      // corresponding sub-filter(need not test calculateReturnCodeByPrevCellAndRC() method), So we
      // need not save current cell to prevCellList for saving heap memory.
      prevCellList.set(index, null);
    } else {
      prevCellList.set(index, currentCell);
    }
  }

  @Override
  public ReturnCode filterCell(Cell c) throws IOException {
    if (isEmpty()) {
      return ReturnCode.INCLUDE;
    }
    ReturnCode rc = null;
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      subFiltersIncludedCell.set(i, false);

      Cell prevCell = this.prevCellList.get(i);
      ReturnCode prevCode = this.prevFilterRCList.get(i);
      if (filter.filterAllRemaining()) {
        continue;
      }
      ReturnCode localRC = calculateReturnCodeByPrevCellAndRC(filter, c, prevCell, prevCode);
      if (localRC == null) {
        // Can not get return code based on previous cell and previous return code. In other words,
        // we should pass the current cell to this sub-filter to get the return code, and it won't
        // impact the sub-filter's internal state.
        localRC = filter.filterCell(c);
      }

      // Update previous return code and previous cell for filter[i].
      updatePrevFilterRCList(i, localRC);
      updatePrevCellList(i, c, localRC);

      rc = mergeReturnCode(rc, localRC);

      // For INCLUDE* case, we need to update the transformed cell.
      if (isInReturnCodes(localRC, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL,
        ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
        subFiltersIncludedCell.set(i, true);
      }
    }
    // Each sub-filter in filter list got true for filterAllRemaining(), if rc is null, so we should
    // return SKIP.
    return rc == null ? ReturnCode.SKIP : rc;
  }

  @Override
  public void reset() throws IOException {
    for (int i = 0, n = filters.size(); i < n; i++) {
      filters.get(i).reset();
      subFiltersIncludedCell.set(i, false);
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
        // Can't just return false here, because there are some filters (such as PrefixFilter) which
        // will catch the row changed event by filterRowKey(). If we return early here, those
        // filters will have no chance to update their row state.
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


  @Override
  public boolean equals(Object obj) {
    if (obj == null || (!(obj instanceof FilterListWithOR))) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    FilterListWithOR f = (FilterListWithOR) obj;
    return this.filters.equals(f.getFilters()) &&
      this.prevFilterRCList.equals(f.prevFilterRCList) &&
      this.prevCellList.equals(f.prevCellList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.prevFilterRCList, this.prevCellList, this.filters);
  }
}
