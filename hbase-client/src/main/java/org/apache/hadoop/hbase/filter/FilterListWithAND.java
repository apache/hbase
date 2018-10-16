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
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * FilterListWithAND represents an ordered list of filters which will be evaluated with an AND
 * operator.
 */
@InterfaceAudience.Private
public class FilterListWithAND extends FilterListBase {

  private List<Filter> seekHintFilters = new ArrayList<>();

  public FilterListWithAND(List<Filter> filters) {
    super(filters);
    // For FilterList with AND, when call FL's transformCell(), we should transform cell for all
    // sub-filters (because all sub-filters return INCLUDE*). So here, fill this array with true. we
    // keep this in FilterListWithAND for abstracting the transformCell() in FilterListBase.
    subFiltersIncludedCell = new ArrayList<>(Collections.nCopies(filters.size(), true));
  }

  @Override
  public void addFilterLists(List<Filter> filters) {
    if (checkAndGetReversed(filters, isReversed()) != isReversed()) {
      throw new IllegalArgumentException("Filters in the list must have the same reversed flag");
    }
    this.filters.addAll(filters);
    this.subFiltersIncludedCell.addAll(Collections.nCopies(filters.size(), true));
  }

  @Override
  protected String formatLogFilters(List<Filter> logFilters) {
    return String.format("FilterList AND (%d/%d): %s", logFilters.size(), this.size(),
      logFilters.toString());
  }

  /**
   * FilterList with MUST_PASS_ALL choose the maximal forward step among sub-filters in filter list.
   * Let's call it: The Maximal Step Rule. So if filter-A in filter list return INCLUDE and filter-B
   * in filter list return INCLUDE_AND_NEXT_COL, then the filter list should return
   * INCLUDE_AND_NEXT_COL. For SEEK_NEXT_USING_HINT, it's more special, and in method
   * filterCellWithMustPassAll(), if any sub-filter return SEEK_NEXT_USING_HINT, then our filter
   * list will return SEEK_NEXT_USING_HINT. so we don't care about the SEEK_NEXT_USING_HINT here.
   * <br/>
   * <br/>
   * The jump step will be:
   *
   * <pre>
   * INCLUDE &lt; SKIP &lt; INCLUDE_AND_NEXT_COL &lt; NEXT_COL &lt; INCLUDE_AND_SEEK_NEXT_ROW &lt; NEXT_ROW &lt; SEEK_NEXT_USING_HINT
   * </pre>
   *
   * Here, we have the following map to describe The Maximal Step Rule. if current return code (for
   * previous sub-filters in filter list) is <strong>ReturnCode</strong>, and current filter returns
   * <strong>localRC</strong>, then we should return map[ReturnCode][localRC] for the merged result,
   * according to The Maximal Step Rule. <br/>
   *
   * <pre>
   * LocalCode\ReturnCode       INCLUDE                    INCLUDE_AND_NEXT_COL      INCLUDE_AND_SEEK_NEXT_ROW  SKIP                  NEXT_COL              NEXT_ROW              SEEK_NEXT_USING_HINT
   * INCLUDE                    INCLUDE                    INCLUDE_AND_NEXT_COL      INCLUDE_AND_SEEK_NEXT_ROW  SKIP                  NEXT_COL              NEXT_ROW              SEEK_NEXT_USING_HINT
   * INCLUDE_AND_NEXT_COL       INCLUDE_AND_NEXT_COL       INCLUDE_AND_NEXT_COL      INCLUDE_AND_SEEK_NEXT_ROW  NEXT_COL              NEXT_COL              NEXT_ROW              SEEK_NEXT_USING_HINT
   * INCLUDE_AND_SEEK_NEXT_ROW  INCLUDE_AND_SEEK_NEXT_ROW  INCLUDE_AND_SEEK_NEXT_ROW INCLUDE_AND_SEEK_NEXT_ROW  NEXT_ROW              NEXT_ROW              NEXT_ROW              SEEK_NEXT_USING_HINT
   * SKIP                       SKIP                       NEXT_COL                  NEXT_ROW                   SKIP                  NEXT_COL              NEXT_ROW              SEEK_NEXT_USING_HINT
   * NEXT_COL                   NEXT_COL                   NEXT_COL                  NEXT_ROW                   NEXT_COL              NEXT_COL              NEXT_ROW              SEEK_NEXT_USING_HINT
   * NEXT_ROW                   NEXT_ROW                   NEXT_ROW                  NEXT_ROW                   NEXT_ROW              NEXT_ROW              NEXT_ROW              SEEK_NEXT_USING_HINT
   * SEEK_NEXT_USING_HINT       SEEK_NEXT_USING_HINT       SEEK_NEXT_USING_HINT      SEEK_NEXT_USING_HINT       SEEK_NEXT_USING_HINT  SEEK_NEXT_USING_HINT  SEEK_NEXT_USING_HINT  SEEK_NEXT_USING_HINT
   * </pre>
   *
   * @param rc Return code which is calculated by previous sub-filter(s) in filter list.
   * @param localRC Return code of the current sub-filter in filter list.
   * @return Return code which is merged by the return code of previous sub-filter(s) and the return
   *         code of current sub-filter.
   */
  private ReturnCode mergeReturnCode(ReturnCode rc, ReturnCode localRC) {
    if (rc == ReturnCode.SEEK_NEXT_USING_HINT) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    switch (localRC) {
      case SEEK_NEXT_USING_HINT:
        return ReturnCode.SEEK_NEXT_USING_HINT;
      case INCLUDE:
        return rc;
      case INCLUDE_AND_NEXT_COL:
        if (isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL)) {
          return ReturnCode.INCLUDE_AND_NEXT_COL;
        }
        if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
          return ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW;
        }
        if (isInReturnCodes(rc, ReturnCode.SKIP, ReturnCode.NEXT_COL)) {
          return ReturnCode.NEXT_COL;
        }
        if (isInReturnCodes(rc, ReturnCode.NEXT_ROW)) {
          return ReturnCode.NEXT_ROW;
        }
        break;
      case INCLUDE_AND_SEEK_NEXT_ROW:
        if (isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL,
          ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
          return ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW;
        }
        if (isInReturnCodes(rc, ReturnCode.SKIP, ReturnCode.NEXT_COL, ReturnCode.NEXT_ROW)) {
          return ReturnCode.NEXT_ROW;
        }
        break;
      case SKIP:
        if (isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.SKIP)) {
          return ReturnCode.SKIP;
        }
        if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_NEXT_COL, ReturnCode.NEXT_COL)) {
          return ReturnCode.NEXT_COL;
        }
        if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW, ReturnCode.NEXT_ROW)) {
          return ReturnCode.NEXT_ROW;
        }
        break;
      case NEXT_COL:
        if (isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL, ReturnCode.SKIP,
          ReturnCode.NEXT_COL)) {
          return ReturnCode.NEXT_COL;
        }
        if (isInReturnCodes(rc, ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW, ReturnCode.NEXT_ROW)) {
          return ReturnCode.NEXT_ROW;
        }
        break;
      case NEXT_ROW:
        return ReturnCode.NEXT_ROW;
    }
    throw new IllegalStateException(
        "Received code is not valid. rc: " + rc + ", localRC: " + localRC);
  }

  private boolean isIncludeRelatedReturnCode(ReturnCode rc) {
    return isInReturnCodes(rc, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL,
      ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW);
  }

  @Override
  public ReturnCode filterCell(Cell c) throws IOException {
    if (isEmpty()) {
      return ReturnCode.INCLUDE;
    }
    ReturnCode rc = ReturnCode.INCLUDE;
    this.seekHintFilters.clear();
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (filter.filterAllRemaining()) {
        return ReturnCode.NEXT_ROW;
      }
      ReturnCode localRC;
      localRC = filter.filterCell(c);
      if (localRC == ReturnCode.SEEK_NEXT_USING_HINT) {
        seekHintFilters.add(filter);
      }
      rc = mergeReturnCode(rc, localRC);
      // Only when rc is INCLUDE* case, we should pass the cell to the following sub-filters.
      // otherwise we may mess up the global state (such as offset, count..) in the following
      // sub-filters. (HBASE-20565)
      if (!isIncludeRelatedReturnCode(rc)) {
        return rc;
      }
    }
    if (!seekHintFilters.isEmpty()) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    return rc;
  }

  @Override
  public void reset() throws IOException {
    for (int i = 0, n = filters.size(); i < n; i++) {
      filters.get(i).reset();
    }
    seekHintFilters.clear();
  }

  @Override
  public boolean filterRowKey(byte[] rowKey, int offset, int length) throws IOException {
    if (isEmpty()) {
      return super.filterRowKey(rowKey, offset, length);
    }
    boolean retVal = false;
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (filter.filterAllRemaining() || filter.filterRowKey(rowKey, offset, length)) {
        retVal = true;
      }
    }
    return retVal;
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) throws IOException {
    if (isEmpty()) {
      return super.filterRowKey(firstRowCell);
    }
    boolean retVal = false;
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (filter.filterAllRemaining() || filter.filterRowKey(firstRowCell)) {
        // Can't just return true here, because there are some filters (such as PrefixFilter) which
        // will catch the row changed event by filterRowKey(). If we return early here, those
        // filters will have no chance to update their row state.
        retVal = true;
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
      if (filters.get(i).filterAllRemaining()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean filterRow() throws IOException {
    if (isEmpty()) {
      return super.filterRow();
    }
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (filter.filterRow()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Cell getNextCellHint(Cell currentCell) throws IOException {
    if (isEmpty()) {
      return super.getNextCellHint(currentCell);
    }
    Cell maxHint = null;
    for (Filter filter : seekHintFilters) {
      if (filter.filterAllRemaining()) {
        continue;
      }
      Cell curKeyHint = filter.getNextCellHint(currentCell);
      if (maxHint == null) {
        maxHint = curKeyHint;
        continue;
      }
      if (this.compareCell(maxHint, curKeyHint) < 0) {
        maxHint = curKeyHint;
      }
    }
    return maxHint;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FilterListWithAND)) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    FilterListWithAND f = (FilterListWithAND) obj;
    return this.filters.equals(f.getFilters()) && this.seekHintFilters.equals(f.seekHintFilters);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.seekHintFilters, this.filters);
  }
}
