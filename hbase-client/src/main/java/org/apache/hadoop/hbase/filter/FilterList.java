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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException;

/**
 * Implementation of {@link Filter} that represents an ordered List of Filters
 * which will be evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL}
 * (<code>AND</code>) or {@link Operator#MUST_PASS_ONE} (<code>OR</code>).
 * Since you can use Filter Lists as children of Filter Lists, you can create a
 * hierarchy of filters to be evaluated.
 *
 * <br>
 * {@link Operator#MUST_PASS_ALL} evaluates lazily: evaluation stops as soon as one filter does
 * not include the KeyValue.
 *
 * <br>
 * {@link Operator#MUST_PASS_ONE} evaluates non-lazily: all filters are always evaluated.
 *
 * <br>
 * Defaults to {@link Operator#MUST_PASS_ALL}.
 */
@InterfaceAudience.Public
final public class FilterList extends FilterBase {
  /** set operator */
  @InterfaceAudience.Public
  public static enum Operator {
    /** !AND */
    MUST_PASS_ALL,
    /** !OR */
    MUST_PASS_ONE
  }

  private static final int MAX_LOG_FILTERS = 5;
  private Operator operator = Operator.MUST_PASS_ALL;
  private final List<Filter> filters;
  private Set<Filter> seekHintFilter = new HashSet<>();

  /**
   * Save previous return code and previous cell for every filter in filter list. For MUST_PASS_ONE,
   * we use the previous return code to decide whether we should pass current cell encountered to
   * the filter. For MUST_PASS_ALL, the two list are meaningless.
   */
  private List<ReturnCode> prevFilterRCList = null;
  private List<Cell> prevCellList = null;

  /** Reference Cell used by {@link #transformCell(Cell)} for validation purpose. */
  private Cell referenceCell = null;

  /**
   * When filtering a given Cell in {@link #filterKeyValue(Cell)},
   * this stores the transformed Cell to be returned by {@link #transformCell(Cell)}.
   *
   * Individual filters transformation are applied only when the filter includes the Cell.
   * Transformations are composed in the order specified by {@link #filters}.
   */
  private Cell transformedCell = null;

  /**
   * Constructor that takes a set of {@link Filter}s and an operator.
   * @param operator Operator to process filter set with.
   * @param rowFilters Set of row filters.
   */
  public FilterList(final Operator operator, final List<Filter> rowFilters) {
    reversed = checkAndGetReversed(rowFilters, reversed);
    this.filters = new ArrayList<>(rowFilters);
    this.operator = operator;
    initPrevListForMustPassOne(rowFilters.size());
  }

  /**
   * Constructor that takes a set of {@link Filter}s. The default operator MUST_PASS_ALL is assumed.
   * All filters are cloned to internal list.
   * @param rowFilters list of filters
   */
  public FilterList(final List<Filter> rowFilters) {
    this(Operator.MUST_PASS_ALL, rowFilters);
  }

  /**
   * Constructor that takes a var arg number of {@link Filter}s. The default operator MUST_PASS_ALL
   * is assumed.
   * @param rowFilters
   */
  public FilterList(final Filter... rowFilters) {
    this(Operator.MUST_PASS_ALL, Arrays.asList(rowFilters));
  }

  /**
   * Constructor that takes an operator.
   * @param operator Operator to process filter set with.
   */
  public FilterList(final Operator operator) {
    this(operator, new ArrayList<>());
  }

  /**
   * Constructor that takes a var arg number of {@link Filter}s and an operator.
   * @param operator Operator to process filter set with.
   * @param rowFilters Filters to use
   */
  public FilterList(final Operator operator, final Filter... rowFilters) {
    this(operator, Arrays.asList(rowFilters));
  }

  private void initPrevListForMustPassOne(int size) {
    if (operator == Operator.MUST_PASS_ONE) {
      if (this.prevFilterRCList == null) {
        prevFilterRCList = new ArrayList<>(Collections.nCopies(size, null));
      }
      if (this.prevCellList == null) {
        prevCellList = new ArrayList<>(Collections.nCopies(size, null));
      }
    }
  }

  /**
   * Get the operator.
   * @return operator
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * Get the filters.
   * @return filters
   */
  public List<Filter> getFilters() {
    return filters;
  }

  public int size() {
    return filters.size();
  }

  private boolean isEmpty() {
    return filters.isEmpty();
  }

  private static boolean checkAndGetReversed(List<Filter> rowFilters, boolean defaultValue) {
    if (rowFilters.isEmpty()) {
      return defaultValue;
    }
    Boolean retValue = rowFilters.get(0).isReversed();
    boolean allEqual = rowFilters.stream().map(Filter::isReversed).allMatch(retValue::equals);
    if (!allEqual) {
      throw new IllegalArgumentException("Filters in the list must have the same reversed flag");
    }
    return retValue;
  }

  public void addFilter(List<Filter> filters) {
    if (checkAndGetReversed(filters, isReversed()) != isReversed()) {
      throw new IllegalArgumentException("Filters in the list must have the same reversed flag");
    }
    this.filters.addAll(filters);
    if (operator == Operator.MUST_PASS_ONE) {
      this.prevFilterRCList.addAll(Collections.nCopies(filters.size(), null));
      this.prevCellList.addAll(Collections.nCopies(filters.size(), null));
    }
  }

  /**
   * Add a filter.
   * @param filter another filter
   */
  public void addFilter(Filter filter) {
    addFilter(Collections.singletonList(filter));
  }

  @Override
  public void reset() throws IOException {
    for (int i = 0, n = filters.size(); i < n; i++) {
      filters.get(i).reset();
      if (operator == Operator.MUST_PASS_ONE) {
        prevFilterRCList.set(i, null);
        prevCellList.set(i, null);
      }
    }
    seekHintFilter.clear();
  }

  @Override
  public boolean filterRowKey(byte[] rowKey, int offset, int length) throws IOException {
    if (isEmpty()) {
      return super.filterRowKey(rowKey, offset, length);
    }
    boolean flag = this.operator == Operator.MUST_PASS_ONE;
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (this.operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining() || filter.filterRowKey(rowKey, offset, length)) {
          flag = true;
        }
      } else if (this.operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining() && !filter.filterRowKey(rowKey, offset, length)) {
          flag = false;
        }
      }
    }
    return flag;
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) throws IOException {
    if (isEmpty()) {
      return super.filterRowKey(firstRowCell);
    }
    boolean flag = this.operator == Operator.MUST_PASS_ONE;
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (this.operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining() || filter.filterRowKey(firstRowCell)) {
          flag = true;
        }
      } else if (this.operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining() && !filter.filterRowKey(firstRowCell)) {
          flag = false;
        }
      }
    }
    return flag;
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    if (isEmpty()) {
      return super.filterAllRemaining();
    }
    for (int i = 0, n = filters.size(); i < n; i++) {
      if (filters.get(i).filterAllRemaining()) {
        if (operator == Operator.MUST_PASS_ALL) {
          return true;
        }
      } else {
        if (operator == Operator.MUST_PASS_ONE) {
          return false;
        }
      }
    }
    return operator == Operator.MUST_PASS_ONE;
  }

  @Override
  public Cell transformCell(Cell c) throws IOException {
    if (isEmpty()) {
      return super.transformCell(c);
    }
    if (!CellUtil.equals(c, referenceCell)) {
      throw new IllegalStateException(
          "Reference Cell: " + this.referenceCell + " does not match: " + c);
    }
    // Copy transformedCell into a new cell and reset transformedCell & referenceCell to null for
    // Java GC optimization
    Cell cell = KeyValueUtil.copyToNewKeyValue(this.transformedCell);
    this.transformedCell = null;
    this.referenceCell = null;
    return cell;
  }

  /**
   * For MUST_PASS_ONE, we cannot make sure that when filter-A in filter list return NEXT_COL then
   * the next cell passing to filterList will be the first cell in next column, because if filter-B
   * in filter list return SKIP, then the filter list will return SKIP. In this case, we should pass
   * the cell following the previous cell, and it's possible that the next cell has the same column
   * as the previous cell even if filter-A has NEXT_COL returned for the previous cell. So we should
   * save the previous cell and the return code list when checking previous cell for every filter in
   * filter list, and verify if currentCell fit the previous return code, if fit then pass the currentCell
   * to the corresponding filter. (HBASE-17678)
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
      return nextHintCell == null
          || CellComparator.COMPARATOR.compare(currentCell, nextHintCell) >= 0;
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
   * FilterList with MUST_PASS_ALL choose the maximal forward step among sub-filters in filter list.
   * Let's call it: The Maximal Step Rule. So if filter-A in filter list return INCLUDE and filter-B
   * in filter list return INCLUDE_AND_NEXT_COL, then the filter list should return
   * INCLUDE_AND_NEXT_COL. For SEEK_NEXT_USING_HINT, it's more special, and in method
   * filterKeyValueWithMustPassAll(), if any sub-filter return SEEK_NEXT_USING_HINT, then our filter
   * list will return SEEK_NEXT_USING_HINT. so we don't care about the SEEK_NEXT_USING_HINT here. <br/>
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
   * @param rc Return code which is calculated by previous sub-filter(s) in filter list.
   * @param localRC Return code of the current sub-filter in filter list.
   * @return Return code which is merged by the return code of previous sub-filter(s) and the return
   *         code of current sub-filter.
   */
  private ReturnCode mergeReturnCodeForAndOperator(ReturnCode rc, ReturnCode localRC) {
    if (rc == ReturnCode.SEEK_NEXT_USING_HINT || localRC == ReturnCode.SEEK_NEXT_USING_HINT) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    switch (localRC) {
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
    throw new IllegalStateException("Received code is not valid. rc: " + rc + ", localRC: "
        + localRC);
  }

  private ReturnCode filterKeyValueWithMustPassAll(Cell c) throws IOException {
    ReturnCode rc = ReturnCode.INCLUDE;
    Cell transformed = c;
    this.seekHintFilter.clear();
    for (int i = 0, n = filters.size(); i < n; i++) {
      Filter filter = filters.get(i);
      if (filter.filterAllRemaining()) {
        return ReturnCode.NEXT_ROW;
      }
      ReturnCode localRC = filter.filterKeyValue(c);
      rc = mergeReturnCodeForAndOperator(rc, localRC);

      // For INCLUDE* case, we need to update the transformed cell.
      if (isInReturnCodes(localRC, ReturnCode.INCLUDE, ReturnCode.INCLUDE_AND_NEXT_COL,
        ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW)) {
        transformed = filter.transformCell(transformed);
      }
      if (localRC == ReturnCode.SEEK_NEXT_USING_HINT) {
        seekHintFilter.add(filter);
      }
    }
    this.transformedCell = transformed;
    if (!seekHintFilter.isEmpty()) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    return rc;
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

  private static boolean isInReturnCodes(ReturnCode testRC, ReturnCode... returnCodes) {
    return Arrays.stream(returnCodes).anyMatch(testRC::equals);
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
   * @param rc Return code which is calculated by previous sub-filter(s) in filter list.
   * @param localRC Return code of the current sub-filter in filter list.
   * @return Return code which is merged by the return code of previous sub-filter(s) and the return
   *         code of current sub-filter.
   */
  private ReturnCode mergeReturnCodeForOrOperator(ReturnCode rc, ReturnCode localRC) {
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

      rc = mergeReturnCodeForOrOperator(rc, localRC);

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

    if (operator == Operator.MUST_PASS_ALL) {
      return filterKeyValueWithMustPassAll(c);
    } else {
      return filterKeyValueWithMustPassOne(c);
    }
  }

  /**
   * Filters that never filter by modifying the returned List of Cells can
   * inherit this implementation that does nothing.
   *
   * {@inheritDoc}
   */
  @Override
  public void filterRowCells(List<Cell> cells) throws IOException {
    for (int i = 0, n = filters.size(); i < n; i++) {
      filters.get(i).filterRowCells(cells);
    }
  }

  @Override
  public boolean hasFilterRow() {
    for (int i = 0, n = filters.size(); i < n; i++) {
      if (filters.get(i).hasFilterRow()) {
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
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterRow()) {
          return true;
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterRow()) {
          return false;
        }
      }
    }
    return operator == Operator.MUST_PASS_ONE;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() throws IOException {
    FilterProtos.FilterList.Builder builder = FilterProtos.FilterList.newBuilder();
    builder.setOperator(FilterProtos.FilterList.Operator.valueOf(operator.name()));
    for (int i = 0, n = filters.size(); i < n; i++) {
      builder.addFilters(ProtobufUtil.toFilter(filters.get(i)));
    }
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link FilterList} instance
   * @return An instance of {@link FilterList} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static FilterList parseFrom(final byte [] pbBytes)
      throws DeserializationException {
    FilterProtos.FilterList proto;
    try {
      proto = FilterProtos.FilterList.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }

    List<Filter> rowFilters = new ArrayList<>(proto.getFiltersCount());
    try {
      List<FilterProtos.Filter> filtersList = proto.getFiltersList();
      for (int i = 0, n = filtersList.size(); i < n; i++) {
        rowFilters.add(ProtobufUtil.toFilter(filtersList.get(i)));
      }
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
    return new FilterList(Operator.valueOf(proto.getOperator().name()), rowFilters);
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter other) {
    if (other == this) return true;
    if (!(other instanceof FilterList)) return false;

    FilterList o = (FilterList)other;
    return this.getOperator().equals(o.getOperator()) &&
      ((this.getFilters() == o.getFilters())
      || this.getFilters().equals(o.getFilters()));
  }

  @Override
  public Cell getNextCellHint(Cell currentCell) throws IOException {
    if (isEmpty()) {
      return super.getNextCellHint(currentCell);
    }
    Cell keyHint = null;
    if (operator == Operator.MUST_PASS_ALL) {
      for (Filter filter : seekHintFilter) {
        if (filter.filterAllRemaining()) continue;
        Cell curKeyHint = filter.getNextCellHint(currentCell);
        if (keyHint == null) {
          keyHint = curKeyHint;
          continue;
        }
        if (CellComparator.COMPARATOR.compare(keyHint, curKeyHint) < 0) {
          keyHint = curKeyHint;
        }
      }
      return keyHint;
    }

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
      if (keyHint == null) {
        keyHint = curKeyHint;
        continue;
      }
      if (CellComparator.COMPARATOR.compare(keyHint, curKeyHint) > 0) {
        keyHint = curKeyHint;
      }
    }
    return keyHint;
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    if (isEmpty()) {
      return super.isFamilyEssential(name);
    }
    for (int i = 0, n = filters.size(); i < n; i++) {
      if (filters.get(i).isFamilyEssential(name)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void setReversed(boolean reversed) {
    for (int i = 0, n = filters.size(); i < n; i++) {
      filters.get(i).setReversed(reversed);
    }
    this.reversed = reversed;
  }

  @Override
  public String toString() {
    return toString(MAX_LOG_FILTERS);
  }

  protected String toString(int maxFilters) {
    int endIndex = this.filters.size() < maxFilters
        ? this.filters.size() : maxFilters;
    return String.format("%s %s (%d/%d): %s",
        this.getClass().getSimpleName(),
        this.operator == Operator.MUST_PASS_ALL ? "AND" : "OR",
        endIndex,
        this.filters.size(),
        this.filters.subList(0, endIndex).toString());
  }
}
