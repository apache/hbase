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

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import com.google.protobuf.InvalidProtocolBufferException;

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
@InterfaceStability.Stable
final public class FilterList extends Filter {
  /** set operator */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public static enum Operator {
    /** !AND */
    MUST_PASS_ALL,
    /** !OR */
    MUST_PASS_ONE
  }

  private static final int MAX_LOG_FILTERS = 5;
  private Operator operator = Operator.MUST_PASS_ALL;
  private List<Filter> filters = new ArrayList<Filter>();
  private Set<Filter> seekHintFilter = new HashSet<>();

  /**
   * Save previous return code and previous cell for every filter in filter list. For MUST_PASS_ONE,
   * we use the previous return code to decide whether we should pass current cell encountered to
   * the filter. For MUST_PASS_ALL, the two list are meaningless.
   */
  private List<ReturnCode> prevFilterRCList = null;
  private List<Cell> prevCellList = null;

  /** Reference Cell used by {@link #transformCell(Cell)} for validation purpose. */
  private Cell referenceKV = null;

  /**
   * When filtering a given Cell in {@link #filterKeyValue(Cell)},
   * this stores the transformed Cell to be returned by {@link #transformCell(Cell)}.
   *
   * Individual filters transformation are applied only when the filter includes the Cell.
   * Transformations are composed in the order specified by {@link #filters}.
   */
  private Cell transformedKV = null;

  /**
   * Constructor that takes a set of {@link Filter}s. The default operator
   * MUST_PASS_ALL is assumed.
   *
   * @param rowFilters list of filters
   */
  public FilterList(final List<Filter> rowFilters) {
    if (rowFilters instanceof ArrayList) {
      this.filters = rowFilters;
    } else {
      this.filters = new ArrayList<Filter>(rowFilters);
    }
    initPrevListForMustPassOne(rowFilters.size());
  }

  /**
   * Constructor that takes a var arg number of {@link Filter}s. The fefault operator
   * MUST_PASS_ALL is assumed.
   * @param rowFilters
   */
  public FilterList(final Filter... rowFilters) {
    this.filters = new ArrayList<Filter>(Arrays.asList(rowFilters));
    initPrevListForMustPassOne(filters.size());
  }

  /**
   * Constructor that takes an operator.
   *
   * @param operator Operator to process filter set with.
   */
  public FilterList(final Operator operator) {
    this.operator = operator;
    initPrevListForMustPassOne(filters.size());
  }

  /**
   * Constructor that takes a set of {@link Filter}s and an operator.
   *
   * @param operator Operator to process filter set with.
   * @param rowFilters Set of row filters.
   */
  public FilterList(final Operator operator, final List<Filter> rowFilters) {
    this.filters = new ArrayList<Filter>(rowFilters);
    this.operator = operator;
    initPrevListForMustPassOne(filters.size());
  }

  /**
   * Constructor that takes a var arg number of {@link Filter}s and an operator.
   *
   * @param operator Operator to process filter set with.
   * @param rowFilters Filters to use
   */
  public FilterList(final Operator operator, final Filter... rowFilters) {
    this.filters = new ArrayList<Filter>(Arrays.asList(rowFilters));
    this.operator = operator;
    initPrevListForMustPassOne(filters.size());
  }

  public void initPrevListForMustPassOne(int size) {
    if (operator == Operator.MUST_PASS_ONE) {
      if (this.prevFilterRCList == null) {
        prevFilterRCList = new ArrayList<ReturnCode>(Collections.nCopies(size, (ReturnCode) null));
      }
      if (this.prevCellList == null) {
        prevCellList = new ArrayList<Cell>(Collections.nCopies(size, (Cell) null));
      }
    }
  }

  /**
   * Get the operator.
   *
   * @return operator
   */
  public Operator getOperator() {
    return operator;
  }

  /**
   * Get the filters.
   *
   * @return filters
   */
  public List<Filter> getFilters() {
    return filters;
  }

  /**
   * Add a filter.
   *
   * @param filter another filter
   */
  public void addFilter(Filter filter) {
    if (this.isReversed() != filter.isReversed()) {
      throw new IllegalArgumentException(
          "Filters in the list must have the same reversed flag, this.reversed="
              + this.isReversed());
    }
    this.filters.add(filter);
    if (operator == Operator.MUST_PASS_ONE) {
      this.prevFilterRCList.add((ReturnCode) null);
      this.prevCellList.add((Cell) null);
    }
  }

  @Override
  public void reset() throws IOException {
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
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
    boolean flag = (this.operator == Operator.MUST_PASS_ONE) ? true : false;
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
      Filter filter = filters.get(i);
      if (this.operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining() ||
            filter.filterRowKey(rowKey, offset, length)) {
          flag =  true;
        }
      } else if (this.operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining() &&
            !filter.filterRowKey(rowKey, offset, length)) {
          flag =  false;
        }
      }
    }
    return flag;
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
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
  public Cell transformCell(Cell v) throws IOException {
    // transformCell() is expected to follow an inclusive filterKeyValue() immediately:
    if (!v.equals(this.referenceKV)) {
      throw new IllegalStateException("Reference Cell: " + this.referenceKV + " does not match: "
          + v);
    }
    return this.transformedKV;
  }

  /**
   * WARNING: please to not override this method.  Instead override {@link #transformCell(Cell)}.
   *
   * When removing this, its body should be placed in transformCell.
   *
   * This is for transition from 0.94 -&gt; 0.96
   */
  @Deprecated
  @Override
  public KeyValue transform(KeyValue v) throws IOException {
    // transform() is expected to follow an inclusive filterKeyValue() immediately:
    if (!v.equals(this.referenceKV)) {
      throw new IllegalStateException(
          "Reference Cell: " + this.referenceKV + " does not match: " + v);
     }
    return KeyValueUtil.ensureKeyValue(this.transformedKV);
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
      return nextHintCell == null || KeyValue.COMPARATOR.compare(currentCell, nextHintCell) >= 0;
    case NEXT_COL:
    case INCLUDE_AND_NEXT_COL:
      return !CellUtil.matchingRowColumn(prevCell, currentCell);
    case NEXT_ROW:
      return !CellUtil.matchingRow(prevCell, currentCell);
    default:
      throw new IllegalStateException("Received code is not valid.");
    }
  }
  
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="SF_SWITCH_FALLTHROUGH",
    justification="Intentional")
  public ReturnCode filterKeyValue(Cell v) throws IOException {
    this.referenceKV = v;
    seekHintFilter.clear();

    // Accumulates successive transformation of every filter that includes the Cell:
    Cell transformed = v;

    ReturnCode rc = operator == Operator.MUST_PASS_ONE?
        ReturnCode.SKIP: ReturnCode.INCLUDE;
    int listize = filters.size();
    /*
     * When all filters in a MUST_PASS_ONE FilterList return a SEEK_USING_NEXT_HINT code,
     * we should return SEEK_NEXT_USING_HINT from the FilterList to utilize the lowest seek value.
     * 
     * The following variable tracks whether any of the Filters returns ReturnCode other than
     * SEEK_NEXT_USING_HINT for MUST_PASS_ONE FilterList, in which case the optimization would
     * be skipped.
     */
    boolean seenNonHintReturnCode = false;
    for (int i = 0; i < listize; i++) {
      Filter filter = filters.get(i);
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining()) {
          return ReturnCode.NEXT_ROW;
        }
        ReturnCode code = filter.filterKeyValue(v);
        switch (code) {
        // Override INCLUDE and continue to evaluate.
        case INCLUDE_AND_NEXT_COL:
          rc = ReturnCode.INCLUDE_AND_NEXT_COL; // FindBugs SF_SWITCH_FALLTHROUGH
        case INCLUDE:
          transformed = filter.transformCell(transformed);
          continue;
        case SEEK_NEXT_USING_HINT:
          seekHintFilter.add(filter);
          continue;
        default:
          if (seekHintFilter.isEmpty()) {
            return code;
          }
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        Cell prevCell = this.prevCellList.get(i);
        if (filter.filterAllRemaining() || !shouldPassCurrentCellToFilter(prevCell, v, i)) {
          seenNonHintReturnCode = true;
          continue;
        }

        ReturnCode localRC = filter.filterKeyValue(v);
        // Update previous cell and return code we encountered.
        prevFilterRCList.set(i, localRC);
        if (v == null || localRC == ReturnCode.INCLUDE || localRC == ReturnCode.SKIP) {
          // If previous return code is INCLUDE or SKIP, we should always pass the next cell to the
          // corresponding sub-filter(need not test shouldPassCurrentCellToFilter() method), So we
          // need not save current cell to prevCellList for saving heap memory.
          prevCellList.set(i, null);
        } else {
          prevCellList.set(i, KeyValueUtil.toNewKeyCell(v));
        }

        if (localRC != ReturnCode.SEEK_NEXT_USING_HINT) {
          seenNonHintReturnCode = true;
        }
        switch (localRC) {
        case INCLUDE:
          if (rc != ReturnCode.INCLUDE_AND_NEXT_COL) {
            rc = ReturnCode.INCLUDE;
          }
          transformed = filter.transformCell(transformed);
          break;
        case INCLUDE_AND_NEXT_COL:
          rc = ReturnCode.INCLUDE_AND_NEXT_COL;
          transformed = filter.transformCell(transformed);
          // must continue here to evaluate all filters
          break;
        case NEXT_ROW:
          break;
        case SKIP:
          break;
        case NEXT_COL:
          break;
        case SEEK_NEXT_USING_HINT:
          break;
        default:
          throw new IllegalStateException("Received code is not valid.");
        }
      }
    }

    if (!seekHintFilter.isEmpty()) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    // Save the transformed Cell for transform():
    this.transformedKV = transformed;

    /*
     * The seenNonHintReturnCode flag is intended only for Operator.MUST_PASS_ONE branch.
     * If we have seen non SEEK_NEXT_USING_HINT ReturnCode, respect that ReturnCode.
     */
    if (operator == Operator.MUST_PASS_ONE && !seenNonHintReturnCode) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    return rc;
  }

  /**
   * Filters that never filter by modifying the returned List of Cells can
   * inherit this implementation that does nothing.
   *
   * {@inheritDoc}
   */
  @Override
  public void filterRowCells(List<Cell> cells) throws IOException {
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
      filters.get(i).filterRowCells(cells);
    }
  }

  @Override
  public boolean hasFilterRow() {
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
      if (filters.get(i).hasFilterRow()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean filterRow() throws IOException {
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
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
    return  operator == Operator.MUST_PASS_ONE;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte[] toByteArray() throws IOException {
    FilterProtos.FilterList.Builder builder =
      FilterProtos.FilterList.newBuilder();
    builder.setOperator(FilterProtos.FilterList.Operator.valueOf(operator.name()));
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
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

    List<Filter> rowFilters = new ArrayList<Filter>(proto.getFiltersCount());
    try {
      List<org.apache.hadoop.hbase.protobuf.generated.FilterProtos.Filter> filtersList =
          proto.getFiltersList();
      int listSize = filtersList.size();
      for (int i = 0; i < listSize; i++) {
        rowFilters.add(ProtobufUtil.toFilter(filtersList.get(i)));
      }
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
    return new FilterList(Operator.valueOf(proto.getOperator().name()),rowFilters);
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
  @Deprecated
  public KeyValue getNextKeyHint(KeyValue currentKV) throws IOException {
    return KeyValueUtil.ensureKeyValue(getNextCellHint((Cell)currentKV));
  }

  @Override
  public Cell getNextCellHint(Cell currentKV) throws IOException {
    Cell keyHint = null;
    if (operator == Operator.MUST_PASS_ALL) {
      for (Filter filter : seekHintFilter) {
        if (filter.filterAllRemaining()) continue;
        Cell curKeyHint = filter.getNextCellHint(currentKV);
        if (keyHint == null) {
          keyHint = curKeyHint;
          continue;
        }
        if (KeyValue.COMPARATOR.compare(keyHint, curKeyHint) < 0) {
          keyHint = curKeyHint;
        }
      }
      return keyHint;
    }

    // If any condition can pass, we need to keep the min hint
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
      if (filters.get(i).filterAllRemaining()) {
        continue;
      }
      Cell curKeyHint = filters.get(i).getNextCellHint(currentKV);
      if (curKeyHint == null) {
        // If we ever don't have a hint and this is must-pass-one, then no hint
        return null;
      }
      if (curKeyHint != null) {
        // If this is the first hint we find, set it
        if (keyHint == null) {
          keyHint = curKeyHint;
          continue;
        }
        if (KeyValue.COMPARATOR.compare(keyHint, curKeyHint) > 0) {
          keyHint = curKeyHint;
        }
      }
    }
    return keyHint;
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
      if (filters.get(i).isFamilyEssential(name)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void setReversed(boolean reversed) {
    int listize = filters.size();
    for (int i = 0; i < listize; i++) {
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
