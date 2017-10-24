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
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for FilterList. Currently, we have two sub-classes to extend this class:
 * {@link FilterListWithOR}, {@link FilterListWithAND}.
 */
@InterfaceAudience.Private
public abstract class FilterListBase extends FilterBase {
  private static final int MAX_LOG_FILTERS = 5;
  protected final ArrayList<Filter> filters;

  /** Reference Cell used by {@link #transformCell(Cell)} for validation purpose. */
  protected Cell referenceCell = null;

  /**
   * When filtering a given Cell in {@link #filterKeyValue(Cell)}, this stores the transformed Cell
   * to be returned by {@link #transformCell(Cell)}. Individual filters transformation are applied
   * only when the filter includes the Cell. Transformations are composed in the order specified by
   * {@link #filters}.
   */
  protected Cell transformedCell = null;

  public FilterListBase(List<Filter> filters) {
    reversed = checkAndGetReversed(filters, reversed);
    this.filters = new ArrayList<>(filters);
  }

  protected static boolean isInReturnCodes(ReturnCode testRC, ReturnCode... returnCodes) {
    return Arrays.stream(returnCodes).anyMatch(testRC::equals);
  }

  protected static boolean checkAndGetReversed(List<Filter> rowFilters, boolean defaultValue) {
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

  public abstract void addFilterLists(List<Filter> filters);

  public int size() {
    return this.filters.size();
  }

  public boolean isEmpty() {
    return this.filters.isEmpty();
  }

  public ArrayList<Filter> getFilters() {
    return this.filters;
  }

  protected int compareCell(Cell a, Cell b) {
    int cmp = CellComparatorImpl.COMPARATOR.compare(a, b);
    return reversed ? -1 * cmp : cmp;
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
   * Internal implementation of {@link #filterKeyValue(Cell)}
   * @param c The cell in question.
   * @param transformedCell The transformed cell of previous filter(s)
   * @return ReturnCode of this filter operation.
   * @throws IOException
   * @see org.apache.hadoop.hbase.filter.FilterList#internalFilterKeyValue(Cell, Cell)
   */
  abstract ReturnCode internalFilterKeyValue(Cell c, Cell transformedCell) throws IOException;

  @Override
  public ReturnCode filterKeyValue(Cell c) throws IOException {
    return internalFilterKeyValue(c, c);
  }

  /**
   * Filters that never filter by modifying the returned List of Cells can inherit this
   * implementation that does nothing. {@inheritDoc}
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
  public boolean isFamilyEssential(byte[] name) throws IOException {
    if (this.filters.isEmpty()) {
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
    int endIndex = this.size() < MAX_LOG_FILTERS ? this.size() : MAX_LOG_FILTERS;
    return formatLogFilters(filters.subList(0, endIndex));
  }

  protected abstract String formatLogFilters(List<Filter> logFilters);
}
