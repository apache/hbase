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
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Base class for FilterList. Currently, we have two sub-classes to extend this class:
 * {@link FilterListWithOR}, {@link FilterListWithAND}.
 */
@InterfaceAudience.Private
public abstract class FilterListBase extends FilterBase {
  private static final int MAX_LOG_FILTERS = 5;
  protected final ArrayList<Filter> filters;
  /**
   * For each sub-filter in filter list, we save a boolean flag to indicate that whether the return
   * code of filterCell(c) for sub-filter is INCLUDE* (INCLUDE, INCLUDE_AND_NEXT_COL,
   * INCLUDE_AND_SEEK_NEXT_ROW) case. if true, we need to transform cell for the sub-filter.
   */
  protected ArrayList<Boolean> subFiltersIncludedCell;

  public FilterListBase(List<Filter> filters) {
    reversed = checkAndGetReversed(filters, reversed);
    this.filters = new ArrayList<>(filters);
  }

  protected static boolean isInReturnCodes(ReturnCode testRC, ReturnCode... returnCodes) {
    for (ReturnCode rc : returnCodes) {
      if (testRC == rc) {
        return true;
      }
    }
    return false;
  }

  protected static boolean checkAndGetReversed(List<Filter> rowFilters, boolean defaultValue) {
    if (rowFilters.isEmpty()) {
      return defaultValue;
    }
    boolean retValue = rowFilters.get(0).isReversed();
    for (int i = 1, n = rowFilters.size(); i < n; i++) {
      if (rowFilters.get(i).isReversed() != retValue) {
        throw new IllegalArgumentException("Filters in the list must have the same reversed flag");
      }
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
    int cmp = CellComparator.getInstance().compare(a, b);
    return reversed ? -1 * cmp : cmp;
  }

  /**
   * For FilterList, we can consider a filter list as a node in a tree. sub-filters of the filter
   * list are children of the relative node. The logic of transforming cell of a filter list, well,
   * we can consider it as the process of post-order tree traverse. For a node , before we traverse
   * the current child, we should set the traverse result (transformed cell) of previous node(s) as
   * the initial value. (HBASE-18879).
   * @param c The cell in question.
   * @return the transformed cell.
   * @throws IOException
   */
  @Override
  public Cell transformCell(Cell c) throws IOException {
    if (isEmpty()) {
      return super.transformCell(c);
    }
    Cell transformed = c;
    for (int i = 0, n = filters.size(); i < n; i++) {
      if (subFiltersIncludedCell.get(i)) {
        transformed = filters.get(i).transformCell(transformed);
      }
    }
    return transformed;
  }

  @Override
  public ReturnCode filterKeyValue(final Cell c) throws IOException {
    return filterCell(c);
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
