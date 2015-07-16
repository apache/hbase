/*
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Abstract base class to help you implement new Filters.  Common "ignore" or NOOP type
 * methods can go here, helping to reduce boiler plate in an ever-expanding filter
 * library.
 *
 * If you could instantiate FilterBase, it would end up being a "null" filter -
 * that is one that never filters anything.
 */
@InterfaceAudience.Private // TODO add filter limited private level
public abstract class FilterBase extends Filter {

  /**
   * Filters that are purely stateless and do nothing in their reset() methods can inherit
   * this null/empty implementation.
   *
   * {@inheritDoc}
   */
  @Override
  public void reset() throws IOException {
  }

  /**
   * Filters that do not filter by row key can inherit this implementation that
   * never filters anything. (ie: returns false).
   *
   * {@inheritDoc}
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Instead use {@link #filterRowKey(Cell)}
   */
  @Override
  @Deprecated
  public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
    return false;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    return filterRowKey(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  /**
   * Filters that never filter all remaining can inherit this implementation that
   * never stops the filter early.
   *
   * {@inheritDoc}
   */
  @Override
  public boolean filterAllRemaining() throws IOException {
    return false;
  }

  /**
   * By default no transformation takes place
   *
   * {@inheritDoc}
   */
  @Override
  public Cell transformCell(Cell v) throws IOException {
    return v;
  }

  /**
   * Filters that never filter by modifying the returned List of Cells can
   * inherit this implementation that does nothing.
   *
   * {@inheritDoc}
   */
  @Override
  public void filterRowCells(List<Cell> ignored) throws IOException {
  }

  /**
   * Fitlers that never filter by modifying the returned List of Cells can
   * inherit this implementation that does nothing.
   *
   * {@inheritDoc}
   */
  @Override
  public boolean hasFilterRow() {
    return false;
  }

  /**
   * Filters that never filter by rows based on previously gathered state from
   * {@link #filterKeyValue(Cell)} can inherit this implementation that
   * never filters a row.
   *
   * {@inheritDoc}
   */
  @Override
  public boolean filterRow() throws IOException {
    return false;
  }

  /**
   * Filters that are not sure which key must be next seeked to, can inherit
   * this implementation that, by default, returns a null Cell.
   *
   * {@inheritDoc}
   */
  public Cell getNextCellHint(Cell currentCell) throws IOException {
    return null;
  }

  /**
   * By default, we require all scan's column families to be present. Our
   * subclasses may be more precise.
   *
   * {@inheritDoc}
   */
  public boolean isFamilyEssential(byte[] name) throws IOException {
    return true;
  }

  /**
   * Given the filter's arguments it constructs the filter
   * <p>
   * @param filterArguments the filter's arguments
   * @return constructed filter object
   */
  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    throw new IllegalArgumentException("This method has not been implemented");
  }

  /**
   * Return filter's info for debugging and logging purpose.
   */
  public String toString() {
    return this.getClass().getSimpleName();
  }

  /**
   * Return length 0 byte array for Filters that don't require special serialization
   */
  public byte[] toByteArray() throws IOException {
    return new byte[0];
  }

  /**
   * Default implementation so that writers of custom filters aren't forced to implement.
   *
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter other) {
    return true;
  }
}
