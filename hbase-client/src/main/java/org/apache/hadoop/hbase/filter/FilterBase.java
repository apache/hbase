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
import org.apache.yetus.audience.InterfaceAudience;

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

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    return filterAllRemaining();
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
   * {@link #filterCell(Cell)} can inherit this implementation that
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
  @Override
  public Cell getNextCellHint(Cell currentCell) throws IOException {
    return null;
  }

  /**
   * By default, we require all scan's column families to be present. Our
   * subclasses may be more precise.
   *
   * {@inheritDoc}
   */
  @Override
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
  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  /**
   * Return length 0 byte array for Filters that don't require special serialization
   */
  @Override
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
  @Override
  boolean areSerializedFieldsEqual(Filter other) {
    return true;
  }
}
