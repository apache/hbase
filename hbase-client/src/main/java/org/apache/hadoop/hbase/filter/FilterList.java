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
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * Implementation of {@link Filter} that represents an ordered List of Filters which will be
 * evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL} (<code>AND</code>) or
 * {@link Operator#MUST_PASS_ONE} (<code>OR</code>). Since you can use Filter Lists as children of
 * Filter Lists, you can create a hierarchy of filters to be evaluated. <br>
 * {@link Operator#MUST_PASS_ALL} evaluates lazily: evaluation stops as soon as one filter does not
 * include the Cell. <br>
 * {@link Operator#MUST_PASS_ONE} evaluates non-lazily: all filters are always evaluated. <br>
 * Defaults to {@link Operator#MUST_PASS_ALL}.
 */
@InterfaceAudience.Public
final public class FilterList extends FilterBase {

  /** set operator */
  @InterfaceAudience.Public
  public enum Operator {
    /** !AND */
    MUST_PASS_ALL,
    /** !OR */
    MUST_PASS_ONE
  }

  private Operator operator;
  private FilterListBase filterListBase;

  /**
   * Constructor that takes a set of {@link Filter}s and an operator.
   * @param operator Operator to process filter set with.
   * @param filters Set of row filters.
   */
  public FilterList(final Operator operator, final List<Filter> filters) {
    if (operator == Operator.MUST_PASS_ALL) {
      filterListBase = new FilterListWithAND(filters);
    } else if (operator == Operator.MUST_PASS_ONE) {
      filterListBase = new FilterListWithOR(filters);
    } else {
      throw new IllegalArgumentException("Invalid operator: " + operator);
    }
    this.operator = operator;
  }

  /**
   * Constructor that takes a set of {@link Filter}s. The default operator MUST_PASS_ALL is assumed.
   * All filters are cloned to internal list.
   * @param filters list of filters
   */
  public FilterList(final List<Filter> filters) {
    this(Operator.MUST_PASS_ALL, filters);
  }

  /**
   * Constructor that takes a var arg number of {@link Filter}s. The default operator MUST_PASS_ALL
   * is assumed.
   * @param filters
   */
  public FilterList(final Filter... filters) {
    this(Operator.MUST_PASS_ALL, Arrays.asList(filters));
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
   * @param filters Filters to use
   */
  public FilterList(final Operator operator, final Filter... filters) {
    this(operator, Arrays.asList(filters));
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
    return filterListBase.getFilters();
  }

  public int size() {
    return filterListBase.size();
  }

  public void addFilter(List<Filter> filters) {
    filterListBase.addFilterLists(filters);
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
    filterListBase.reset();
  }

  @Override
  public boolean filterRowKey(byte[] rowKey, int offset, int length) throws IOException {
    return filterListBase.filterRowKey(rowKey, offset, length);
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) throws IOException {
    return filterListBase.filterRowKey(firstRowCell);
  }

  @Override
  public boolean filterAllRemaining() throws IOException {
    return filterListBase.filterAllRemaining();
  }

  @Override
  public Cell transformCell(Cell c) throws IOException {
    return filterListBase.transformCell(c);
  }

  @Override
  @Deprecated
  public ReturnCode filterKeyValue(final Cell c) throws IOException {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) throws IOException {
    return filterListBase.filterCell(c);
  }

  /**
   * Filters that never filter by modifying the returned List of Cells can inherit this
   * implementation that does nothing. {@inheritDoc}
   */
  @Override
  public void filterRowCells(List<Cell> cells) throws IOException {
    filterListBase.filterRowCells(cells);
  }

  @Override
  public boolean hasFilterRow() {
    return filterListBase.hasFilterRow();
  }

  @Override
  public boolean filterRow() throws IOException {
    return filterListBase.filterRow();
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte[] toByteArray() throws IOException {
    FilterProtos.FilterList.Builder builder = FilterProtos.FilterList.newBuilder();
    builder.setOperator(FilterProtos.FilterList.Operator.valueOf(operator.name()));
    ArrayList<Filter> filters = filterListBase.getFilters();
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
  public static FilterList parseFrom(final byte[] pbBytes) throws DeserializationException {
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
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter other) {
    if (other == this) return true;
    if (!(other instanceof FilterList)) return false;

    FilterList o = (FilterList) other;
    return this.getOperator().equals(o.getOperator())
        && ((this.getFilters() == o.getFilters()) || this.getFilters().equals(o.getFilters()));
  }

  @Override
  public Cell getNextCellHint(Cell currentCell) throws IOException {
    return this.filterListBase.getNextCellHint(currentCell);
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    return this.filterListBase.isFamilyEssential(name);
  }

  @Override
  public void setReversed(boolean reversed) {
    this.reversed = reversed;
    this.filterListBase.setReversed(reversed);
  }

  @Override
  public boolean isReversed() {
    assert this.reversed == this.filterListBase.isReversed();
    return this.reversed;
  }

  @Override
  public String toString() {
    return this.filterListBase.toString();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getOperator(), getFilters());
  }
}
