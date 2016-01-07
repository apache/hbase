/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implementation of {@link Filter} that represents an ordered List of Filters
 * which will be evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL}
 * (<code>AND</code>) or {@link Operator#MUST_PASS_ONE} (<code>OR</code>).
 * Since you can use Filter Lists as children of Filter Lists, you can create a
 * hierarchy of filters to be evaluated.
 *
 * <br/>
 * {@link Operator#MUST_PASS_ALL} evaluates lazily: evaluation stops as soon as one filter does
 * not include the KeyValue.
 *
 * <br/>
 * {@link Operator#MUST_PASS_ONE} evaluates non-lazily: all filters are always evaluated.
 *
 * <br/>
 * Defaults to {@link Operator#MUST_PASS_ALL}.
 * <p>TODO: Fix creation of Configuration on serialization and deserialization.
 */
public class FilterList implements Filter {
  /** set operator */
  public static enum Operator {
    /** !AND */
    MUST_PASS_ALL,
    /** !OR */
    MUST_PASS_ONE
  }

  private static final Configuration CONF;
  static {
    // We don't know which thread will load this class, so we don't know what
    // the state of the context classloader will be when this class is loaded.
    // HBaseConfiguration.create is dependent on the state of the context
    // classloader of the current thread, so we set it to be the classloader
    // that was used to load the Filter class to guarantee the consistent
    // ability to load this class from any thread
    ClassLoader saveCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(
          Filter.class.getClassLoader());
      CONF = HBaseConfiguration.create();
    } finally {
      Thread.currentThread().setContextClassLoader(saveCtxCl);
    }
  }
  private static final int MAX_LOG_FILTERS = 5;
  private Operator operator = Operator.MUST_PASS_ALL;
  private List<Filter> filters = new ArrayList<Filter>();
  private Filter seekHintFilter = null;

  /** Reference KeyValue used by {@link #transform(KeyValue)} for validation purpose. */
  private KeyValue referenceKV = null;

  /**
   * When filtering a given KeyValue in {@link #filterKeyValue(KeyValue)},
   * this stores the transformed KeyValue to be returned by {@link #transform(KeyValue)}.
   *
   * Individual filters transformation are applied only when the filter includes the KeyValue.
   * Transformations are composed in the order specified by {@link #filters}.
   */
  private KeyValue transformedKV = null;

  /**
   * Default constructor, filters nothing. Required though for RPC
   * deserialization.
   */
  public FilterList() {
    super();
  }

  /**
   * Constructor that takes a set of {@link Filter}s. The default operator
   * MUST_PASS_ALL is assumed.
   *
   * @param rowFilters list of filters
   */
  public FilterList(final List<Filter> rowFilters) {
    this.filters = rowFilters;
  }

  /**
   * Constructor that takes a var arg number of {@link Filter}s. The fefault operator
   * MUST_PASS_ALL is assumed.
   * @param rowFilters
   */
  public FilterList(final Filter... rowFilters) {
    this.filters = Arrays.asList(rowFilters);
  }

  /**
   * Constructor that takes an operator.
   *
   * @param operator Operator to process filter set with.
   */
  public FilterList(final Operator operator) {
    this.operator = operator;
  }

  /**
   * Constructor that takes a set of {@link Filter}s and an operator.
   *
   * @param operator Operator to process filter set with.
   * @param rowFilters Set of row filters.
   */
  public FilterList(final Operator operator, final List<Filter> rowFilters) {
    this.filters = rowFilters;
    this.operator = operator;
  }

  /**
   * Constructor that takes a var arg number of {@link Filter}s and an operator.
   *
   * @param operator Operator to process filter set with.
   * @param rowFilters Filters to use
   */
  public FilterList(final Operator operator, final Filter... rowFilters) {
    this.filters = Arrays.asList(rowFilters);
    this.operator = operator;
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
    this.filters.add(filter);
  }

  @Override
  public void reset() {
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      filters.get(i).reset();
    }
    seekHintFilter = null;
  }

  @Override
  public boolean filterRowKey(byte[] rowKey, int offset, int length) {
    boolean flag = (this.operator == Operator.MUST_PASS_ONE) ? true : false;
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      Filter filter = filters.get(i);
      if (this.operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining() ||
            filter.filterRowKey(rowKey, offset, length)) {
          flag = true;
        }
      } else if (this.operator == Operator.MUST_PASS_ONE) {
        if (!filter.filterAllRemaining() &&
            !filter.filterRowKey(rowKey, offset, length)) {
          flag = false;
        }
      }
    }
    return flag;
  }

  @Override
  public boolean filterAllRemaining() {
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      Filter filter = filters.get(i);
      if (filter.filterAllRemaining()) {
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
  public KeyValue transform(KeyValue v) {
    // transform() is expected to follow an inclusive filterKeyValue() immediately:
    if (!v.equals(this.referenceKV)) {
      throw new IllegalStateException(
          "Reference KeyValue: " + this.referenceKV + " does not match: " + v);
     }
    return this.transformedKV;
  }

  @Override
  public ReturnCode filterKeyValue(KeyValue v) {
    this.referenceKV = v;

    // Accumulates successive transformation of every filter that includes the KeyValue:
    KeyValue transformed = v;

    ReturnCode rc = operator == Operator.MUST_PASS_ONE?
        ReturnCode.SKIP: ReturnCode.INCLUDE;
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      Filter filter = filters.get(i);
      if (operator == Operator.MUST_PASS_ALL) {
        if (filter.filterAllRemaining()) {
          return ReturnCode.NEXT_ROW;
        }
        ReturnCode code = filter.filterKeyValue(v);
        switch (code) {
        // Override INCLUDE and continue to evaluate.
        case INCLUDE_AND_NEXT_COL:
          rc = ReturnCode.INCLUDE_AND_NEXT_COL;
        case INCLUDE:
          transformed = filter.transform(transformed);
          continue;
        case SEEK_NEXT_USING_HINT:
          seekHintFilter = filter;
          return code;
        default:
          return code;
        }
      } else if (operator == Operator.MUST_PASS_ONE) {
        if (filter.filterAllRemaining()) {
          continue;
        }

        ReturnCode code = filter.filterKeyValue(v);
        switch (code) {
        case INCLUDE:
          if (rc != ReturnCode.INCLUDE_AND_NEXT_COL) {
            rc = ReturnCode.INCLUDE;
          }
          transformed = filter.transform(transformed);
          break;
        case INCLUDE_AND_NEXT_COL:
          rc = ReturnCode.INCLUDE_AND_NEXT_COL;
          transformed = filter.transform(transformed);
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

    // Save the transformed KeyValue for transform():
    this.transformedKV = transformed;

    return rc;
  }

  @Override
  public void filterRow(List<KeyValue> kvs) {
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      Filter filter = filters.get(i);
      filter.filterRow(kvs);
    }
  }

  @Override
  public boolean hasFilterRow() {
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      Filter filter = filters.get(i);
      if (filter.hasFilterRow()) {
    	return true;
      }
    }
    return false;
  }

  @Override
  public boolean filterRow() {
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
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

  public void readFields(final DataInput in) throws IOException {
    byte opByte = in.readByte();
    operator = Operator.values()[opByte];
    int size = in.readInt();
    if (size > 0) {
      filters = new ArrayList<Filter>(size);
      for (int i = 0; i < size; i++) {
        Filter filter = HbaseObjectWritable.readFilter(in, CONF);
        filters.add(filter);
      }
    }
  }

  public void write(final DataOutput out) throws IOException {
    out.writeByte(operator.ordinal());
    out.writeInt(filters.size());
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      Filter filter = filters.get(i);
      HbaseObjectWritable.writeObject(out, filter, Writable.class, CONF);
    }
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue currentKV) {
    KeyValue keyHint = null;
    if (operator == Operator.MUST_PASS_ALL) {
      keyHint = seekHintFilter.getNextKeyHint(currentKV);
      return keyHint;
    }

    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      Filter filter = filters.get(i);
      KeyValue curKeyHint = filter.getNextKeyHint(currentKV);
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
          // If any condition can pass, we need to keep the min hint
          keyHint = curKeyHint;
        }
      }
    }
    return keyHint;
  }

  public boolean isFamilyEssential(byte[] name) {
    int listSize = filters.size();
    for (int i=0; i < listSize; i++) {
      Filter filter = filters.get(i);
      if (FilterBase.isFamilyEssential(filter, name)) {
        return true;
      }
    }
    return false;
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
