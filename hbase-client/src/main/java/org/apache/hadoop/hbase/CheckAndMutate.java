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
package org.apache.hadoop.hbase;

import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Used to perform CheckAndMutate operations.
 *
 * Use the builder classes to instantiate a CheckAndMutate object.
 * This builder classes are fluent style APIs, the code are like:
 *
 * <pre>
 * <code>
 * CheckAndMutate checkAndMutate = CheckAndMutate.builder(row, family).qualifier(qualifier)
 *   .ifNotExists().build(put);
 * </code>
 * <code>
 * CheckAndMutate checkAndMutate = CheckAndMutate.builder(row, filter).build(put);
 * </code>
 * </pre>
 */
@InterfaceAudience.Public
public final class CheckAndMutate extends Mutation {

  /**
   * A builder class for building a CheckAndMutate object.
   */
  public static final class Builder {
    private final byte[] row;
    private final byte[] family;
    private byte[] qualifier;
    private CompareOperator op;
    private byte[] value;
    private TimeRange timeRange;

    private Builder(byte[] row, byte[] family) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.family = Preconditions.checkNotNull(family, "family is null");
    }

    /**
     * @param qualifier column qualifier to check
     * @return the builder object
     */
    public Builder qualifier(byte[] qualifier) {
      this.qualifier = Preconditions.checkNotNull(qualifier, "qualifier is null. Consider using" +
        " an empty byte array, or just do not call this method if you want a null qualifier");
      return this;
    }

    /**
     * @param timeRange time range to check
     * @return the builder object
     */
    public Builder timeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
      return this;
    }

    /**
     * Check for lack of column
     * @return the builder object
     */
    public Builder ifNotExists() {
      this.op = CompareOperator.EQUAL;
      this.value = null;
      return this;
    }

    /**
     * Check for equality
     * @param value the expected value
     * @return the builder object
     */
    public Builder ifEquals(byte[] value) {
      return ifMatches(CompareOperator.EQUAL, value);
    }

    /**
     * @param compareOp comparison operator to use
     * @param value the expected value
     * @return the builder object
     */
    public Builder ifMatches(CompareOperator compareOp, byte[] value) {
      this.op = Preconditions.checkNotNull(compareOp, "compareOp is null");
      this.value = Preconditions.checkNotNull(value, "value is null");
      return this;
    }

    private void preCheck() {
      Preconditions.checkNotNull(op, "condition is null. You need to specify the condition by" +
        " calling ifNotExists/ifEquals/ifMatches before executing the request");
    }

    /**
     * @param put data to put if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Put put) {
      preCheck();
      return new CheckAndMutate(row, family, qualifier, op, value, timeRange, put);
    }

    /**
     * @param delete data to delete if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Delete delete) {
      preCheck();
      return new CheckAndMutate(row, family, qualifier, op, value, timeRange, delete);
    }

    /**
     * @param mutation mutations to perform if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(RowMutations mutation) {
      preCheck();
      return new CheckAndMutate(row, family, qualifier, op, value, timeRange, mutation);
    }
  }

  /**
   * A builder class for building a CheckAndMutate object with a filter.
   */
  public static final class WithFilterBuilder {
    private final byte[] row;
    private final Filter filter;
    private TimeRange timeRange;

    private WithFilterBuilder(byte[] row, Filter filter) {
      this.row = Preconditions.checkNotNull(row, "row is null");
      this.filter = Preconditions.checkNotNull(filter, "filter is null");
    }

    /**
     * @param timeRange timeRange to check
     * @return the builder object
     */
    public WithFilterBuilder timeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
      return this;
    }

    /**
     * @param put data to put if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Put put) {
      return new CheckAndMutate(row, filter, timeRange, put);
    }

    /**
     * @param delete data to delete if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Delete delete) {
      return new CheckAndMutate(row, filter, timeRange, delete);
    }

    /**
     * @param mutation mutations to perform if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(RowMutations mutation) {
      return new CheckAndMutate(row, filter, timeRange, mutation);
    }
  }

  /**
   * returns a builder object to build a CheckAndMutate object
   *
   * @param row row
   * @param family column family to check
   * @return a builder object
   */
  public static Builder builder(byte[] row, byte[] family) {
    return new Builder(row, family);
  }

  /**
   * returns a builder object to build a CheckAndMutate object with a filter
   *
   * @param row row
   * @param filter filter to check
   * @return a builder object
   */
  public static WithFilterBuilder builder(byte[] row, Filter filter) {
    return new WithFilterBuilder(row, filter);
  }

  private final byte[] family;
  private final byte[] qualifier;
  private final CompareOperator op;
  private final byte[] value;
  private final Filter filter;
  private final TimeRange timeRange;
  private final Row action;

  private CheckAndMutate(byte[] row, byte[] family, byte[] qualifier, CompareOperator op,
    byte[] value, TimeRange timeRange, Row action) {
    super(row, HConstants.LATEST_TIMESTAMP, Collections.emptyNavigableMap());

    this.family = family;
    this.qualifier = qualifier;
    this.timeRange = timeRange;
    this.op = op;
    this.value = value;
    this.filter = null;
    this.action = action;
  }

  private CheckAndMutate(byte[] row, Filter filter, TimeRange timeRange, Row action) {
    super(row, HConstants.LATEST_TIMESTAMP, Collections.emptyNavigableMap());

    this.family = null;
    this.qualifier = null;
    this.timeRange = timeRange;
    this.op = null;
    this.value = null;
    this.filter = filter;
    this.action = action;
  }

  /**
   * @return the family to check
   */
  public byte[] getFamily() {
    return family;
  }

  /**
   * @return the qualifier to check
   */
  public byte[] getQualifier() {
    return qualifier;
  }

  /**
   * @return the comparison operator
   */
  public CompareOperator getCompareOp() {
    return op;
  }

  /**
   * @return the expected value
   */
  public byte[] getValue() {
    return value;
  }

  /**
   * @return the filter to check
   */
  public Filter getFilter() {
    return filter;
  }

  /**
   * @return the timeRange to check
   */
  public TimeRange getTimeRange() {
    return timeRange;
  }

  /**
   * @return the action done if check succeeds
   */
  public Row getAction() {
    return action;
  }

  @Override
  public NavigableMap<byte[], List<Cell>> getFamilyCellMap() {
    if (action instanceof Mutation) {
      return ((Mutation) action).getFamilyCellMap();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public CellBuilder getCellBuilder(CellBuilderType cellBuilderType) {
    if (action instanceof Mutation) {
      return ((Mutation) action).getCellBuilder();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public long getTimestamp() {
    if (action instanceof Mutation) {
      return ((Mutation) action).getTimestamp();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Mutation setTimestamp(long timestamp) {
    if (action instanceof Mutation) {
      return ((Mutation) action).setTimestamp(timestamp);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Durability getDurability() {
    if (action instanceof Mutation) {
      return ((Mutation) action).getDurability();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Mutation setDurability(Durability d) {
    if (action instanceof Mutation) {
      return ((Mutation) action).setDurability(d);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public byte[] getAttribute(String name) {
    if (action instanceof Mutation) {
      return ((Mutation) action).getAttribute(name);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public OperationWithAttributes setAttribute(String name, byte[] value) {
    if (action instanceof Mutation) {
      return ((Mutation) action).setAttribute(name, value);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public int getPriority() {
    if (action instanceof Mutation) {
      return ((Mutation) action).getPriority();
    } else {
      return ((RowMutations) action).getMaxPriority();
    }
  }

  @Override
  public OperationWithAttributes setPriority(int priority) {
    if (action instanceof Mutation) {
      return ((Mutation) action).setPriority(priority);
    } else {
      throw new UnsupportedOperationException();
    }
  }
}
