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
package org.apache.hadoop.hbase.client;

import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Used to perform CheckAndMutate operations.
 * <p>
 * Use the builder class to instantiate a CheckAndMutate object.
 * This builder class is fluent style APIs, the code are like:
 * <pre>
 * <code>
 * // A CheckAndMutate operation where do the specified action if the column (specified by the
 * // family and the qualifier) of the row equals to the specified value
 * CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(row)
 *   .ifEquals(family, qualifier, value)
 *   .build(put);
 *
 * // A CheckAndMutate operation where do the specified action if the column (specified by the
 * // family and the qualifier) of the row doesn't exist
 * CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(row)
 *   .ifNotExists(family, qualifier)
 *   .build(put);
 *
 * // A CheckAndMutate operation where do the specified action if the row matches the filter
 * CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(row)
 *   .ifMatches(filter)
 *   .build(delete);
 * </code>
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class CheckAndMutate extends Mutation {

  /**
   * A builder class for building a CheckAndMutate object.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public static final class Builder {
    private final byte[] row;
    private byte[] family;
    private byte[] qualifier;
    private CompareOperator op;
    private byte[] value;
    private Filter filter;
    private TimeRange timeRange;

    private Builder(byte[] row) {
      this.row = Preconditions.checkNotNull(row, "row is null");
    }

    /**
     * Check for lack of column
     *
     * @param family family to check
     * @param qualifier qualifier to check
     * @return the CheckAndMutate object
     */
    public Builder ifNotExists(byte[] family, byte[] qualifier) {
      return ifEquals(family, qualifier, null);
    }

    /**
     * Check for equality
     *
     * @param family family to check
     * @param qualifier qualifier to check
     * @param value the expected value
     * @return the CheckAndMutate object
     */
    public Builder ifEquals(byte[] family, byte[] qualifier, byte[] value) {
      return ifMatches(family, qualifier, CompareOperator.EQUAL, value);
    }

    /**
     * @param family family to check
     * @param qualifier qualifier to check
     * @param compareOp comparison operator to use
     * @param value the expected value
     * @return the CheckAndMutate object
     */
    public Builder ifMatches(byte[] family, byte[] qualifier, CompareOperator compareOp,
      byte[] value) {
      this.family = Preconditions.checkNotNull(family, "family is null");
      this.qualifier = qualifier;
      this.op = Preconditions.checkNotNull(compareOp, "compareOp is null");
      this.value = value;
      return this;
    }

    /**
     * @param filter filter to check
     * @return the CheckAndMutate object
     */
    public Builder ifMatches(Filter filter) {
      this.filter = Preconditions.checkNotNull(filter, "filter is null");
      return this;
    }

    /**
     * @param timeRange time range to check
     * @return the CheckAndMutate object
     */
    public Builder timeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
      return this;
    }

    private void preCheck(Row action) {
      Preconditions.checkNotNull(action, "action is null");
      if (!Bytes.equals(row, action.getRow())) {
        throw new IllegalArgumentException("The row of the action <" +
          Bytes.toStringBinary(action.getRow()) + "> doesn't match the original one <" +
          Bytes.toStringBinary(this.row) + ">");
      }
      Preconditions.checkState(op != null || filter != null, "condition is null. You need to"
        + " specify the condition by calling ifNotExists/ifEquals/ifMatches before building a"
        + " CheckAndMutate object");
    }

    /**
     * @param put data to put if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Put put) {
      preCheck(put);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, put);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, put);
      }
    }

    /**
     * @param delete data to delete if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Delete delete) {
      preCheck(delete);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, delete);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, delete);
      }
    }

    /**
     * @param increment data to increment if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Increment increment) {
      preCheck(increment);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, increment);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, increment);
      }
    }

    /**
     * @param append data to append if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Append append) {
      preCheck(append);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, append);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, append);
      }
    }

    /**
     * @param mutation mutations to perform if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(RowMutations mutation) {
      preCheck(mutation);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, mutation);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, mutation);
      }
    }
  }

  /**
   * returns a builder object to build a CheckAndMutate object
   *
   * @param row row
   * @return a builder object
   */
  public static Builder newBuilder(byte[] row) {
    return new Builder(row);
  }

  private final byte[] family;
  private final byte[] qualifier;
  private final CompareOperator op;
  private final byte[] value;
  private final Filter filter;
  private final TimeRange timeRange;
  private final Row action;

  private CheckAndMutate(byte[] row, byte[] family, byte[] qualifier,final CompareOperator op,
    byte[] value, TimeRange timeRange, Row action) {
    super(row, HConstants.LATEST_TIMESTAMP, Collections.emptyNavigableMap());
    this.family = family;
    this.qualifier = qualifier;
    this.op = op;
    this.value = value;
    this.filter = null;
    this.timeRange = timeRange != null ? timeRange : TimeRange.allTime();
    this.action = action;
  }

  private CheckAndMutate(byte[] row, Filter filter, TimeRange timeRange, Row action) {
    super(row, HConstants.LATEST_TIMESTAMP, Collections.emptyNavigableMap());
    this.family = null;
    this.qualifier = null;
    this.op = null;
    this.value = null;
    this.filter = filter;
    this.timeRange = timeRange != null ? timeRange : TimeRange.allTime();
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
   * @return whether this has a filter or not
   */
  public boolean hasFilter() {
    return filter != null;
  }

  /**
   * @return the time range to check
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
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public long getTimestamp() {
    if (action instanceof Mutation) {
      return ((Mutation) action).getTimestamp();
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public Mutation setTimestamp(long timestamp) {
    if (action instanceof Mutation) {
      return ((Mutation) action).setTimestamp(timestamp);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public Durability getDurability() {
    if (action instanceof Mutation) {
      return ((Mutation) action).getDurability();
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public Mutation setDurability(Durability d) {
    if (action instanceof Mutation) {
      return ((Mutation) action).setDurability(d);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getAttribute(String name) {
    if (action instanceof Mutation) {
      return ((Mutation) action).getAttribute(name);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public OperationWithAttributes setAttribute(String name, byte[] value) {
    if (action instanceof Mutation) {
      return ((Mutation) action).setAttribute(name, value);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public int getPriority() {
    if (action instanceof Mutation) {
      return ((Mutation) action).getPriority();
    }
    return ((RowMutations) action).getMaxPriority();
  }

  @Override
  public OperationWithAttributes setPriority(int priority) {
    if (action instanceof Mutation) {
      return ((Mutation) action).setPriority(priority);
    }
    throw new UnsupportedOperationException();
  }
}
