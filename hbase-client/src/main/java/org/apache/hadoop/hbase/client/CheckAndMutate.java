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

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Used to perform CheckAndMutate operations.
 * <p>
 * Use the builder class to instantiate a CheckAndMutate object. This builder class is fluent style
 * APIs, the code are like:
 *
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
public final class CheckAndMutate implements Row {

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
    private boolean queryMetricsEnabled = false;

    private Builder(byte[] row) {
      this.row = Preconditions.checkNotNull(row, "row is null");
    }

    /**
     * Check for lack of column
     * @param family    family to check
     * @param qualifier qualifier to check
     * @return the CheckAndMutate object
     */
    public Builder ifNotExists(byte[] family, byte[] qualifier) {
      return ifEquals(family, qualifier, null);
    }

    /**
     * Check for equality
     * @param family    family to check
     * @param qualifier qualifier to check
     * @param value     the expected value
     * @return the CheckAndMutate object
     */
    public Builder ifEquals(byte[] family, byte[] qualifier, byte[] value) {
      return ifMatches(family, qualifier, CompareOperator.EQUAL, value);
    }

    /**
     * Check for match
     * @param family    family to check
     * @param qualifier qualifier to check
     * @param compareOp comparison operator to use
     * @param value     the expected value
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
     * Check for match
     * @param filter filter to check
     * @return the CheckAndMutate object
     */
    public Builder ifMatches(Filter filter) {
      this.filter = Preconditions.checkNotNull(filter, "filter is null");
      return this;
    }

    /**
     * Specify a timerange
     * @param timeRange time range to check
     * @return the CheckAndMutate object
     */
    public Builder timeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
      return this;
    }

    /**
     * Enables the return of {@link QueryMetrics} alongside the corresponding result for this query
     * <p>
     * This is intended for advanced users who need result-granular, server-side metrics
     * <p>
     * Does not work
     * @param queryMetricsEnabled {@code true} to enable collection of per-result query metrics
     *                            {@code false} to disable metrics collection (resulting in
     *                            {@code null} metrics)
     */
    public Builder queryMetricsEnabled(boolean queryMetricsEnabled) {
      this.queryMetricsEnabled = queryMetricsEnabled;
      return this;
    }

    private void preCheck(Row action) {
      Preconditions.checkNotNull(action, "action is null");
      if (!Bytes.equals(row, action.getRow())) {
        throw new IllegalArgumentException(
          "The row of the action <" + Bytes.toStringBinary(action.getRow())
            + "> doesn't match the original one <" + Bytes.toStringBinary(this.row) + ">");
      }
      Preconditions.checkState(op != null || filter != null,
        "condition is null. You need to"
          + " specify the condition by calling ifNotExists/ifEquals/ifMatches before building a"
          + " CheckAndMutate object");
    }

    /**
     * Build the CheckAndMutate object
     * @param put data to put if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Put put) {
      preCheck(put);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, put, queryMetricsEnabled);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, put,
          queryMetricsEnabled);
      }
    }

    /**
     * Build the CheckAndMutate object
     * @param delete data to delete if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Delete delete) {
      preCheck(delete);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, delete, queryMetricsEnabled);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, delete,
          queryMetricsEnabled);
      }
    }

    /**
     * Build the CheckAndMutate object with an Increment to commit if the check succeeds.
     * @param increment data to increment if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Increment increment) {
      preCheck(increment);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, increment, queryMetricsEnabled);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, increment,
          queryMetricsEnabled);
      }
    }

    /**
     * Build the CheckAndMutate object with an Append to commit if the check succeeds.
     * @param append data to append if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(Append append) {
      preCheck(append);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, append, queryMetricsEnabled);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, append,
          queryMetricsEnabled);
      }
    }

    /**
     * Build the CheckAndMutate object with a RowMutations to commit if the check succeeds.
     * @param mutations mutations to perform if check succeeds
     * @return a CheckAndMutate object
     */
    public CheckAndMutate build(RowMutations mutations) {
      preCheck(mutations);
      if (filter != null) {
        return new CheckAndMutate(row, filter, timeRange, mutations, queryMetricsEnabled);
      } else {
        return new CheckAndMutate(row, family, qualifier, op, value, timeRange, mutations,
          queryMetricsEnabled);
      }
    }
  }

  /**
   * returns a builder object to build a CheckAndMutate object
   * @param row row
   * @return a builder object
   */
  public static Builder newBuilder(byte[] row) {
    return new Builder(row);
  }

  private final byte[] row;
  private final byte[] family;
  private final byte[] qualifier;
  private final CompareOperator op;
  private final byte[] value;
  private final Filter filter;
  private final TimeRange timeRange;
  private final Row action;
  private final boolean queryMetricsEnabled;

  private CheckAndMutate(byte[] row, byte[] family, byte[] qualifier, final CompareOperator op,
    byte[] value, TimeRange timeRange, Row action, boolean queryMetricsEnabled) {
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
    this.op = op;
    this.value = value;
    this.filter = null;
    this.timeRange = timeRange != null ? timeRange : TimeRange.allTime();
    this.action = action;
    this.queryMetricsEnabled = queryMetricsEnabled;
  }

  private CheckAndMutate(byte[] row, Filter filter, TimeRange timeRange, Row action,
    boolean queryMetricsEnabled) {
    this.row = row;
    this.family = null;
    this.qualifier = null;
    this.op = null;
    this.value = null;
    this.filter = filter;
    this.timeRange = timeRange != null ? timeRange : TimeRange.allTime();
    this.action = action;
    this.queryMetricsEnabled = queryMetricsEnabled;
  }

  /** Returns the row */
  @Override
  public byte[] getRow() {
    return row;
  }

  /** Returns the family to check */
  public byte[] getFamily() {
    return family;
  }

  /** Returns the qualifier to check */
  public byte[] getQualifier() {
    return qualifier;
  }

  /** Returns the comparison operator */
  public CompareOperator getCompareOp() {
    return op;
  }

  /** Returns the expected value */
  public byte[] getValue() {
    return value;
  }

  /** Returns the filter to check */
  public Filter getFilter() {
    return filter;
  }

  /** Returns whether this has a filter or not */
  public boolean hasFilter() {
    return filter != null;
  }

  /** Returns the time range to check */
  public TimeRange getTimeRange() {
    return timeRange;
  }

  /** Returns the action done if check succeeds */
  public Row getAction() {
    return action;
  }

  /** Returns whether query metrics are enabled */
  public boolean isQueryMetricsEnabled() {
    return queryMetricsEnabled;
  }
}
