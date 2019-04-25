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

import static org.apache.hadoop.hbase.util.Bytes.len;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * This filter is used for selecting only those keys with columns that are
 * between minColumn to maxColumn. For example, if minColumn is 'an', and
 * maxColumn is 'be', it will pass keys with columns like 'ana', 'bad', but not
 * keys with columns like 'bed', 'eye'
 *
 * If minColumn is null, there is no lower bound. If maxColumn is null, there is
 * no upper bound.
 *
 * minColumnInclusive and maxColumnInclusive specify if the ranges are inclusive
 * or not.
 */
@InterfaceAudience.Public
public class ColumnRangeFilter extends FilterBase {
  protected byte[] minColumn = null;
  protected boolean minColumnInclusive = true;
  protected byte[] maxColumn = null;
  protected boolean maxColumnInclusive = false;

  /**
   * Create a filter to select those keys with columns that are between minColumn
   * and maxColumn.
   * @param minColumn minimum value for the column range. If if it's null,
   * there is no lower bound.
   * @param minColumnInclusive if true, include minColumn in the range.
   * @param maxColumn maximum value for the column range. If it's null,
   * @param maxColumnInclusive if true, include maxColumn in the range.
   * there is no upper bound.
   */
  public ColumnRangeFilter(final byte[] minColumn, boolean minColumnInclusive,
      final byte[] maxColumn, boolean maxColumnInclusive) {
    this.minColumn = minColumn;
    this.minColumnInclusive = minColumnInclusive;
    this.maxColumn = maxColumn;
    this.maxColumnInclusive = maxColumnInclusive;
  }

  /**
   * @return if min column range is inclusive.
   */
  public boolean isMinColumnInclusive() {
    return minColumnInclusive;
  }

  /**
   * @return if max column range is inclusive.
   */
  public boolean isMaxColumnInclusive() {
    return maxColumnInclusive;
  }

  /**
   * @return the min column range for the filter
   */
  public byte[] getMinColumn() {
    return this.minColumn;
  }

  /**
   * @return true if min column is inclusive, false otherwise
   */
  public boolean getMinColumnInclusive() {
    return this.minColumnInclusive;
  }

  /**
   * @return the max column range for the filter
   */
  public byte[] getMaxColumn() {
    return this.maxColumn;
  }

  /**
   * @return true if max column is inclusive, false otherwise
   */
  public boolean getMaxColumnInclusive() {
    return this.maxColumnInclusive;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    int cmpMin = 1;

    if (this.minColumn != null) {
      cmpMin = CellUtil.compareQualifiers(c, this.minColumn, 0, this.minColumn.length);
    }

    if (cmpMin < 0) {
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }

    if (!this.minColumnInclusive && cmpMin == 0) {
      return ReturnCode.NEXT_COL;
    }

    if (this.maxColumn == null) {
      return ReturnCode.INCLUDE;
    }

    int cmpMax = CellUtil.compareQualifiers(c, this.maxColumn, 0, this.maxColumn.length);

    if ((this.maxColumnInclusive && cmpMax <= 0) || (!this.maxColumnInclusive && cmpMax < 0)) {
      return ReturnCode.INCLUDE;
    }

    return ReturnCode.NEXT_ROW;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 4,
                                "Expected 4 but got: %s", filterArguments.size());
    byte [] minColumn = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    boolean minColumnInclusive = ParseFilter.convertByteArrayToBoolean(filterArguments.get(1));
    byte [] maxColumn = ParseFilter.removeQuotesFromByteArray(filterArguments.get(2));
    boolean maxColumnInclusive = ParseFilter.convertByteArrayToBoolean(filterArguments.get(3));

    if (minColumn.length == 0)
      minColumn = null;
    if (maxColumn.length == 0)
      maxColumn = null;
    return new ColumnRangeFilter(minColumn, minColumnInclusive,
                                 maxColumn, maxColumnInclusive);
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    FilterProtos.ColumnRangeFilter.Builder builder =
      FilterProtos.ColumnRangeFilter.newBuilder();
    if (this.minColumn != null) builder.setMinColumn(
        UnsafeByteOperations.unsafeWrap(this.minColumn));
    builder.setMinColumnInclusive(this.minColumnInclusive);
    if (this.maxColumn != null) builder.setMaxColumn(
        UnsafeByteOperations.unsafeWrap(this.maxColumn));
    builder.setMaxColumnInclusive(this.maxColumnInclusive);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link ColumnRangeFilter} instance
   * @return An instance of {@link ColumnRangeFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static ColumnRangeFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.ColumnRangeFilter proto;
    try {
      proto = FilterProtos.ColumnRangeFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new ColumnRangeFilter(proto.hasMinColumn()?proto.getMinColumn().toByteArray():null,
      proto.getMinColumnInclusive(),proto.hasMaxColumn()?proto.getMaxColumn().toByteArray():null,
      proto.getMaxColumnInclusive());
  }

  /**
   * @param o filter to serialize.
   * @return true if and only if the fields of the filter that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ColumnRangeFilter)) {
      return false;
    }
    ColumnRangeFilter other = (ColumnRangeFilter) o;
    return Bytes.equals(this.getMinColumn(), other.getMinColumn())
        && this.getMinColumnInclusive() == other.getMinColumnInclusive()
        && Bytes.equals(this.getMaxColumn(), other.getMaxColumn())
        && this.getMaxColumnInclusive() == other.getMaxColumnInclusive();
  }

  @Override
  public Cell getNextCellHint(Cell cell) {
    return PrivateCellUtil.createFirstOnRowCol(cell, this.minColumn, 0, len(this.minColumn));
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " "
        + (this.minColumnInclusive ? "[" : "(") + Bytes.toStringBinary(this.minColumn)
        + ", " + Bytes.toStringBinary(this.maxColumn)
        + (this.maxColumnInclusive ? "]" : ")");
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Bytes.hashCode(getMinColumn()), getMinColumnInclusive(),
      Bytes.hashCode(getMaxColumn()), getMaxColumnInclusive());
  }
}
