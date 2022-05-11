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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;

/**
 * A filter for adding inter-column timestamp matching Only cells with a correspondingly timestamped
 * entry in the target column will be retained Not compatible with Scan.setBatch as operations need
 * full rows for correct filtering
 */
@InterfaceAudience.Public
public class DependentColumnFilter extends CompareFilter {

  protected byte[] columnFamily;
  protected byte[] columnQualifier;
  protected boolean dropDependentColumn;

  protected Set<Long> stampSet = new HashSet<>();

  /**
   * Build a dependent column filter with value checking dependent column varies will be compared
   * using the supplied compareOp and comparator, for usage of which refer to {@link CompareFilter}
   * @param family              dependent column family
   * @param qualifier           dependent column qualifier
   * @param dropDependentColumn whether the column should be discarded after
   * @param valueCompareOp      comparison op
   * @param valueComparator     comparator
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use
   *             {@link #DependentColumnFilter(byte[], byte[], boolean, CompareOperator, ByteArrayComparable)}
   *             instead.
   */
  @Deprecated
  public DependentColumnFilter(final byte[] family, final byte[] qualifier,
    final boolean dropDependentColumn, final CompareOp valueCompareOp,
    final ByteArrayComparable valueComparator) {
    this(family, qualifier, dropDependentColumn, CompareOperator.valueOf(valueCompareOp.name()),
      valueComparator);
  }

  /**
   * Build a dependent column filter with value checking dependent column varies will be compared
   * using the supplied compareOp and comparator, for usage of which refer to {@link CompareFilter}
   * @param family              dependent column family
   * @param qualifier           dependent column qualifier
   * @param dropDependentColumn whether the column should be discarded after
   * @param op                  Value comparison op
   * @param valueComparator     comparator
   */
  public DependentColumnFilter(final byte[] family, final byte[] qualifier,
    final boolean dropDependentColumn, final CompareOperator op,
    final ByteArrayComparable valueComparator) {
    // set up the comparator
    super(op, valueComparator);
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    this.dropDependentColumn = dropDependentColumn;
  }

  /**
   * Constructor for DependentColumn filter. Cells where a Cell from target column with the same
   * timestamp do not exist will be dropped.
   * @param family    name of target column family
   * @param qualifier name of column qualifier
   */
  public DependentColumnFilter(final byte[] family, final byte[] qualifier) {
    this(family, qualifier, false);
  }

  /**
   * Constructor for DependentColumn filter. Cells where a Cell from target column with the same
   * timestamp do not exist will be dropped.
   * @param family              name of dependent column family
   * @param qualifier           name of dependent qualifier
   * @param dropDependentColumn whether the dependent columns Cells should be discarded
   */
  public DependentColumnFilter(final byte[] family, final byte[] qualifier,
    final boolean dropDependentColumn) {
    this(family, qualifier, dropDependentColumn, CompareOp.NO_OP, null);
  }

  /**
   * @return the column family
   */
  public byte[] getFamily() {
    return this.columnFamily;
  }

  /**
   * @return the column qualifier
   */
  public byte[] getQualifier() {
    return this.columnQualifier;
  }

  /**
   * @return true if we should drop the dependent column, false otherwise
   */
  public boolean dropDependentColumn() {
    return this.dropDependentColumn;
  }

  public boolean getDropDependentColumn() {
    return this.dropDependentColumn;
  }

  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    // Check if the column and qualifier match
    if (!CellUtil.matchingColumn(c, this.columnFamily, this.columnQualifier)) {
      // include non-matches for the time being, they'll be discarded afterwards
      return ReturnCode.INCLUDE;
    }
    // If it doesn't pass the op, skip it
    if (comparator != null && compareValue(getCompareOperator(), comparator, c))
      return ReturnCode.SKIP;

    stampSet.add(c.getTimestamp());
    if (dropDependentColumn) {
      return ReturnCode.SKIP;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public void filterRowCells(List<Cell> kvs) {
    kvs.removeIf(kv -> !stampSet.contains(kv.getTimestamp()));
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public boolean filterRow() {
    return false;
  }

  @Override
  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    return false;
  }

  @Override
  public void reset() {
    stampSet.clear();
  }

  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
    Preconditions.checkArgument(
      filterArguments.size() == 2 || filterArguments.size() == 3 || filterArguments.size() == 5,
      "Expected 2, 3 or 5 but got: %s", filterArguments.size());
    if (filterArguments.size() == 2) {
      byte[] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
      byte[] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
      return new DependentColumnFilter(family, qualifier);

    } else if (filterArguments.size() == 3) {
      byte[] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
      byte[] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
      boolean dropDependentColumn = ParseFilter.convertByteArrayToBoolean(filterArguments.get(2));
      return new DependentColumnFilter(family, qualifier, dropDependentColumn);

    } else if (filterArguments.size() == 5) {
      byte[] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
      byte[] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
      boolean dropDependentColumn = ParseFilter.convertByteArrayToBoolean(filterArguments.get(2));
      CompareOperator op = ParseFilter.createCompareOperator(filterArguments.get(3));
      ByteArrayComparable comparator =
        ParseFilter.createComparator(ParseFilter.removeQuotesFromByteArray(filterArguments.get(4)));
      return new DependentColumnFilter(family, qualifier, dropDependentColumn, op, comparator);
    } else {
      throw new IllegalArgumentException("Expected 2, 3 or 5 but got: " + filterArguments.size());
    }
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte[] toByteArray() {
    FilterProtos.DependentColumnFilter.Builder builder =
      FilterProtos.DependentColumnFilter.newBuilder();
    builder.setCompareFilter(super.convert());
    if (this.columnFamily != null) {
      builder.setColumnFamily(UnsafeByteOperations.unsafeWrap(this.columnFamily));
    }
    if (this.columnQualifier != null) {
      builder.setColumnQualifier(UnsafeByteOperations.unsafeWrap(this.columnQualifier));
    }
    builder.setDropDependentColumn(this.dropDependentColumn);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link DependentColumnFilter} instance
   * @return An instance of {@link DependentColumnFilter} made from <code>bytes</code> n * @see
   *         #toByteArray
   */
  public static DependentColumnFilter parseFrom(final byte[] pbBytes)
    throws DeserializationException {
    FilterProtos.DependentColumnFilter proto;
    try {
      proto = FilterProtos.DependentColumnFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    final CompareOperator valueCompareOp =
      CompareOperator.valueOf(proto.getCompareFilter().getCompareOp().name());
    ByteArrayComparable valueComparator = null;
    try {
      if (proto.getCompareFilter().hasComparator()) {
        valueComparator = ProtobufUtil.toComparator(proto.getCompareFilter().getComparator());
      }
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
    return new DependentColumnFilter(
      proto.hasColumnFamily() ? proto.getColumnFamily().toByteArray() : null,
      proto.hasColumnQualifier() ? proto.getColumnQualifier().toByteArray() : null,
      proto.getDropDependentColumn(), valueCompareOp, valueComparator);
  }

  /**
   * n * @return true if and only if the fields of the filter that are serialized are equal to the
   * corresponding fields in other. Used for testing.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value = "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE")
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof DependentColumnFilter)) return false;

    DependentColumnFilter other = (DependentColumnFilter) o;
    return other != null && super.areSerializedFieldsEqual(other)
      && Bytes.equals(this.getFamily(), other.getFamily())
      && Bytes.equals(this.getQualifier(), other.getQualifier())
      && this.dropDependentColumn() == other.dropDependentColumn();
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s, %s, %s, %s)", this.getClass().getSimpleName(),
      Bytes.toStringBinary(this.columnFamily), Bytes.toStringBinary(this.columnQualifier),
      this.dropDependentColumn, this.op.name(),
      this.comparator != null ? Bytes.toStringBinary(this.comparator.getValue()) : "null");
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Bytes.hashCode(getFamily()), Bytes.hashCode(getQualifier()),
      dropDependentColumn(), getComparator(), getCompareOperator());
  }
}
