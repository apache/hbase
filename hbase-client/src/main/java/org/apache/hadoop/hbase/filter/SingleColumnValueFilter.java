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
import java.util.Objects;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * This filter is used to filter cells based on value. It takes a {@link CompareFilter.CompareOp}
 * operator (equal, greater, not equal, etc), and either a byte [] value or
 * a ByteArrayComparable.
 * <p>
 * If we have a byte [] value then we just do a lexicographic compare. For
 * example, if passed value is 'b' and cell has 'a' and the compare operator
 * is LESS, then we will filter out this cell (return true).  If this is not
 * sufficient (eg you want to deserialize a long and then compare it to a fixed
 * long value), then you can pass in your own comparator instead.
 * <p>
 * You must also specify a family and qualifier.  Only the value of this column
 * will be tested. When using this filter on a 
 * {@link org.apache.hadoop.hbase.CellScanner} with specified
 * inputs, the column to be tested should also be added as input (otherwise
 * the filter will regard the column as missing).
 * <p>
 * To prevent the entire row from being emitted if the column is not found
 * on a row, use {@link #setFilterIfMissing}.
 * Otherwise, if the column is found, the entire row will be emitted only if
 * the value passes.  If the value fails, the row will be filtered out.
 * <p>
 * In order to test values of previous versions (timestamps), set
 * {@link #setLatestVersionOnly} to false. The default is true, meaning that
 * only the latest version's value is tested and all previous versions are ignored.
 * <p>
 * To filter based on the value of all scanned columns, use {@link ValueFilter}.
 */
@InterfaceAudience.Public
public class SingleColumnValueFilter extends FilterBase {

  protected byte [] columnFamily;
  protected byte [] columnQualifier;
  protected CompareOperator op;
  protected org.apache.hadoop.hbase.filter.ByteArrayComparable comparator;
  protected boolean foundColumn = false;
  protected boolean matchedColumn = false;
  protected boolean filterIfMissing = false;
  protected boolean latestVersionOnly = true;

  /**
   * Constructor for binary compare of the value of a single column.  If the
   * column is found and the condition passes, all columns of the row will be
   * emitted.  If the condition fails, the row will not be emitted.
   * <p>
   * Use the filterIfColumnMissing flag to set whether the rest of the columns
   * in a row will be emitted if the specified column to check is not found in
   * the row.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param value value to compare column values against
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use
   * {@link #SingleColumnValueFilter(byte[], byte[], CompareOperator, byte[])} instead.
   */
  @Deprecated
  public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final byte[] value) {
    this(family, qualifier, CompareOperator.valueOf(compareOp.name()),
      new org.apache.hadoop.hbase.filter.BinaryComparator(value));
  }

  /**
   * Constructor for binary compare of the value of a single column.  If the
   * column is found and the condition passes, all columns of the row will be
   * emitted.  If the condition fails, the row will not be emitted.
   * <p>
   * Use the filterIfColumnMissing flag to set whether the rest of the columns
   * in a row will be emitted if the specified column to check is not found in
   * the row.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param op operator
   * @param value value to compare column values against
   */
  public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
                                 final CompareOperator op, final byte[] value) {
    this(family, qualifier, op,
      new org.apache.hadoop.hbase.filter.BinaryComparator(value));
  }

  /**
   * Constructor for binary compare of the value of a single column.  If the
   * column is found and the condition passes, all columns of the row will be
   * emitted.  If the condition fails, the row will not be emitted.
   * <p>
   * Use the filterIfColumnMissing flag to set whether the rest of the columns
   * in a row will be emitted if the specified column to check is not found in
   * the row.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param comparator Comparator to use.
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use
   * {@link #SingleColumnValueFilter(byte[], byte[], CompareOperator, ByteArrayComparable)} instead.
   */
  @Deprecated
  public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp,
      final org.apache.hadoop.hbase.filter.ByteArrayComparable comparator) {
    this(family, qualifier, CompareOperator.valueOf(compareOp.name()), comparator);
  }

  /**
   * Constructor for binary compare of the value of a single column.  If the
   * column is found and the condition passes, all columns of the row will be
   * emitted.  If the condition fails, the row will not be emitted.
   * <p>
   * Use the filterIfColumnMissing flag to set whether the rest of the columns
   * in a row will be emitted if the specified column to check is not found in
   * the row.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param op operator
   * @param comparator Comparator to use.
   */
  public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOperator op,
      final org.apache.hadoop.hbase.filter.ByteArrayComparable comparator) {
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    this.op = op;
    this.comparator = comparator;
  }

  /**
   * Constructor for protobuf deserialization only.
   * @param family
   * @param qualifier
   * @param compareOp
   * @param comparator
   * @param filterIfMissing
   * @param latestVersionOnly
   * @deprecated Since 2.0.0. Will be removed in 3.0.0. Use
   * {@link #SingleColumnValueFilter(byte[], byte[], CompareOperator, ByteArrayComparable,
   *   boolean, boolean)} instead.
   */
  @Deprecated
  protected SingleColumnValueFilter(final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, org.apache.hadoop.hbase.filter.ByteArrayComparable comparator,
      final boolean filterIfMissing,
      final boolean latestVersionOnly) {
    this(family, qualifier, CompareOperator.valueOf(compareOp.name()), comparator, filterIfMissing,
      latestVersionOnly);
  }

  /**
   * Constructor for protobuf deserialization only.
   * @param family
   * @param qualifier
   * @param op
   * @param comparator
   * @param filterIfMissing
   * @param latestVersionOnly
   */
  protected SingleColumnValueFilter(final byte[] family, final byte[] qualifier,
      final CompareOperator op, org.apache.hadoop.hbase.filter.ByteArrayComparable comparator,
       final boolean filterIfMissing, final boolean latestVersionOnly) {
    this(family, qualifier, op, comparator);
    this.filterIfMissing = filterIfMissing;
    this.latestVersionOnly = latestVersionOnly;
  }

  /**
   * @return operator
   * @deprecated  since 2.0.0. Will be removed in 3.0.0. Use {@link #getCompareOperator()} instead.
   */
  @Deprecated
  public CompareOp getOperator() {
    return CompareOp.valueOf(op.name());
  }

  public CompareOperator getCompareOperator() {
    return op;
  }

  /**
   * @return the comparator
   */
  public org.apache.hadoop.hbase.filter.ByteArrayComparable getComparator() {
    return comparator;
  }

  /**
   * @return the family
   */
  public byte[] getFamily() {
    return columnFamily;
  }

  /**
   * @return the qualifier
   */
  public byte[] getQualifier() {
    return columnQualifier;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    // Impl in FilterBase might do unnecessary copy for Off heap backed Cells.
    return false;
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    // System.out.println("REMOVE KEY=" + keyValue.toString() + ", value=" + Bytes.toString(keyValue.getValue()));
    if (this.matchedColumn) {
      // We already found and matched the single column, all keys now pass
      return ReturnCode.INCLUDE;
    } else if (this.latestVersionOnly && this.foundColumn) {
      // We found but did not match the single column, skip to next row
      return ReturnCode.NEXT_ROW;
    }
    if (!CellUtil.matchingColumn(c, this.columnFamily, this.columnQualifier)) {
      return ReturnCode.INCLUDE;
    }
    foundColumn = true;
    if (filterColumnValue(c)) {
      return this.latestVersionOnly? ReturnCode.NEXT_ROW: ReturnCode.INCLUDE;
    }
    this.matchedColumn = true;
    return ReturnCode.INCLUDE;
  }

  private boolean filterColumnValue(final Cell cell) {
    int compareResult = PrivateCellUtil.compareValue(cell, this.comparator);
    return CompareFilter.compare(this.op, compareResult);
  }

  @Override
  public boolean filterRow() {
    // If column was found, return false if it was matched, true if it was not
    // If column not found, return true if we filter if missing, false if not
    return this.foundColumn? !this.matchedColumn: this.filterIfMissing;
  }
  
  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public void reset() {
    foundColumn = false;
    matchedColumn = false;
  }

  /**
   * Get whether entire row should be filtered if column is not found.
   * @return true if row should be skipped if column not found, false if row
   * should be let through anyways
   */
  public boolean getFilterIfMissing() {
    return filterIfMissing;
  }

  /**
   * Set whether entire row should be filtered if column is not found.
   * <p>
   * If true, the entire row will be skipped if the column is not found.
   * <p>
   * If false, the row will pass if the column is not found.  This is default.
   * @param filterIfMissing flag
   */
  public void setFilterIfMissing(boolean filterIfMissing) {
    this.filterIfMissing = filterIfMissing;
  }

  /**
   * Get whether only the latest version of the column value should be compared.
   * If true, the row will be returned if only the latest version of the column
   * value matches. If false, the row will be returned if any version of the
   * column value matches. The default is true.
   * @return return value
   */
  public boolean getLatestVersionOnly() {
    return latestVersionOnly;
  }

  /**
   * Set whether only the latest version of the column value should be compared.
   * If true, the row will be returned if only the latest version of the column
   * value matches. If false, the row will be returned if any version of the
   * column value matches. The default is true.
   * @param latestVersionOnly flag
   */
  public void setLatestVersionOnly(boolean latestVersionOnly) {
    this.latestVersionOnly = latestVersionOnly;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 4 || filterArguments.size() == 6,
                                "Expected 4 or 6 but got: %s", filterArguments.size());
    byte [] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    byte [] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
    CompareOperator op = ParseFilter.createCompareOperator(filterArguments.get(2));
    org.apache.hadoop.hbase.filter.ByteArrayComparable comparator = ParseFilter.createComparator(
      ParseFilter.removeQuotesFromByteArray(filterArguments.get(3)));

    if (comparator instanceof RegexStringComparator ||
        comparator instanceof SubstringComparator) {
      if (op != CompareOperator.EQUAL &&
          op != CompareOperator.NOT_EQUAL) {
        throw new IllegalArgumentException ("A regexstring comparator and substring comparator " +
                                            "can only be used with EQUAL and NOT_EQUAL");
      }
    }

    SingleColumnValueFilter filter = new SingleColumnValueFilter(family, qualifier,
                                                                 op, comparator);

    if (filterArguments.size() == 6) {
      boolean filterIfMissing = ParseFilter.convertByteArrayToBoolean(filterArguments.get(4));
      boolean latestVersionOnly = ParseFilter.convertByteArrayToBoolean(filterArguments.get(5));
      filter.setFilterIfMissing(filterIfMissing);
      filter.setLatestVersionOnly(latestVersionOnly);
    }
    return filter;
  }

  FilterProtos.SingleColumnValueFilter convert() {
    FilterProtos.SingleColumnValueFilter.Builder builder =
      FilterProtos.SingleColumnValueFilter.newBuilder();
    if (this.columnFamily != null) {
      builder.setColumnFamily(UnsafeByteOperations.unsafeWrap(this.columnFamily));
    }
    if (this.columnQualifier != null) {
      builder.setColumnQualifier(UnsafeByteOperations.unsafeWrap(this.columnQualifier));
    }
    HBaseProtos.CompareType compareOp = CompareType.valueOf(this.op.name());
    builder.setCompareOp(compareOp);
    builder.setComparator(ProtobufUtil.toComparator(this.comparator));
    builder.setFilterIfMissing(this.filterIfMissing);
    builder.setLatestVersionOnly(this.latestVersionOnly);

    return builder.build();
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    return convert().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link SingleColumnValueFilter} instance
   * @return An instance of {@link SingleColumnValueFilter} made from <code>bytes</code>
   * @see #toByteArray
   */
  public static SingleColumnValueFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.SingleColumnValueFilter proto;
    try {
      proto = FilterProtos.SingleColumnValueFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }

    final CompareOperator compareOp =
      CompareOperator.valueOf(proto.getCompareOp().name());
    final org.apache.hadoop.hbase.filter.ByteArrayComparable comparator;
    try {
      comparator = ProtobufUtil.toComparator(proto.getComparator());
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }

    return new SingleColumnValueFilter(proto.hasColumnFamily() ? proto.getColumnFamily()
        .toByteArray() : null, proto.hasColumnQualifier() ? proto.getColumnQualifier()
        .toByteArray() : null, compareOp, comparator, proto.getFilterIfMissing(), proto
        .getLatestVersionOnly());
  }

  /**
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof SingleColumnValueFilter)) return false;

    SingleColumnValueFilter other = (SingleColumnValueFilter)o;
    return Bytes.equals(this.getFamily(), other.getFamily())
      && Bytes.equals(this.getQualifier(), other.getQualifier())
      && this.op.equals(other.op)
      && this.getComparator().areSerializedFieldsEqual(other.getComparator())
      && this.getFilterIfMissing() == other.getFilterIfMissing()
      && this.getLatestVersionOnly() == other.getLatestVersionOnly();
  }

  /**
   * The only CF this filter needs is given column family. So, it's the only essential
   * column in whole scan. If filterIfMissing == false, all families are essential,
   * because of possibility of skipping the rows without any data in filtered CF.
   */
  @Override
  public boolean isFamilyEssential(byte[] name) {
    return !this.filterIfMissing || Bytes.equals(name, this.columnFamily);
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s, %s, %s)",
        this.getClass().getSimpleName(), Bytes.toStringBinary(this.columnFamily),
        Bytes.toStringBinary(this.columnQualifier), this.op.name(),
        Bytes.toStringBinary(this.comparator.getValue()));
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Bytes.hashCode(getFamily()), Bytes.hashCode(getQualifier()),
      this.op, getComparator(), getFilterIfMissing(), getLatestVersionOnly());
  }
}
