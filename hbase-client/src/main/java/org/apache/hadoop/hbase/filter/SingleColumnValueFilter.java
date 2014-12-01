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

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.CompareType;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

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
@InterfaceStability.Stable
public class SingleColumnValueFilter extends FilterBase {
  static final Log LOG = LogFactory.getLog(SingleColumnValueFilter.class);

  protected byte [] columnFamily;
  protected byte [] columnQualifier;
  protected CompareOp compareOp;
  protected ByteArrayComparable comparator;
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
   */
  public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final byte[] value) {
    this(family, qualifier, compareOp, new BinaryComparator(value));
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
   */
  public SingleColumnValueFilter(final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final ByteArrayComparable comparator) {
    this.columnFamily = family;
    this.columnQualifier = qualifier;
    this.compareOp = compareOp;
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
   */
  protected SingleColumnValueFilter(final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, ByteArrayComparable comparator, final boolean filterIfMissing,
      final boolean latestVersionOnly) {
    this(family, qualifier, compareOp, comparator);
    this.filterIfMissing = filterIfMissing;
    this.latestVersionOnly = latestVersionOnly;
  }

  /**
   * @return operator
   */
  public CompareOp getOperator() {
    return compareOp;
  }

  /**
   * @return the comparator
   */
  public ByteArrayComparable getComparator() {
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
  public ReturnCode filterKeyValue(Cell c) {
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
    if (filterColumnValue(c.getValueArray(), c.getValueOffset(), c.getValueLength())) {
      return this.latestVersionOnly? ReturnCode.NEXT_ROW: ReturnCode.INCLUDE;
    }
    this.matchedColumn = true;
    return ReturnCode.INCLUDE;
  }

  // Override here explicitly as the method in super class FilterBase might do a KeyValue recreate.
  // See HBASE-12068
  @Override
  public Cell transformCell(Cell v) {
    return v;
  }

  private boolean filterColumnValue(final byte [] data, final int offset,
      final int length) {
    int compareResult = this.comparator.compareTo(data, offset, length);
    switch (this.compareOp) {
    case LESS:
      return compareResult <= 0;
    case LESS_OR_EQUAL:
      return compareResult < 0;
    case EQUAL:
      return compareResult != 0;
    case NOT_EQUAL:
      return compareResult == 0;
    case GREATER_OR_EQUAL:
      return compareResult > 0;
    case GREATER:
      return compareResult >= 0;
    default:
      throw new RuntimeException("Unknown Compare op " + compareOp.name());
    }
  }

  public boolean filterRow() {
    // If column was found, return false if it was matched, true if it was not
    // If column not found, return true if we filter if missing, false if not
    return this.foundColumn? !this.matchedColumn: this.filterIfMissing;
  }
  
  public boolean hasFilterRow() {
    return true;
  }

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
    CompareOp compareOp = ParseFilter.createCompareOp(filterArguments.get(2));
    ByteArrayComparable comparator = ParseFilter.createComparator(
      ParseFilter.removeQuotesFromByteArray(filterArguments.get(3)));

    if (comparator instanceof RegexStringComparator ||
        comparator instanceof SubstringComparator) {
      if (compareOp != CompareOp.EQUAL &&
          compareOp != CompareOp.NOT_EQUAL) {
        throw new IllegalArgumentException ("A regexstring comparator and substring comparator " +
                                            "can only be used with EQUAL and NOT_EQUAL");
      }
    }

    SingleColumnValueFilter filter = new SingleColumnValueFilter(family, qualifier,
                                                                 compareOp, comparator);

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
      builder.setColumnFamily(ByteStringer.wrap(this.columnFamily));
    }
    if (this.columnQualifier != null) {
      builder.setColumnQualifier(ByteStringer.wrap(this.columnQualifier));
    }
    HBaseProtos.CompareType compareOp = CompareType.valueOf(this.compareOp.name());
    builder.setCompareOp(compareOp);
    builder.setComparator(ProtobufUtil.toComparator(this.comparator));
    builder.setFilterIfMissing(this.filterIfMissing);
    builder.setLatestVersionOnly(this.latestVersionOnly);

    return builder.build();
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    return convert().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link SingleColumnValueFilter} instance
   * @return An instance of {@link SingleColumnValueFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
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

    final CompareOp compareOp =
      CompareOp.valueOf(proto.getCompareOp().name());
    final ByteArrayComparable comparator;
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
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof SingleColumnValueFilter)) return false;

    SingleColumnValueFilter other = (SingleColumnValueFilter)o;
    return Bytes.equals(this.getFamily(), other.getFamily())
      && Bytes.equals(this.getQualifier(), other.getQualifier())
      && this.compareOp.equals(other.compareOp)
      && this.getComparator().areSerializedFieldsEqual(other.getComparator())
      && this.getFilterIfMissing() == other.getFilterIfMissing()
      && this.getLatestVersionOnly() == other.getLatestVersionOnly();
  }

  /**
   * The only CF this filter needs is given column family. So, it's the only essential
   * column in whole scan. If filterIfMissing == false, all families are essential,
   * because of possibility of skipping the rows without any data in filtered CF.
   */
  public boolean isFamilyEssential(byte[] name) {
    return !this.filterIfMissing || Bytes.equals(name, this.columnFamily);
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s, %s, %s)",
        this.getClass().getSimpleName(), Bytes.toStringBinary(this.columnFamily),
        Bytes.toStringBinary(this.columnQualifier), this.compareOp.name(),
        Bytes.toStringBinary(this.comparator.getValue()));
  }
}
