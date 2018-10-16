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
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Different from {@link SingleColumnValueFilter} which returns an <b>entire</b> row
 * when specified condition is matched, {@link ColumnValueFilter} return the matched cell only.
 * <p>
 * This filter is used to filter cells based on column and value.
 * It takes a {@link org.apache.hadoop.hbase.CompareOperator} operator (<, <=, =, !=, >, >=), and
 * and a {@link ByteArrayComparable} comparator.
 */
@InterfaceAudience.Public
public class ColumnValueFilter extends FilterBase {
  private final byte[] family;
  private final byte[] qualifier;
  private final CompareOperator op;
  private final ByteArrayComparable comparator;

  // This flag is used to speed up seeking cells when matched column is found, such that following
  // columns in the same row can be skipped faster by NEXT_ROW instead of NEXT_COL.
  private boolean columnFound = false;

  public ColumnValueFilter(final byte[] family, final byte[] qualifier,
                           final CompareOperator op, final byte[] value) {
    this(family, qualifier, op, new BinaryComparator(value));
  }

  public ColumnValueFilter(final byte[] family, final byte[] qualifier,
                           final CompareOperator op,
                           final ByteArrayComparable comparator) {
    this.family = Preconditions.checkNotNull(family, "family should not be null.");
    this.qualifier = qualifier == null ? new byte[0] : qualifier;
    this.op = Preconditions.checkNotNull(op, "CompareOperator should not be null");
    this.comparator = Preconditions.checkNotNull(comparator, "Comparator should not be null");
  }

  /**
   * @return operator
   */
  public CompareOperator getCompareOperator() {
    return op;
  }

  /**
   * @return the comparator
   */
  public ByteArrayComparable getComparator() {
    return comparator;
  }

  /**
   * @return the column family
   */
  public byte[] getFamily() {
    return family;
  }

  /**
   * @return the qualifier
   */
  public byte[] getQualifier() {
    return qualifier;
  }

  @Override
  public void reset() throws IOException {
    columnFound = false;
  }

  @Override
  public boolean filterRowKey(Cell cell) throws IOException {
    return false;
  }

  @Override
  public ReturnCode filterCell(Cell c) throws IOException {
    // 1. Check column match
    if (!CellUtil.matchingColumn(c, this.family, this.qualifier)) {
      return columnFound ? ReturnCode.NEXT_ROW : ReturnCode.NEXT_COL;
    }
    // Column found
    columnFound = true;
    // 2. Check value match:
    // True means filter out, just skip this cell, else include it.
    return compareValue(getCompareOperator(), getComparator(), c) ?
      ReturnCode.SKIP : ReturnCode.INCLUDE;
  }

  /**
   * This method is used to determine a cell should be included or filtered out.
   * @param op one of operators {@link CompareOperator}
   * @param comparator comparator used to compare cells.
   * @param cell cell to be compared.
   * @return true means cell should be filtered out, included otherwise.
   */
  private boolean compareValue(final CompareOperator op, final ByteArrayComparable comparator,
    final Cell cell) {
    if (op == CompareOperator.NO_OP) {
      return true;
    }
    int compareResult = PrivateCellUtil.compareValue(cell, comparator);
    return CompareFilter.compare(op, compareResult);
  }

  /**
   * Creating this filter by reflection, it is used by {@link ParseFilter},
   * @param filterArguments arguments for creating a ColumnValueFilter
   * @return a ColumnValueFilter
   */
  public static Filter createFilterFromArguments(ArrayList<byte[]> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 4,
      "Expect 4 arguments: %s", filterArguments.size());
    byte[] family = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    byte[] qualifier = ParseFilter.removeQuotesFromByteArray(filterArguments.get(1));
    CompareOperator operator = ParseFilter.createCompareOperator(filterArguments.get(2));
    ByteArrayComparable comparator =
      ParseFilter.createComparator(ParseFilter.removeQuotesFromByteArray(filterArguments.get(3)));

    if (comparator instanceof RegexStringComparator ||
        comparator instanceof SubstringComparator) {
      if (operator != CompareOperator.EQUAL &&
          operator != CompareOperator.NOT_EQUAL) {
        throw new IllegalArgumentException("A regexstring comparator and substring comparator " +
            "can only be used with EQUAL and NOT_EQUAL");
      }
    }

    return new ColumnValueFilter(family, qualifier, operator, comparator);
  }

  /**
   * @return A pb instance to represent this instance.
   */
  FilterProtos.ColumnValueFilter convert() {
    FilterProtos.ColumnValueFilter.Builder builder =
      FilterProtos.ColumnValueFilter.newBuilder();

    builder.setFamily(UnsafeByteOperations.unsafeWrap(this.family));
    builder.setQualifier(UnsafeByteOperations.unsafeWrap(this.qualifier));
    builder.setCompareOp(HBaseProtos.CompareType.valueOf(this.op.name()));
    builder.setComparator(ProtobufUtil.toComparator(this.comparator));

    return builder.build();
  }

  /**
   * Parse protobuf bytes to a ColumnValueFilter
   * @param pbBytes pbBytes
   * @return a ColumnValueFilter
   * @throws DeserializationException deserialization exception
   */
  public static ColumnValueFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
    FilterProtos.ColumnValueFilter proto;
    try {
      proto = FilterProtos.ColumnValueFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }

    final CompareOperator compareOp = CompareOperator.valueOf(proto.getCompareOp().name());
    final ByteArrayComparable comparator;
    try {
      comparator = ProtobufUtil.toComparator(proto.getComparator());
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }

    return new ColumnValueFilter(proto.getFamily().toByteArray(),
      proto.getQualifier().toByteArray(), compareOp, comparator);
  }

  @Override
  public byte[] toByteArray() throws IOException {
    return convert().toByteArray();
  }

  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) {
      return true;
    } else if (!(o instanceof ColumnValueFilter)) {
      return false;
    }

    ColumnValueFilter other = (ColumnValueFilter) o;
    return Bytes.equals(this.getFamily(), other.getFamily()) &&
      Bytes.equals(this.getQualifier(), other.getQualifier()) &&
      this.getCompareOperator().equals(other.getCompareOperator()) &&
      this.getComparator().areSerializedFieldsEqual(other.getComparator());
  }

  @Override
  public boolean isFamilyEssential(byte[] name) throws IOException {
    return Bytes.equals(name, this.family);
  }

  @Override
  public String toString() {
    return String.format("%s (%s, %s, %s, %s)",
      getClass().getSimpleName(), Bytes.toStringBinary(this.family),
      Bytes.toStringBinary(this.qualifier), this.op.name(),
      Bytes.toStringBinary(this.comparator.getValue()));
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Objects.hash(Bytes.hashCode(getFamily()), Bytes.hashCode(getQualifier()),
      getCompareOperator(), getComparator());
  }
}
