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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A {@link Filter} that checks a single column value, but does not emit the
 * tested column. This will enable a performance boost over
 * {@link SingleColumnValueFilter}, if the tested column value is not actually
 * needed as input (besides for the filtering itself).
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SingleColumnValueExcludeFilter extends SingleColumnValueFilter {

  /**
   * Constructor for binary compare of the value of a single column. If the
   * column is found and the condition passes, all columns of the row will be
   * emitted; except for the tested column value. If the column is not found or
   * the condition fails, the row will not be emitted.
   *
   * @param family name of column family
   * @param qualifier name of column qualifier
   * @param compareOp operator
   * @param value value to compare column values against
   */
  public SingleColumnValueExcludeFilter(byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value) {
    super(family, qualifier, compareOp, value);
  }

  /**
   * Constructor for binary compare of the value of a single column. If the
   * column is found and the condition passes, all columns of the row will be
   * emitted; except for the tested column value. If the condition fails, the
   * row will not be emitted.
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
  public SingleColumnValueExcludeFilter(byte[] family, byte[] qualifier,
      CompareOp compareOp, ByteArrayComparable comparator) {
    super(family, qualifier, compareOp, comparator);
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
  protected SingleColumnValueExcludeFilter(final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, ByteArrayComparable comparator, final boolean filterIfMissing,
      final boolean latestVersionOnly) {
    super(family, qualifier, compareOp, comparator, filterIfMissing, latestVersionOnly);
  }

  // We cleaned result row in FilterRow to be consistent with scanning process.
  public boolean hasFilterRow() {
   return true;
  }

  // Here we remove from row all key values from testing column
  @Override
  public void filterRowCells(List<Cell> kvs) {
    Iterator<? extends Cell> it = kvs.iterator();
    while (it.hasNext()) {
      // If the current column is actually the tested column,
      // we will skip it instead.
      if (CellUtil.matchingColumn(it.next(), this.columnFamily, this.columnQualifier)) {
        it.remove();
      }
    }
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    SingleColumnValueFilter tempFilter = (SingleColumnValueFilter)
      SingleColumnValueFilter.createFilterFromArguments(filterArguments);
    SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter (
      tempFilter.getFamily(), tempFilter.getQualifier(),
      tempFilter.getOperator(), tempFilter.getComparator());

    if (filterArguments.size() == 6) {
      filter.setFilterIfMissing(tempFilter.getFilterIfMissing());
      filter.setLatestVersionOnly(tempFilter.getLatestVersionOnly());
    }
    return filter;
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.SingleColumnValueExcludeFilter.Builder builder =
      FilterProtos.SingleColumnValueExcludeFilter.newBuilder();
    builder.setSingleColumnValueFilter(super.convert());
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link SingleColumnValueExcludeFilter} instance
   * @return An instance of {@link SingleColumnValueExcludeFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static SingleColumnValueExcludeFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.SingleColumnValueExcludeFilter proto;
    try {
      proto = FilterProtos.SingleColumnValueExcludeFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }

    FilterProtos.SingleColumnValueFilter parentProto = proto.getSingleColumnValueFilter();
    final CompareOp compareOp =
      CompareOp.valueOf(parentProto.getCompareOp().name());
    final ByteArrayComparable comparator;
    try {
      comparator = ProtobufUtil.toComparator(parentProto.getComparator());
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }

    return new SingleColumnValueExcludeFilter(parentProto.hasColumnFamily() ? parentProto
        .getColumnFamily().toByteArray() : null, parentProto.hasColumnQualifier() ? parentProto
        .getColumnQualifier().toByteArray() : null, compareOp, comparator, parentProto
        .getFilterIfMissing(), parentProto.getLatestVersionOnly());
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof SingleColumnValueExcludeFilter)) return false;

    return super.areSerializedFieldsEqual(o);
  }
}
