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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.InvalidProtocolBufferException;

/**
 * This filter is used to filter based on the key. It takes an operator
 * (equal, greater, not equal, etc) and a byte [] comparator for the row,
 * and column qualifier portions of a key.
 * <p>
 * This filter can be wrapped with {@link WhileMatchFilter} to add more control.
 * <p>
 * Multiple filters can be combined using {@link FilterList}.
 * <p>
 * If an already known row range needs to be scanned, 
 * use {@link org.apache.hadoop.hbase.CellScanner} start
 * and stop rows directly rather than a filter.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class RowFilter extends CompareFilter {

  private boolean filterOutRow = false;

  /**
   * Constructor.
   * @param rowCompareOp the compare op for row matching
   * @param rowComparator the comparator for row matching
   */
  public RowFilter(final CompareOp rowCompareOp,
      final ByteArrayComparable rowComparator) {
    super(rowCompareOp, rowComparator);
  }

  @Override
  public void reset() {
    this.filterOutRow = false;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    if(this.filterOutRow) {
      return ReturnCode.NEXT_ROW;
    }
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) {
    if (compareRow(this.compareOp, this.comparator, firstRowCell)) {
      this.filterOutRow = true;
    }
    return this.filterOutRow;
  }

  @Override
  public boolean filterRow() {
    return this.filterOutRow;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    @SuppressWarnings("rawtypes") // for arguments
    ArrayList arguments = CompareFilter.extractArguments(filterArguments);
    CompareOp compareOp = (CompareOp)arguments.get(0);
    ByteArrayComparable comparator = (ByteArrayComparable)arguments.get(1);
    return new RowFilter(compareOp, comparator);
  }

 /**
  * @return The filter serialized using pb
  */
  public byte [] toByteArray() {
    FilterProtos.RowFilter.Builder builder =
      FilterProtos.RowFilter.newBuilder();
    builder.setCompareFilter(super.convert());
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link RowFilter} instance
   * @return An instance of {@link RowFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static RowFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.RowFilter proto;
    try {
      proto = FilterProtos.RowFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    final CompareOp valueCompareOp =
      CompareOp.valueOf(proto.getCompareFilter().getCompareOp().name());
    ByteArrayComparable valueComparator = null;
    try {
      if (proto.getCompareFilter().hasComparator()) {
        valueComparator = ProtobufUtil.toComparator(proto.getCompareFilter().getComparator());
      }
    } catch (IOException ioe) {
      throw new DeserializationException(ioe);
    }
    return new RowFilter(valueCompareOp,valueComparator);
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof RowFilter)) return false;

    return super.areSerializedFieldsEqual(o);
  }
}
