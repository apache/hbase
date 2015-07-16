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
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * <p>
 * This filter is used to filter based on the column family. It takes an
 * operator (equal, greater, not equal, etc) and a byte [] comparator for the
 * column family portion of a key.
 * </p><p>
 * This filter can be wrapped with {@link org.apache.hadoop.hbase.filter.WhileMatchFilter} and {@link org.apache.hadoop.hbase.filter.SkipFilter}
 * to add more control.
 * </p><p>
 * Multiple filters can be combined using {@link org.apache.hadoop.hbase.filter.FilterList}.
 * </p>
 * If an already known column family is looked for, use {@link org.apache.hadoop.hbase.client.Get#addFamily(byte[])}
 * directly rather than a filter.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class FamilyFilter extends CompareFilter {

  /**
   * Constructor.
   *
   * @param familyCompareOp  the compare op for column family matching
   * @param familyComparator the comparator for column family matching
   */
  public FamilyFilter(final CompareOp familyCompareOp,
                      final ByteArrayComparable familyComparator) {
      super(familyCompareOp, familyComparator);
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    int familyLength = v.getFamilyLength();
    if (familyLength > 0) {
      if (compareFamily(this.compareOp, this.comparator, v)) {
        return ReturnCode.NEXT_ROW;
      }
    }
    return ReturnCode.INCLUDE;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    ArrayList<?> arguments = CompareFilter.extractArguments(filterArguments);
    CompareOp compareOp = (CompareOp)arguments.get(0);
    ByteArrayComparable comparator = (ByteArrayComparable)arguments.get(1);
    return new FamilyFilter(compareOp, comparator);
  }

  /**
   * @return The filter serialized using pb
   */
  public byte [] toByteArray() {
    FilterProtos.FamilyFilter.Builder builder =
      FilterProtos.FamilyFilter.newBuilder();
    builder.setCompareFilter(super.convert());
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link FamilyFilter} instance
   * @return An instance of {@link FamilyFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static FamilyFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.FamilyFilter proto;
    try {
      proto = FilterProtos.FamilyFilter.parseFrom(pbBytes);
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
    return new FamilyFilter(valueCompareOp,valueComparator);
  }

  /**
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof FamilyFilter)) return false;

    FamilyFilter other = (FamilyFilter)o;
    return super.areSerializedFieldsEqual(other);
 }
}
