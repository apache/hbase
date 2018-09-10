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
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * This filter is used to filter based on the column qualifier. It takes an
 * operator (equal, greater, not equal, etc) and a byte [] comparator for the
 * column qualifier portion of a key.
 * <p>
 * This filter can be wrapped with {@link WhileMatchFilter} and {@link SkipFilter}
 * to add more control.
 * <p>
 * Multiple filters can be combined using {@link FilterList}.
 * <p>
 * If an already known column qualifier is looked for, 
 * use {@link org.apache.hadoop.hbase.client.Get#addColumn}
 * directly rather than a filter.
 */
@InterfaceAudience.Public
public class QualifierFilter extends CompareFilter {

  /**
   * Constructor.
   * @param op the compare op for column qualifier matching
   * @param qualifierComparator the comparator for column qualifier matching
   * @deprecated Since 2.0.0. Will be removed in 3.0.0.
   * Use {@link #QualifierFilter(CompareOperator, ByteArrayComparable)} instead.
   */
  @Deprecated
  public QualifierFilter(final CompareOp op,
      final ByteArrayComparable qualifierComparator) {
    super(op, qualifierComparator);
  }

  /**
   * Constructor.
   * @param op the compare op for column qualifier matching
   * @param qualifierComparator the comparator for column qualifier matching
   */
  public QualifierFilter(final CompareOperator op,
                         final ByteArrayComparable qualifierComparator) {
    super(op, qualifierComparator);
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    if (compareQualifier(getCompareOperator(), this.comparator, c)) {
      return ReturnCode.SKIP;
    }
    return ReturnCode.INCLUDE;
  }

  public static Filter createFilterFromArguments(ArrayList<byte []> filterArguments) {
    ArrayList<?> arguments = CompareFilter.extractArguments(filterArguments);
    CompareOperator compareOp = (CompareOperator)arguments.get(0);
    ByteArrayComparable comparator = (ByteArrayComparable)arguments.get(1);
    return new QualifierFilter(compareOp, comparator);
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    FilterProtos.QualifierFilter.Builder builder =
      FilterProtos.QualifierFilter.newBuilder();
    builder.setCompareFilter(super.convert());
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link QualifierFilter} instance
   * @return An instance of {@link QualifierFilter} made from <code>bytes</code>
   * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
   * @see #toByteArray
   */
  public static QualifierFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.QualifierFilter proto;
    try {
      proto = FilterProtos.QualifierFilter.parseFrom(pbBytes);
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
    return new QualifierFilter(valueCompareOp,valueComparator);
  }

  /**
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof QualifierFilter)) return false;

    return super.areSerializedFieldsEqual(o);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
