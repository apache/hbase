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

import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * A Filter that stops after the given row.  There is no "RowStopFilter" because
 * the Scan spec allows you to specify a stop row.
 *
 * Use this filter to include the stop row, eg: [A,Z].
 */
@InterfaceAudience.Public
public class InclusiveStopFilter extends FilterBase {
  private byte [] stopRowKey;
  private boolean done = false;

  public InclusiveStopFilter(final byte [] stopRowKey) {
    this.stopRowKey = stopRowKey;
  }

  public byte[] getStopRowKey() {
    return this.stopRowKey;
  }

  @Deprecated
  @Override
  public ReturnCode filterKeyValue(final Cell c) {
    return filterCell(c);
  }

  @Override
  public ReturnCode filterCell(final Cell c) {
    if (done) return ReturnCode.NEXT_ROW;
    return ReturnCode.INCLUDE;
  }

  @Override
  public boolean filterRowKey(Cell firstRowCell) {
    // if stopRowKey is <= buffer, then true, filter row.
    if (filterAllRemaining()) return true;
    int cmp = CellComparator.getInstance().compareRows(firstRowCell, stopRowKey, 0, stopRowKey.length);
    done = reversed ? cmp < 0 : cmp > 0;
    return done;
  }

  @Override
  public boolean filterAllRemaining() {
    return done;
  }

  public static Filter createFilterFromArguments (ArrayList<byte []> filterArguments) {
    Preconditions.checkArgument(filterArguments.size() == 1,
                                "Expected 1 but got: %s", filterArguments.size());
    byte [] stopRowKey = ParseFilter.removeQuotesFromByteArray(filterArguments.get(0));
    return new InclusiveStopFilter(stopRowKey);
  }

  /**
   * @return The filter serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    FilterProtos.InclusiveStopFilter.Builder builder =
      FilterProtos.InclusiveStopFilter.newBuilder();
    if (this.stopRowKey != null) builder.setStopRowKey(
        UnsafeByteOperations.unsafeWrap(this.stopRowKey));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link InclusiveStopFilter} instance
   * @return An instance of {@link InclusiveStopFilter} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static InclusiveStopFilter parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    FilterProtos.InclusiveStopFilter proto;
    try {
      proto = FilterProtos.InclusiveStopFilter.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new InclusiveStopFilter(proto.hasStopRowKey()?proto.getStopRowKey().toByteArray():null);
  }

  /**
   * @param o the other filter to compare with
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(Filter o) {
    if (o == this) return true;
    if (!(o instanceof InclusiveStopFilter)) return false;

    InclusiveStopFilter other = (InclusiveStopFilter)o;
    return Bytes.equals(this.getStopRowKey(), other.getStopRowKey());
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " " + Bytes.toStringBinary(this.stopRowKey);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
  }

  @Override
  public int hashCode() {
    return Bytes.hashCode(this.stopRowKey);
  }
}
