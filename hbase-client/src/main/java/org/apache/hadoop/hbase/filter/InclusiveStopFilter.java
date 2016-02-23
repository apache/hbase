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

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A Filter that stops after the given row.  There is no "RowStopFilter" because
 * the Scan spec allows you to specify a stop row.
 *
 * Use this filter to include the stop row, eg: [A,Z].
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class InclusiveStopFilter extends FilterBase {
  private byte [] stopRowKey;
  private boolean done = false;

  public InclusiveStopFilter(final byte [] stopRowKey) {
    this.stopRowKey = stopRowKey;
  }

  public byte[] getStopRowKey() {
    return this.stopRowKey;
  }

  @Override
  public ReturnCode filterKeyValue(Cell v) {
    if (done) return ReturnCode.NEXT_ROW;
    return ReturnCode.INCLUDE;
  }

  public boolean filterRowKey(byte[] buffer, int offset, int length) {
    if (buffer == null) {
      //noinspection RedundantIfStatement
      if (this.stopRowKey == null) {
        return true; //filter...
      }
      return false;
    }
    // if stopRowKey is <= buffer, then true, filter row.
    int cmp = Bytes.compareTo(stopRowKey, 0, stopRowKey.length,
      buffer, offset, length);

    done = reversed ? cmp > 0 : cmp < 0;
    return done;
  }

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
  public byte [] toByteArray() {
    FilterProtos.InclusiveStopFilter.Builder builder =
      FilterProtos.InclusiveStopFilter.newBuilder();
    if (this.stopRowKey != null) builder.setStopRowKey(ByteStringer.wrap(this.stopRowKey));
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
   * @param other
   * @return true if and only if the fields of the filter that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
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
}
