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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A BigDecimal comparator which numerical compares against the specified byte array
 */
@InterfaceAudience.Public
@SuppressWarnings("ComparableType") // Should this move to Comparator usage?
public class BigDecimalComparator extends ByteArrayComparable {
  private BigDecimal bigDecimal;

  public BigDecimalComparator(BigDecimal value) {
    super(Bytes.toBytes(value));
    this.bigDecimal = value;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof BigDecimalComparator)) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    BigDecimalComparator bdc = (BigDecimalComparator) obj;
    return this.bigDecimal.equals(bdc.bigDecimal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.bigDecimal);
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    BigDecimal that = Bytes.toBigDecimal(value, offset, length);
    return this.bigDecimal.compareTo(that);
  }

  @Override
  public int compareTo(ByteBuffer value, int offset, int length) {
    BigDecimal that = ByteBufferUtils.toBigDecimal(value, offset, length);
    return this.bigDecimal.compareTo(that);
  }

  /**
   * @return The comparator serialized using pb
   */
  @Override
  public byte[] toByteArray() {
    ComparatorProtos.BigDecimalComparator.Builder builder =
        ComparatorProtos.BigDecimalComparator.newBuilder();
    builder.setComparable(ProtobufUtil.toByteArrayComparable(this.value));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link BigDecimalComparator} instance
   * @return An instance of {@link BigDecimalComparator} made from <code>bytes</code>
   * @throws DeserializationException A deserialization exception
   * @see #toByteArray
   */
  public static BigDecimalComparator parseFrom(final byte[] pbBytes)
      throws DeserializationException {
    ComparatorProtos.BigDecimalComparator proto;
    try {
      proto = ComparatorProtos.BigDecimalComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new BigDecimalComparator(Bytes.toBigDecimal(proto.getComparable().getValue()
        .toByteArray()));
  }

  /**
   * @param other the other comparator
   * @return true if and only if the fields of the comparator that are serialized are equal to the
   *         corresponding fields in other. Used for testing.
   */
  boolean areSerializedFieldsEqual(BigDecimalComparator other) {
    if (other == this) {
      return true;
    }
    return super.areSerializedFieldsEqual(other);
  }
}
