/*
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

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ComparatorProtos;

/**
 * A binary comparator which lexicographically compares against the specified byte array using
 * {@link org.apache.hadoop.hbase.util.Bytes#compareTo(byte[], byte[])}.
 * @since 2.0.0
 */
@InterfaceAudience.Public
@SuppressWarnings("ComparableType") // Should this move to Comparator usage?
public class BinaryComparator extends org.apache.hadoop.hbase.filter.ByteArrayComparable {
  /**
   * Constructor
   * @param value value
   */
  public BinaryComparator(byte[] value) {
    super(value);
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return Bytes.compareTo(this.value, 0, this.value.length, value, offset, length);
  }

  @Override
  public int compareTo(ByteBuffer value, int offset, int length) {
    return ByteBufferUtils.compareTo(this.value, 0, this.value.length, value, offset, length);
  }

  /**
   * @return The comparator serialized using pb
   */
  @Override
  public byte[] toByteArray() {
    ComparatorProtos.BinaryComparator.Builder builder =
      ComparatorProtos.BinaryComparator.newBuilder();
    builder.setComparable(ProtobufUtil.toByteArrayComparable(this.value));
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link BinaryComparator} instance
   * @return An instance of {@link BinaryComparator} made from <code>bytes</code> n * @see
   *         #toByteArray
   */
  public static BinaryComparator parseFrom(final byte[] pbBytes) throws DeserializationException {
    ComparatorProtos.BinaryComparator proto;
    try {
      proto = ComparatorProtos.BinaryComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new BinaryComparator(proto.getComparable().getValue().toByteArray());
  }

  /**
   * n * @return true if and only if the fields of the comparator that are serialized are equal to
   * the corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(ByteArrayComparable other) {
    if (other == this) return true;
    if (!(other instanceof BinaryComparator)) return false;

    return super.areSerializedFieldsEqual(other);
  }
}
