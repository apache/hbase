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

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A comparator which compares against a specified byte array, but only compares
 * up to the length of this byte array. For the rest it is similar to
 * {@link BinaryComparator}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BinaryPrefixComparator extends ByteArrayComparable {

  /**
   * Constructor
   * @param value value
   */
  public BinaryPrefixComparator(byte[] value) {
    super(value);
  }

  @Override
  public int compareTo(byte [] value, int offset, int length) {
    return Bytes.compareTo(this.value, 0, this.value.length, value, offset,
        this.value.length <= length ? this.value.length : length);
  }

  /**
   * @return The comparator serialized using pb
   */
  public byte [] toByteArray() {
    ComparatorProtos.BinaryPrefixComparator.Builder builder =
      ComparatorProtos.BinaryPrefixComparator.newBuilder();
    builder.setComparable(super.convert());
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link BinaryPrefixComparator} instance
   * @return An instance of {@link BinaryPrefixComparator} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static BinaryPrefixComparator parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    ComparatorProtos.BinaryPrefixComparator proto;
    try {
      proto = ComparatorProtos.BinaryPrefixComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new BinaryPrefixComparator(proto.getComparable().getValue().toByteArray());
  }

  /**
   * @param other
   * @return true if and only if the fields of the comparator that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(ByteArrayComparable other) {
    if (other == this) return true;
    if (!(other instanceof BinaryPrefixComparator)) return false;

    return super.areSerializedFieldsEqual(other);
  }
}
