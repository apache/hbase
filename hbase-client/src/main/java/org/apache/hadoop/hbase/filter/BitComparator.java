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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A bit comparator which performs the specified bitwise operation on each of the bytes
 * with the specified byte array. Then returns whether the result is non-zero.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class BitComparator extends ByteArrayComparable {

  /** Bit operators. */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public enum BitwiseOp {
    /** and */
    AND,
    /** or */
    OR,
    /** xor */
    XOR
  }
  protected BitwiseOp bitOperator;

  /**
   * Constructor
   * @param value value
   * @param bitOperator operator to use on the bit comparison
   */
  public BitComparator(byte[] value, BitwiseOp bitOperator) {
    super(value);
    this.bitOperator = bitOperator;
  }

  /**
   * @return the bitwise operator
   */
  public BitwiseOp getOperator() {
    return bitOperator;
  }

  /**
   * @return The comparator serialized using pb
   */
  public byte [] toByteArray() {
    ComparatorProtos.BitComparator.Builder builder =
      ComparatorProtos.BitComparator.newBuilder();
    builder.setComparable(super.convert());
    ComparatorProtos.BitComparator.BitwiseOp bitwiseOpPb =
      ComparatorProtos.BitComparator.BitwiseOp.valueOf(bitOperator.name());
    builder.setBitwiseOp(bitwiseOpPb);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link BitComparator} instance
   * @return An instance of {@link BitComparator} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static BitComparator parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    ComparatorProtos.BitComparator proto;
    try {
      proto = ComparatorProtos.BitComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    BitwiseOp bitwiseOp = BitwiseOp.valueOf(proto.getBitwiseOp().name());
    return new BitComparator(proto.getComparable().getValue().toByteArray(),bitwiseOp);
  }

  /**
   * @param other
   * @return true if and only if the fields of the comparator that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(ByteArrayComparable other) {
    if (other == this) return true;
    if (!(other instanceof BitComparator)) return false;

    BitComparator comparator = (BitComparator)other;
    return super.areSerializedFieldsEqual(other)
      && this.getOperator().equals(comparator.getOperator());
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    if (length != this.value.length) {
      return 1;
    }
    int b = 0;
    //Iterating backwards is faster because we can quit after one non-zero byte.
    for (int i = length - 1; i >= 0 && b == 0; i--) {
      switch (bitOperator) {
        case AND:
          b = (this.value[i] & value[i+offset]) & 0xff;
          break;
        case OR:
          b = (this.value[i] | value[i+offset]) & 0xff;
          break;
        case XOR:
          b = (this.value[i] ^ value[i+offset]) & 0xff;
          break;
      }
    }
    return b == 0 ? 1 : 0;
  }
}

