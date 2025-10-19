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

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ComparatorProtos;

/**
 * A binary comparator which lexicographically compares against the specified byte array similar to
 * {@link BinaryComparator} but starts the comparison from the specified offset. Will throw a
 * runtime exception if a comparison is attempted with a byte array that is too short for the
 * specified offest. See HBASE-22969 for examples on how to use this comparator
 */
@InterfaceAudience.Public
@SuppressWarnings("ComparableType")
public class BinaryComponentComparator extends ByteArrayComparable {
  private int offset; // offset of component from beginning.

  /**
   * Exception thrown when the offset provided to BinaryComponentComparator at construction time
   * exceeds the bounds of an input byte array provided to `compareTo`
   */
  public static class OffsetOutOfBoundsException extends ArrayIndexOutOfBoundsException {
    public OffsetOutOfBoundsException(String message) {
      super(message);
    }
  }

  /**
   * @param value  the byte array to compare against
   * @param offset the starting position (0-based) from which to start comparisons Must be less than
   *               the length of input arrays being passed for comparison, otherwise an
   *               {@link OffsetOutOfBoundsException} will be thrown on comparison.
   */
  public BinaryComponentComparator(byte[] value, int offset) {
    super(value);
    this.offset = offset;
  }

  /**
   * @throws OffsetOutOfBoundsException if input byte array is too small for the offset provided to
   *                                    comparator when it was constructed
   */
  @Override
  public int compareTo(byte[] value) {
    return compareTo(value, 0, value.length);
  }

  /**
   * @throws OffsetOutOfBoundsException if input byte array is too small for the offset provided to
   *                                    comparator when it was constructed
   */
  @Override
  public int compareTo(byte[] value, int offset, int length) {
    if (offset + this.offset >= value.length) {
      String message = String.format(
        "A byte array was encountered with a length %d that is too"
          + " short/incompatible with the offset value %d provided to BinaryComponentComparator",
        value.length, offset + this.offset);
      throw new OffsetOutOfBoundsException(message);
    }
    return Bytes.compareTo(this.value, 0, this.value.length, value, offset + this.offset,
      this.value.length);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof BinaryComponentComparator)) {
      return false;
    }
    BinaryComponentComparator bcc = (BinaryComponentComparator) other;
    return offset == bcc.offset && (compareTo(bcc.value) == 0);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + offset;
    return result;
  }

  /** Returns The comparator serialized using pb */
  @Override
  public byte[] toByteArray() {
    ComparatorProtos.BinaryComponentComparator.Builder builder =
      ComparatorProtos.BinaryComponentComparator.newBuilder();
    builder.setValue(ByteString.copyFrom(this.value));
    builder.setOffset(this.offset);
    return builder.build().toByteArray();
  }

  /**
   * Parse a serialized representation of {@link BinaryComponentComparator}
   * @param pbBytes A pb serialized {@link BinaryComponentComparator} instance
   * @return An instance of {@link BinaryComponentComparator} made from <code>bytes</code>
   * @throws DeserializationException if an error occurred
   * @see #toByteArray
   */
  public static BinaryComponentComparator parseFrom(final byte[] pbBytes)
    throws DeserializationException {
    ComparatorProtos.BinaryComponentComparator proto;
    try {
      proto = ComparatorProtos.BinaryComponentComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new BinaryComponentComparator(proto.getValue().toByteArray(), proto.getOffset());
  }

  /**
   * Returns true if and only if the fields of the comparator that are serialized are equal to the
   * corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(ByteArrayComparable other) {
    if (other == this) {
      return true;
    }
    if (!(other instanceof BinaryComponentComparator)) {
      return false;
    }
    return super.areSerializedFieldsEqual(other);
  }
}
