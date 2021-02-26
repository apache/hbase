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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A comparator which compares against a specified byte array, but only
 * compares specific portion of the byte array. For the rest it is similar to
 * {@link BinaryComparator}.
 */
@InterfaceAudience.Public
@SuppressWarnings("ComparableType")
public class BinaryComponentComparator extends ByteArrayComparable {
  private int offset; //offset of component from beginning.

  /**
   * Constructor
   *
   * @param value  value of the component
   * @param offset offset of the component from begining
   */
  public BinaryComponentComparator(byte[] value, int offset) {
    super(value);
    this.offset = offset;
  }

  @Override
  public int compareTo(byte[] value) {
    return compareTo(value, 0, value.length);
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return Bytes.compareTo(this.value, 0, this.value.length, value, offset + this.offset,
        this.value.length);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this){
      return true;
    }
    if (!(other instanceof BinaryComponentComparator)){
      return false;
    }
    BinaryComponentComparator bcc = (BinaryComponentComparator)other;
    return offset == bcc.offset &&
         (compareTo(bcc.value) == 0);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + offset;
    return result;
  }

  /**
   * @return The comparator serialized using pb
   */
  @Override
  public byte[] toByteArray() {
    ComparatorProtos.BinaryComponentComparator.Builder builder =
        ComparatorProtos.BinaryComponentComparator.newBuilder();
    builder.setValue(ByteString.copyFrom(this.value));
    builder.setOffset(this.offset);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link BinaryComponentComparator} instance
   * @return An instance of {@link BinaryComponentComparator} made from <code>bytes</code>
   * @throws DeserializationException DeserializationException
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
   * @param other paramemter to compare against
   * @return true if and only if the fields of the comparator that are
   *         serialized are equal to the corresponding fields in other. Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(ByteArrayComparable other) {
    if (other == this){
      return true;
    }
    if (!(other instanceof BinaryComponentComparator)){
      return false;
    }
    return super.areSerializedFieldsEqual(other);
  }
}
