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

import java.nio.ByteBuffer;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ComparatorProtos;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

/**
 * A binary comparator which lexicographically compares against the specified
 * byte array using {@link org.apache.hadoop.hbase.util.Bytes#compareTo(byte[], byte[])}.
 */
@InterfaceAudience.Public
@SuppressWarnings("ComparableType") // Should this move to Comparator usage?
public class NullComparator extends ByteArrayComparable {

  public NullComparator() {
    super(new byte[0]);
  }

  @Override
  public int compareTo(byte[] value) {
    return value != null ? 1 : 0;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings (value="EQ_UNUSUAL", justification="")
  public boolean equals(Object obj) {
    return obj == null;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return compareTo(value);
  }

  @Override
  public int compareTo(ByteBuffer value, int offset, int length) {
    return value != null ? 1 : 0;
  }

  /**
   * @return The comparator serialized using pb
   */
  @Override
  public byte [] toByteArray() {
    ComparatorProtos.NullComparator.Builder builder =
      ComparatorProtos.NullComparator.newBuilder();
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link NullComparator} instance
   * @return An instance of {@link NullComparator} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static NullComparator parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    try {
      // Just parse.  Don't use what we parse since on end we are returning new NullComparator.
      ComparatorProtos.NullComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new NullComparator();
  }

  /**
   * @param other
   * @return true if and only if the fields of the comparator that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  @Override
  boolean areSerializedFieldsEqual(ByteArrayComparable other) {
    if (other == this) return true;
    if (!(other instanceof NullComparator)) return false;

    return super.areSerializedFieldsEqual(other);
  }
}
