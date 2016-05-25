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

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Locale;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * This comparator is for use with SingleColumnValueFilter, for filtering based on
 * the value of a given column. Use it to test if a given substring appears
 * in a cell value in the column. The comparison is case insensitive.
 * <p>
 * Only EQUAL or NOT_EQUAL tests are valid with this comparator.
 * <p>
 * For example:
 * <p>
 * <pre>
 * SingleColumnValueFilter scvf =
 *   new SingleColumnValueFilter("col", CompareOp.EQUAL,
 *     new SubstringComparator("substr"));
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class SubstringComparator extends ByteArrayComparable {

  private String substr;

  /**
   * Constructor
   * @param substr the substring
   */
  public SubstringComparator(String substr) {
    super(Bytes.toBytes(substr.toLowerCase(Locale.ROOT)));
    this.substr = substr.toLowerCase(Locale.ROOT);
  }

  @Override
  public byte[] getValue() {
    return Bytes.toBytes(substr);
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    return Bytes.toString(value, offset, length).toLowerCase(Locale.ROOT).contains(substr) ? 0
        : 1;
  }

  /**
   * @return The comparator serialized using pb
   */
  public byte [] toByteArray() {
    ComparatorProtos.SubstringComparator.Builder builder =
      ComparatorProtos.SubstringComparator.newBuilder();
    builder.setSubstr(this.substr);
    return builder.build().toByteArray();
  }

  /**
   * @param pbBytes A pb serialized {@link SubstringComparator} instance
   * @return An instance of {@link SubstringComparator} made from <code>bytes</code>
   * @throws DeserializationException
   * @see #toByteArray
   */
  public static SubstringComparator parseFrom(final byte [] pbBytes)
  throws DeserializationException {
    ComparatorProtos.SubstringComparator proto;
    try {
      proto = ComparatorProtos.SubstringComparator.parseFrom(pbBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new DeserializationException(e);
    }
    return new SubstringComparator(proto.getSubstr());
  }

  /**
   * @param other
   * @return true if and only if the fields of the comparator that are serialized
   * are equal to the corresponding fields in other.  Used for testing.
   */
  boolean areSerializedFieldsEqual(ByteArrayComparable other) {
    if (other == this) return true;
    if (!(other instanceof SubstringComparator)) return false;

    SubstringComparator comparator = (SubstringComparator)other;
    return super.areSerializedFieldsEqual(comparator)
      && this.substr.equals(comparator.substr);
  }

}
