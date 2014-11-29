/**
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
package org.apache.hadoop.hbase.types;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;

/**
 * An {@code DataType} that encodes variable-length values encoded using
 * {@link Bytes#putBytes(byte[], int, byte[], int, int)}. Includes a
 * termination marker following the raw {@code byte[]} value. Intended to
 * make it easier to transition away from direct use of {@link Bytes}.
 * @see Bytes#putBytes(byte[], int, byte[], int, int)
 * @see RawBytes
 * @see OrderedBlob
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RawBytesTerminated extends TerminatedWrapper<byte[]> {

  /**
   * Create a {@code RawBytesTerminated} using the specified terminator and
   * {@code order}.
   * @throws IllegalArgumentException if {@code term} is {@code null} or empty.
   */
  public RawBytesTerminated(Order order, byte[] term) {
    super(new RawBytes(order), term);
  }

  /**
   * Create a {@code RawBytesTerminated} using the specified terminator and
   * {@code order}.
   * @throws IllegalArgumentException if {@code term} is {@code null} or empty.
   */
  public RawBytesTerminated(Order order, String term) {
    super(new RawBytes(order), term);
  }

  /**
   * Create a {@code RawBytesTerminated} using the specified terminator.
   * @throws IllegalArgumentException if {@code term} is {@code null} or empty.
   */
  public RawBytesTerminated(byte[] term) {
    super(new RawBytes(), term);
  }

  /**
   * Create a {@code RawBytesTerminated} using the specified terminator.
   * @throws IllegalArgumentException if {@code term} is {@code null} or empty.
   */
  public RawBytesTerminated(String term) {
    super(new RawBytes(), term);
  }

  /**
   * Read a {@code byte[]} from the buffer {@code src}.
   */
  public byte[] decode(PositionedByteRange src, int length) {
    return ((RawBytes) wrapped).decode(src, length);
  }

  /**
   * Write {@code val} into {@code dst}, respecting {@code offset} and
   * {@code length}.
   * @return number of bytes written.
   */
  public int encode(PositionedByteRange dst, byte[] val, int voff, int vlen) {
    return ((RawBytes) wrapped).encode(dst, val, voff, vlen);
  }
}
