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
 * An {@code DataType} that encodes fixed-length values encoded using
 * {@link org.apache.hadoop.hbase.util.Bytes#putBytes(byte[], int, byte[], int, int)}. 
 * Intended to make it easier to transition away from direct use of 
 * {@link org.apache.hadoop.hbase.util.Bytes}.
 * @see org.apache.hadoop.hbase.util.Bytes#putBytes(byte[], int, byte[], int, int)
 * @see RawBytes
 * @see OrderedBlob
 * @see OrderedBlobVar
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RawBytesFixedLength extends FixedLengthWrapper<byte[]> {

  /**
   * Create a {@code RawBytesFixedLength} using the specified {@code order}
   * and {@code length}.
   */
  public RawBytesFixedLength(Order order, int length) {
    super(new RawBytes(order), length);
  }

  /**
   * Create a {@code RawBytesFixedLength} of the specified {@code length}.
   */
  public RawBytesFixedLength(int length) {
    super(new RawBytes(), length);
  }

  /**
   * Read a {@code byte[]} from the buffer {@code src}.
   */
  public byte[] decode(PositionedByteRange src, int length) {
    return ((RawBytes) base).decode(src, length);
  }

  /**
   * Write {@code val} into {@code buff}, respecting {@code offset} and
   * {@code length}.
   */
  public int encode(PositionedByteRange dst, byte[] val, int voff, int vlen) {
    return ((RawBytes) base).encode(dst, val, voff, vlen);
  }
}
