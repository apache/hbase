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

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A {@code byte[]} of variable-length. Build on
 * {@link OrderedBytes#encodeBlobCopy(PositionedByteRange, byte[], int, int, Order)}.
 */
@InterfaceAudience.Public
public class OrderedBlob extends OrderedBytesBase<byte[]> {

  /**
   * @deprecated since 3.0.0 and will be removed in 4.0.0
   */
  @Deprecated
  public static final OrderedBlob ASCENDING = new OrderedBlob(Order.ASCENDING);
  /**
   * @deprecated since 3.0.0 and will b removed in 4.0.0
   */
  @Deprecated
  public static final OrderedBlob DESCENDING = new OrderedBlob(Order.DESCENDING);

  /**
   * Creates a new {@code byte[]} with variable length.
   *
   * @param order the {@link Order} to use
   */
  public OrderedBlob(Order order) {
    super(order);
  }

  @Override
  public boolean isSkippable() {
    return false;
  }

  @Override
  public int encodedLength(byte[] val) {
    return null == val ?
      (Order.ASCENDING == order ? 1 : 2) :
      (Order.ASCENDING == order ? val.length + 1 : val.length + 2);
  }

  @Override
  public Class<byte[]> encodedClass() {
    return byte[].class;
  }

  @Override
  public byte[] decode(PositionedByteRange src) {
    return OrderedBytes.decodeBlobCopy(src);
  }

  @Override
  public int encode(PositionedByteRange dst, byte[] val) {
    return OrderedBytes.encodeBlobCopy(dst, val, order);
  }

  /**
   * Write a subset of {@code val} to {@code dst}.
   *
   * @param dst the {@link PositionedByteRange} to write to
   * @param val  the value to write to {@code dst}
   * @param voff the offset in {@code dst} where to write {@code val} to
   * @param vlen the lenght of {@code val}
   * @return the number of bytes written
   */
  public int encode(PositionedByteRange dst, byte[] val, int voff, int vlen) {
    return OrderedBytes.encodeBlobCopy(dst, val, voff, vlen, order);
  }
}
