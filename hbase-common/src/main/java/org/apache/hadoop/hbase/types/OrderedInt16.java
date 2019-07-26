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
 * A {@code short} of 16-bits using a fixed-length encoding. Built on
 * {@link OrderedBytes#encodeInt16(PositionedByteRange, short, Order)}.
 */
@InterfaceAudience.Public
public class OrderedInt16 extends OrderedBytesBase<Short> {
  /**
   * @deprecated since 3.0.0 and will be removed in 4.0.0
   */
  @Deprecated
  public static final OrderedInt16 ASCENDING = new OrderedInt16(Order.ASCENDING);
  /**
   * @deprecated since 3.0.0 and will be removed in 4.0.0
   */
  @Deprecated
  public static final OrderedInt16 DESCENDING = new OrderedInt16(Order.DESCENDING);

  /**
   * Creates a new 16-bit {@code short} with a fixed-length encoding.
   *
   * @param order the {@link Order} to use
   */
  public OrderedInt16(Order order) {
    super(order);
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  @Override
  public int encodedLength(Short val) {
    return 3;
  }

  @Override
  public Class<Short> encodedClass() {
    return Short.class;
  }

  @Override
  public Short decode(PositionedByteRange src) {
    return OrderedBytes.decodeInt16(src);
  }

  @Override
  public int encode(PositionedByteRange dst, Short val) {
    if (null == val) {
      throw new IllegalArgumentException("Null values not supported.");
    }
    return OrderedBytes.encodeInt16(dst, val, order);
  }

  /**
   * Read a {@code short} value from the buffer {@code src}.
   *
   * @param src the {@link PositionedByteRange} to read the {@code float} from
   * @return the {@code short} read from buffer
   */
  public short decodeShort(PositionedByteRange src) {
    return OrderedBytes.decodeInt16(src);
  }

  /**
   * Write instance {@code val} into buffer {@code dst}.
   *
   * @param dst the {@link PositionedByteRange} to write to
   * @param val the value to write to {@code dst}
   * @return the number of bytes written
   */
  public int encodeShort(PositionedByteRange dst, short val) {
    return OrderedBytes.encodeInt16(dst, val, order);
  }
}
