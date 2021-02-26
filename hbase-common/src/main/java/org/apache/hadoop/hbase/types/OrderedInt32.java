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
 * An {@code int} of 32-bits using a fixed-length encoding. Built on
 * {@link OrderedBytes#encodeInt32(PositionedByteRange, int, Order)}.
 */
@InterfaceAudience.Public
public class OrderedInt32 extends OrderedBytesBase<Integer> {

  public static final OrderedInt32 ASCENDING = new OrderedInt32(Order.ASCENDING);
  public static final OrderedInt32 DESCENDING = new OrderedInt32(Order.DESCENDING);

  protected OrderedInt32(Order order) {
    super(order);
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  @Override
  public int encodedLength(Integer val) {
    return 5;
  }

  @Override
  public Class<Integer> encodedClass() {
    return Integer.class;
  }

  @Override
  public Integer decode(PositionedByteRange src) {
    return OrderedBytes.decodeInt32(src);
  }

  @Override
  public int encode(PositionedByteRange dst, Integer val) {
    if (null == val) {
      throw new IllegalArgumentException("Null values not supported.");
    }
    return OrderedBytes.encodeInt32(dst, val, order);
  }

  /**
   * Read an {@code int} value from the buffer {@code src}.
   */
  public int decodeInt(PositionedByteRange src) {
    return OrderedBytes.decodeInt32(src);
  }

  /**
   * Write instance {@code val} into buffer {@code dst}.
   */
  public int encodeInt(PositionedByteRange dst, int val) {
    return OrderedBytes.encodeInt32(dst, val, order);
  }
}
