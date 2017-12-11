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
 * A {@code float} of 32-bits using a fixed-length encoding. Based on
 * {@link OrderedBytes#encodeFloat32(PositionedByteRange, float, Order)}.
 */
@InterfaceAudience.Public
public class OrderedFloat32 extends OrderedBytesBase<Float> {

  public static final OrderedFloat32 ASCENDING = new OrderedFloat32(Order.ASCENDING);
  public static final OrderedFloat32 DESCENDING = new OrderedFloat32(Order.DESCENDING);

  protected OrderedFloat32(Order order) {
    super(order);
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  @Override
  public int encodedLength(Float val) {
    return 5;
  }

  @Override
  public Class<Float> encodedClass() {
    return Float.class;
  }

  @Override
  public Float decode(PositionedByteRange src) {
    return OrderedBytes.decodeFloat32(src);
  }

  @Override
  public int encode(PositionedByteRange dst, Float val) {
    if (null == val) {
      throw new IllegalArgumentException("Null values not supported.");
    }
    return OrderedBytes.encodeFloat32(dst, val, order);
  }

  /**
   * Read a {@code float} value from the buffer {@code dst}.
   */
  public float decodeFloat(PositionedByteRange dst) {
    return OrderedBytes.decodeFloat32(dst);
  }

  /**
   * Write instance {@code val} into buffer {@code buff}.
   */
  public int encodeFloat(PositionedByteRange dst, float val) {
    return OrderedBytes.encodeFloat32(dst, val, order);
  }
}
