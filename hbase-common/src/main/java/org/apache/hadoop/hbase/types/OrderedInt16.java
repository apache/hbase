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
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;


/**
 * A {@code short} of 16-bits using a fixed-length encoding. Built on
 * {@link OrderedBytes#encodeInt16(PositionedByteRange, short, Order)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedInt16 extends OrderedBytesBase<Short> {

  public static final OrderedInt16 ASCENDING = new OrderedInt16(Order.ASCENDING);
  public static final OrderedInt16 DESCENDING = new OrderedInt16(Order.DESCENDING);

  protected OrderedInt16(Order order) { super(order); }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public int encodedLength(Short val) { return 3; }

  @Override
  public Class<Short> encodedClass() { return Short.class; }

  @Override
  public Short decode(PositionedByteRange src) {
    return OrderedBytes.decodeInt16(src);
  }

  @Override
  public int encode(PositionedByteRange dst, Short val) {
    if (null == val) throw new IllegalArgumentException("Null values not supported.");
    return OrderedBytes.encodeInt16(dst, val, order);
  }

  /**
   * Read a {@code short} value from the buffer {@code src}.
   */
  public short decodeShort(PositionedByteRange src) {
    return OrderedBytes.decodeInt16(src);
  }

  /**
   * Write instance {@code val} into buffer {@code dst}.
   */
  public int encodeShort(PositionedByteRange dst, short val) {
    return OrderedBytes.encodeInt16(dst, val, order);
  }
}
