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
 * A {@code byte} of 8-bits using a fixed-length encoding. Built on
 * {@link OrderedBytes#encodeInt8(PositionedByteRange, byte, Order)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedInt8 extends OrderedBytesBase<Byte> {

  public static final OrderedInt8 ASCENDING = new OrderedInt8(Order.ASCENDING);
  public static final OrderedInt8 DESCENDING = new OrderedInt8(Order.DESCENDING);

  protected OrderedInt8(Order order) { super(order); }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public int encodedLength(Byte val) { return 2; }

  @Override
  public Class<Byte> encodedClass() { return Byte.class; }

  @Override
  public Byte decode(PositionedByteRange src) {
    return OrderedBytes.decodeInt8(src);
  }

  @Override
  public int encode(PositionedByteRange dst, Byte val) {
    if (null == val) throw new IllegalArgumentException("Null values not supported.");
    return OrderedBytes.encodeInt8(dst, val, order);
  }

  /**
   * Read a {@code byte} value from the buffer {@code src}.
   */
  public byte decodeByte(PositionedByteRange src) {
    return OrderedBytes.decodeInt8(src);
  }

  /**
   * Write instance {@code val} into buffer {@code dst}.
   */
  public int encodeByte(PositionedByteRange dst, byte val) {
    return OrderedBytes.encodeInt8(dst, val, order);
  }
}
