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
 * A {@code double} of 64-bits using a fixed-length encoding. Built on
 * {@link OrderedBytes#encodeFloat64(PositionedByteRange, double, Order)}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class OrderedFloat64 extends OrderedBytesBase<Double> {

  public static final OrderedFloat64 ASCENDING = new OrderedFloat64(Order.ASCENDING);
  public static final OrderedFloat64 DESCENDING = new OrderedFloat64(Order.DESCENDING);

  protected OrderedFloat64(Order order) { super(order); }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public int encodedLength(Double val) { return 9; }

  @Override
  public Class<Double> encodedClass() { return Double.class; }

  @Override
  public Double decode(PositionedByteRange src) {
    return OrderedBytes.decodeFloat64(src);
  }

  @Override
  public int encode(PositionedByteRange dst, Double val) {
    if (null == val) throw new IllegalArgumentException("Null values not supported.");
    return OrderedBytes.encodeFloat64(dst, val, order);
  }

  /**
   * Read a {@code double} value from the buffer {@code src}.
   */
  public double decodeDouble(PositionedByteRange src) {
    return OrderedBytes.decodeFloat64(src);
  }

  /**
   * Write instance {@code val} into buffer {@code dst}.
   */
  public int encodeDouble(PositionedByteRange dst, double val) {
    return OrderedBytes.encodeFloat64(dst, val, order);
  }
}
