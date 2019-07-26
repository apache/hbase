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
 * A {@code String} of variable-length. Built on
 * {@link OrderedBytes#encodeString(PositionedByteRange, String, Order)}.
 */
@InterfaceAudience.Public
public class OrderedString extends OrderedBytesBase<String> {
  /**
   * @deprecated since 3.0.0 and will be removed in 4.0.0
   */
  @Deprecated
  public static final OrderedString ASCENDING = new OrderedString(Order.ASCENDING);
  /**
   * @deprecated since 3.0.0 and will be removed in 4.0.0
   */
  @Deprecated
  public static final OrderedString DESCENDING = new OrderedString(Order.DESCENDING);

  /**
   * Creates a new variable-length {@link String}.
   *
   * @param order the {@link Order} to use
   */
  public OrderedString(Order order) {
    super(order);
  }

  @Override
  public int encodedLength(String val) {
    // TODO: use of UTF8 here is a leaky abstraction.
    return null == val ? 1 : val.getBytes(OrderedBytes.UTF8).length + 2;
  }

  @Override
  public Class<String> encodedClass() {
    return String.class;
  }

  @Override
  public String decode(PositionedByteRange src) {
    return OrderedBytes.decodeString(src);
  }

  @Override
  public int encode(PositionedByteRange dst, String val) {
    return OrderedBytes.encodeString(dst, val, order);
  }
}
