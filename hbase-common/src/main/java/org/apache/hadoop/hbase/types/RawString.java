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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;

/**
 * An {@code DataType} for interacting with values encoded using
 * {@link Bytes#toBytes(String)}. Intended to make it easier to transition
 * away from direct use of {@link Bytes}.
 * @see Bytes#toBytes(String)
 * @see Bytes#toString(byte[])
 * @see RawStringTerminated
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RawString implements DataType<String> {

  public static final RawString ASCENDING = new RawString(Order.ASCENDING);
  public static final RawString DESCENDING = new RawString(Order.DESCENDING);

  protected final Order order;

  protected RawString() { this.order = Order.ASCENDING; }
  protected RawString(Order order) { this.order = order; }

  @Override
  public boolean isOrderPreserving() { return true; }

  @Override
  public Order getOrder() { return order; }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public boolean isSkippable() { return false; }

  @Override
  public int skip(PositionedByteRange src) {
    int skipped = src.getRemaining();
    src.setPosition(src.getLength());
    return skipped;
  }

  @Override
  public int encodedLength(String val) { return Bytes.toBytes(val).length; }

  @Override
  public Class<String> encodedClass() { return String.class; }

  @Override
  public String decode(PositionedByteRange src) {
    if (Order.ASCENDING == this.order) {
      // avoid unnecessary array copy for ASC case.
      String val =
          Bytes.toString(src.getBytes(), src.getOffset() + src.getPosition(), src.getRemaining());
      src.setPosition(src.getLength());
      return val;
    } else {
      byte[] b = new byte[src.getRemaining()];
      src.get(b);
      order.apply(b, 0, b.length);
      return Bytes.toString(b);
    }
  }

  @Override
  public int encode(PositionedByteRange dst, String val) {
    byte[] s = Bytes.toBytes(val);
    order.apply(s);
    dst.put(s);
    return s.length;
  }
}
