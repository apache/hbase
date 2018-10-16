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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An {@code DataType} for interacting with variable-length values
 * encoded using {@link Bytes#putBytes(byte[], int, byte[], int, int)}.
 * Intended to make it easier to transition away from direct use of
 * {@link Bytes}.
 * @see Bytes#putBytes(byte[], int, byte[], int, int)
 * @see RawBytesTerminated
 * @see RawBytesFixedLength
 * @see OrderedBlob
 * @see OrderedBlobVar
 */
@InterfaceAudience.Public
public class RawBytes implements DataType<byte[]> {

  public static final RawBytes ASCENDING = new RawBytes(Order.ASCENDING);
  public static final RawBytes DESCENDING = new RawBytes(Order.DESCENDING);

  protected final Order order;

  protected RawBytes() {
    this.order = Order.ASCENDING;
  }

  protected RawBytes(Order order) {
    this.order = order;
  }

  @Override
  public boolean isOrderPreserving() {
    return true;
  }

  @Override
  public Order getOrder() {
    return order;
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  @Override
  public boolean isSkippable() {
    return false;
  }

  @Override
  public int skip(PositionedByteRange src) {
    int skipped = src.getRemaining();
    src.setPosition(src.getLength());
    return skipped;
  }

  @Override
  public int encodedLength(byte[] val) {
    return val.length;
  }

  @Override
  public Class<byte[]> encodedClass() {
    return byte[].class;
  }

  @Override
  public byte[] decode(PositionedByteRange src) {
    return decode(src, src.getRemaining());
  }

  @Override
  public int encode(PositionedByteRange dst, byte[] val) {
    return encode(dst, val, 0, val.length);
  }

  /**
   * Read a {@code byte[]} from the buffer {@code src}.
   */
  public byte[] decode(PositionedByteRange src, int length) {
    byte[] val = new byte[length];
    src.get(val);
    return val;
  }

  /**
   * Write {@code val} into {@code dst}, respecting {@code voff} and {@code vlen}.
   * @return number of bytes written.
   */
  public int encode(PositionedByteRange dst, byte[] val, int voff, int vlen) {
    Bytes.putBytes(dst.getBytes(), dst.getOffset() + dst.getPosition(), val, voff, vlen);
    dst.setPosition(dst.getPosition() + vlen);
    return vlen;
  }
}
