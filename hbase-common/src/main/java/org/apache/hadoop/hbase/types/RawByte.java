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
 * {@link Bytes#putByte(byte[], int, byte)}. Intended to make it easier to
 * transition away from direct use of {@link Bytes}.
 * @see Bytes#putByte(byte[], int, byte)
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RawByte implements DataType<Byte> {

  @Override
  public boolean isOrderPreserving() { return false; }

  @Override
  public Order getOrder() { return null; }

  @Override
  public boolean isNullable() { return false; }

  @Override
  public boolean isSkippable() { return true; }

  @Override
  public int encodedLength(Byte val) { return Bytes.SIZEOF_BYTE; }

  @Override
  public Class<Byte> encodedClass() { return Byte.class; }

  @Override
  public int skip(PositionedByteRange src) {
    src.setPosition(src.getPosition() + Bytes.SIZEOF_BYTE);
    return Bytes.SIZEOF_BYTE;
  }

  @Override
  public Byte decode(PositionedByteRange src) {
    byte val = src.getBytes()[src.getOffset() + src.getPosition()];
    skip(src);
    return val;
  }

  @Override
  public int encode(PositionedByteRange dst, Byte val) {
    Bytes.putByte(dst.getBytes(), dst.getOffset() + dst.getPosition(), val);
    return skip(dst);
  }

  /**
   * Read a {@code byte} value from the buffer {@code buff}.
   */
  public byte decodeByte(byte[] buff, int offset) {
    return buff[offset];
  }

  /**
   * Write instance {@code val} into buffer {@code buff}.
   */
  public int encodeByte(byte[] buff, int offset, byte val) {
    return Bytes.putByte(buff, offset, val);
  }
}
