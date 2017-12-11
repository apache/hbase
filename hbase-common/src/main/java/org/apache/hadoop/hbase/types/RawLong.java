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
 * An {@code DataType} for interacting with values encoded using
 * {@link Bytes#putLong(byte[], int, long)}. Intended to make it easier to
 * transition away from direct use of {@link Bytes}.
 * @see Bytes#putLong(byte[], int, long)
 * @see Bytes#toLong(byte[])
 */
@InterfaceAudience.Public
public class RawLong implements DataType<Long> {

  @Override
  public boolean isOrderPreserving() {
    return false;
  }

  @Override
  public Order getOrder() {
    return null;
  }

  @Override
  public boolean isNullable() {
    return false;
  }

  @Override
  public boolean isSkippable() {
    return true;
  }

  @Override
  public int encodedLength(Long val) {
    return Bytes.SIZEOF_LONG;
  }

  @Override
  public Class<Long> encodedClass() {
    return Long.class;
  }

  @Override
  public int skip(PositionedByteRange src) {
    src.setPosition(src.getPosition() + Bytes.SIZEOF_LONG);
    return Bytes.SIZEOF_LONG;
  }

  @Override
  public Long decode(PositionedByteRange src) {
    long val = Bytes.toLong(src.getBytes(), src.getOffset() + src.getPosition());
    skip(src);
    return val;
  }

  @Override
  public int encode(PositionedByteRange dst, Long val) {
    Bytes.putLong(dst.getBytes(), dst.getOffset() + dst.getPosition(), val);
    return skip(dst);
  }

  /**
   * Read a {@code long} value from the buffer {@code buff}.
   */
  public long decodeLong(byte[] buff, int offset) {
    return Bytes.toLong(buff, offset);
  }

  /**
   * Write instance {@code val} into buffer {@code buff}.
   */
  public int encodeLong(byte[] buff, int offset, long val) {
    return Bytes.putLong(buff, offset, val);
  }
}
