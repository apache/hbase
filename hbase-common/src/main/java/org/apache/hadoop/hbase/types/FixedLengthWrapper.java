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
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

/**
 * Wraps an existing {@link DataType} implementation as a fixed-length
 * version of itself. This has the useful side-effect of turning an existing
 * {@link DataType} which is not {@code skippable} into a {@code skippable}
 * variant.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class FixedLengthWrapper<T> implements DataType<T> {

  protected final DataType<T> base;
  protected final int length;

  /**
   * Create a fixed-length version of the {@code wrapped}.
   * @param base the {@link DataType} to restrict to a fixed length.
   * @param length the maximum length (in bytes) for encoded values.
   */
  public FixedLengthWrapper(DataType<T> base, int length) {
    this.base = base;
    this.length = length;
  }

  /**
   * Retrieve the maximum length (in bytes) of encoded values.
   */
  public int getLength() { return length; }

  @Override
  public boolean isOrderPreserving() { return base.isOrderPreserving(); }

  @Override
  public Order getOrder() { return base.getOrder(); }

  @Override
  public boolean isNullable() { return base.isNullable(); }

  @Override
  public boolean isSkippable() { return true; }

  @Override
  public int encodedLength(T val) { return length; }

  @Override
  public Class<T> encodedClass() { return base.encodedClass(); }

  @Override
  public int skip(PositionedByteRange src) {
    src.setPosition(src.getPosition() + this.length);
    return this.length;
  }

  @Override
  public T decode(PositionedByteRange src) {
    if (src.getRemaining() < length) {
      throw new IllegalArgumentException("Not enough buffer remaining. src.offset: "
          + src.getOffset() + " src.length: " + src.getLength() + " src.position: "
          + src.getPosition() + " max length: " + length);
    }
    // create a copy range limited to length bytes. boo.
    PositionedByteRange b = new SimplePositionedByteRange(length);
    src.get(b.getBytes());
    return base.decode(b);
  }

  @Override
  public int encode(PositionedByteRange dst, T val) {
    if (dst.getRemaining() < length) {
      throw new IllegalArgumentException("Not enough buffer remaining. dst.offset: "
          + dst.getOffset() + " dst.length: " + dst.getLength() + " dst.position: "
          + dst.getPosition() + " max length: " + length);
    }
    int written = base.encode(dst, val);
    if (written > length) {
      throw new IllegalArgumentException("Length of encoded value (" + written
          + ") exceeds max length (" + length + ").");
    }
    // TODO: is the zero-padding appropriate?
    for (; written < length; written++) { dst.put((byte) 0x00); }
    return written;
  }
}
