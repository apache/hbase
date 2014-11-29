/*
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
package org.apache.hadoop.hbase.util;


import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import com.google.common.annotations.VisibleForTesting;

/**
 * Extends the basic {@link SimpleByteRange} implementation with position
 * support. {@code position} is considered transient, not fundamental to the
 * definition of the range, and does not participate in
 * {@link #compareTo(ByteRange)}, {@link #hashCode()}, or
 * {@link #equals(Object)}. {@code Position} is retained by copy operations.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractPositionedByteRange extends AbstractByteRange implements
    PositionedByteRange {
  /**
   * The current index into the range. Like {@link ByteBuffer} position, it
   * points to the next value that will be read/written in the array. It
   * provides the appearance of being 0-indexed, even though its value is
   * calculated according to offset.
   * <p>
   * Position is considered transient and does not participate in
   * {@link #equals(Object)} or {@link #hashCode()} comparisons.
   * </p>
   */
  protected int position = 0;

  protected int limit = 0;

  @Override
  public abstract PositionedByteRange unset();

  @Override
  public PositionedByteRange set(int capacity) {
    this.position = 0;
    super.set(capacity);
    this.limit = capacity;
    return this;
  }

  @Override
  public PositionedByteRange set(byte[] bytes) {
    this.position = 0;
    super.set(bytes);
    this.limit = bytes.length;
    return this;
  }

  @Override
  public PositionedByteRange set(byte[] bytes, int offset, int length) {
    this.position = 0;
    super.set(bytes, offset, length);
    limit = length;
    return this;
  }

  /**
   * Update the beginning of this range. {@code offset + length} may not be
   * greater than {@code bytes.length}. Resets {@code position} to 0.
   *
   * @param offset
   *          the new start of this range.
   * @return this.
   */
  @Override
  public PositionedByteRange setOffset(int offset) {
    this.position = 0;
    super.setOffset(offset);
    return this;
  }

  /**
   * Update the length of this range. {@code offset + length} should not be
   * greater than {@code bytes.length}. If {@code position} is greater than the
   * new {@code length}, sets {@code position} to {@code length}.
   *
   * @param length
   *          The new length of this range.
   * @return this.
   */
  @Override
  public PositionedByteRange setLength(int length) {
    this.position = Math.min(position, length);
    super.setLength(length);
    return this;
  }

  @Override
  public int getPosition() {
    return position;
  }

  @Override
  public PositionedByteRange setPosition(int position) {
    this.position = position;
    return this;
  }

  @Override
  public int getRemaining() {
    return length - position;
  }

  @Override
  public byte peek() {
    return bytes[offset + position];
  }

  @Override
  public byte get() {
    return get(position++);
  }

  @Override
  public PositionedByteRange get(byte[] dst) {
    if (0 == dst.length)
      return this;
    return this.get(dst, 0, dst.length); // be clear we're calling self, not
                                         // super
  }

  @Override
  public PositionedByteRange get(byte[] dst, int offset, int length) {
    if (0 == length)
      return this;
    super.get(this.position, dst, offset, length);
    this.position += length;
    return this;
  }

  @Override
  public abstract PositionedByteRange put(byte val);

  @Override
  public abstract PositionedByteRange put(byte[] val);

  @Override
  public abstract PositionedByteRange put(byte[] val, int offset, int length);

  @Override
  public abstract PositionedByteRange putInt(int index, int val);

  @Override
  public abstract PositionedByteRange putLong(int index, long val);

  @Override
  public abstract PositionedByteRange putShort(int index, short val);

  @Override
  public abstract PositionedByteRange putInt(int val);

  @Override
  public abstract PositionedByteRange putLong(long val);

  @Override
  public abstract PositionedByteRange putShort(short val);

  @Override
  public abstract int putVLong(int index, long val);

  @Override
  public abstract int putVLong(long val);
  /**
   * Similar to {@link ByteBuffer#flip()}. Sets length to position, position to
   * offset.
   */
  @VisibleForTesting
  PositionedByteRange flip() {
    clearHashCache();
    length = position;
    position = offset;
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#clear()}. Sets position to 0, length to
   * capacity.
   */
  @VisibleForTesting
  PositionedByteRange clear() {
    clearHashCache();
    position = 0;
    length = bytes.length - offset;
    return this;
  }

  // java boilerplate

  @Override
  public PositionedByteRange get(int index, byte[] dst) {
    super.get(index, dst);
    return this;
  }

  @Override
  public PositionedByteRange get(int index, byte[] dst, int offset, int length) {
    super.get(index, dst, offset, length);
    return this;
  }

  @Override
  public short getShort() {
    short s = getShort(position);
    position += Bytes.SIZEOF_SHORT;
    return s;
  }

  @Override
  public int getInt() {
    int i = getInt(position);
    position += Bytes.SIZEOF_INT;
    return i;
  }

  @Override
  public long getLong() {
    long l = getLong(position);
    position += Bytes.SIZEOF_LONG;
    return l;
  }

  @Override
  public long getVLong() {
    long p = getVLong(position);
    position += getVLongSize(p);
    return p;
  }

  @Override
  public abstract PositionedByteRange put(int index, byte val);

  @Override
  public abstract PositionedByteRange put(int index, byte[] val);

  @Override
  public abstract PositionedByteRange put(int index, byte[] val, int offset, int length);

  @Override
  public abstract PositionedByteRange deepCopy();

  @Override
  public abstract PositionedByteRange shallowCopy();

  @Override
  public abstract PositionedByteRange shallowCopySubRange(int innerOffset, int copyLength);

  @Override
  public PositionedByteRange setLimit(int limit) {
    this.limit = limit;
    return this;
  }

  @Override
  public int getLimit() {
    return this.limit;
  }
}
