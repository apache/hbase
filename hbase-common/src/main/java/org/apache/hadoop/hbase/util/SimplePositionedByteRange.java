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

import java.nio.ByteBuffer;

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
@InterfaceAudience.Public
@InterfaceStability.Evolving
@edu.umd.cs.findbugs.annotations.SuppressWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class SimplePositionedByteRange extends SimpleByteRange implements PositionedByteRange {

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
  private int position = 0;

  /**
   * Create a new {@code PositionedByteRange} lacking a backing array and with
   * an undefined viewport.
   */
  public SimplePositionedByteRange() {
    super();
  }

  /**
   * Create a new {@code PositionedByteRange} over a new backing array of
   * size {@code capacity}. The range's offset and length are 0 and
   * {@code capacity}, respectively.
   * @param capacity the size of the backing array.
   */
  public SimplePositionedByteRange(int capacity) {
    super(capacity);
  }

  /**
   * Create a new {@code PositionedByteRange} over the provided {@code bytes}.
   * @param bytes The array to wrap.
   */
  public SimplePositionedByteRange(byte[] bytes) {
    super(bytes);
  }

  /**
   * Create a new {@code PositionedByteRange} over the provided {@code bytes}.
   * @param bytes The array to wrap.
   * @param offset The offset into {@code bytes} considered the beginning
   *          of this range.
   * @param length The length of this range.
   */
  public SimplePositionedByteRange(byte[] bytes, int offset, int length) {
    super(bytes, offset, length);
  }

  @Override
  public PositionedByteRange unset() {
    this.position = 0;
    super.unset();
    return this;
  }

  @Override
  public PositionedByteRange set(int capacity) {
    this.position = 0;
    super.set(capacity);
    return this;
  }

  @Override
  public PositionedByteRange set(byte[] bytes) {
    this.position = 0;
    super.set(bytes);
    return this;
  }

  @Override
  public PositionedByteRange set(byte[] bytes, int offset, int length) {
    this.position = 0;
    super.set(bytes, offset, length);
    return this;
  }

  /**
   * Update the beginning of this range. {@code offset + length} may not be greater than
   * {@code bytes.length}. Resets {@code position} to 0.
   * @param offset the new start of this range.
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
   * greater than {@code bytes.length}. If {@code position} is greater than
   * the new {@code length}, sets {@code position} to {@code length}.
   * @param length The new length of this range.
   * @return this.
   */
  @Override
  public PositionedByteRange setLength(int length) {
    this.position = Math.min(position, length);
    super.setLength(length);
    return this;
  }

  @Override
  public int getPosition() { return position; }

  @Override
  public PositionedByteRange setPosition(int position) { this.position = position; return this; }

  @Override
  public int getRemaining() { return length - position; }

  @Override
  public byte peek() { return bytes[offset + position]; }

  @Override
  public byte get() { return get(position++); }

  @Override
  public PositionedByteRange get(byte[] dst) {
    if (0 == dst.length) return this;
    return this.get(dst, 0, dst.length); // be clear we're calling self, not super
  }

  @Override
  public PositionedByteRange get(byte[] dst, int offset, int length) {
    if (0 == length) return this;
    super.get(this.position, dst, offset, length);
    this.position += length;
    return this;
  }

  @Override
  public PositionedByteRange put(byte val) {
    put(position++, val);
    return this;
  }

  @Override
  public PositionedByteRange put(byte[] val) {
    if (0 == val.length) return this;
    return this.put(val, 0, val.length);
  }

  @Override
  public PositionedByteRange put(byte[] val, int offset, int length) {
    if (0 == length) return this;
    super.put(position, val, offset, length);
    this.position += length;
    return this;
  }

  /**
   * Similar to {@link ByteBuffer#flip()}. Sets length to position, position
   * to offset.
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
  public PositionedByteRange get(int index, byte[] dst) { super.get(index, dst); return this; }

  @Override
  public PositionedByteRange get(int index, byte[] dst, int offset, int length) {
    super.get(index, dst, offset, length);
    return this;
  }

  @Override
  public PositionedByteRange put(int index, byte val) { super.put(index, val); return this; }

  @Override
  public PositionedByteRange put(int index, byte[] val) { super.put(index, val); return this; }

  @Override
  public PositionedByteRange put(int index, byte[] val, int offset, int length) {
    super.put(index, val, offset, length);
    return this;
  }

  @Override
  public PositionedByteRange deepCopy() {
    SimplePositionedByteRange clone = new SimplePositionedByteRange(deepCopyToNewArray());
    clone.position = this.position;
    return clone;
  }

  @Override
  public PositionedByteRange shallowCopy() {
    SimplePositionedByteRange clone = new SimplePositionedByteRange(bytes, offset, length);
    clone.position = this.position;
    return clone;
  }

  @Override
  public PositionedByteRange shallowCopySubRange(int innerOffset, int copyLength) {
    SimplePositionedByteRange clone =
        new SimplePositionedByteRange(bytes, offset + innerOffset, copyLength);
    clone.position = this.position;
    return clone;
  }
}
