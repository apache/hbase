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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Extends the basic {@link SimpleMutableByteRange} implementation with position
 * support and it is a readonly version. {@code position} is considered
 * transient, not fundamental to the definition of the range, and does not
 * participate in {@link #compareTo(ByteRange)}, {@link #hashCode()}, or
 * {@link #equals(Object)}. {@code Position} is retained by copy operations.
 */
@InterfaceAudience.Public
@edu.umd.cs.findbugs.annotations.SuppressWarnings("EQ_DOESNT_OVERRIDE_EQUALS")
public class SimplePositionedByteRange extends AbstractPositionedByteRange {

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
    this(new byte[capacity]);
  }

  /**
   * Create a new {@code PositionedByteRange} over the provided {@code bytes}.
   * @param bytes The array to wrap.
   */
  public SimplePositionedByteRange(byte[] bytes) {
    set(bytes);
  }

  /**
   * Create a new {@code PositionedByteRange} over the provided {@code bytes}.
   * @param bytes The array to wrap.
   * @param offset The offset into {@code bytes} considered the beginning
   *          of this range.
   * @param length The length of this range.
   */
  public SimplePositionedByteRange(byte[] bytes, int offset, int length) {
    set(bytes, offset, length);
  }

  @Override
  public PositionedByteRange set(int capacity) {
    if (super.bytes != null) {
      throw new ReadOnlyByteRangeException();
    }
    return super.set(capacity);
  }

  @Override
  public PositionedByteRange set(byte[] bytes) {
    if (super.bytes != null) {
      throw new ReadOnlyByteRangeException();
    }
    return super.set(bytes);
  }

  @Override
  public PositionedByteRange set(byte[] bytes, int offset, int length) {
    if (super.bytes != null) {
      throw new ReadOnlyByteRangeException();
    }
    return super.set(bytes, offset, length);
  }

  @Override
  public PositionedByteRange put(byte val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange putShort(short val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange putInt(int val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange putLong(long val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public int putVLong(long val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange put(byte[] val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange put(byte[] val, int offset, int length) {
    throw new ReadOnlyByteRangeException();
  }


  @Override
  public PositionedByteRange get(int index, byte[] dst) { super.get(index, dst); return this; }

  @Override
  public PositionedByteRange get(int index, byte[] dst, int offset, int length) {
    super.get(index, dst, offset, length);
    return this;
  }

  @Override
  public PositionedByteRange put(int index, byte val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange putShort(int index, short val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange putInt(int index, int val) {
    throw new ReadOnlyByteRangeException();
  }
  
  @Override
  public int putVLong(int index, long val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange putLong(int index, long val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange put(int index, byte[] val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public PositionedByteRange put(int index, byte[] val, int offset, int length) {
    throw new ReadOnlyByteRangeException();
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

  @Override
  public PositionedByteRange setLimit(int limit) {
    throw new ReadOnlyByteRangeException();
  }
  
  @Override
  public PositionedByteRange unset() {
    throw new ReadOnlyByteRangeException();
  }

}
