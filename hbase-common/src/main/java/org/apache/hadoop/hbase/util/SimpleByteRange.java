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
 *  A read only version of the {@link ByteRange}.
 */
@InterfaceAudience.Public
public class SimpleByteRange extends AbstractByteRange {
  public SimpleByteRange() {
  }

  public SimpleByteRange(int capacity) {
    this(new byte[capacity]);
  }

  /**
   * Create a new {@code ByteRange} over the provided {@code bytes}.
   * @param bytes The array to wrap.
   */
  public SimpleByteRange(byte[] bytes) {
    set(bytes);
  }

  /**
   * Create a new {@code ByteRange} over the provided {@code bytes}.
   * @param bytes The array to wrap.
   * @param offset The offset into {@code bytes} considered the beginning
   *          of this range.
   * @param length The length of this range.
   */
  public SimpleByteRange(byte[] bytes, int offset, int length) {
    set(bytes, offset, length);
  }

  //
  // methods for managing the backing array and range viewport
  //

  @Override
  public ByteRange unset() {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public ByteRange set(int capacity) {
    if (super.bytes != null) {
      throw new ReadOnlyByteRangeException();
    }
    return super.set(capacity);
  }

  @Override
  public ByteRange set(byte[] bytes) {
    if (super.bytes != null) {
      throw new ReadOnlyByteRangeException();
    }
    return super.set(bytes);
  }

  @Override
  public ByteRange set(byte[] bytes, int offset, int length) {
    if (super.bytes != null) {
      throw new ReadOnlyByteRangeException();
    }
    return super.set(bytes, offset, length);
  }

  //
  // methods for retrieving data
  //
  @Override
  public ByteRange put(int index, byte val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public ByteRange put(int index, byte[] val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public ByteRange put(int index, byte[] val, int offset, int length) {
    throw new ReadOnlyByteRangeException();
  }

  //
  // methods for duplicating the current instance
  //

  @Override
  public ByteRange shallowCopy() {
    SimpleByteRange clone = new SimpleByteRange(bytes, offset, length);
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  @Override
  public ByteRange shallowCopySubRange(int innerOffset, int copyLength) {
    SimpleByteRange clone = new SimpleByteRange(bytes, offset + innerOffset,
        copyLength);
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  @Override
  public boolean equals(Object thatObject) {
    if (thatObject == null){
      return false;
    }
    if (this == thatObject) {
      return true;
    }
    if (hashCode() != thatObject.hashCode()) {
      return false;
    }
    if (!(thatObject instanceof SimpleByteRange)) {
      return false;
    }
    SimpleByteRange that = (SimpleByteRange) thatObject;
    return Bytes.equals(bytes, offset, length, that.bytes, that.offset, that.length);
  }

  @Override
  public ByteRange deepCopy() {
    SimpleByteRange clone = new SimpleByteRange(deepCopyToNewArray());
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  @Override
  public ByteRange putInt(int index, int val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public ByteRange putLong(int index, long val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public ByteRange putShort(int index, short val) {
    throw new ReadOnlyByteRangeException();
  }

  @Override
  public int putVLong(int index, long val) {
    throw new ReadOnlyByteRangeException();
  }
}
