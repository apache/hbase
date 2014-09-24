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

/**
 * A basic {@link ByteRange} implementation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class SimpleByteRange implements ByteRange {

  private static final int UNSET_HASH_VALUE = -1;

  // Note to maintainers: Do not make these final, as the intention is to
  // reuse objects of this class

  /**
   * The array containing the bytes in this range. It will be >= length.
   */
  protected byte[] bytes;

  /**
   * The index of the first byte in this range. {@code ByteRange.get(0)} will
   * return bytes[offset].
   */
  protected int offset;

  /**
   * The number of bytes in the range.  Offset + length must be <= bytes.length
   */
  protected int length;

  /**
   * Variable for lazy-caching the hashCode of this range. Useful for
   * frequently used ranges, long-lived ranges, or long ranges.
   */
  private int hash = UNSET_HASH_VALUE;

  /**
   * Create a new {@code ByteRange} lacking a backing array and with an
   * undefined viewport.
   */
  public SimpleByteRange() {
    unset();
  }

  /**
   * Create a new {@code ByteRange} over a new backing array of size
   * {@code capacity}. The range's offset and length are 0 and {@code capacity},
   * respectively.
   * @param capacity the size of the backing array.
   */
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
  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public ByteRange unset() {
    clearHashCache();
    this.bytes = null;
    this.offset = 0;
    this.length = 0;
    return this;
  }

  @Override
  public ByteRange set(int capacity) {
    return set(new byte[capacity]);
  }

  @Override
  public ByteRange set(byte[] bytes) {
    if (null == bytes) return unset();
    clearHashCache();
    this.bytes = bytes;
    this.offset = 0;
    this.length = bytes.length;
    return this;
  }

  @Override
  public ByteRange set(byte[] bytes, int offset, int length) {
    if (null == bytes) return unset();
    clearHashCache();
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
    return this;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public ByteRange setOffset(int offset) {
    clearHashCache();
    this.offset = offset;
    return this;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public ByteRange setLength(int length) {
    clearHashCache();
    this.length = length;
    return this;
  }

  @Override
  public boolean isEmpty() {
    return isEmpty(this);
  }

  /**
   * @return true when {@code range} is of zero length, false otherwise.
   */
  public static boolean isEmpty(ByteRange range) {
    return range == null || range.getLength() == 0;
  }

  //
  // methods for retrieving data
  //

  @Override
  public byte get(int index) {
    return bytes[offset + index];
  }

  @Override
  public ByteRange get(int index, byte[] dst) {
    if (0 == dst.length) return this;
    return get(index, dst, 0, dst.length);
  }

  @Override
  public ByteRange get(int index, byte[] dst, int offset, int length) {
    if (0 == length) return this;
    System.arraycopy(this.bytes, this.offset + index, dst, offset, length);
    return this;
  }

  @Override
  public ByteRange put(int index, byte val) {
    bytes[offset + index] = val;
    return this;
  }

  @Override
  public ByteRange put(int index, byte[] val) {
    if (0 == val.length) return this;
    return put(index, val, 0, val.length);
  }

  @Override
  public ByteRange put(int index, byte[] val, int offset, int length) {
    if (0 == length) return this;
    System.arraycopy(val, offset, this.bytes, this.offset + index, length);
    return this;
  }

  //
  // methods for duplicating the current instance
  //

  @Override
  public byte[] deepCopyToNewArray() {
    byte[] result = new byte[length];
    System.arraycopy(bytes, offset, result, 0, length);
    return result;
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
  public void deepCopyTo(byte[] destination, int destinationOffset) {
    System.arraycopy(bytes, offset, destination, destinationOffset, length);
  }

  @Override
  public void deepCopySubRangeTo(int innerOffset, int copyLength, byte[] destination,
      int destinationOffset) {
    System.arraycopy(bytes, offset + innerOffset, destination, destinationOffset, copyLength);
  }

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
    SimpleByteRange clone = new SimpleByteRange(bytes, offset + innerOffset, copyLength);
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  //
  // methods used for comparison
  //

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
  public int hashCode() {
    if (isHashCached()) {// hash is already calculated and cached
      return hash;
    }
    if (this.isEmpty()) {// return 0 for empty ByteRange
      hash = 0;
      return hash;
    }
    int off = offset;
    hash = 0;
    for (int i = 0; i < length; i++) {
      hash = 31 * hash + bytes[off++];
    }
    return hash;
  }

  private boolean isHashCached() {
    return hash != UNSET_HASH_VALUE;
  }

  protected void clearHashCache() {
    hash = UNSET_HASH_VALUE;
  }

  /**
   * Bitwise comparison of each byte in the array.  Unsigned comparison, not
   * paying attention to java's signed bytes.
   */
  @Override
  public int compareTo(ByteRange other) {
    return Bytes.compareTo(bytes, offset, length, other.getBytes(), other.getOffset(),
      other.getLength());
  }

  @Override
  public String toString() {
    return Bytes.toStringBinary(bytes, offset, length);
  }
}
