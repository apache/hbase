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




/**
 * Lightweight, reusable class for specifying ranges of byte[]'s. CompareTo and equals methods are
 * lexicographic, which is native to HBase.
 * <p/>
 * This class differs from ByteBuffer:
 * <li/>On-heap bytes only
 * <li/>Implements equals, hashCode, and compareTo so that it can be used in standard java
 * Collections, similar to String.
 * <li/>Does not maintain mark/position iterator state inside the class. Doing so leads to many bugs
 * in complex applications.
 * <li/>Allows the addition of simple core methods like this.copyTo(that, offset).
 * <li/>Can be reused in tight loops like a major compaction which can save significant amounts of
 * garbage.
 * <li/>(Without reuse, we throw off garbage like this thing:
 * http://www.youtube.com/watch?v=lkmBH-MjZF4
 * <p/>
 * Mutable, and always evaluates equals, hashCode, and compareTo based on the current contents.
 * <p/>
 * Can contain convenience methods for comparing, printing, cloning, spawning new arrays, copying to
 * other arrays, etc. Please place non-core methods into {@link ByteRangeTool}.
 * <p/>
 * We may consider converting this to an interface and creating separate implementations for a
 * single byte[], a paged byte[] (growable byte[][]), a ByteBuffer, etc
 */
public class ByteRange implements Comparable<ByteRange> {

  private static final int UNSET_HASH_VALUE = -1;


  /********************** fields *****************************/

  // Do not make these final, as the intention is to reuse objects of this class

  /**
   * The array containing the bytes in this range.  It will be >= length.
   */
  private byte[] bytes;

  /**
   * The index of the first byte in this range.  ByteRange.get(0) will return bytes[offset].
   */
  private int offset;

  /**
   * The number of bytes in the range.  Offset + length must be <= bytes.length
   */
  private int length;

  /**
   * Variable for lazy-caching the hashCode of this range.  Useful for frequently used ranges,
   * long-lived ranges, or long ranges.
   */
  private int hash = UNSET_HASH_VALUE;


  /********************** construct ***********************/

  public ByteRange() {
    set(new byte[0]);//Could probably get away with a null array if the need arises.
  }

  public ByteRange(byte[] bytes) {
    set(bytes);
  }

  public ByteRange(byte[] bytes, int offset, int length) {
    set(bytes, offset, length);
  }


  /********************** write methods *************************/

  public ByteRange clear() {
    clearHashCache();
    bytes = null;
    offset = 0;
    length = 0;
    return this;
  }

  public ByteRange set(byte[] bytes) {
    clearHashCache();
    this.bytes = bytes;
    this.offset = 0;
    this.length = ArrayUtils.length(bytes);
    return this;
  }

  public ByteRange set(byte[] bytes, int offset, int length) {
    clearHashCache();
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
    return this;
  }

  public void setLength(int length) {
    clearHashCache();
    this.length = length;
  }


  /*********** read methods (add non-core methods to ByteRangeUtils) *************/

  /**
   * @param index zero-based index
   * @return single byte at index
   */
  public byte get(int index) {
    return bytes[offset + index];
  }

  /**
   * Instantiate a new byte[] with exact length, which is at least 24 bytes + length.  Copy the
   * contents of this range into it.
   * @return The newly cloned byte[].
   */
  public byte[] deepCopyToNewArray() {
    byte[] result = new byte[length];
    System.arraycopy(bytes, offset, result, 0, length);
    return result;
  }

  /**
   * Create a new ByteRange with new backing byte[] and copy the state of this range into the new
   * range.  Copy the hash over if it is already calculated.
   * @return 
   */
  public ByteRange deepCopy() {
    ByteRange clone = new ByteRange(deepCopyToNewArray());
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  /**
   * Wrapper for System.arraycopy.  Copy the contents of this range into the provided array.
   * @param destination Copy to this array
   * @param destinationOffset First index in the destination array.
   * @return void to avoid confusion between which ByteRange should be returned
   */
  public void deepCopyTo(byte[] destination, int destinationOffset) {
    System.arraycopy(bytes, offset, destination, destinationOffset, length);
  }

  /**
   * Wrapper for System.arraycopy. Copy the contents of this range into the provided array.
   * @param innerOffset Start copying from this index in this source ByteRange. First byte copied is
   *          bytes[offset + innerOffset]
   * @param copyLength Copy this many bytes
   * @param destination Copy to this array
   * @param destinationOffset First index in the destination array.
   * @return void to avoid confusion between which ByteRange should be returned
   */
  public void deepCopySubRangeTo(int innerOffset, int copyLength, byte[] destination,
      int destinationOffset) {
    System.arraycopy(bytes, offset + innerOffset, destination, destinationOffset, copyLength);
  }

  /**
   * Create a new ByteRange that points at this range's byte[]. The new range can have different
   * values for offset and length, but modifying the shallowCopy will modify the bytes in this
   * range's array. Pass over the hash code if it is already cached.
   * @param innerOffset First byte of clone will be this.offset + copyOffset.
   * @param copyLength Number of bytes in the clone.
   * @return new ByteRange object referencing this range's byte[].
   */
  public ByteRange shallowCopySubRange(int innerOffset, int copyLength) {
    ByteRange clone = new ByteRange(bytes, offset + innerOffset, copyLength);
    if (isHashCached()) {
      clone.hash = hash;
    }
    return clone;
  }

  //TODO move to ByteRangeUtils because it is non-core method
  public int numEqualPrefixBytes(ByteRange that, int thatInnerOffset) {
    int maxCompares = Math.min(length, that.length - thatInnerOffset);
    for (int i = 0; i < maxCompares; ++i) {
      if (bytes[offset + i] != that.bytes[that.offset + thatInnerOffset + i]) {
        return i;
      }
    }
    return maxCompares;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getOffset() {
    return offset;
  }

  public int getLength() {
    return length;
  }

  public boolean isEmpty(){
    return isEmpty(this);
  }

  public boolean notEmpty(){
    return notEmpty(this);
  }


  /******************* static methods ************************/

  public static boolean isEmpty(ByteRange range){
    return range == null || range.length == 0;
  }

  public static boolean notEmpty(ByteRange range){
    return range != null && range.length > 0;
  }

  /******************* standard methods *********************/

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
    if (!(thatObject instanceof ByteRange)) {
      return false;
    }
    ByteRange that = (ByteRange) thatObject;
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

  private void clearHashCache() {
    hash = UNSET_HASH_VALUE;
  }

  /**
   * Bitwise comparison of each byte in the array.  Unsigned comparison, not paying attention to
   * java's signed bytes.
   */
  @Override
  public int compareTo(ByteRange other) {
    return Bytes.compareTo(bytes, offset, length, other.bytes, other.offset, other.length);
  }

  @Override
  public String toString() {
    return Bytes.toStringBinary(bytes, offset, length);
  }

}
