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
package org.apache.hadoop.hbase.nio;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ObjectIntPair;
import org.apache.hadoop.io.WritableUtils;

/**
 * An abstract class that abstracts out as to how the byte buffers are used,
 * either single or multiple. We have this interface because the java's ByteBuffers
 * cannot be sub-classed. This class provides APIs similar to the ones provided
 * in java's nio ByteBuffers and allows you to do positional reads/writes and relative
 * reads and writes on the underlying BB. In addition to it, we have some additional APIs which
 * helps us in the read path.
 */
@InterfaceAudience.Private
public abstract class ByteBuff {
  /**
   * @return this ByteBuff's current position
   */
  public abstract int position();

  /**
   * Sets this ByteBuff's position to the given value.
   * @param position
   * @return this object
   */
  public abstract ByteBuff position(int position);

  /**
   * Jumps the current position of this ByteBuff by specified length.
   * @param len the length to be skipped
   */
  public abstract ByteBuff skip(int len);

  /**
   * Jumps back the current position of this ByteBuff by specified length.
   * @param len the length to move back
   */
  public abstract ByteBuff moveBack(int len);

  /**
   * @return the total capacity of this ByteBuff.
   */
  public abstract int capacity();

  /**
   * Returns the limit of this ByteBuff
   * @return limit of the ByteBuff
   */
  public abstract int limit();

  /**
   * Marks the limit of this ByteBuff.
   * @param limit
   * @return This ByteBuff
   */
  public abstract ByteBuff limit(int limit);

  /**
   * Rewinds this ByteBuff and the position is set to 0
   * @return this object
   */
  public abstract ByteBuff rewind();

  /**
   * Marks the current position of the ByteBuff
   * @return this object
   */
  public abstract ByteBuff mark();

  /**
   * Returns bytes from current position till length specified, as a single ByteBuffer. When all
   * these bytes happen to be in a single ByteBuffer, which this object wraps, that ByteBuffer item
   * as such will be returned. So users are warned not to change the position or limit of this
   * returned ByteBuffer. The position of the returned byte buffer is at the begin of the required
   * bytes. When the required bytes happen to span across multiple ByteBuffers, this API will copy
   * the bytes to a newly created ByteBuffer of required size and return that.
   *
   * @param length number of bytes required.
   * @return bytes from current position till length specified, as a single ByteButter.
   */
  public abstract ByteBuffer asSubByteBuffer(int length);

  /**
   * Returns bytes from given offset till length specified, as a single ByteBuffer. When all these
   * bytes happen to be in a single ByteBuffer, which this object wraps, that ByteBuffer item as
   * such will be returned (with offset in this ByteBuffer where the bytes starts). So users are
   * warned not to change the position or limit of this returned ByteBuffer. When the required bytes
   * happen to span across multiple ByteBuffers, this API will copy the bytes to a newly created
   * ByteBuffer of required size and return that.
   *
   * @param offset the offset in this ByteBuff from where the subBuffer should be created
   * @param length the length of the subBuffer
   * @param pair a pair that will have the bytes from the current position till length specified,
   *        as a single ByteBuffer and offset in that Buffer where the bytes starts.
   *        Since this API gets called in a loop we are passing a pair to it which could be created
   *        outside the loop and the method would set the values on the pair that is passed in by
   *        the caller. Thus it avoids more object creations that would happen if the pair that is
   *        returned is created by this method every time.
   */
  public abstract void asSubByteBuffer(int offset, int length, ObjectIntPair<ByteBuffer> pair);

  /**
   * Returns the number of elements between the current position and the
   * limit.
   * @return the remaining elements in this ByteBuff
   */
  public abstract int remaining();

  /**
   * Returns true if there are elements between the current position and the limt
   * @return true if there are elements, false otherwise
   */
  public abstract boolean hasRemaining();

  /**
   * Similar to {@link ByteBuffer}.reset(), ensures that this ByteBuff
   * is reset back to last marked position.
   * @return This ByteBuff
   */
  public abstract ByteBuff reset();

  /**
   * Returns an ByteBuff which is a sliced version of this ByteBuff. The position, limit and mark
   * of the new ByteBuff will be independent than that of the original ByteBuff.
   * The content of the new ByteBuff will start at this ByteBuff's current position
   * @return a sliced ByteBuff
   */
  public abstract ByteBuff slice();

  /**
   * Returns an ByteBuff which is a duplicate version of this ByteBuff. The
   * position, limit and mark of the new ByteBuff will be independent than that
   * of the original ByteBuff. The content of the new ByteBuff will start at
   * this ByteBuff's current position The position, limit and mark of the new
   * ByteBuff would be identical to this ByteBuff in terms of values.
   *
   * @return a sliced ByteBuff
   */
  public abstract ByteBuff duplicate();

  /**
   * A relative method that returns byte at the current position.  Increments the
   * current position by the size of a byte.
   * @return the byte at the current position
   */
  public abstract byte get();

  /**
   * Fetches the byte at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the byte at the given index
   */
  public abstract byte get(int index);

  /**
   * Fetches the byte at the given offset from current position. Does not change position
   * of the underlying ByteBuffers.
   *
   * @param offset
   * @return the byte value at the given index.
   */
  public abstract byte getByteAfterPosition(int offset);

  /**
   * Writes a byte to this ByteBuff at the current position and increments the position
   * @param b
   * @return this object
   */
  public abstract ByteBuff put(byte b);

  /**
   * Writes a byte to this ByteBuff at the given index
   * @param index
   * @param b
   * @return this object
   */
  public abstract ByteBuff put(int index, byte b);

  /**
   * Copies the specified number of bytes from this ByteBuff's current position to
   * the byte[]'s offset. Also advances the position of the ByteBuff by the given length.
   * @param dst
   * @param offset within the current array
   * @param length upto which the bytes to be copied
   */
  public abstract void get(byte[] dst, int offset, int length);

  /**
   * Copies the specified number of bytes from this ByteBuff's given position to
   * the byte[]'s offset. The position of the ByteBuff remains in the current position only
   * @param sourceOffset the offset in this ByteBuff from where the copy should happen
   * @param dst the byte[] to which the ByteBuff's content is to be copied
   * @param offset within the current array
   * @param length upto which the bytes to be copied
   */
  public abstract void get(int sourceOffset, byte[] dst, int offset, int length);

  /**
   * Copies the content from this ByteBuff's current position to the byte array and fills it. Also
   * advances the position of the ByteBuff by the length of the byte[].
   * @param dst
   */
  public abstract void get(byte[] dst);

  /**
   * Copies from the given byte[] to this ByteBuff
   * @param src
   * @param offset the position in the byte array from which the copy should be done
   * @param length the length upto which the copy should happen
   * @return this ByteBuff
   */
  public abstract ByteBuff put(byte[] src, int offset, int length);

  /**
   * Copies from the given byte[] to this ByteBuff
   * @param src
   * @return this ByteBuff
   */
  public abstract ByteBuff put(byte[] src);

  /**
   * @return true or false if the underlying BB support hasArray
   */
  public abstract boolean hasArray();

  /**
   * @return the byte[] if the underlying BB has single BB and hasArray true
   */
  public abstract byte[] array();

  /**
   * @return the arrayOffset of the byte[] incase of a single BB backed ByteBuff
   */
  public abstract int arrayOffset();

  /**
   * Returns the short value at the current position. Also advances the position by the size
   * of short
   *
   * @return the short value at the current position
   */
  public abstract short getShort();

  /**
   * Fetches the short value at the given index. Does not change position of the
   * underlying ByteBuffers. The caller is sure that the index will be after
   * the current position of this ByteBuff. So even if the current short does not fit in the
   * current item we can safely move to the next item and fetch the remaining bytes forming
   * the short
   *
   * @param index
   * @return the short value at the given index
   */
  public abstract short getShort(int index);

  /**
   * Fetches the short value at the given offset from current position. Does not change position
   * of the underlying ByteBuffers.
   *
   * @param offset
   * @return the short value at the given index.
   */
  public abstract short getShortAfterPosition(int offset);

  /**
   * Returns the int value at the current position. Also advances the position by the size of int
   *
   * @return the int value at the current position
   */
  public abstract int getInt();

  /**
   * Writes an int to this ByteBuff at its current position. Also advances the position
   * by size of int
   * @param value Int value to write
   * @return this object
   */
  public abstract ByteBuff putInt(int value);

  /**
   * Fetches the int at the given index. Does not change position of the underlying ByteBuffers.
   * Even if the current int does not fit in the
   * current item we can safely move to the next item and fetch the remaining bytes forming
   * the int
   *
   * @param index
   * @return the int value at the given index
   */
  public abstract int getInt(int index);

  /**
   * Fetches the int value at the given offset from current position. Does not change position
   * of the underlying ByteBuffers.
   *
   * @param offset
   * @return the int value at the given index.
   */
  public abstract int getIntAfterPosition(int offset);

  /**
   * Returns the long value at the current position. Also advances the position by the size of long
   *
   * @return the long value at the current position
   */
  public abstract long getLong();

  /**
   * Writes a long to this ByteBuff at its current position.
   * Also advances the position by size of long
   * @param value Long value to write
   * @return this object
   */
  public abstract ByteBuff putLong(long value);

  /**
   * Fetches the long at the given index. Does not change position of the
   * underlying ByteBuffers. The caller is sure that the index will be after
   * the current position of this ByteBuff. So even if the current long does not fit in the
   * current item we can safely move to the next item and fetch the remaining bytes forming
   * the long
   *
   * @param index
   * @return the long value at the given index
   */
  public abstract long getLong(int index);

  /**
   * Fetches the long value at the given offset from current position. Does not change position
   * of the underlying ByteBuffers.
   *
   * @param offset
   * @return the long value at the given index.
   */
  public abstract long getLongAfterPosition(int offset);

  /**
   * Copy the content from this ByteBuff to a byte[] based on the given offset and
   * length
   *
   * @param offset
   *          the position from where the copy should start
   * @param length
   *          the length upto which the copy has to be done
   * @return byte[] with the copied contents from this ByteBuff.
   */
  public abstract byte[] toBytes(int offset, int length);

  /**
   * Copies the content from this ByteBuff to a ByteBuffer
   * Note : This will advance the position marker of {@code out} but not change the position maker
   * for this ByteBuff
   * @param out the ByteBuffer to which the copy has to happen
   * @param sourceOffset the offset in the ByteBuff from which the elements has
   * to be copied
   * @param length the length in this ByteBuff upto which the elements has to be copied
   */
  public abstract void get(ByteBuffer out, int sourceOffset, int length);

  /**
   * Copies the contents from the src ByteBuff to this ByteBuff. This will be
   * absolute positional copying and
   * won't affect the position of any of the buffers.
   * @param offset the position in this ByteBuff to which the copy should happen
   * @param src the src ByteBuff
   * @param srcOffset the offset in the src ByteBuff from where the elements should be read
   * @param length the length up to which the copy should happen
   */
  public abstract ByteBuff put(int offset, ByteBuff src, int srcOffset, int length);

  // static helper methods
  /**
   * Read integer from ByteBuff coded in 7 bits and increment position.
   * @return Read integer.
   */
  public static int readCompressedInt(ByteBuff buf) {
    byte b = buf.get();
    if ((b & ByteBufferUtils.NEXT_BIT_MASK) != 0) {
      return (b & ByteBufferUtils.VALUE_MASK)
          + (readCompressedInt(buf) << ByteBufferUtils.NEXT_BIT_SHIFT);
    }
    return b & ByteBufferUtils.VALUE_MASK;
  }

  /**
   * Compares two ByteBuffs
   *
   * @param buf1 the first ByteBuff
   * @param o1 the offset in the first ByteBuff from where the compare has to happen
   * @param len1 the length in the first ByteBuff upto which the compare has to happen
   * @param buf2 the second ByteBuff
   * @param o2 the offset in the second ByteBuff from where the compare has to happen
   * @param len2 the length in the second ByteBuff upto which the compare has to happen
   * @return Positive if buf1 is bigger than buf2, 0 if they are equal, and negative if buf1 is
   *         smaller than buf2.
   */
  public static int compareTo(ByteBuff buf1, int o1, int len1, ByteBuff buf2,
      int o2, int len2) {
    if (buf1.hasArray() && buf2.hasArray()) {
      return Bytes.compareTo(buf1.array(), buf1.arrayOffset() + o1, len1, buf2.array(),
          buf2.arrayOffset() + o2, len2);
    }
    int end1 = o1 + len1;
    int end2 = o2 + len2;
    for (int i = o1, j = o2; i < end1 && j < end2; i++, j++) {
      int a = buf1.get(i) & 0xFF;
      int b = buf2.get(j) & 0xFF;
      if (a != b) {
        return a - b;
      }
    }
    return len1 - len2;
  }

  /**
   * Read long which was written to fitInBytes bytes and increment position.
   * @param fitInBytes In how many bytes given long is stored.
   * @return The value of parsed long.
   */
  public static long readLong(ByteBuff in, final int fitInBytes) {
    long tmpLength = 0;
    for (int i = 0; i < fitInBytes; ++i) {
      tmpLength |= (in.get() & 0xffl) << (8l * i);
    }
    return tmpLength;
  }

  /**
   * Similar to {@link WritableUtils#readVLong(DataInput)} but reads from a
   * {@link ByteBuff}.
   */
  public static long readVLong(ByteBuff in) {
    byte firstByte = in.get();
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = in.get();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * Search sorted array "a" for byte "key".
   * 
   * @param a Array to search. Entries must be sorted and unique.
   * @param fromIndex First index inclusive of "a" to include in the search.
   * @param toIndex Last index exclusive of "a" to include in the search.
   * @param key The byte to search for.
   * @return The index of key if found. If not found, return -(index + 1), where
   *         negative indicates "not found" and the "index + 1" handles the "-0"
   *         case.
   */
  public static int unsignedBinarySearch(ByteBuff a, int fromIndex, int toIndex, byte key) {
    int unsignedKey = key & 0xff;
    int low = fromIndex;
    int high = toIndex - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      int midVal = a.get(mid) & 0xff;

      if (midVal < unsignedKey) {
        low = mid + 1;
      } else if (midVal > unsignedKey) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }
    return -(low + 1); // key not found.
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "[pos=" + position() + ", lim=" + limit() +
        ", cap= " + capacity() + "]";
  }

  public static String toStringBinary(final ByteBuff b, int off, int len) {
    StringBuilder result = new StringBuilder();
    // Just in case we are passed a 'len' that is > buffer length...
    if (off >= b.capacity())
      return result.toString();
    if (off + len > b.capacity())
      len = b.capacity() - off;
    for (int i = off; i < off + len; ++i) {
      int ch = b.get(i) & 0xFF;
      if ((ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z')
          || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0) {
        result.append((char) ch);
      } else {
        result.append(String.format("\\x%02X", ch));
      }
    }
    return result.toString();
  }
}
