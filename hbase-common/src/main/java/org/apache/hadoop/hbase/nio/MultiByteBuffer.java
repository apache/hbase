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

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.InvalidMarkException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.WritableUtils;

/**
 * Provides a unified view of all the underlying ByteBuffers and will look as if a bigger
 * sequential buffer. This class provides similar APIs as in {@link ByteBuffer} to put/get int,
 * short, long etc and doing operations like mark, reset, slice etc. This has to be used when
 * data is split across multiple byte buffers and we don't want copy them to single buffer
 * for reading from it.
 */
@InterfaceAudience.Private
public class MultiByteBuffer {

  private final ByteBuffer[] items;
  // Pointer to the current item in the MBB
  private ByteBuffer curItem = null;
  // Index of the current item in the MBB
  private int curItemIndex = 0;
  /**
   * An indicator that helps in short circuiting some of the APIs functionality
   * if the MBB is backed by single item
   */
  private final boolean singleItem;
  private int limit = 0;
  private int limitedItemIndex;
  private int markedItemIndex = -1;
  private final int[] itemBeginPos;

  public MultiByteBuffer(ByteBuffer... items) {
    assert items != null;
    assert items.length > 0;
    this.items = items;
    this.curItem = this.items[this.curItemIndex];
    this.singleItem = items.length == 1;
    // See below optimization in getInt(int) where we check whether the given index land in current
    // item. For this we need to check whether the passed index is less than the next item begin
    // offset. To handle this effectively for the last item buffer, we add an extra item into this
    // array.
    itemBeginPos = new int[items.length + 1];
    int offset = 0;
    for (int i = 0; i < items.length; i++) {
      ByteBuffer item = items[i];
      item.rewind();
      itemBeginPos[i] = offset;
      int l = item.limit() - item.position();
      offset += l;
    }
    this.limit = offset;
    this.itemBeginPos[items.length] = offset + 1;
    this.limitedItemIndex = this.items.length - 1;
  }

  private MultiByteBuffer(ByteBuffer[] items, int[] itemBeginPos, int limit, int limitedIndex,
      int curItemIndex, int markedIndex) {
    this.items = items;
    this.curItemIndex = curItemIndex;
    this.curItem = this.items[this.curItemIndex];
    this.singleItem = items.length == 1;
    this.itemBeginPos = itemBeginPos;
    this.limit = limit;
    this.limitedItemIndex = limitedIndex;
    this.markedItemIndex = markedIndex;
  }

  /**
   * @return the underlying array if this MultiByteBuffer is made up of single on heap ByteBuffer.
   * @throws UnsupportedOperationException - if the MBB is not made up of single item
   * or if the single item is a Direct Byte Buffer
   */
  public byte[] array() {
    if (hasArray()) {
      return this.curItem.array();
    }
    throw new UnsupportedOperationException();
  }

  /**
   * @return the array offset of the item ByteBuffer if the MBB is made up of
   * single on heap ByteBuffer
   * @throws UnsupportedOperationException if the MBB is not made up of single item or
   * the single item is a Direct byte Buffer
   */
  public int arrayOffset() {
    if (hasArray()) {
      return this.curItem.arrayOffset();
    }
    throw new UnsupportedOperationException();
  }

  /**
   * @return true if the MBB is made up of single item and that single item is an
   * on heap Byte Buffer
   */
  public boolean hasArray() {
    return this.singleItem && this.curItem.hasArray();
  }

  /**
   * @return the total capacity of this MultiByteBuffer.
   */
  public int capacity() {
    int c = 0;
    for (ByteBuffer item : this.items) {
      c += item.capacity();
    }
    return c;
  }

  /**
   * Fetches the byte at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the byte at the given index
   */
  public byte get(int index) {
    if (singleItem) {
      return this.curItem.get(index);
    }
    int itemIndex = getItemIndex(index);
    return this.items[itemIndex].get(index - this.itemBeginPos[itemIndex]);
  }

  /*
   * Returns in which sub ByteBuffer, the given element index will be available.
   */
  private int getItemIndex(int elemIndex) {
    int index = 1;
    while (elemIndex > this.itemBeginPos[index]) {
      index++;
      if (index == this.itemBeginPos.length) {
        throw new IndexOutOfBoundsException();
      }
    }
    return index - 1;
  }

  /*
   * Returns in which sub ByteBuffer, the given element index will be available. In this case we are
   * sure that the item will be after MBB's current position
   */
  private int getItemIndexFromCurItemIndex(int elemIndex) {
    int index = this.curItemIndex;
    while (elemIndex < this.itemBeginPos[index]) {
      index++;
      if (index == this.itemBeginPos.length) {
        throw new IndexOutOfBoundsException();
      }
    }
    return index - 1;
  }

  /**
   * Fetches the short at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the short value at the given index
   */
  public short getShort(int index) {
    if (singleItem) {
      return ByteBufferUtils.toShort(curItem, index);
    }
    // Mostly the index specified will land within this current item. Short circuit for that
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex] <= index
        && this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndex(index);
    }
    ByteBuffer item = items[itemIndex];
    int offsetInItem = index - this.itemBeginPos[itemIndex];
    if (item.limit() - offsetInItem >= Bytes.SIZEOF_SHORT) {
      return ByteBufferUtils.toShort(item, offsetInItem);
    }
    if (items.length - 1 == itemIndex) {
      // means cur item is the last one and we wont be able to read a int. Throw exception
      throw new BufferUnderflowException();
    }
    ByteBuffer nextItem = items[itemIndex + 1];
    // Get available one byte from this item and remaining one from next
    short n = 0;
    n ^= item.get(offsetInItem) & 0xFF;
    n <<= 8;
    n ^= nextItem.get(0) & 0xFF;
    return n;
  }

  /**
   * Fetches the int at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the int value at the given index
   */
  public int getInt(int index) {
    if (singleItem) {
      return ByteBufferUtils.toInt(this.curItem, index);
    }
    // Mostly the index specified will land within this current item. Short circuit for that
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex] <= index
        && this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndex(index);
    }
    return getInt(index, itemIndex);
  }

  /**
   * Fetches the int at the given index. Does not change position of the underlying ByteBuffers. The
   * difference for this API from {@link #getInt(int)} is the caller is sure that the index will be
   * after the current position of this MBB.
   *
   * @param index
   * @return the int value at the given index
   */
  public int getIntStrictlyForward(int index) {
    if (singleItem) {
      return ByteBufferUtils.toInt(this.curItem, index);
    }
    // Mostly the index specified will land within this current item. Short circuit for that
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndexFromCurItemIndex(index);
    }
    return getInt(index, itemIndex);
  }

  private int getInt(int index, int itemIndex) {
    ByteBuffer item = items[itemIndex];
    int offsetInItem = index - this.itemBeginPos[itemIndex];
    int remainingLen = item.limit() - offsetInItem;
    if (remainingLen >= Bytes.SIZEOF_INT) {
      return ByteBufferUtils.toInt(item, offsetInItem);
    }
    if (items.length - 1 == itemIndex) {
      // means cur item is the last one and we wont be able to read a int. Throw exception
      throw new BufferUnderflowException();
    }
    ByteBuffer nextItem = items[itemIndex + 1];
    // Get available bytes from this item and remaining from next
    int l = 0;
    for (int i = offsetInItem; i < item.capacity(); i++) {
      l <<= 8;
      l ^= item.get(i) & 0xFF;
    }
    for (int i = 0; i < Bytes.SIZEOF_INT - remainingLen; i++) {
      l <<= 8;
      l ^= nextItem.get(i) & 0xFF;
    }
    return l;
  }

  /**
   * Fetches the long at the given index. Does not change position of the underlying ByteBuffers
   * @param index
   * @return the long value at the given index
   */
  public long getLong(int index) {
    if (singleItem) {
      return this.curItem.getLong(index);
    }
    // Mostly the index specified will land within this current item. Short circuit for that
    int itemIndex;
    if (this.itemBeginPos[this.curItemIndex] <= index
        && this.itemBeginPos[this.curItemIndex + 1] > index) {
      itemIndex = this.curItemIndex;
    } else {
      itemIndex = getItemIndex(index);
    }
    ByteBuffer item = items[itemIndex];
    int offsetInItem = index - this.itemBeginPos[itemIndex];
    int remainingLen = item.limit() - offsetInItem;
    if (remainingLen >= Bytes.SIZEOF_LONG) {
      return ByteBufferUtils.toLong(item, offsetInItem);
    }
    if (items.length - 1 == itemIndex) {
      // means cur item is the last one and we wont be able to read a long. Throw exception
      throw new BufferUnderflowException();
    }
    ByteBuffer nextItem = items[itemIndex + 1];
    // Get available bytes from this item and remaining from next
    long l = 0;
    for (int i = offsetInItem; i < item.capacity(); i++) {
      l <<= 8;
      l ^= item.get(i) & 0xFF;
    }
    for (int i = 0; i < Bytes.SIZEOF_LONG - remainingLen; i++) {
      l <<= 8;
      l ^= nextItem.get(i) & 0xFF;
    }
    return l;
  }

  /**
   * @return this MBB's current position
   */
  public int position() {
    if (this.singleItem) return this.curItem.position();
    return itemBeginPos[this.curItemIndex] + this.curItem.position();
  }

  /**
   * Sets this MBB's position to the given value.
   * @param position
   * @return this object
   */
  public MultiByteBuffer position(int position) {
    if (this.singleItem) {
      this.curItem.position(position);
      return this;
    }
    // Short circuit for positioning within the cur item. Mostly that is the case.
    if (this.itemBeginPos[this.curItemIndex] <= position
        && this.itemBeginPos[this.curItemIndex + 1] > position) {
      this.curItem.position(position - this.itemBeginPos[this.curItemIndex]);
      return this;
    }
    int itemIndex = getItemIndex(position);
    // All items from 0 - curItem-1 set position at end.
    for (int i = 0; i < itemIndex; i++) {
      this.items[i].position(this.items[i].limit());
    }
    // All items after curItem set position at begin
    for (int i = itemIndex + 1; i < this.items.length; i++) {
      this.items[i].position(0);
    }
    this.curItem = this.items[itemIndex];
    this.curItem.position(position - this.itemBeginPos[itemIndex]);
    this.curItemIndex = itemIndex;
    return this;
  }

  /**
   * Rewinds this MBB and the position is set to 0
   * @return this object
   */
  public MultiByteBuffer rewind() {
    for (int i = 0; i < this.items.length; i++) {
      this.items[i].rewind();
    }
    this.curItemIndex = 0;
    this.curItem = this.items[this.curItemIndex];
    this.markedItemIndex = -1;
    return this;
  }

  /**
   * Marks the current position of the MBB
   * @return this object
   */
  public MultiByteBuffer mark() {
    this.markedItemIndex = this.curItemIndex;
    this.curItem.mark();
    return this;
  }

  /**
   * Similar to {@link ByteBuffer}.reset(), ensures that this MBB
   * is reset back to last marked position.
   * @return This MBB
   */
  public MultiByteBuffer reset() {
    // when the buffer is moved to the next one.. the reset should happen on the previous marked
    // item and the new one should be taken as the base
    if (this.markedItemIndex < 0) throw new InvalidMarkException();
    ByteBuffer markedItem = this.items[this.markedItemIndex];
    markedItem.reset();
    this.curItem = markedItem;
    // All items after the marked position upto the current item should be reset to 0
    for (int i = this.curItemIndex; i > this.markedItemIndex; i--) {
      this.items[i].position(0);
    }
    this.curItemIndex = this.markedItemIndex;
    return this;
  }

  /**
   * Returns the number of elements between the current position and the
   * limit. </p>
   * @return the remaining elements in this MBB
   */
  public int remaining() {
    int remain = 0;
    for (int i = curItemIndex; i < items.length; i++) {
      remain += items[i].remaining();
    }
    return remain;
  }

  /**
   * Returns true if there are elements between the current position and the limt
   * @return true if there are elements, false otherwise
   */
  public final boolean hasRemaining() {
    return this.curItem.hasRemaining() || this.curItemIndex < this.items.length - 1;
  }

  /**
   * A relative method that returns byte at the current position.  Increments the
   * current position by the size of a byte.
   * @return the byte at the current position
   */
  public byte get() {
    if (!singleItem && this.curItem.remaining() == 0) {
      if (items.length - 1 == this.curItemIndex) {
        // means cur item is the last one and we wont be able to read a long. Throw exception
        throw new BufferUnderflowException();
      }
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
    }
    return this.curItem.get();
  }

  /**
   * Returns the short value at the current position. Also advances the position by the size
   * of short
   *
   * @return the short value at the current position
   */
  public short getShort() {
    if (singleItem) {
      return this.curItem.getShort();
    }
    int remaining = this.curItem.remaining();
    if (remaining >= Bytes.SIZEOF_SHORT) {
      return this.curItem.getShort();
    }
    if (remaining == 0) {
      if (items.length - 1 == this.curItemIndex) {
        // means cur item is the last one and we wont be able to read a long. Throw exception
        throw new BufferUnderflowException();
      }
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
      return this.curItem.getShort();
    }
    short n = 0;
    n ^= get() & 0xFF;
    n <<= 8;
    n ^= get() & 0xFF;
    return n;
  }

  /**
   * Returns the int value at the current position. Also advances the position by the size of int
   *
   * @return the int value at the current position
   */
  public int getInt() {
    if (singleItem) {
      return this.curItem.getInt();
    }
    int remaining = this.curItem.remaining();
    if (remaining >= Bytes.SIZEOF_INT) {
      return this.curItem.getInt();
    }
    if (remaining == 0) {
      if (items.length - 1 == this.curItemIndex) {
        // means cur item is the last one and we wont be able to read a long. Throw exception
        throw new BufferUnderflowException();
      }
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
      return this.curItem.getInt();
    }
    // Get available bytes from this item and remaining from next
    int n = 0;
    for (int i = 0; i < Bytes.SIZEOF_INT; i++) {
      n <<= 8;
      n ^= get() & 0xFF;
    }
    return n;
  }


  /**
   * Returns the long value at the current position. Also advances the position by the size of long
   *
   * @return the long value at the current position
   */
  public long getLong() {
    if (singleItem) {
      return this.curItem.getLong();
    }
    int remaining = this.curItem.remaining();
    if (remaining >= Bytes.SIZEOF_LONG) {
      return this.curItem.getLong();
    }
    if (remaining == 0) {
      if (items.length - 1 == this.curItemIndex) {
        // means cur item is the last one and we wont be able to read a long. Throw exception
        throw new BufferUnderflowException();
      }
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
      return this.curItem.getLong();
    }
    // Get available bytes from this item and remaining from next
    long l = 0;
    for (int i = 0; i < Bytes.SIZEOF_LONG; i++) {
      l <<= 8;
      l ^= get() & 0xFF;
    }
    return l;
  }

  /**
   * Returns the long value, stored as variable long at the current position of this
   * MultiByteBuffer. Also advances it's position accordingly.
   * This is similar to {@link WritableUtils#readVLong(DataInput)} but reads from a
   * {@link MultiByteBuffer}
   *
   * @return the long value at the current position
   */
  public long getVLong() {
    byte firstByte = get();
    int len = WritableUtils.decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    byte b;
    for (int idx = 0; idx < len - 1; idx++) {
      b = get();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * Copies the content from this MBB's current position to the byte array and fills it. Also
   * advances the position of the MBB by the length of the byte[].
   * @param dst
   * @return this object
   */
  public MultiByteBuffer get(byte[] dst) {
    return get(dst, 0, dst.length);
  }

  /**
   * Copies the specified number of bytes from this MBB's current position to the byte[]'s offset.
   * Also advances the position of the MBB by the given length.
   * @param dst
   * @param offset within the current array
   * @param length upto which the bytes to be copied
   * @return this object
   */
  public MultiByteBuffer get(byte[] dst, int offset, int length) {
    if (this.singleItem) {
      this.curItem.get(dst, offset, length);
    } else {
      while (length > 0) {
        int toRead = Math.min(length, this.curItem.remaining());
        this.curItem.get(dst, offset, toRead);
        length -= toRead;
        if (length == 0)
          break;
        this.curItemIndex++;
        this.curItem = this.items[this.curItemIndex];
        offset += toRead;
      }
    }
    return this;
  }

  /**
   * Marks the limit of this MBB.
   * @param limit
   * @return This MBB
   */
  public MultiByteBuffer limit(int limit) {
    this.limit = limit;
    if (singleItem) {
      this.curItem.limit(limit);
      return this;
    }
    // Normally the limit will try to limit within the last BB item
    int limitedIndexBegin = this.itemBeginPos[this.limitedItemIndex];
    if (limit >= limitedIndexBegin && limit < this.itemBeginPos[this.limitedItemIndex + 1]) {
      this.items[this.limitedItemIndex].limit(limit - limitedIndexBegin);
      return this;
    }
    int itemIndex = getItemIndex(limit);
    int beginOffset = this.itemBeginPos[itemIndex];
    int offsetInItem = limit - beginOffset;
    ByteBuffer item = items[itemIndex];
    item.limit(offsetInItem);
    for (int i = this.limitedItemIndex; i < itemIndex; i++) {
      this.items[i].limit(this.items[i].capacity());
    }
    this.limitedItemIndex = itemIndex;
    for (int i = itemIndex + 1; i < this.items.length; i++) {
      this.items[i].limit(this.items[i].position());
    }
    return this;
  }

  /**
   * Returns the limit of this MBB
   * @return limit of the MBB
   */
  public int limit() {
    return this.limit;
  }

  /**
   * Returns an MBB which is a sliced version of this MBB. The position, limit and mark
   * of the new MBB will be independent than that of the original MBB.
   * The content of the new MBB will start at this MBB's current position
   * @return a sliced MBB
   */
  public MultiByteBuffer slice() {
    if (this.singleItem) {
      return new MultiByteBuffer(curItem.slice());
    }
    ByteBuffer[] copy = new ByteBuffer[this.limitedItemIndex - this.curItemIndex + 1];
    for (int i = curItemIndex, j = 0; i <= this.limitedItemIndex; i++, j++) {
      copy[j] = this.items[i].slice();
    }
    return new MultiByteBuffer(copy);
  }

  /**
   * Returns an MBB which is a duplicate version of this MBB. The position, limit and mark
   * of the new MBB will be independent than that of the original MBB.
   * The content of the new MBB will start at this MBB's current position
   * The position, limit and mark of the new MBB would be identical to this MBB in terms of
   * values.
   * @return a sliced MBB
   */
  public MultiByteBuffer duplicate() {
    if (this.singleItem) {
      return new MultiByteBuffer(new ByteBuffer[] { curItem.duplicate() }, this.itemBeginPos,
          this.limit, this.limitedItemIndex, this.curItemIndex, this.markedItemIndex);
    }
    ByteBuffer[] itemsCopy = new ByteBuffer[this.items.length];
    for (int i = 0; i < this.items.length; i++) {
      itemsCopy[i] = items[i].duplicate();
    }
    return new MultiByteBuffer(itemsCopy, this.itemBeginPos, this.limit, this.limitedItemIndex,
        this.curItemIndex, this.markedItemIndex);
  }

  /**
   * Writes a byte to this MBB at the current position and increments the position
   * @param b
   * @return this object
   */
  public MultiByteBuffer put(byte b) {
    if (!singleItem && this.curItem.remaining() == 0) {
      if (this.curItemIndex == this.items.length - 1) {
        throw new BufferOverflowException();
      }
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
    }
    this.curItem.put(b);
    return this;
  }

  /**
   * Writes a byte to this MBB at the given index
   * @param index
   * @param b
   * @return this object
   */
  public MultiByteBuffer put(int index, byte b) {
    if (this.singleItem) {
      this.curItem.put(index, b);
      return this;
    }
    int itemIndex = getItemIndex(limit);
    ByteBuffer item = items[itemIndex];
    item.put(index - itemBeginPos[itemIndex], b);
    return this;
  }

  /**
   * Copies from a src MBB to this MBB.
   * @param offset the position in this MBB to which the copy should happen
   * @param src the src MBB
   * @param srcOffset the offset in the src MBB from where the elements should be read
   * @param length the length upto which the copy should happen
   */
  public void put(int offset, MultiByteBuffer src, int srcOffset, int length) {
    if (src.hasArray() && this.hasArray()) {
      System.arraycopy(src.array(), srcOffset + src.arrayOffset(), this.array(), this.arrayOffset()
          + offset, length);
    } else {
      int destItemIndex = getItemIndex(offset);
      int srcItemIndex = getItemIndex(srcOffset);
      ByteBuffer destItem = this.items[destItemIndex];
      offset = offset - this.itemBeginPos[destItemIndex];

      ByteBuffer srcItem = src.items[srcItemIndex];
      srcOffset = srcOffset - this.itemBeginPos[srcItemIndex];
      int toRead, toWrite, toMove;
      while (length > 0) {
        toWrite = destItem.limit() - offset;
        toRead = srcItem.limit() - srcOffset;
        toMove = Math.min(length, Math.min(toRead, toWrite));
        ByteBufferUtils.copyFromBufferToBuffer(destItem, srcItem, srcOffset, offset, toMove);
        length -= toMove;
        if (length == 0) break;
        if (toRead < toWrite) {
          srcItem = src.items[++srcItemIndex];
          srcOffset = 0;
          offset += toMove;
        } else if (toRead > toWrite) {
          destItem = this.items[++destItemIndex];
          offset = 0;
          srcOffset += toMove;
        } else {
          // toRead = toWrite case
          srcItem = src.items[++srcItemIndex];
          srcOffset = 0;
          destItem = this.items[++destItemIndex];
          offset = 0;
        }
      }
    }
  }

  /**
   * Writes an int to this MBB at its current position. Also advances the position by size of int
   * @param val Int value to write
   * @return this object
   */
  public MultiByteBuffer putInt(int val) {
    if (singleItem || this.curItem.remaining() >= Bytes.SIZEOF_INT) {
      this.curItem.putInt(val);
      return this;
    }
    if (this.curItemIndex == this.items.length - 1) {
      throw new BufferOverflowException();
    }
    // During read, we will read as byte by byte for this case. So just write in Big endian
    put(int3(val));
    put(int2(val));
    put(int1(val));
    put(int0(val));
    return this;
  }

  private static byte int3(int x) {
    return (byte) (x >> 24);
  }

  private static byte int2(int x) {
    return (byte) (x >> 16);
  }

  private static byte int1(int x) {
    return (byte) (x >> 8);
  }

  private static byte int0(int x) {
    return (byte) (x);
  }

  /**
   * Copies from the given byte[] to this MBB
   * @param src
   * @return this MBB
   */
  public final MultiByteBuffer put(byte[] src) {
    return put(src, 0, src.length);
  }

  /**
   * Copies from the given byte[] to this MBB
   * @param src
   * @param offset the position in the byte array from which the copy should be done
   * @param length the length upto which the copy should happen
   * @return this MBB
   */
  public MultiByteBuffer put(byte[] src, int offset, int length) {
    if (singleItem || this.curItem.remaining() >= length) {
      ByteBufferUtils.copyFromArrayToBuffer(this.curItem, src, offset, length);
      return this;
    }
    int end = offset + length;
    for (int i = offset; i < end; i++) {
      this.put(src[i]);
    }
    return this;
  }


  /**
   * Writes a long to this MBB at its current position. Also advances the position by size of long
   * @param val Long value to write
   * @return this object
   */
  public MultiByteBuffer putLong(long val) {
    if (singleItem || this.curItem.remaining() >= Bytes.SIZEOF_LONG) {
      this.curItem.putLong(val);
      return this;
    }
    if (this.curItemIndex == this.items.length - 1) {
      throw new BufferOverflowException();
    }
    // During read, we will read as byte by byte for this case. So just write in Big endian
    put(long7(val));
    put(long6(val));
    put(long5(val));
    put(long4(val));
    put(long3(val));
    put(long2(val));
    put(long1(val));
    put(long0(val));
    return this;
  }

  private static byte long7(long x) {
    return (byte) (x >> 56);
  }

  private static byte long6(long x) {
    return (byte) (x >> 48);
  }

  private static byte long5(long x) {
    return (byte) (x >> 40);
  }

  private static byte long4(long x) {
    return (byte) (x >> 32);
  }

  private static byte long3(long x) {
    return (byte) (x >> 24);
  }

  private static byte long2(long x) {
    return (byte) (x >> 16);
  }

  private static byte long1(long x) {
    return (byte) (x >> 8);
  }

  private static byte long0(long x) {
    return (byte) (x);
  }

  /**
   * Jumps the current position of this MBB by specified length.
   * @param length
   */
  public void skip(int length) {
    if (this.singleItem) {
      this.curItem.position(this.curItem.position() + length);
      return;
    }
    // Get available bytes from this item and remaining from next
    int jump = 0;
    while (true) {
      jump = this.curItem.remaining();
      if (jump >= length) {
        this.curItem.position(this.curItem.position() + length);
        break;
      }
      this.curItem.position(this.curItem.position() + jump);
      length -= jump;
      this.curItemIndex++;
      this.curItem = this.items[this.curItemIndex];
    }
  }

  /**
   * Jumps back the current position of this MBB by specified length.
   * @param length
   */
  public void moveBack(int length) {
    if (this.singleItem) {
      this.curItem.position(curItem.position() - length);
      return;
    }
    while (length != 0) {
      if (length > curItem.position()) {
        length -= curItem.position();
        this.curItem.position(0);
        this.curItemIndex--;
        this.curItem = this.items[curItemIndex];
      } else {
        this.curItem.position(curItem.position() - length);
        break;
      }
    }
  }

 /**
   * Returns bytes from current position till length specified, as a single ByteButter. When all
   * these bytes happen to be in a single ByteBuffer, which this object wraps, that ByteBuffer item
   * as such will be returned. So users are warned not to change the position or limit of this
   * returned ByteBuffer. The position of the returned byte buffer is at the begin of the required
   * bytes. When the required bytes happen to span across multiple ByteBuffers, this API will copy
   * the bytes to a newly created ByteBuffer of required size and return that.
   *
   * @param length number of bytes required.
   * @return bytes from current position till length specified, as a single ByteButter.
   */
  public ByteBuffer asSubBuffer(int length) {
    if (this.singleItem || this.curItem.remaining() >= length) {
      return this.curItem;
    }
    int offset = 0;
    byte[] dupB = new byte[length];
    int locCurItemIndex = curItemIndex;
    ByteBuffer locCurItem = curItem;
    while (length > 0) {
      int toRead = Math.min(length, locCurItem.remaining());
      ByteBufferUtils
          .copyFromBufferToArray(dupB, locCurItem, locCurItem.position(), offset, toRead);
      length -= toRead;
      if (length == 0)
        break;
      locCurItemIndex++;
      locCurItem = this.items[locCurItemIndex];
      offset += toRead;
    }
    return ByteBuffer.wrap(dupB);
  }

  /**
   * Returns bytes from given offset till length specified, as a single ByteButter. When all these
   * bytes happen to be in a single ByteBuffer, which this object wraps, that ByteBuffer item as
   * such will be returned (with offset in this ByteBuffer where the bytes starts). So users are
   * warned not to change the position or limit of this returned ByteBuffer. When the required bytes
   * happen to span across multiple ByteBuffers, this API will copy the bytes to a newly created
   * ByteBuffer of required size and return that.
   *
   * @param offset the offset in this MBB from where the subBuffer should be created
   * @param length the length of the subBuffer
   * @return a pair of bytes from current position till length specified, as a single ByteButter and
   *         offset in that Buffer where the bytes starts.
   */
  public Pair<ByteBuffer, Integer> asSubBuffer(int offset, int length) {
    if (this.singleItem) {
      return new Pair<ByteBuffer, Integer>(this.curItem, offset);
    }
    if (this.itemBeginPos[this.curItemIndex] <= offset) {
      int relOffsetInCurItem = offset - this.itemBeginPos[this.curItemIndex];
      if (this.curItem.limit() - relOffsetInCurItem >= length) {
        return new Pair<ByteBuffer, Integer>(this.curItem, relOffsetInCurItem);
      }
    }
    int itemIndex = getItemIndex(offset);
    ByteBuffer item = this.items[itemIndex];
    offset = offset - this.itemBeginPos[itemIndex];
    if (item.limit() - offset >= length) {
      return new Pair<ByteBuffer, Integer>(item, offset);
    }
    byte[] dst = new byte[length];
    int destOffset = 0;
    while (length > 0) {
      int toRead = Math.min(length, item.limit() - offset);
      ByteBufferUtils.copyFromBufferToArray(dst, item, offset, destOffset, toRead);
      length -= toRead;
      if (length == 0) break;
      itemIndex++;
      item = this.items[itemIndex];
      destOffset += toRead;
      offset = 0;
    }
    return new Pair<ByteBuffer, Integer>(ByteBuffer.wrap(dst), 0);
  }

  /**
   * Compares two MBBs
   *
   * @param buf1 the first MBB
   * @param o1 the offset in the first MBB from where the compare has to happen
   * @param len1 the length in the first MBB upto which the compare has to happen
   * @param buf2 the second MBB
   * @param o2 the offset in the second MBB from where the compare has to happen
   * @param len2 the length in the second MBB upto which the compare has to happen
   * @return Positive if buf1 is bigger than buf2, 0 if they are equal, and negative if buf1 is
   *         smaller than buf2.
   */
  public static int compareTo(MultiByteBuffer buf1, int o1, int len1, MultiByteBuffer buf2, int o2,
      int len2) {
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

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof MultiByteBuffer)) return false;
    if (this == obj) return true;
    MultiByteBuffer that = (MultiByteBuffer) obj;
    if (this.capacity() != that.capacity()) return false;
    if (compareTo(this, 0, this.capacity(), that, 0, this.capacity()) == 0) return true;
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 0;
    for (ByteBuffer b : this.items) {
      hash += b.hashCode();
    }
    return hash;
  }
}