/**
 *
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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is a key only Cell implementation which is identical to {@link KeyValue.KeyOnlyKeyValue}
 * with respect to key serialization but have its data in the form of Byte buffer
 * (onheap and offheap).
 */
@InterfaceAudience.Private
public class ByteBufferKeyOnlyKeyValue extends ByteBufferKeyValue {
  public static final int FIXED_OVERHEAD =
      ByteBufferKeyOnlyKeyValue.FIXED_OVERHEAD + Bytes.SIZEOF_SHORT;
  private short rowLen;

  /**
   * Used in cases where we want to avoid lot of garbage by allocating new objects with different
   * keys. Use the emtpy construtor and set the keys using {@link #setKey(ByteBuffer, int, int)}
   */
  public ByteBufferKeyOnlyKeyValue() {
  }

  public ByteBufferKeyOnlyKeyValue(ByteBuffer buf, int offset, int length) {
    setKey(buf, offset, length);
  }

  /**
   * A setter that helps to avoid object creation every time and whenever there is a need to create
   * new OffheapKeyOnlyKeyValue.
   * @param key
   * @param offset
   * @param length
   */
  public void setKey(ByteBuffer key, int offset, int length) {
    setKey(key, offset, length, ByteBufferUtils.toShort(key, offset));
  }

  /**
   * A setter that helps to avoid object creation every time and whenever
   * there is a need to create new OffheapKeyOnlyKeyValue.
   * @param key - the key part of the cell
   * @param offset - offset of the cell
   * @param length - length of the cell
   * @param rowLen - the rowlen part of the cell
   */
  public void setKey(ByteBuffer key, int offset, int length, short rowLen) {
    this.buf = key;
    this.offset = offset;
    this.length = length;
    this.rowLen = rowLen;
  }

  @Override
  public byte[] getRowArray() {
    if (this.buf.hasArray()) {
      return this.buf.array();
    }
    return CellUtil.cloneRow(this);
  }

  @Override
  public int getRowOffset() {
    if (this.buf.hasArray()) {
      return getRowPosition() + this.buf.arrayOffset();
    }
    return 0;
  }

  @Override
  public short getRowLength() {
    return this.rowLen;
  }

  @Override
  public byte[] getFamilyArray() {
    if (this.buf.hasArray()) {
      return this.buf.array();
    }
    return CellUtil.cloneFamily(this);
  }

  @Override
  public int getFamilyOffset() {
    if (this.buf.hasArray()) {
      return getFamilyPosition() + this.buf.arrayOffset();
    }
    return 0;
  }

  @Override
  public byte getFamilyLength() {
    return getFamilyLength(getFamilyLengthPosition());
  }

  @Override
  public byte getFamilyLength(int famLenPos) {
    return ByteBufferUtils.toByte(this.buf, famLenPos);
  }

  @Override
  public byte[] getQualifierArray() {
    if (this.buf.hasArray()) {
      return this.buf.array();
    }
    return CellUtil.cloneQualifier(this);
  }

  @Override
  public int getQualifierOffset() {
    if (this.buf.hasArray()) {
      return getQualifierPosition() + this.buf.arrayOffset();
    }
    return 0;
  }

  @Override
  public int getQualifierLength() {
    return getQualifierLength(this.length, getRowLength(), getFamilyLength());
  }

  @Override
  public int getQualifierLength(int keyLength, int rlength, int flength) {
    return keyLength - (int) KeyValue.getKeyDataStructureSize(rlength, flength, 0);
  }

  @Override
  public long getTimestamp() {
    return ByteBufferUtils.toLong(this.buf, getTimestampOffset());
  }

  private int getTimestampOffset() {
    return this.offset + this.length - KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  @Override
  public byte getTypeByte() {
    return getTypeByte(this.length);
  }

  @Override
  public void setSequenceId(long seqId) throws IOException {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public void setTimestamp(long ts) throws IOException {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public void setTimestamp(byte[] ts) throws IOException {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public long getSequenceId() {
    return 0;
  }

  @Override
  public byte[] getValueArray() {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public int getValueOffset() {
    return 0;
  }

  @Override
  public int getValueLength() {
    return 0;
  }

  @Override
  public byte[] getTagsArray() {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public int getTagsOffset() {
    return 0;
  }

  @Override
  public int getTagsLength() {
    return 0;
  }

  @Override
  public ByteBuffer getRowByteBuffer() {
    return this.buf;
  }

  @Override
  public int getRowPosition() {
    return this.offset + Bytes.SIZEOF_SHORT;
  }

  @Override
  public ByteBuffer getFamilyByteBuffer() {
    return this.buf;
  }

  @Override
  public int getFamilyPosition() {
    return getFamilyLengthPosition() + Bytes.SIZEOF_BYTE;
  }

  @Override
  public ByteBuffer getQualifierByteBuffer() {
    return this.buf;
  }

  @Override
  public int getQualifierPosition() {
    int famLenPos = getFamilyLengthPosition();
    return famLenPos + Bytes.SIZEOF_BYTE + getFamilyLength(famLenPos);
  }

  @Override
  public ByteBuffer getValueByteBuffer() {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public int getValuePosition() {
    return 0;
  }

  @Override
  public ByteBuffer getTagsByteBuffer() {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public int getTagsPosition() {
    return 0;
  }

  @Override
  public String toString() {
    return CellUtil.toString(this, false);
  }

  @Override
  public Iterator<Tag> getTags() {
    return Collections.emptyIterator();
  }

  @Override
  public Optional<Tag> getTag(byte type) {
    return Optional.empty();
  }

  @Override
  public long heapSize() {
    if (this.buf.hasArray()) {
      return ClassSize.align(FIXED_OVERHEAD + length);
    }
    return ClassSize.align(FIXED_OVERHEAD);
  }

  @Override
  public int getFamilyLengthPosition(int rowLen) {
    return this.offset + Bytes.SIZEOF_SHORT + rowLen;
  }

  @Override
  public int getFamilyInternalPosition(int familyLengthPosition) {
    return familyLengthPosition + Bytes.SIZEOF_BYTE;
  }

  @Override
  public int getQualifierInternalPosition(int famOffset, byte famLength) {
    return famOffset + famLength;
  }

  private int getTimestampOffset(final int keylength) {
    return this.offset + keylength - KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  @Override
  public long getTimestamp(int keyLength) {
    int tsOffset = getTimestampOffset(keyLength);
    return ByteBufferUtils.toLong(this.buf, tsOffset);
  }

  @Override
  public byte getTypeByte(int keyLength) {
    return ByteBufferUtils.toByte(this.buf, this.offset + keyLength - 1);
  }

  @Override
  public int getKeyLength() {
    return this.length;
  }

  @Override
  public boolean equals(Object other) {
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
