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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This is a key only Cell implementation which is identical to {@link KeyValue.KeyOnlyKeyValue}
 * with respect to key serialization but have its data in the form of Byte buffer
 * (onheap and offheap).
 */
@InterfaceAudience.Private
public class ByteBufferKeyOnlyKeyValue extends ByteBufferCell {

  private ByteBuffer buf;
  private int offset = 0; // offset into buffer where key starts at
  private int length = 0; // length of this.
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
   * A setter that helps to avoid object creation every time and whenever
   * there is a need to create new OffheapKeyOnlyKeyValue.
   * @param key
   * @param offset
   * @param length
   */
  public void setKey(ByteBuffer key, int offset, int length) {
    this.buf = key;
    this.offset = offset;
    this.length = length;
    this.rowLen = ByteBufferUtils.toShort(this.buf, this.offset);
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

  private byte getFamilyLength(int famLenPos) {
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
    return getQualifierLength(getRowLength(), getFamilyLength());
  }

  private int getQualifierLength(int rlength, int flength) {
    return this.length - (int) KeyValue.getKeyDataStructureSize(rlength, flength, 0);
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
    return ByteBufferUtils.toByte(this.buf, this.offset + this.length - 1);
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

  // The position in BB where the family length is added.
  private int getFamilyLengthPosition() {
    return this.offset + Bytes.SIZEOF_SHORT + getRowLength();
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
}
