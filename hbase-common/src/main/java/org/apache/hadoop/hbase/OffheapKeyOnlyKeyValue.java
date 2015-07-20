/**
 * Copyright The Apache Software Foundation
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
 * with respect to key serialization but have its data in off heap memory.
 */
@InterfaceAudience.Private
public class OffheapKeyOnlyKeyValue extends ByteBufferedCell {

  private ByteBuffer buf;
  private int offset = 0; // offset into buffer where key starts at
  private int length = 0; // length of this.
  private short rowLen;

  public OffheapKeyOnlyKeyValue(ByteBuffer buf, int offset, int length) {
    assert buf.isDirect();
    this.buf = buf;
    this.offset = offset;
    this.length = length;
    this.rowLen = ByteBufferUtils.toShort(this.buf, this.offset);
  }

  @Override
  public byte[] getRowArray() {
    return CellUtil.cloneRow(this);
  }

  @Override
  public int getRowOffset() {
    return 0;
  }

  @Override
  public short getRowLength() {
    return this.rowLen;
  }

  @Override
  public byte[] getFamilyArray() {
    return CellUtil.cloneFamily(this);
  }

  @Override
  public int getFamilyOffset() {
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
    return CellUtil.cloneQualifier(this);
  }

  @Override
  public int getQualifierOffset() {
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
  public int getRowPositionInByteBuffer() {
    return this.offset + Bytes.SIZEOF_SHORT;
  }

  @Override
  public ByteBuffer getFamilyByteBuffer() {
    return this.buf;
  }

  @Override
  public int getFamilyPositionInByteBuffer() {
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
  public int getQualifierPositionInByteBuffer() {
    int famLenPos = getFamilyLengthPosition();
    return famLenPos + Bytes.SIZEOF_BYTE + getFamilyLength(famLenPos);
  }

  @Override
  public ByteBuffer getValueByteBuffer() {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public int getValuePositionInByteBuffer() {
    return 0;
  }

  @Override
  public ByteBuffer getTagsByteBuffer() {
    throw new IllegalArgumentException("This is a key only Cell");
  }

  @Override
  public int getTagsPositionInByteBuffer() {
    return 0;
  }

  @Override
  public String toString() {
    return CellUtil.toString(this, false);
  }
}
