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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * This Cell is an implementation of {@link ByteBufferedCell} where the data resides in off heap
 * memory.
 */
@InterfaceAudience.Private
public class OffheapKeyValue extends ByteBufferedCell implements HeapSize, Cloneable,
    SettableSequenceId, Streamable {

  protected final ByteBuffer buf;
  protected final int offset;
  protected final int length;
  private final short rowLen;
  private final int keyLen;
  private long seqId = 0;
  private final boolean hasTags;
  // TODO : See if famLen can be cached or not?

  private static final int FIXED_HEAP_SIZE_OVERHEAD = ClassSize.OBJECT + ClassSize.REFERENCE
      + ClassSize.align(ClassSize.BYTE_BUFFER) + (3 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_SHORT
      + Bytes.SIZEOF_BOOLEAN + Bytes.SIZEOF_LONG;

  public OffheapKeyValue(ByteBuffer buf, int offset, int length, boolean hasTags) {
    assert buf.isDirect();
    this.buf = buf;
    this.offset = offset;
    this.length = length;
    rowLen = ByteBufferUtils.toShort(this.buf, this.offset + KeyValue.ROW_OFFSET);
    keyLen = ByteBufferUtils.toInt(this.buf, this.offset);
    this.hasTags = hasTags;
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

  private int getFamilyLengthPosition() {
    return this.offset + KeyValue.ROW_KEY_OFFSET + rowLen;
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
    return this.keyLen - (int) KeyValue.getKeyDataStructureSize(rlength, flength, 0);
  }

  @Override
  public long getTimestamp() {
    int offset = getTimestampOffset(this.keyLen);
    return ByteBufferUtils.toLong(this.buf, offset);
  }

  private int getTimestampOffset(int keyLen) {
    return this.offset + KeyValue.ROW_OFFSET + keyLen - KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  @Override
  public byte getTypeByte() {
    return ByteBufferUtils.toByte(this.buf, this.offset + this.keyLen - 1 + KeyValue.ROW_OFFSET);
  }

  @Override
  public long getSequenceId() {
    return this.seqId;
  }

  public void setSequenceId(long seqId) {
    this.seqId = seqId;
  }

  @Override
  public byte[] getValueArray() {
    return CellUtil.cloneValue(this);
  }

  @Override
  public int getValueOffset() {
    return 0;
  }

  @Override
  public int getValueLength() {
    return ByteBufferUtils.toInt(this.buf, this.offset + Bytes.SIZEOF_INT);
  }

  @Override
  public byte[] getTagsArray() {
    return CellUtil.cloneTags(this);
  }

  @Override
  public int getTagsOffset() {
    return 0;
  }

  @Override
  public int getTagsLength() {
    if(!hasTags) {
      return 0;
    }
    int tagsLen = this.length
        - (this.keyLen + getValueLength() + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE);
    if (tagsLen > 0) {
      // There are some Tag bytes in the byte[]. So reduce 2 bytes which is
      // added to denote the tags
      // length
      tagsLen -= KeyValue.TAGS_LENGTH_SIZE;
    }
    return tagsLen;
  }

  @Override
  public ByteBuffer getRowByteBuffer() {
    return this.buf;
  }

  @Override
  public int getRowPositionInByteBuffer() {
    return this.offset + KeyValue.ROW_KEY_OFFSET;
  }

  @Override
  public ByteBuffer getFamilyByteBuffer() {
    return this.buf;
  }

  @Override
  public int getFamilyPositionInByteBuffer() {
    return getFamilyLengthPosition() + Bytes.SIZEOF_BYTE;
  }

  @Override
  public ByteBuffer getQualifierByteBuffer() {
    return this.buf;
  }

  @Override
  public int getQualifierPositionInByteBuffer() {
    return getFamilyPositionInByteBuffer() + getFamilyLength();
  }

  @Override
  public ByteBuffer getValueByteBuffer() {
    return this.buf;
  }

  @Override
  public int getValuePositionInByteBuffer() {
    return this.offset + KeyValue.ROW_OFFSET + this.keyLen;
  }

  @Override
  public ByteBuffer getTagsByteBuffer() {
    return this.buf;
  }

  @Override
  public int getTagsPositionInByteBuffer() {
    int tagsLen = getTagsLength();
    if (tagsLen == 0) {
      return this.offset + this.length;
    }
    return this.offset + this.length - tagsLen;
  }

  @Override
  public long heapSize() {
    return ClassSize.align(FIXED_HEAP_SIZE_OVERHEAD + ClassSize.align(length));
  }

  @Override
  public int write(OutputStream out) throws IOException {
    return write(out, true);
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    // In KeyValueUtil#oswrite we do a Cell serialization as KeyValue. Any
    // changes doing here, pls check KeyValueUtil#oswrite also and do necessary changes.
    int length = this.length;
    if (hasTags && !withTags) {
      length = keyLen + this.getValueLength() + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
    }
    ByteBufferUtils.putInt(out, length);
    ByteBufferUtils.writeByteBuffer(out, this.buf, this.offset, length);
    return length + Bytes.SIZEOF_INT;
  }

  @Override
  public String toString() {
    return CellUtil.toString(this, true);
  }
}
