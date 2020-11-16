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
import java.util.List;

import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;


import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * This Cell is an implementation of {@link ByteBufferExtendedCell} where the data resides in
 * off heap/ on heap ByteBuffer
 */
@InterfaceAudience.Private
public class ByteBufferKeyValue extends ByteBufferExtendedCell implements ContiguousCellFormat {

  protected ByteBuffer buf;
  protected int offset;
  protected int length;
  private long seqId = 0;

  public static final int FIXED_OVERHEAD = ClassSize.OBJECT + ClassSize.REFERENCE
      + (2 * Bytes.SIZEOF_INT) + Bytes.SIZEOF_LONG;

  /** Writable Constructor -- DO NOT USE */
  public ByteBufferKeyValue() {
  }
  public ByteBufferKeyValue(ByteBuffer buf, int offset, int length, long seqId) {
    this.buf = buf;
    this.offset = offset;
    this.length = length;
    this.seqId = seqId;
  }

  @VisibleForTesting
  public ByteBufferKeyValue(final byte[] row, final int roffset, final int rlength,
      final byte[] family, final int foffset, final int flength, final byte[] qualifier,
      final int qoffset, final int qlength, final long timestamp,
      final org.apache.hadoop.hbase.KeyValue.Type type, final byte[] value, final int voffset,
      final int vlength, final List<Tag> tags) {
    this.buf = createByteBuffer(row, roffset, rlength, family, foffset, flength, qualifier, qoffset,
      qlength, timestamp, type, value, voffset, vlength, tags);
    this.length = this.buf.limit();
    this.offset = 0;
  }

  /**
   * Create offheap backed BBKVs
   * @param qualifier can be a ByteBuffer or a byte[], or null.
   * @param value can be a ByteBuffer or a byte[], or null.
   */
  private static ByteBuffer createByteBuffer(final byte[] row, final int roffset, final int rlength,
      final byte[] family, final int foffset, int flength, final Object qualifier,
      final int qoffset, int qlength, final long timestamp,
      final org.apache.hadoop.hbase.KeyValue.Type type, final Object value, final int voffset,
      int vlength, List<Tag> tags) {

    // Calculate length of tags area
    int tagsLength = 0;
    if (tags != null && !tags.isEmpty()) {
      for (Tag t : tags) {
        tagsLength += t.getValueLength() + Tag.INFRASTRUCTURE_SIZE;
      }
    }
    RawCell.checkForTagsLength(tagsLength);
    // Allocate right-sized byte array.
    int keyLength = (int) KeyValue.getKeyDataStructureSize(rlength, flength, qlength);
    byte[] bytes = new byte[(int) KeyValue.getKeyValueDataStructureSize(rlength, flength, qlength,
      vlength, tagsLength)];
    ByteBuffer bb = ByteBuffer.allocateDirect(bytes.length);
    // Write key, value and key row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keyLength);

    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short) (rlength & 0x0000ffff));
    pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    pos = Bytes.putByte(bytes, pos, (byte) (flength & 0x0000ff));
    if (flength != 0) {
      pos = Bytes.putBytes(bytes, pos, family, foffset, flength);
    }
    if (qlength > 0) {
      if (qualifier instanceof ByteBuffer) {
        pos = Bytes.putByteBuffer(bytes, pos, (ByteBuffer) qualifier);
      } else {
        pos = Bytes.putBytes(bytes, pos, (byte[]) qualifier, qoffset, qlength);
      }
    }
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    if (vlength > 0) {
      if (value instanceof ByteBuffer) {
        pos = Bytes.putByteBuffer(bytes, pos, (ByteBuffer) value);
      } else {
        pos = Bytes.putBytes(bytes, pos, (byte[]) value, voffset, vlength);
      }
    }
    // Add the tags after the value part
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(bytes, pos, tagsLength);
      for (Tag t : tags) {
        int tlen = t.getValueLength();
        pos = Bytes.putAsShort(bytes, pos, tlen + Tag.TYPE_LENGTH_SIZE);
        pos = Bytes.putByte(bytes, pos, t.getType());
        Tag.copyValueTo(t, bytes, pos);
        pos += tlen;
      }
    }
    bb.put(bytes);
    bb.rewind();
    return bb;
  }

  public ByteBufferKeyValue(ByteBuffer buf, int offset, int length) {
    this.buf = buf;
    this.offset = offset;
    this.length = length;
  }



  @VisibleForTesting
  public ByteBuffer getBuffer() {
    return this.buf;
  }

  @VisibleForTesting
  public int getOffset() {
    return this.offset;
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
    return ByteBufferUtils.toShort(this.buf, this.offset + KeyValue.ROW_OFFSET);
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

  int getFamilyLengthPosition() {
    return getFamilyLengthPosition(getRowLength());
  }

  @Override
  public int getFamilyLengthPosition(int rowLength) {
    return this.offset + KeyValue.ROW_KEY_OFFSET + rowLength;
  }

  @Override
  public byte getFamilyLength(int famLenPos) {
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
    return getQualifierLength(getKeyLength(), getRowLength(), getFamilyLength());
  }

  @Override
  public int getQualifierLength(int keyLength, int rlength, int flength) {
    return keyLength - (int) KeyValue.getKeyDataStructureSize(rlength, flength, 0);
  }

  @Override
  public long getTimestamp() {
    return getTimestamp(getKeyLength());
  }

  @Override
  public long getTimestamp(int keyLength) {
    int offset = getTimestampOffset(keyLength);
    return ByteBufferUtils.toLong(this.buf, offset);
  }

  @Override
  public int getKeyLength() {
    return ByteBufferUtils.toInt(this.buf, this.offset);
  }

  private int getTimestampOffset(int keyLen) {
    return this.offset + KeyValue.ROW_OFFSET + keyLen - KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  @Override
  public byte getTypeByte() {
    return getTypeByte(getKeyLength());
  }

  @Override
  public byte getTypeByte(int keyLen) {
    return ByteBufferUtils.toByte(this.buf, this.offset + keyLen - 1 + KeyValue.ROW_OFFSET);
  }

  @Override
  public long getSequenceId() {
    return this.seqId;
  }

  @Override
  public void setSequenceId(long seqId) throws IOException {
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
    int tagsLen = this.length - (getKeyLength() + getValueLength()
        + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE);
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
  public int getRowPosition() {
    return this.offset + KeyValue.ROW_KEY_OFFSET;
  }

  @Override
  public ByteBuffer getFamilyByteBuffer() {
    return this.buf;
  }

  @Override
  public int getFamilyPosition() {
    return getFamilyInternalPosition(getFamilyLengthPosition());
  }

  @Override
  public int getFamilyInternalPosition(int familyLengthPosition) {
    return familyLengthPosition + Bytes.SIZEOF_BYTE;
  }

  @Override
  public ByteBuffer getQualifierByteBuffer() {
    return this.buf;
  }

  @Override
  public int getQualifierPosition() {
    return getQualifierInternalPosition(getFamilyPosition(), getFamilyLength());
  }

  @Override
  public int getQualifierInternalPosition(int familyPosition, byte familyLength) {
    return familyPosition + familyLength;
  }

  @Override
  public ByteBuffer getValueByteBuffer() {
    return this.buf;
  }

  @Override
  public int getValuePosition() {
    return this.offset + KeyValue.ROW_OFFSET + getKeyLength();
  }

  @Override
  public ByteBuffer getTagsByteBuffer() {
    return this.buf;
  }

  @Override
  public int getTagsPosition() {
    int tagsLen = getTagsLength();
    if (tagsLen == 0) {
      return this.offset + this.length;
    }
    return this.offset + this.length - tagsLen;
  }

  @Override
  public long heapSize() {
    if (this.buf.hasArray()) {
      return ClassSize.align(FIXED_OVERHEAD + length);
    }
    return ClassSize.align(FIXED_OVERHEAD) + this.getSerializedSize();
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    int length = getSerializedSize(withTags);
    ByteBufferUtils.copyBufferToStream(out, this.buf, this.offset, length);
    return length;
  }

  @Override
  public int getSerializedSize(boolean withTags) {
    if (withTags) {
      return this.length;
    }
    return getKeyLength() + this.getValueLength() + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE;
  }

  @Override
  public int getSerializedSize() {
    return this.length;
  }

  @Override
  public void write(ByteBuffer buf, int offset) {
    ByteBufferUtils.copyFromBufferToBuffer(this.buf, buf, this.offset, offset, this.length);
  }

  @Override
  public String toString() {
    return CellUtil.toString(this, true);
  }

  @Override
  public void setTimestamp(long ts) throws IOException {
    ByteBufferUtils.copyFromArrayToBuffer(this.buf, this.getTimestampOffset(), Bytes.toBytes(ts), 0,
      Bytes.SIZEOF_LONG);
  }

  private int getTimestampOffset() {
    return this.offset + KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE
        + getKeyLength() - KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  @Override
  public void setTimestamp(byte[] ts) throws IOException {
    ByteBufferUtils.copyFromArrayToBuffer(this.buf, this.getTimestampOffset(), ts, 0,
        Bytes.SIZEOF_LONG);
  }

  @Override
  public ExtendedCell deepClone() {
    byte[] copy = new byte[this.length];
    ByteBufferUtils.copyFromBufferToArray(copy, this.buf, this.offset, 0, this.length);
    KeyValue kv = new KeyValue(copy, 0, copy.length);
    kv.setSequenceId(this.getSequenceId());
    return kv;
  }

  /**
   * Needed doing 'contains' on List. Only compares the key portion, not the value.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Cell)) {
      return false;
    }
    return CellUtil.equals(this, (Cell) other);
  }

  /**
   * In line with {@link #equals(Object)}, only uses the key portion, not the value.
   */
  @Override
  public int hashCode() {
    return calculateHashForKey(this);
  }

  private int calculateHashForKey(ByteBufferExtendedCell cell) {
    int rowHash = ByteBufferUtils.hashCode(cell.getRowByteBuffer(), cell.getRowPosition(),
      cell.getRowLength());
    int familyHash = ByteBufferUtils.hashCode(cell.getFamilyByteBuffer(), cell.getFamilyPosition(),
      cell.getFamilyLength());
    int qualifierHash = ByteBufferUtils.hashCode(cell.getQualifierByteBuffer(),
      cell.getQualifierPosition(), cell.getQualifierLength());

    int hash = 31 * rowHash + familyHash;
    hash = 31 * hash + qualifierHash;
    hash = 31 * hash + (int) cell.getTimestamp();
    hash = 31 * hash + cell.getTypeByte();
    return hash;
  }
}
