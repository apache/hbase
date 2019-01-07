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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A wrapper for a cell to be used with mapreduce, as the output value class for mappers/reducers.
 */
@InterfaceAudience.Private
public class MapReduceExtendedCell extends ByteBufferExtendedCell {

  private final Cell cell;

  public MapReduceExtendedCell(Cell cell) {
    this.cell = cell;
  }

  @Override
  public byte[] getRowArray() {
    return this.cell.getRowArray();
  }

  @Override
  public int getRowOffset() {
    return this.cell.getRowOffset();
  }

  @Override
  public short getRowLength() {
    return this.cell.getRowLength();
  }

  @Override
  public byte[] getFamilyArray() {
    return this.cell.getFamilyArray();
  }

  @Override
  public int getFamilyOffset() {
    return this.cell.getFamilyOffset();
  }

  @Override
  public byte getFamilyLength() {
    return this.cell.getFamilyLength();
  }

  @Override
  public byte[] getQualifierArray() {
    return this.cell.getQualifierArray();
  }

  @Override
  public int getQualifierOffset() {
    return this.cell.getQualifierOffset();
  }

  @Override
  public int getQualifierLength() {
    return this.cell.getQualifierLength();
  }

  @Override
  public long getTimestamp() {
    return this.cell.getTimestamp();
  }

  @Override
  public byte getTypeByte() {
    return this.cell.getTypeByte();
  }

  @Override
  public long getSequenceId() {
    return this.cell.getSequenceId();
  }

  @Override
  public byte[] getValueArray() {
    return this.cell.getValueArray();
  }

  @Override
  public int getValueOffset() {
    return this.cell.getValueOffset();
  }

  @Override
  public int getValueLength() {
    return this.cell.getValueLength();
  }

  @Override
  public byte[] getTagsArray() {
    return this.cell.getTagsArray();
  }

  @Override
  public int getTagsOffset() {
    return this.cell.getTagsOffset();
  }

  @Override
  public int getTagsLength() {
    return this.cell.getTagsLength();
  }

  @Override
  public ByteBuffer getRowByteBuffer() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getRowByteBuffer();
    } else {
      return ByteBuffer.wrap(CellUtil.cloneRow(this.cell));
    }
  }

  @Override
  public int getRowPosition() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getRowPosition();
    } else {
      return 0;
    }
  }

  @Override
  public ByteBuffer getFamilyByteBuffer() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getFamilyByteBuffer();
    } else {
      return ByteBuffer.wrap(CellUtil.cloneFamily(this.cell));
    }
  }

  @Override
  public int getFamilyPosition() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getFamilyPosition();
    } else {
      return 0;
    }
  }

  @Override
  public ByteBuffer getQualifierByteBuffer() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getQualifierByteBuffer();
    } else {
      return ByteBuffer.wrap(CellUtil.cloneQualifier(this.cell));
    }
  }

  @Override
  public int getQualifierPosition() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getQualifierPosition();
    } else {
      return 0;
    }
  }

  @Override
  public ByteBuffer getValueByteBuffer() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getValueByteBuffer();
    } else {
      return ByteBuffer.wrap(CellUtil.cloneValue(this.cell));
    }
  }

  @Override
  public int getValuePosition() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getValuePosition();
    } else {
      return 0;
    }
  }

  @Override
  public ByteBuffer getTagsByteBuffer() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getTagsByteBuffer();
    } else {
      return ByteBuffer.wrap(CellUtil.cloneTags(this.cell));
    }
  }

  @Override
  public int getTagsPosition() {
    if (cell instanceof ByteBufferExtendedCell) {
      return ((ByteBufferExtendedCell) this.cell).getTagsPosition();
    } else {
      return 0;
    }
  }

  @Override
  public String toString() {
    return this.cell.toString();
  }

  @Override
  public void setSequenceId(long seqId) throws IOException {
    PrivateCellUtil.setSequenceId(cell, seqId);
  }

  @Override
  public void setTimestamp(long ts) throws IOException {
    PrivateCellUtil.setTimestamp(cell, ts);
  }

  @Override
  public void setTimestamp(byte[] ts) throws IOException {
    PrivateCellUtil.setTimestamp(cell, ts);
  }

  @Override
  public long heapSize() {
    return cell.heapSize();
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    return PrivateCellUtil.writeCell(cell, out, withTags);
  }

  @Override
  public int getSerializedSize(boolean withTags) {
    return PrivateCellUtil.estimatedSerializedSizeOf(cell) - Bytes.SIZEOF_INT;
  }

  @Override
  public void write(ByteBuffer buf, int offset) {
    PrivateCellUtil.writeCellToBuffer(cell, buf, offset);
  }

  @Override
  public ExtendedCell deepClone() {
    try {
      return (ExtendedCell) PrivateCellUtil.deepClone(cell);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
  }
}
