/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * This can be used when a Cell has to change with addition/removal of one or more tags. This is an
 * efficient way to do so in which only the tags bytes part need to recreated and copied. All other
 * parts, refer to the original Cell.
 */
@InterfaceAudience.Private
public class TagRewriteCell implements Cell, SettableSequenceId, SettableTimestamp, HeapSize {

  private Cell cell;
  private byte[] tags;

  /**
   * @param cell The original Cell which it rewrites
   * @param tags the tags bytes. The array suppose to contain the tags bytes alone.
   */
  public TagRewriteCell(Cell cell, byte[] tags) {
    assert cell instanceof SettableSequenceId;
    assert cell instanceof SettableTimestamp;
    assert tags != null;
    this.cell = cell;
    this.tags = tags;
    // tag offset will be treated as 0 and length this.tags.length
    if (this.cell instanceof TagRewriteCell) {
      // Cleaning the ref so that the byte[] can be GCed
      ((TagRewriteCell) this.cell).tags = null;
    }
  }

  @Override
  public byte[] getRowArray() {
    return cell.getRowArray();
  }

  @Override
  public int getRowOffset() {
    return cell.getRowOffset();
  }

  @Override
  public short getRowLength() {
    return cell.getRowLength();
  }

  @Override
  public byte[] getFamilyArray() {
    return cell.getFamilyArray();
  }

  @Override
  public int getFamilyOffset() {
    return cell.getFamilyOffset();
  }

  @Override
  public byte getFamilyLength() {
    return cell.getFamilyLength();
  }

  @Override
  public byte[] getQualifierArray() {
    return cell.getQualifierArray();
  }

  @Override
  public int getQualifierOffset() {
    return cell.getQualifierOffset();
  }

  @Override
  public int getQualifierLength() {
    return cell.getQualifierLength();
  }

  @Override
  public long getTimestamp() {
    return cell.getTimestamp();
  }

  @Override
  public byte getTypeByte() {
    return cell.getTypeByte();
  }

  @Override
  @Deprecated
  public long getMvccVersion() {
    return getSequenceId();
  }

  @Override
  public long getSequenceId() {
    return cell.getSequenceId();
  }

  @Override
  public byte[] getValueArray() {
    return cell.getValueArray();
  }

  @Override
  public int getValueOffset() {
    return cell.getValueOffset();
  }

  @Override
  public int getValueLength() {
    return cell.getValueLength();
  }

  @Override
  public byte[] getTagsArray() {
    return this.tags;
  }

  @Override
  public int getTagsOffset() {
    return 0;
  }

  @Override
  public int getTagsLength() {
    if (null == this.tags) {
      // Nulled out tags array optimization in constructor
      return 0;
    }
    return this.tags.length;
  }

  @Override
  @Deprecated
  public byte[] getValue() {
    return cell.getValue();
  }

  @Override
  @Deprecated
  public byte[] getFamily() {
    return cell.getFamily();
  }

  @Override
  @Deprecated
  public byte[] getQualifier() {
    return cell.getQualifier();
  }

  @Override
  @Deprecated
  public byte[] getRow() {
    return cell.getRow();
  }

  @Override
  public long heapSize() {
    long sum = CellUtil.estimatedHeapSizeOf(cell) - cell.getTagsLength();
    sum += ClassSize.OBJECT;// this object itself
    sum += (2 * ClassSize.REFERENCE);// pointers to cell and tags array
    if (this.tags != null) {
      sum += ClassSize.align(ClassSize.ARRAY);// "tags"
      sum += this.tags.length;
    }
    return sum;
  }

  @Override
  public void setTimestamp(long ts) throws IOException {
    // The incoming cell is supposed to be SettableTimestamp type.
    CellUtil.setTimestamp(cell, ts);
  }

  @Override
  public void setTimestamp(byte[] ts, int tsOffset) throws IOException {
    // The incoming cell is supposed to be SettableTimestamp type.
    CellUtil.setTimestamp(cell, ts, tsOffset);
  }

  @Override
  public void setSequenceId(long seqId) throws IOException {
    // The incoming cell is supposed to be SettableSequenceId type.
    CellUtil.setSequenceId(cell, seqId);
  }
}
