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

package org.apache.hadoop.hbase.codec.prefixtree;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.ByteBufferCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.SettableSequenceId;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.decode.DecoderFactory;
import org.apache.hadoop.hbase.codec.prefixtree.decode.PrefixTreeArraySearcher;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder.EncodedSeeker;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * These methods have the same definition as any implementation of the EncodedSeeker.
 *
 * In the future, the EncodedSeeker could be modified to work with the Cell interface directly.  It
 * currently returns a new KeyValue object each time getKeyValue is called.  This is not horrible,
 * but in order to create a new KeyValue object, we must first allocate a new byte[] and copy in
 * the data from the PrefixTreeCell.  It is somewhat heavyweight right now.
 */
@InterfaceAudience.Private
public class PrefixTreeSeeker implements EncodedSeeker {

  protected ByteBuffer block;
  protected boolean includeMvccVersion;
  protected PrefixTreeArraySearcher ptSearcher;

  public PrefixTreeSeeker(boolean includeMvccVersion) {
    this.includeMvccVersion = includeMvccVersion;
  }

  @Override
  public void setCurrentBuffer(ByteBuff fullBlockBuffer) {
    ptSearcher = DecoderFactory.checkOut(fullBlockBuffer, includeMvccVersion);
    rewind();
  }

  /**
   * <p>
   * Currently unused.
   * </p>
   * TODO performance leak. should reuse the searchers. hbase does not currently have a hook where
   * this can be called
   */
  public void releaseCurrentSearcher(){
    DecoderFactory.checkIn(ptSearcher);
  }


  @Override
  public Cell getKey() {
    return ptSearcher.current();
  }


  @Override
  public ByteBuffer getValueShallowCopy() {
    return CellUtil.getValueBufferShallowCopy(ptSearcher.current());
  }

  /**
   * currently must do deep copy into new array
   */
  @Override
  public Cell getCell() {
    // The PrefixTreecell is of type BytebufferedCell and the value part of the cell
    // determines whether we are offheap cell or onheap cell.  All other parts of the cell-
    // row, fam and col are all represented as onheap byte[]
    ByteBufferCell cell = (ByteBufferCell)ptSearcher.current();
    if (cell == null) {
      return null;
    }
    // Use the ByteBuffered cell to see if the Cell is onheap or offheap
    if (cell.getValueByteBuffer().hasArray()) {
      return new OnheapPrefixTreeCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
          cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
          cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), cell.getTagsArray(),
          cell.getTagsOffset(), cell.getTagsLength(), cell.getTimestamp(), cell.getTypeByte(),
          cell.getSequenceId());
    } else {
      return new OffheapPrefixTreeCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
          cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
          cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
          cell.getValueByteBuffer(), cell.getValuePosition(), cell.getValueLength(),
          cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength(), cell.getTimestamp(),
          cell.getTypeByte(), cell.getSequenceId());
    }
  }

  /**
   * <p>
   * Currently unused.
   * </p><p>
   * A nice, lightweight reference, though the underlying cell is transient. This method may return
   * the same reference to the backing PrefixTreeCell repeatedly, while other implementations may
   * return a different reference for each Cell.
   * </p>
   * The goal will be to transition the upper layers of HBase, like Filters and KeyValueHeap, to
   * use this method instead of the getKeyValue() methods above.
   */
  public Cell get() {
    return ptSearcher.current();
  }

  @Override
  public void rewind() {
    ptSearcher.positionAtFirstCell();
  }

  @Override
  public boolean next() {
    return ptSearcher.advance();
  }

  public boolean advance() {
    return ptSearcher.advance();
  }


  private static final boolean USE_POSITION_BEFORE = false;

  /*
   * Support both of these options since the underlying PrefixTree supports
   * both. Possibly expand the EncodedSeeker to utilize them both.
   */

  protected int seekToOrBeforeUsingPositionAtOrBefore(Cell kv, boolean seekBefore) {
    // this does a deep copy of the key byte[] because the CellSearcher
    // interface wants a Cell
    CellScannerPosition position = ptSearcher.seekForwardToOrBefore(kv);

    if (CellScannerPosition.AT == position) {
      if (seekBefore) {
        ptSearcher.previous();
        return 1;
      }
      return 0;
    }

    return 1;
  }

  protected int seekToOrBeforeUsingPositionAtOrAfter(Cell kv, boolean seekBefore) {
    // should probably switch this to use the seekForwardToOrBefore method
    CellScannerPosition position = ptSearcher.seekForwardToOrAfter(kv);

    if (CellScannerPosition.AT == position) {
      if (seekBefore) {
        ptSearcher.previous();
        return 1;
      }
      return 0;

    }

    if (CellScannerPosition.AFTER == position) {
      if (!ptSearcher.isBeforeFirst()) {
        ptSearcher.previous();
      }
      return 1;
    }

    if (position == CellScannerPosition.AFTER_LAST) {
      if (seekBefore) {
        ptSearcher.previous();
      }
      return 1;
    }

    throw new RuntimeException("unexpected CellScannerPosition:" + position);
  }

  @Override
  public int seekToKeyInBlock(Cell key, boolean forceBeforeOnExactMatch) {
    if (USE_POSITION_BEFORE) {
      return seekToOrBeforeUsingPositionAtOrBefore(key, forceBeforeOnExactMatch);
    } else {
      return seekToOrBeforeUsingPositionAtOrAfter(key, forceBeforeOnExactMatch);
    }
  }

  @Override
  public int compareKey(CellComparator comparator, Cell key) {
    return comparator.compare(key,
        ptSearcher.current());
  }

  /**
   * Cloned version of the PrefixTreeCell where except the value part, the rest
   * of the key part is deep copied
   *
   */
  private static class OnheapPrefixTreeCell implements Cell, SettableSequenceId, HeapSize {
    private static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
        + (5 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG) + (4 * Bytes.SIZEOF_INT)
        + (Bytes.SIZEOF_SHORT) + (2 * Bytes.SIZEOF_BYTE) + (5 * ClassSize.ARRAY));
    private byte[] row;
    private short rowLength;
    private byte[] fam;
    private byte famLength;
    private byte[] qual;
    private int qualLength;
    private byte[] val;
    private int valOffset;
    private int valLength;
    private byte[] tag;
    private int tagsLength;
    private long ts;
    private long seqId;
    private byte type;

    public OnheapPrefixTreeCell(byte[] row, int rowOffset, short rowLength, byte[] fam,
        int famOffset, byte famLength, byte[] qual, int qualOffset, int qualLength, byte[] val,
        int valOffset, int valLength, byte[] tag, int tagOffset, int tagLength, long ts, byte type,
        long seqId) {
      this.row = new byte[rowLength];
      System.arraycopy(row, rowOffset, this.row, 0, rowLength);
      this.rowLength = rowLength;
      this.fam = new byte[famLength];
      System.arraycopy(fam, famOffset, this.fam, 0, famLength);
      this.famLength = famLength;
      this.qual = new byte[qualLength];
      System.arraycopy(qual, qualOffset, this.qual, 0, qualLength);
      this.qualLength = qualLength;
      this.tag = new byte[tagLength];
      System.arraycopy(tag, tagOffset, this.tag, 0, tagLength);
      this.tagsLength = tagLength;
      this.val = val;
      this.valLength = valLength;
      this.valOffset = valOffset;
      this.ts = ts;
      this.seqId = seqId;
      this.type = type;
    }

    @Override
    public void setSequenceId(long seqId) {
      this.seqId = seqId;
    }

    @Override
    public byte[] getRowArray() {
      return this.row;
    }

    @Override
    public int getRowOffset() {
      return 0;
    }

    @Override
    public short getRowLength() {
      return this.rowLength;
    }

    @Override
    public byte[] getFamilyArray() {
      return this.fam;
    }

    @Override
    public int getFamilyOffset() {
      return 0;
    }

    @Override
    public byte getFamilyLength() {
      return this.famLength;
    }

    @Override
    public byte[] getQualifierArray() {
      return this.qual;
    }

    @Override
    public int getQualifierOffset() {
      return 0;
    }

    @Override
    public int getQualifierLength() {
      return this.qualLength;
    }

    @Override
    public long getTimestamp() {
      return ts;
    }

    @Override
    public byte getTypeByte() {
      return type;
    }

    @Override
    public long getSequenceId() {
      return seqId;
    }

    @Override
    public byte[] getValueArray() {
      return val;
    }

    @Override
    public int getValueOffset() {
      return this.valOffset;
    }

    @Override
    public int getValueLength() {
      return this.valLength;
    }

    @Override
    public byte[] getTagsArray() {
      return this.tag;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return this.tagsLength;
    }

    @Override
    public String toString() {
      String row = Bytes.toStringBinary(getRowArray(), getRowOffset(), getRowLength());
      String family = Bytes.toStringBinary(getFamilyArray(), getFamilyOffset(), getFamilyLength());
      String qualifier = Bytes.toStringBinary(getQualifierArray(), getQualifierOffset(),
          getQualifierLength());
      String timestamp = String.valueOf((getTimestamp()));
      return row + "/" + family + (family != null && family.length() > 0 ? ":" : "") + qualifier
          + "/" + timestamp + "/" + Type.codeToType(type);
    }

    @Override
    public long heapSize() {
      return FIXED_OVERHEAD + rowLength + famLength + qualLength + valLength + tagsLength;
    }
  }

  private static class OffheapPrefixTreeCell extends ByteBufferCell implements Cell,
      SettableSequenceId, HeapSize {
    private static final long FIXED_OVERHEAD = ClassSize.align(ClassSize.OBJECT
        + (5 * ClassSize.REFERENCE) + (2 * Bytes.SIZEOF_LONG) + (4 * Bytes.SIZEOF_INT)
        + (Bytes.SIZEOF_SHORT) + (2 * Bytes.SIZEOF_BYTE) + (5 * ClassSize.BYTE_BUFFER));
    private ByteBuffer rowBuff;
    private short rowLength;
    private ByteBuffer famBuff;
    private byte famLength;
    private ByteBuffer qualBuff;
    private int qualLength;
    private ByteBuffer val;
    private int valOffset;
    private int valLength;
    private ByteBuffer tagBuff;
    private int tagsLength;
    private long ts;
    private long seqId;
    private byte type;
    public OffheapPrefixTreeCell(byte[] row, int rowOffset, short rowLength, byte[] fam,
        int famOffset, byte famLength, byte[] qual, int qualOffset, int qualLength, ByteBuffer val,
        int valOffset, int valLength, byte[] tag, int tagOffset, int tagLength, long ts, byte type,
        long seqId) {
      byte[] tmpRow = new byte[rowLength];
      System.arraycopy(row, rowOffset, tmpRow, 0, rowLength);
      this.rowBuff = ByteBuffer.wrap(tmpRow);
      this.rowLength = rowLength;
      byte[] tmpFam = new byte[famLength];
      System.arraycopy(fam, famOffset, tmpFam, 0, famLength);
      this.famBuff = ByteBuffer.wrap(tmpFam);
      this.famLength = famLength;
      byte[] tmpQual = new byte[qualLength];
      System.arraycopy(qual, qualOffset, tmpQual, 0, qualLength);
      this.qualBuff = ByteBuffer.wrap(tmpQual);
      this.qualLength = qualLength;
      byte[] tmpTag = new byte[tagLength];
      System.arraycopy(tag, tagOffset, tmpTag, 0, tagLength);
      this.tagBuff = ByteBuffer.wrap(tmpTag);
      this.tagsLength = tagLength;
      this.val = val;
      this.valLength = valLength;
      this.valOffset = valOffset;
      this.ts = ts;
      this.seqId = seqId;
      this.type = type;
    }
    
    @Override
    public void setSequenceId(long seqId) {
      this.seqId = seqId;
    }

    @Override
    public byte[] getRowArray() {
      return this.rowBuff.array();
    }

    @Override
    public int getRowOffset() {
      return getRowPosition();
    }

    @Override
    public short getRowLength() {
      return this.rowLength;
    }

    @Override
    public byte[] getFamilyArray() {
      return this.famBuff.array();
    }

    @Override
    public int getFamilyOffset() {
      return getFamilyPosition();
    }

    @Override
    public byte getFamilyLength() {
      return this.famLength;
    }

    @Override
    public byte[] getQualifierArray() {
      return this.qualBuff.array();
    }

    @Override
    public int getQualifierOffset() {
      return getQualifierPosition();
    }

    @Override
    public int getQualifierLength() {
      return this.qualLength;
    }

    @Override
    public long getTimestamp() {
      return ts;
    }

    @Override
    public byte getTypeByte() {
      return type;
    }

    @Override
    public long getSequenceId() {
      return seqId;
    }

    @Override
    public byte[] getValueArray() {
      byte[] tmpVal = new byte[valLength];
      ByteBufferUtils.copyFromBufferToArray(tmpVal, val, valOffset, 0, valLength);
      return tmpVal;
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return this.valLength;
    }

    @Override
    public byte[] getTagsArray() {
      return this.tagBuff.array();
    }

    @Override
    public int getTagsOffset() {
      return getTagsPosition();
    }

    @Override
    public int getTagsLength() {
      return this.tagsLength;
    }
    
    @Override
    public ByteBuffer getRowByteBuffer() {
      return this.rowBuff;
    }
    
    @Override
    public int getRowPosition() {
      return 0;
    }
    
    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return this.famBuff;
    }
    
    @Override
    public int getFamilyPosition() {
      return 0;
    }
    
    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return this.qualBuff;
    }

    @Override
    public int getQualifierPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      return this.tagBuff;
    }

    @Override
    public int getTagsPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return this.val;
    }

    @Override
    public int getValuePosition() {
      return this.valOffset;
    }

    @Override
    public long heapSize() {
      return FIXED_OVERHEAD + rowLength + famLength + qualLength + valLength + tagsLength;
    }

    @Override
    public String toString() {
      String row = Bytes.toStringBinary(getRowArray(), getRowOffset(), getRowLength());
      String family = Bytes.toStringBinary(getFamilyArray(), getFamilyOffset(), getFamilyLength());
      String qualifier = Bytes.toStringBinary(getQualifierArray(), getQualifierOffset(),
          getQualifierLength());
      String timestamp = String.valueOf((getTimestamp()));
      return row + "/" + family + (family != null && family.length() > 0 ? ":" : "") + qualifier
          + "/" + timestamp + "/" + Type.codeToType(type);
    }
  }
}
