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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.SettableSequenceId;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.prefixtree.decode.DecoderFactory;
import org.apache.hadoop.hbase.codec.prefixtree.decode.PrefixTreeArraySearcher;
import org.apache.hadoop.hbase.codec.prefixtree.scanner.CellScannerPosition;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoder.EncodedSeeker;
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
  public void setCurrentBuffer(ByteBuffer fullBlockBuffer) {
    block = fullBlockBuffer;
    ptSearcher = DecoderFactory.checkOut(block, includeMvccVersion);
    rewind();
  }

  /**
   * Currently unused.
   * <p/>
   * TODO performance leak. should reuse the searchers. hbase does not currently have a hook where
   * this can be called
   */
  public void releaseCurrentSearcher(){
    DecoderFactory.checkIn(ptSearcher);
  }


  @Override
  public ByteBuffer getKeyDeepCopy() {
    return KeyValueUtil.copyKeyToNewByteBuffer(ptSearcher.current());
  }


  @Override
  public ByteBuffer getValueShallowCopy() {
    return CellUtil.getValueBufferShallowCopy(ptSearcher.current());
  }

  /**
   * currently must do deep copy into new array
   */
  @Override
  public ByteBuffer getKeyValueBuffer() {
    return KeyValueUtil.copyToNewByteBuffer(ptSearcher.current());
  }

  /**
   * currently must do deep copy into new array
   */
  @Override
  public Cell getKeyValue() {
    Cell cell = ptSearcher.current();
    if (cell == null) {
      return null;
    }
    return new ClonedPrefixTreeCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(),
        cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(), cell.getTagsArray(),
        cell.getTagsOffset(), cell.getTagsLength(), cell.getTimestamp(), cell.getTypeByte(),
        cell.getSequenceId());
  }

  /**
   * Currently unused.
   * <p/>
   * A nice, lightweight reference, though the underlying cell is transient. This method may return
   * the same reference to the backing PrefixTreeCell repeatedly, while other implementations may
   * return a different reference for each Cell.
   * <p/>
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
   * Support both of these options since the underlying PrefixTree supports both.  Possibly
   * expand the EncodedSeeker to utilize them both.
   */

  protected int seekToOrBeforeUsingPositionAtOrBefore(byte[] keyOnlyBytes, int offset, int length,
      boolean seekBefore){
    // this does a deep copy of the key byte[] because the CellSearcher interface wants a Cell
    KeyValue kv = new KeyValue.KeyOnlyKeyValue(keyOnlyBytes, offset, length);

    return seekToOrBeforeUsingPositionAtOrBefore(kv, seekBefore);
  }

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

  protected int seekToOrBeforeUsingPositionAtOrAfter(byte[] keyOnlyBytes, int offset, int length,
      boolean seekBefore) {
    // this does a deep copy of the key byte[] because the CellSearcher
    // interface wants a Cell
    KeyValue kv = new KeyValue.KeyOnlyKeyValue(keyOnlyBytes, offset, length);
    return seekToOrBeforeUsingPositionAtOrAfter(kv, seekBefore);
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
  public int compareKey(KVComparator comparator, byte[] key, int offset, int length) {
    // can't optimize this, make a copy of the key
    ByteBuffer bb = getKeyDeepCopy();
    return comparator.compareFlatKey(key, offset, length, bb.array(), bb.arrayOffset(), bb.limit());
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
  public int compareKey(KVComparator comparator, Cell key) {
    ByteBuffer bb = getKeyDeepCopy();
    return comparator.compare(key,
        new KeyValue.KeyOnlyKeyValue(bb.array(), bb.arrayOffset(), bb.limit()));
  }
  /**
   * Cloned version of the PrefixTreeCell where except the value part, the rest
   * of the key part is deep copied
   *
   */
  private static class ClonedPrefixTreeCell implements Cell, SettableSequenceId, HeapSize {
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

    public ClonedPrefixTreeCell(byte[] row, int rowOffset, short rowLength, byte[] fam,
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
    @Deprecated
    public long getMvccVersion() {
      return getSequenceId();
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
    @Deprecated
    public byte[] getValue() {
      return this.val;
    }

    @Override
    @Deprecated
    public byte[] getFamily() {
      return this.fam;
    }

    @Override
    @Deprecated
    public byte[] getQualifier() {
      return this.qual;
    }

    @Override
    @Deprecated
    public byte[] getRow() {
      return this.row;
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
}
