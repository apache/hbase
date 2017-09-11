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

package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.Tag.TAG_LENGTH_SIZE;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience.Private;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.util.Dictionary;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

/**
 * Utility methods helpful slinging {@link Cell} instances.
 * Some methods below are for internal use only and are marked InterfaceAudience.Private at the
 * method level.
 */
@InterfaceAudience.Public
public final class CellUtil {

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private CellUtil(){}

  /******************* ByteRange *******************************/

  public static ByteRange fillRowRange(Cell cell, ByteRange range) {
    return range.set(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  public static ByteRange fillFamilyRange(Cell cell, ByteRange range) {
    return range.set(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
  }

  public static ByteRange fillQualifierRange(Cell cell, ByteRange range) {
    return range.set(cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength());
  }

  public static ByteRange fillValueRange(Cell cell, ByteRange range) {
    return range.set(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  public static ByteRange fillTagRange(Cell cell, ByteRange range) {
    return range.set(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
  }

  /***************** get individual arrays for tests ************/

  public static byte[] cloneRow(Cell cell){
    byte[] output = new byte[cell.getRowLength()];
    copyRowTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneFamily(Cell cell){
    byte[] output = new byte[cell.getFamilyLength()];
    copyFamilyTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneQualifier(Cell cell){
    byte[] output = new byte[cell.getQualifierLength()];
    copyQualifierTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneValue(Cell cell){
    byte[] output = new byte[cell.getValueLength()];
    copyValueTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneTags(Cell cell) {
    byte[] output = new byte[cell.getTagsLength()];
    copyTagTo(cell, output, 0);
    return output;
  }

  /**
   * Returns tag value in a new byte array. If server-side, use
   * {@link Tag#getValueArray()} with appropriate {@link Tag#getValueOffset()} and
   * {@link Tag#getValueLength()} instead to save on allocations.
   * @param cell
   * @return tag value in a new byte array.
   */
  public static byte[] getTagArray(Cell cell){
    byte[] output = new byte[cell.getTagsLength()];
    copyTagTo(cell, output, 0);
    return output;
  }


  /******************** copyTo **********************************/

  public static int copyRowTo(Cell cell, byte[] destination, int destinationOffset) {
    short rowLen = cell.getRowLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferCell) cell).getRowByteBuffer(),
          ((ByteBufferCell) cell).getRowPosition(), destinationOffset, rowLen);
    } else {
      System.arraycopy(cell.getRowArray(), cell.getRowOffset(), destination, destinationOffset,
          rowLen);
    }
    return destinationOffset + rowLen;
  }

  public static int copyRowTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    short rowLen = cell.getRowLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferCell) cell).getRowByteBuffer(),
          destination, ((ByteBufferCell) cell).getRowPosition(), destinationOffset, rowLen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getRowArray(),
          cell.getRowOffset(), rowLen);
    }
    return destinationOffset + rowLen;
  }

  /**
   * Copies the row to a new byte[]
   * @param cell the cell from which row has to copied
   * @return the byte[] containing the row
   */
  public static byte[] copyRow(Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return ByteBufferUtils.copyOfRange(((ByteBufferCell) cell).getRowByteBuffer(),
        ((ByteBufferCell) cell).getRowPosition(),
        ((ByteBufferCell) cell).getRowPosition() + cell.getRowLength());
    } else {
      return Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowOffset() + cell.getRowLength());
    }
  }

  public static int copyFamilyTo(Cell cell, byte[] destination, int destinationOffset) {
    byte fLen = cell.getFamilyLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferCell) cell).getFamilyByteBuffer(),
          ((ByteBufferCell) cell).getFamilyPosition(), destinationOffset, fLen);
    } else {
      System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(), destination,
          destinationOffset, fLen);
    }
    return destinationOffset + fLen;
  }

  public static int copyFamilyTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    byte fLen = cell.getFamilyLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferCell) cell).getFamilyByteBuffer(),
          destination, ((ByteBufferCell) cell).getFamilyPosition(), destinationOffset, fLen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getFamilyArray(),
          cell.getFamilyOffset(), fLen);
    }
    return destinationOffset + fLen;
  }

  public static int copyQualifierTo(Cell cell, byte[] destination, int destinationOffset) {
    int qlen = cell.getQualifierLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferCell) cell).getQualifierByteBuffer(),
          ((ByteBufferCell) cell).getQualifierPosition(), destinationOffset, qlen);
    } else {
      System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), destination,
          destinationOffset, qlen);
    }
    return destinationOffset + qlen;
  }

  public static int copyQualifierTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    int qlen = cell.getQualifierLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferCell) cell).getQualifierByteBuffer(),
          destination, ((ByteBufferCell) cell).getQualifierPosition(), destinationOffset, qlen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset,
          cell.getQualifierArray(), cell.getQualifierOffset(), qlen);
    }
    return destinationOffset + qlen;
  }

  public static int copyValueTo(Cell cell, byte[] destination, int destinationOffset) {
    int vlen = cell.getValueLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferCell) cell).getValueByteBuffer(),
          ((ByteBufferCell) cell).getValuePosition(), destinationOffset, vlen);
    } else {
      System.arraycopy(cell.getValueArray(), cell.getValueOffset(), destination, destinationOffset,
          vlen);
    }
    return destinationOffset + vlen;
  }

  public static int copyValueTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    int vlen = cell.getValueLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferCell) cell).getValueByteBuffer(),
          destination, ((ByteBufferCell) cell).getValuePosition(), destinationOffset, vlen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getValueArray(),
          cell.getValueOffset(), vlen);
    }
    return destinationOffset + vlen;
  }

  /**
   * Copies the tags info into the tag portion of the cell
   * @param cell
   * @param destination
   * @param destinationOffset
   * @return position after tags
   */
  public static int copyTagTo(Cell cell, byte[] destination, int destinationOffset) {
    int tlen = cell.getTagsLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferCell) cell).getTagsByteBuffer(),
          ((ByteBufferCell) cell).getTagsPosition(), destinationOffset, tlen);
    } else {
      System.arraycopy(cell.getTagsArray(), cell.getTagsOffset(), destination, destinationOffset,
          tlen);
    }
    return destinationOffset + tlen;
  }

  public static int copyTagTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    int tlen = cell.getTagsLength();
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferCell) cell).getTagsByteBuffer(),
          destination, ((ByteBufferCell) cell).getTagsPosition(), destinationOffset, tlen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getTagsArray(),
          cell.getTagsOffset(), tlen);
    }
    return destinationOffset + tlen;
  }

  /********************* misc *************************************/

  @Private
  public static byte getRowByte(Cell cell, int index) {
    if (cell instanceof ByteBufferCell) {
      return ((ByteBufferCell) cell).getRowByteBuffer().get(
          ((ByteBufferCell) cell).getRowPosition() + index);
    }
    return cell.getRowArray()[cell.getRowOffset() + index];
  }

  @Private
  public static byte getQualifierByte(Cell cell, int index) {
    if (cell instanceof ByteBufferCell) {
      return ((ByteBufferCell) cell).getQualifierByteBuffer().get(
          ((ByteBufferCell) cell).getQualifierPosition() + index);
    }
    return cell.getQualifierArray()[cell.getQualifierOffset() + index];
  }

  public static ByteBuffer getValueBufferShallowCopy(Cell cell) {
    ByteBuffer buffer = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(),
      cell.getValueLength());
    return buffer;
  }

  /**
   * @param cell
   * @return cell's qualifier wrapped into a ByteBuffer.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static ByteBuffer getQualifierBufferShallowCopy(Cell cell) {
    // No usage of this in code.
    ByteBuffer buffer = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
    return buffer;
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link CellBuilder} instead
   */
  @Deprecated
  public static Cell createCell(final byte [] row, final byte [] family, final byte [] qualifier,
      final long timestamp, final byte type, final byte [] value) {
    return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier)
            .setTimestamp(timestamp)
            .setType(type)
            .setValue(value)
            .build();
  }

  /**
   * Creates a cell with deep copy of all passed bytes.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link CellBuilder} instead
   */
  @Deprecated
  public static Cell createCell(final byte [] rowArray, final int rowOffset, final int rowLength,
      final byte [] familyArray, final int familyOffset, final int familyLength,
      final byte [] qualifierArray, final int qualifierOffset, final int qualifierLength) {
    // See createCell(final byte [] row, final byte [] value) for why we default Maximum type.
    return CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(rowArray, rowOffset, rowLength)
            .setFamily(familyArray, familyOffset, familyLength)
            .setQualifier(qualifierArray, qualifierOffset, qualifierLength)
            .setTimestamp(HConstants.LATEST_TIMESTAMP)
            .setType(KeyValue.Type.Maximum.getCode())
            .setValue(HConstants.EMPTY_BYTE_ARRAY, 0, HConstants.EMPTY_BYTE_ARRAY.length)
            .build();
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with a memstoreTS/mvcc is an internal implementation detail not for
   * public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link ExtendedCellBuilder} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, final byte type, final byte[] value, final long memstoreTS) {
    return createCell(row, family, qualifier, timestamp, type, value, null, memstoreTS);
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with tags and a memstoreTS/mvcc is an internal implementation detail not for
   * public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link ExtendedCellBuilder} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, final byte type, final byte[] value, byte[] tags,
      final long memstoreTS) {
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setRow(row)
            .setFamily(family)
            .setQualifier(qualifier)
            .setTimestamp(timestamp)
            .setType(type)
            .setValue(value)
            .setTags(tags)
            .setSequenceId(memstoreTS)
            .build();
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with tags is an internal implementation detail not for
   * public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link ExtendedCellBuilder} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, Type type, final byte[] value, byte[] tags) {
    return createCell(row, family, qualifier, timestamp, type.getCode(), value,
            tags, 0);
  }

  /**
   * Create a Cell with specific row.  Other fields defaulted.
   * @param row
   * @return Cell with passed row but all other fields are arbitrary
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link CellBuilder} instead
   */
  @Deprecated
  public static Cell createCell(final byte [] row) {
    return createCell(row, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Create a Cell with specific row and value.  Other fields are defaulted.
   * @param row
   * @param value
   * @return Cell with passed row and value but all other fields are arbitrary
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link CellBuilder} instead
   */
  @Deprecated
  public static Cell createCell(final byte [] row, final byte [] value) {
    // An empty family + empty qualifier + Type.Minimum is used as flag to indicate last on row.
    // See the CellComparator and KeyValue comparator.  Search for compareWithoutRow.
    // Lets not make a last-on-row key as default but at same time, if you are making a key
    // without specifying type, etc., flag it as weird by setting type to be Maximum.
    return createCell(row, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
      HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), value);
  }

  /**
   * Create a Cell with specific row.  Other fields defaulted.
   * @param row
   * @param family
   * @param qualifier
   * @return Cell with passed row but all other fields are arbitrary
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link CellBuilder} instead
   */
  @Deprecated
  public static Cell createCell(final byte [] row, final byte [] family, final byte [] qualifier) {
    // See above in createCell(final byte [] row, final byte [] value) why we set type to Maximum.
    return createCell(row, family, qualifier,
        HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * @return A new cell which is having the extra tags also added to it.
   */
  public static Cell createCell(Cell cell, List<Tag> tags) {
    return createCell(cell, TagUtil.fromList(tags));
  }

  /**
   * @return A new cell which is having the extra tags also added to it.
   */
  public static Cell createCell(Cell cell, byte[] tags) {
    if (cell instanceof ByteBufferCell) {
      return new TagRewriteByteBufferCell((ByteBufferCell) cell, tags);
    }
    return new TagRewriteCell(cell, tags);
  }

  public static Cell createCell(Cell cell, byte[] value, byte[] tags) {
    if (cell instanceof ByteBufferCell) {
      return new ValueAndTagRewriteByteBufferCell((ByteBufferCell) cell, value, tags);
    }
    return new ValueAndTagRewriteCell(cell, value, tags);
  }

  /**
   * This can be used when a Cell has to change with addition/removal of one or more tags. This is an
   * efficient way to do so in which only the tags bytes part need to recreated and copied. All other
   * parts, refer to the original Cell.
   */
  @InterfaceAudience.Private
  private static class TagRewriteCell implements ExtendedCell {
    protected Cell cell;
    protected byte[] tags;
    private static final long HEAP_SIZE_OVERHEAD = ClassSize.OBJECT + 2 * ClassSize.REFERENCE;

    /**
     * @param cell The original Cell which it rewrites
     * @param tags the tags bytes. The array suppose to contain the tags bytes alone.
     */
    public TagRewriteCell(Cell cell, byte[] tags) {
      assert cell instanceof ExtendedCell;
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
    public long heapSize() {
      long sum = HEAP_SIZE_OVERHEAD + CellUtil.estimatedHeapSizeOf(cell);
      if (this.tags != null) {
        sum += ClassSize.sizeOf(this.tags);
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

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      int len = ((ExtendedCell) this.cell).write(out, false);
      if (withTags && this.tags != null) {
        // Write the tagsLength 2 bytes
        out.write((byte) (0xff & (this.tags.length >> 8)));
        out.write((byte) (0xff & this.tags.length));
        out.write(this.tags);
        len += KeyValue.TAGS_LENGTH_SIZE + this.tags.length;
      }
      return len;
    }

    @Override
    public int getSerializedSize(boolean withTags) {
      int len = ((ExtendedCell) this.cell).getSerializedSize(false);
      if (withTags && this.tags != null) {
        len += KeyValue.TAGS_LENGTH_SIZE + this.tags.length;
      }
      return len;
    }

    @Override
    public void write(ByteBuffer buf, int offset) {
      offset = KeyValueUtil.appendTo(this.cell, buf, offset, false);
      int tagsLen = this.tags == null ? 0 : this.tags.length;
      if (tagsLen > 0) {
        offset = ByteBufferUtils.putAsShort(buf, offset, tagsLen);
        ByteBufferUtils.copyFromArrayToBuffer(buf, offset, this.tags, 0, tagsLen);
      }
    }

    @Override
    public Cell deepClone() {
      Cell clonedBaseCell = ((ExtendedCell) this.cell).deepClone();
      return new TagRewriteCell(clonedBaseCell, this.tags);
    }
  }

  @InterfaceAudience.Private
  private static class TagRewriteByteBufferCell extends ByteBufferCell implements ExtendedCell {

    protected ByteBufferCell cell;
    protected byte[] tags;
    private static final long HEAP_SIZE_OVERHEAD = ClassSize.OBJECT + 2 * ClassSize.REFERENCE;

    /**
     * @param cell The original ByteBufferCell which it rewrites
     * @param tags the tags bytes. The array suppose to contain the tags bytes alone.
     */
    public TagRewriteByteBufferCell(ByteBufferCell cell, byte[] tags) {
      assert cell instanceof ExtendedCell;
      assert tags != null;
      this.cell = cell;
      this.tags = tags;
      // tag offset will be treated as 0 and length this.tags.length
      if (this.cell instanceof TagRewriteByteBufferCell) {
        // Cleaning the ref so that the byte[] can be GCed
        ((TagRewriteByteBufferCell) this.cell).tags = null;
      }
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
    public void setSequenceId(long seqId) throws IOException {
      CellUtil.setSequenceId(this.cell, seqId);
    }

    @Override
    public void setTimestamp(long ts) throws IOException {
      CellUtil.setTimestamp(this.cell, ts);
    }

    @Override
    public void setTimestamp(byte[] ts, int tsOffset) throws IOException {
      CellUtil.setTimestamp(this.cell, ts, tsOffset);
    }

    @Override
    public long heapSize() {
      long sum = HEAP_SIZE_OVERHEAD + CellUtil.estimatedHeapSizeOf(cell);
      // this.tags is on heap byte[]
      if (this.tags != null) {
        sum += ClassSize.sizeOf(this.tags);
      }
      return sum;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      int len = ((ExtendedCell) this.cell).write(out, false);
      if (withTags && this.tags != null) {
        // Write the tagsLength 2 bytes
        out.write((byte) (0xff & (this.tags.length >> 8)));
        out.write((byte) (0xff & this.tags.length));
        out.write(this.tags);
        len += KeyValue.TAGS_LENGTH_SIZE + this.tags.length;
      }
      return len;
    }

    @Override
    public int getSerializedSize(boolean withTags) {
      int len = ((ExtendedCell) this.cell).getSerializedSize(false);
      if (withTags && this.tags != null) {
        len += KeyValue.TAGS_LENGTH_SIZE + this.tags.length;
      }
      return len;
    }

    @Override
    public void write(ByteBuffer buf, int offset) {
      offset = KeyValueUtil.appendTo(this.cell, buf, offset, false);
      int tagsLen = this.tags == null ? 0 : this.tags.length;
      if (tagsLen > 0) {
        offset = ByteBufferUtils.putAsShort(buf, offset, tagsLen);
        ByteBufferUtils.copyFromArrayToBuffer(buf, offset, this.tags, 0, tagsLen);
      }
    }

    @Override
    public Cell deepClone() {
      Cell clonedBaseCell = ((ExtendedCell) this.cell).deepClone();
      if (clonedBaseCell instanceof ByteBufferCell) {
        return new TagRewriteByteBufferCell((ByteBufferCell) clonedBaseCell, this.tags);
      }
      return new TagRewriteCell(clonedBaseCell, this.tags);
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return this.cell.getRowByteBuffer();
    }

    @Override
    public int getRowPosition() {
      return this.cell.getRowPosition();
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return this.cell.getFamilyByteBuffer();
    }

    @Override
    public int getFamilyPosition() {
      return this.cell.getFamilyPosition();
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return this.cell.getQualifierByteBuffer();
    }

    @Override
    public int getQualifierPosition() {
      return this.cell.getQualifierPosition();
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return this.cell.getValueByteBuffer();
    }

    @Override
    public int getValuePosition() {
      return this.cell.getValuePosition();
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      return this.tags == null ? HConstants.EMPTY_BYTE_BUFFER : ByteBuffer.wrap(this.tags);
    }

    @Override
    public int getTagsPosition() {
      return 0;
    }
  }

  @InterfaceAudience.Private
  private static class ValueAndTagRewriteCell extends TagRewriteCell {

    protected byte[] value;

    public ValueAndTagRewriteCell(Cell cell, byte[] value, byte[] tags) {
      super(cell, tags);
      this.value = value;
    }

    @Override
    public byte[] getValueArray() {
      return this.value;
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return this.value == null ? 0 : this.value.length;
    }

    @Override
    public long heapSize() {
      long sum = ClassSize.REFERENCE + super.heapSize();
      if (this.value != null) {
        sum += ClassSize.sizeOf(this.value);
      }
      return sum;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      return write(out, withTags, this.cell, this.value, this.tags);
    }

    // Made into a static method so as to reuse the logic within ValueAndTagRewriteByteBufferCell
    static int write(OutputStream out, boolean withTags, Cell cell, byte[] value, byte[] tags)
        throws IOException {
      int valLen = value == null ? 0 : value.length;
      ByteBufferUtils.putInt(out, KeyValueUtil.keyLength(cell));// Key length
      ByteBufferUtils.putInt(out, valLen);// Value length
      int len = 2 * Bytes.SIZEOF_INT;
      len += CellUtil.writeFlatKey(cell, out);// Key
      if (valLen > 0) out.write(value);// Value
      len += valLen;
      if (withTags && tags != null) {
        // Write the tagsLength 2 bytes
        out.write((byte) (0xff & (tags.length >> 8)));
        out.write((byte) (0xff & tags.length));
        out.write(tags);
        len += KeyValue.TAGS_LENGTH_SIZE + tags.length;
      }
      return len;
    }

    @Override
    public int getSerializedSize(boolean withTags) {
      return super.getSerializedSize(withTags) - this.cell.getValueLength() + this.value.length;
    }

    @Override
    public void write(ByteBuffer buf, int offset) {
      write(buf, offset, this.cell, this.value, this.tags);
    }

    // Made into a static method so as to reuse the logic within ValueAndTagRewriteByteBufferCell
    static void write(ByteBuffer buf, int offset, Cell cell, byte[] value, byte[] tags) {
      offset = ByteBufferUtils.putInt(buf, offset, KeyValueUtil.keyLength(cell));// Key length
      offset = ByteBufferUtils.putInt(buf, offset, value.length);// Value length
      offset = KeyValueUtil.appendKeyTo(cell, buf, offset);
      ByteBufferUtils.copyFromArrayToBuffer(buf, offset, value, 0, value.length);
      offset += value.length;
      int tagsLen = tags == null ? 0 : tags.length;
      if (tagsLen > 0) {
        offset = ByteBufferUtils.putAsShort(buf, offset, tagsLen);
        ByteBufferUtils.copyFromArrayToBuffer(buf, offset, tags, 0, tagsLen);
      }
    }

    @Override
    public Cell deepClone() {
      Cell clonedBaseCell = ((ExtendedCell) this.cell).deepClone();
      return new ValueAndTagRewriteCell(clonedBaseCell, this.value, this.tags);
    }
  }

  @InterfaceAudience.Private
  private static class ValueAndTagRewriteByteBufferCell extends TagRewriteByteBufferCell {

    protected byte[] value;

    public ValueAndTagRewriteByteBufferCell(ByteBufferCell cell, byte[] value, byte[] tags) {
      super(cell, tags);
      this.value = value;
    }

    @Override
    public byte[] getValueArray() {
      return this.value;
    }

    @Override
    public int getValueOffset() {
      return 0;
    }

    @Override
    public int getValueLength() {
      return this.value == null ? 0 : this.value.length;
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return ByteBuffer.wrap(this.value);
    }

    @Override
    public int getValuePosition() {
      return 0;
    }

    @Override
    public long heapSize() {
      long sum = ClassSize.REFERENCE + super.heapSize();
      if (this.value != null) {
        sum += ClassSize.sizeOf(this.value);
      }
      return sum;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      return ValueAndTagRewriteCell.write(out, withTags, this.cell, this.value, this.tags);
    }

    @Override
    public int getSerializedSize(boolean withTags) {
      return super.getSerializedSize(withTags) - this.cell.getValueLength() + this.value.length;
    }

    @Override
    public void write(ByteBuffer buf, int offset) {
      ValueAndTagRewriteCell.write(buf, offset, this.cell, this.value, this.tags);
    }

    @Override
    public Cell deepClone() {
      Cell clonedBaseCell = ((ExtendedCell) this.cell).deepClone();
      if (clonedBaseCell instanceof ByteBufferCell) {
        return new ValueAndTagRewriteByteBufferCell((ByteBufferCell) clonedBaseCell, this.value,
            this.tags);
      }
      return new ValueAndTagRewriteCell(clonedBaseCell, this.value, this.tags);
    }
  }

  /**
   * @param cellScannerables
   * @return CellScanner interface over <code>cellIterables</code>
   */
  public static CellScanner createCellScanner(
      final List<? extends CellScannable> cellScannerables) {
    return new CellScanner() {
      private final Iterator<? extends CellScannable> iterator = cellScannerables.iterator();
      private CellScanner cellScanner = null;

      @Override
      public Cell current() {
        return this.cellScanner != null? this.cellScanner.current(): null;
      }

      @Override
      public boolean advance() throws IOException {
        while (true) {
          if (this.cellScanner == null) {
            if (!this.iterator.hasNext()) return false;
            this.cellScanner = this.iterator.next().cellScanner();
          }
          if (this.cellScanner.advance()) return true;
          this.cellScanner = null;
        }
      }
    };
  }

  /**
   * @param cellIterable
   * @return CellScanner interface over <code>cellIterable</code>
   */
  public static CellScanner createCellScanner(final Iterable<Cell> cellIterable) {
    if (cellIterable == null) return null;
    return createCellScanner(cellIterable.iterator());
  }

  /**
   * @param cells
   * @return CellScanner interface over <code>cellIterable</code> or null if <code>cells</code> is
   * null
   */
  public static CellScanner createCellScanner(final Iterator<Cell> cells) {
    if (cells == null) return null;
    return new CellScanner() {
      private final Iterator<Cell> iterator = cells;
      private Cell current = null;

      @Override
      public Cell current() {
        return this.current;
      }

      @Override
      public boolean advance() {
        boolean hasNext = this.iterator.hasNext();
        this.current = hasNext? this.iterator.next(): null;
        return hasNext;
      }
    };
  }

  /**
   * @param cellArray
   * @return CellScanner interface over <code>cellArray</code>
   */
  public static CellScanner createCellScanner(final Cell[] cellArray) {
    return new CellScanner() {
      private final Cell [] cells = cellArray;
      private int index = -1;

      @Override
      public Cell current() {
        if (cells == null) return null;
        return (index < 0)? null: this.cells[index];
      }

      @Override
      public boolean advance() {
        if (cells == null) return false;
        return ++index < this.cells.length;
      }
    };
  }

  /**
   * Flatten the map of cells out under the CellScanner
   * @param map Map of Cell Lists; for example, the map of families to Cells that is used
   * inside Put, etc., keeping Cells organized by family.
   * @return CellScanner interface over <code>cellIterable</code>
   */
  public static CellScanner createCellScanner(final NavigableMap<byte [], List<Cell>> map) {
    return new CellScanner() {
      private final Iterator<Entry<byte[], List<Cell>>> entries = map.entrySet().iterator();
      private Iterator<Cell> currentIterator = null;
      private Cell currentCell;

      @Override
      public Cell current() {
        return this.currentCell;
      }

      @Override
      public boolean advance() {
        while(true) {
          if (this.currentIterator == null) {
            if (!this.entries.hasNext()) return false;
            this.currentIterator = this.entries.next().getValue().iterator();
          }
          if (this.currentIterator.hasNext()) {
            this.currentCell = this.currentIterator.next();
            return true;
          }
          this.currentCell = null;
          this.currentIterator = null;
        }
      }
    };
  }

  /**
   * @param left
   * @param right
   * @return True if the rows in <code>left</code> and <code>right</code> Cells match
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Instead use {@link #matchingRows(Cell, Cell)}
   */
  @Deprecated
  public static boolean matchingRow(final Cell left, final Cell right) {
    return matchingRows(left, right);
  }

  public static boolean matchingRow(final Cell left, final byte[] buf) {
    if (buf == null) {
      return left.getRowLength() == 0;
    }
    return matchingRow(left, buf, 0, buf.length);
  }

  public static boolean matchingRow(final Cell left, final byte[] buf, final int offset,
      final int length) {
    if (left instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getRowByteBuffer(),
          ((ByteBufferCell) left).getRowPosition(), left.getRowLength(), buf, offset,
          length);
    }
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), left.getRowLength(), buf, offset,
        length);
  }

  public static boolean matchingFamily(final Cell left, final Cell right) {
    byte lfamlength = left.getFamilyLength();
    byte rfamlength = right.getFamilyLength();
    if (left instanceof ByteBufferCell && right instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getFamilyByteBuffer(),
          ((ByteBufferCell) left).getFamilyPosition(), lfamlength,
          ((ByteBufferCell) right).getFamilyByteBuffer(),
          ((ByteBufferCell) right).getFamilyPosition(), rfamlength);
    }
    if (left instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getFamilyByteBuffer(),
          ((ByteBufferCell) left).getFamilyPosition(), lfamlength,
          right.getFamilyArray(), right.getFamilyOffset(), rfamlength);
    }
    if (right instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) right).getFamilyByteBuffer(),
          ((ByteBufferCell) right).getFamilyPosition(), rfamlength,
          left.getFamilyArray(), left.getFamilyOffset(), lfamlength);
    }
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), lfamlength,
        right.getFamilyArray(), right.getFamilyOffset(), rfamlength);
  }

  public static boolean matchingFamily(final Cell left, final byte[] buf) {
    if (buf == null) {
      return left.getFamilyLength() == 0;
    }
    return matchingFamily(left, buf, 0, buf.length);
  }

  public static boolean matchingFamily(final Cell left, final byte[] buf, final int offset,
      final int length) {
    if (left instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getFamilyByteBuffer(),
          ((ByteBufferCell) left).getFamilyPosition(), left.getFamilyLength(), buf,
          offset, length);
    }
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), buf,
        offset, length);
  }

  public static boolean matchingQualifier(final Cell left, final Cell right) {
    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();
    if (left instanceof ByteBufferCell && right instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getQualifierByteBuffer(),
          ((ByteBufferCell) left).getQualifierPosition(), lqlength,
          ((ByteBufferCell) right).getQualifierByteBuffer(),
          ((ByteBufferCell) right).getQualifierPosition(), rqlength);
    }
    if (left instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getQualifierByteBuffer(),
          ((ByteBufferCell) left).getQualifierPosition(), lqlength,
          right.getQualifierArray(), right.getQualifierOffset(), rqlength);
    }
    if (right instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) right).getQualifierByteBuffer(),
          ((ByteBufferCell) right).getQualifierPosition(), rqlength,
          left.getQualifierArray(), left.getQualifierOffset(), lqlength);
    }
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
        lqlength, right.getQualifierArray(), right.getQualifierOffset(),
        rqlength);
  }

  /**
   * Finds if the qualifier part of the cell and the KV serialized
   * byte[] are equal
   * @param left
   * @param buf the serialized keyvalue format byte[]
   * @return true if the qualifier matches, false otherwise
   */
  public static boolean matchingQualifier(final Cell left, final byte[] buf) {
    if (buf == null) {
      return left.getQualifierLength() == 0;
    }
    return matchingQualifier(left, buf, 0, buf.length);
  }

  /**
   * Finds if the qualifier part of the cell and the KV serialized
   * byte[] are equal
   * @param left
   * @param buf the serialized keyvalue format byte[]
   * @param offset the offset of the qualifier in the byte[]
   * @param length the length of the qualifier in the byte[]
   * @return true if the qualifier matches, false otherwise
   */
  public static boolean matchingQualifier(final Cell left, final byte[] buf, final int offset,
      final int length) {
    if (buf == null) {
      return left.getQualifierLength() == 0;
    }
    if (left instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getQualifierByteBuffer(),
          ((ByteBufferCell) left).getQualifierPosition(), left.getQualifierLength(),
          buf, offset, length);
    }
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), buf, offset, length);
  }

  public static boolean matchingColumn(final Cell left, final byte[] fam, final byte[] qual) {
    if (!matchingFamily(left, fam))
      return false;
    return matchingQualifier(left, qual);
  }

  public static boolean matchingColumn(final Cell left, final byte[] fam, final int foffset,
      final int flength, final byte[] qual, final int qoffset, final int qlength) {
    if (!matchingFamily(left, fam, foffset, flength))
      return false;
    return matchingQualifier(left, qual, qoffset, qlength);
  }

  public static boolean matchingColumn(final Cell left, final Cell right) {
    if (!matchingFamily(left, right))
      return false;
    return matchingQualifier(left, right);
  }

  public static boolean matchingValue(final Cell left, final Cell right) {
    return matchingValue(left, right, left.getValueLength(), right.getValueLength());
  }

  public static boolean matchingValue(final Cell left, final Cell right, int lvlength,
      int rvlength) {
    if (left instanceof ByteBufferCell && right instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getValueByteBuffer(),
        ((ByteBufferCell) left).getValuePosition(), lvlength,
        ((ByteBufferCell) right).getValueByteBuffer(),
        ((ByteBufferCell) right).getValuePosition(), rvlength);
    }
    if (left instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getValueByteBuffer(),
        ((ByteBufferCell) left).getValuePosition(), lvlength, right.getValueArray(),
        right.getValueOffset(), rvlength);
    }
    if (right instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) right).getValueByteBuffer(),
        ((ByteBufferCell) right).getValuePosition(), rvlength, left.getValueArray(),
        left.getValueOffset(), lvlength);
    }
    return Bytes.equals(left.getValueArray(), left.getValueOffset(), lvlength,
      right.getValueArray(), right.getValueOffset(), rvlength);
  }

  public static boolean matchingValue(final Cell left, final byte[] buf) {
    if (left instanceof ByteBufferCell) {
      return ByteBufferUtils.compareTo(((ByteBufferCell) left).getValueByteBuffer(),
          ((ByteBufferCell) left).getValuePosition(), left.getValueLength(), buf, 0,
          buf.length) == 0;
    }
    return Bytes.equals(left.getValueArray(), left.getValueOffset(), left.getValueLength(), buf, 0,
        buf.length);
  }

  /**
   * @return True if a delete type, a {@link KeyValue.Type#Delete} or a
   *         {KeyValue.Type#DeleteFamily} or a
   *         {@link KeyValue.Type#DeleteColumn} KeyValue type.
   */
  public static boolean isDelete(final Cell cell) {
    return isDelete(cell.getTypeByte());
  }

  /**
   * @return True if a delete type, a {@link KeyValue.Type#Delete} or a
   *         {KeyValue.Type#DeleteFamily} or a
   *         {@link KeyValue.Type#DeleteColumn} KeyValue type.
   */
  public static boolean isDelete(final byte type) {
    return Type.Delete.getCode() <= type
        && type <= Type.DeleteFamily.getCode();
  }

  /**
   * @return True if this cell is a {@link KeyValue.Type#Delete} type.
   */
  public static boolean isDeleteType(Cell cell) {
    return cell.getTypeByte() == Type.Delete.getCode();
  }

  public static boolean isDeleteFamily(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteFamily.getCode();
  }

  public static boolean isDeleteFamilyVersion(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteFamilyVersion.getCode();
  }

  public static boolean isDeleteColumns(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteColumn.getCode();
  }

  public static boolean isDeleteColumnVersion(final Cell cell) {
    return cell.getTypeByte() == Type.Delete.getCode();
  }

  /**
   *
   * @return True if this cell is a delete family or column type.
   */
  public static boolean isDeleteColumnOrFamily(Cell cell) {
    int t = cell.getTypeByte();
    return t == Type.DeleteColumn.getCode() || t == Type.DeleteFamily.getCode();
  }

  /**
   * Estimate based on keyvalue's serialization format.
   * @param cell
   * @return Estimate of the <code>cell</code> size in bytes.
   */
  public static int estimatedSerializedSizeOf(final Cell cell) {
    if (cell instanceof ExtendedCell) {
      return ((ExtendedCell) cell).getSerializedSize(true) + Bytes.SIZEOF_INT;
    }

    return getSumOfCellElementLengths(cell) +
      // Use the KeyValue's infrastructure size presuming that another implementation would have
      // same basic cost.
      KeyValue.ROW_LENGTH_SIZE + KeyValue.FAMILY_LENGTH_SIZE +
      // Serialization is probably preceded by a length (it is in the KeyValueCodec at least).
      Bytes.SIZEOF_INT;
  }

  /**
   * @param cell
   * @return Sum of the lengths of all the elements in a Cell; does not count in any infrastructure
   */
  private static int getSumOfCellElementLengths(final Cell cell) {
    return getSumOfCellKeyElementLengths(cell) + cell.getValueLength() + cell.getTagsLength();
  }

  /**
   * @param cell
   * @return Sum of all elements that make up a key; does not include infrastructure, tags or
   * values.
   */
  private static int getSumOfCellKeyElementLengths(final Cell cell) {
    return cell.getRowLength() + cell.getFamilyLength() +
    cell.getQualifierLength() +
    KeyValue.TIMESTAMP_TYPE_SIZE;
  }

  /**
   * Calculates the serialized key size. We always serialize in the KeyValue's serialization
   * format.
   * @param cell the cell for which the key size has to be calculated.
   * @return the key size
   */
  public static int estimatedSerializedSizeOfKey(final Cell cell) {
    if (cell instanceof KeyValue) return ((KeyValue)cell).getKeyLength();
    return cell.getRowLength() + cell.getFamilyLength() +
        cell.getQualifierLength() +
        KeyValue.KEY_INFRASTRUCTURE_SIZE;
  }

  /**
   * This is an estimate of the heap space occupied by a cell. When the cell is of type
   * {@link HeapSize} we call {@link HeapSize#heapSize()} so cell can give a correct value. In other
   * cases we just consider the bytes occupied by the cell components ie. row, CF, qualifier,
   * timestamp, type, value and tags.
   * @param cell
   * @return estimate of the heap space
   */
  public static long estimatedHeapSizeOf(final Cell cell) {
    if (cell instanceof HeapSize) {
      return ((HeapSize) cell).heapSize();
    }
    // TODO: Add sizing of references that hold the row, family, etc., arrays.
    return estimatedSerializedSizeOf(cell);
  }

  /********************* tags *************************************/
  /**
   * Util method to iterate through the tags
   *
   * @param tags
   * @param offset
   * @param length
   * @return iterator for the tags
   * @deprecated As of 2.0.0 and will be removed in 3.0.0
   *             Instead use {@link #tagsIterator(Cell)}
   */
  @Deprecated
  public static Iterator<Tag> tagsIterator(final byte[] tags, final int offset, final int length) {
    return new Iterator<Tag>() {
      private int pos = offset;
      private int endOffset = offset + length - 1;

      @Override
      public boolean hasNext() {
        return this.pos < endOffset;
      }

      @Override
      public Tag next() {
        if (hasNext()) {
          int curTagLen = Bytes.readAsInt(tags, this.pos, Tag.TAG_LENGTH_SIZE);
          Tag tag = new ArrayBackedTag(tags, pos, curTagLen + TAG_LENGTH_SIZE);
          this.pos += Bytes.SIZEOF_SHORT + curTagLen;
          return tag;
        }
        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static Iterator<Tag> tagsIterator(final ByteBuffer tags, final int offset,
      final int length) {
    return new Iterator<Tag>() {
      private int pos = offset;
      private int endOffset = offset + length - 1;

      @Override
      public boolean hasNext() {
        return this.pos < endOffset;
      }

      @Override
      public Tag next() {
        if (hasNext()) {
          int curTagLen = ByteBufferUtils.readAsInt(tags, this.pos, Tag.TAG_LENGTH_SIZE);
          Tag tag = new ByteBufferTag(tags, pos, curTagLen + Tag.TAG_LENGTH_SIZE);
          this.pos += Bytes.SIZEOF_SHORT + curTagLen;
          return tag;
        }
        return null;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    };
  }

  /**
   * Util method to iterate through the tags in the given cell.
   *
   * @param cell The Cell over which tags iterator is needed.
   * @return iterator for the tags
   */
  public static Iterator<Tag> tagsIterator(final Cell cell) {
    final int tagsLength = cell.getTagsLength();
    // Save an object allocation where we can
    if (tagsLength == 0) {
      return TagUtil.EMPTY_TAGS_ITR;
    }
    if (cell instanceof ByteBufferCell) {
      return tagsIterator(((ByteBufferCell) cell).getTagsByteBuffer(),
          ((ByteBufferCell) cell).getTagsPosition(), tagsLength);
    }
    return tagsIterator(cell.getTagsArray(), cell.getTagsOffset(), tagsLength);
  }

  /**
   * @param cell The Cell
   * @return Tags in the given Cell as a List
   */
  public static List<Tag> getTags(Cell cell) {
    List<Tag> tags = new ArrayList<>();
    Iterator<Tag> tagsItr = tagsIterator(cell);
    while (tagsItr.hasNext()) {
      tags.add(tagsItr.next());
    }
    return tags;
  }

  /**
   * Retrieve Cell's first tag, matching the passed in type
   *
   * @param cell The Cell
   * @param type Type of the Tag to retrieve
   * @return null if there is no tag of the passed in tag type
   */
  public static Tag getTag(Cell cell, byte type){
    boolean bufferBacked = cell instanceof ByteBufferCell;
    int length = cell.getTagsLength();
    int offset = bufferBacked? ((ByteBufferCell)cell).getTagsPosition():cell.getTagsOffset();
    int pos = offset;
    while (pos < offset + length) {
      int tagLen;
      if (bufferBacked) {
        ByteBuffer tagsBuffer = ((ByteBufferCell)cell).getTagsByteBuffer();
        tagLen = ByteBufferUtils.readAsInt(tagsBuffer, pos, TAG_LENGTH_SIZE);
        if (ByteBufferUtils.toByte(tagsBuffer, pos + TAG_LENGTH_SIZE) == type) {
          return new ByteBufferTag(tagsBuffer, pos, tagLen + TAG_LENGTH_SIZE);
        }
      } else {
        tagLen = Bytes.readAsInt(cell.getTagsArray(), pos, TAG_LENGTH_SIZE);
        if (cell.getTagsArray()[pos + TAG_LENGTH_SIZE] == type) {
          return new ArrayBackedTag(cell.getTagsArray(), pos, tagLen + TAG_LENGTH_SIZE);
        }
      }
      pos += TAG_LENGTH_SIZE + tagLen;
    }
    return null;
  }

  /**
   * Returns true if the first range start1...end1 overlaps with the second range
   * start2...end2, assuming the byte arrays represent row keys
   */
  public static boolean overlappingKeys(final byte[] start1, final byte[] end1,
      final byte[] start2, final byte[] end2) {
    return (end2.length == 0 || start1.length == 0 || Bytes.compareTo(start1,
        end2) < 0)
        && (end1.length == 0 || start2.length == 0 || Bytes.compareTo(start2,
            end1) < 0);
  }

  /**
   * Sets the given seqId to the cell.
   * Marked as audience Private as of 1.2.0.
   * Setting a Cell sequenceid is an internal implementation detail not for general public use.
   * @param cell
   * @param seqId
   * @throws IOException when the passed cell is not of type {@link SettableSequenceId}
   */
  @InterfaceAudience.Private
  public static void setSequenceId(Cell cell, long seqId) throws IOException {
    if (cell instanceof SettableSequenceId) {
      ((SettableSequenceId) cell).setSequenceId(seqId);
    } else {
      throw new IOException(new UnsupportedOperationException("Cell is not of type "
          + SettableSequenceId.class.getName()));
    }
  }

  /**
   * Sets the given timestamp to the cell.
   * @param cell
   * @param ts
   * @throws IOException when the passed cell is not of type {@link SettableTimestamp}
   */
  public static void setTimestamp(Cell cell, long ts) throws IOException {
    if (cell instanceof SettableTimestamp) {
      ((SettableTimestamp) cell).setTimestamp(ts);
    } else {
      throw new IOException(new UnsupportedOperationException("Cell is not of type "
          + SettableTimestamp.class.getName()));
    }
  }

  /**
   * Sets the given timestamp to the cell.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @param tsOffset offset to the new timestamp
   * @throws IOException when the passed cell is not of type {@link SettableTimestamp}
   */
  public static void setTimestamp(Cell cell, byte[] ts, int tsOffset) throws IOException {
    if (cell instanceof SettableTimestamp) {
      ((SettableTimestamp) cell).setTimestamp(ts, tsOffset);
    } else {
      throw new IOException(new UnsupportedOperationException("Cell is not of type "
          + SettableTimestamp.class.getName()));
    }
  }

  /**
   * Sets the given timestamp to the cell iff current timestamp is
   * {@link HConstants#LATEST_TIMESTAMP}.
   * @param cell
   * @param ts
   * @return True if cell timestamp is modified.
   * @throws IOException when the passed cell is not of type {@link SettableTimestamp}
   */
  public static boolean updateLatestStamp(Cell cell, long ts) throws IOException {
    if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
      setTimestamp(cell, ts);
      return true;
    }
    return false;
  }

  /**
   * Sets the given timestamp to the cell iff current timestamp is
   * {@link HConstants#LATEST_TIMESTAMP}.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @param tsOffset offset to the new timestamp
   * @return True if cell timestamp is modified.
   * @throws IOException when the passed cell is not of type {@link SettableTimestamp}
   */
  public static boolean updateLatestStamp(Cell cell, byte[] ts, int tsOffset) throws IOException {
    if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
      setTimestamp(cell, ts, tsOffset);
      return true;
    }
    return false;
  }

  /**
   * Writes the Cell's key part as it would have serialized in a KeyValue. The format is &lt;2 bytes
   * rk len&gt;&lt;rk&gt;&lt;1 byte cf len&gt;&lt;cf&gt;&lt;qualifier&gt;&lt;8 bytes
   * timestamp&gt;&lt;1 byte type&gt;
   * @param cell
   * @param out
   * @throws IOException
   */
  public static void writeFlatKey(Cell cell, DataOutputStream out) throws IOException {
    short rowLen = cell.getRowLength();
    byte fLen = cell.getFamilyLength();
    int qLen = cell.getQualifierLength();
    // Using just one if/else loop instead of every time checking before writing every
    // component of cell
    if (cell instanceof ByteBufferCell) {
      out.writeShort(rowLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getRowByteBuffer(),
        ((ByteBufferCell) cell).getRowPosition(), rowLen);
      out.writeByte(fLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getFamilyByteBuffer(),
        ((ByteBufferCell) cell).getFamilyPosition(), fLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getQualifierByteBuffer(),
        ((ByteBufferCell) cell).getQualifierPosition(), qLen);
    } else {
      out.writeShort(rowLen);
      out.write(cell.getRowArray(), cell.getRowOffset(), rowLen);
      out.writeByte(fLen);
      out.write(cell.getFamilyArray(), cell.getFamilyOffset(), fLen);
      out.write(cell.getQualifierArray(), cell.getQualifierOffset(), qLen);
    }
    out.writeLong(cell.getTimestamp());
    out.writeByte(cell.getTypeByte());
  }

  public static int writeFlatKey(Cell cell, OutputStream out) throws IOException {
    short rowLen = cell.getRowLength();
    byte fLen = cell.getFamilyLength();
    int qLen = cell.getQualifierLength();
    // Using just one if/else loop instead of every time checking before writing every
    // component of cell
    if (cell instanceof ByteBufferCell) {
      StreamUtils.writeShort(out, rowLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getRowByteBuffer(),
        ((ByteBufferCell) cell).getRowPosition(), rowLen);
      out.write(fLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getFamilyByteBuffer(),
        ((ByteBufferCell) cell).getFamilyPosition(), fLen);
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getQualifierByteBuffer(),
        ((ByteBufferCell) cell).getQualifierPosition(), qLen);
    } else {
      StreamUtils.writeShort(out, rowLen);
      out.write(cell.getRowArray(), cell.getRowOffset(), rowLen);
      out.write(fLen);
      out.write(cell.getFamilyArray(), cell.getFamilyOffset(), fLen);
      out.write(cell.getQualifierArray(), cell.getQualifierOffset(), qLen);
    }
    StreamUtils.writeLong(out, cell.getTimestamp());
    out.write(cell.getTypeByte());
    return Bytes.SIZEOF_SHORT + rowLen + Bytes.SIZEOF_BYTE + fLen + qLen + Bytes.SIZEOF_LONG
        + Bytes.SIZEOF_BYTE;
  }

  /**
   * Writes the row from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param rlength the row length
   * @throws IOException
   */
  public static void writeRow(OutputStream out, Cell cell, short rlength) throws IOException {
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getRowByteBuffer(),
        ((ByteBufferCell) cell).getRowPosition(), rlength);
    } else {
      out.write(cell.getRowArray(), cell.getRowOffset(), rlength);
    }
  }

  /**
   * Writes the row from the given cell to the output stream excluding the common prefix
   * @param out The dataoutputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param rlength the row length
   * @throws IOException
   */
  public static void writeRowSkippingBytes(DataOutputStream out, Cell cell, short rlength,
      int commonPrefix) throws IOException {
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getRowByteBuffer(),
        ((ByteBufferCell) cell).getRowPosition() + commonPrefix, rlength - commonPrefix);
    } else {
      out.write(cell.getRowArray(), cell.getRowOffset() + commonPrefix, rlength - commonPrefix);
    }
  }

  /**
   * Writes the family from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param flength the family length
   * @throws IOException
   */
  public static void writeFamily(OutputStream out, Cell cell, byte flength) throws IOException {
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getFamilyByteBuffer(),
        ((ByteBufferCell) cell).getFamilyPosition(), flength);
    } else {
      out.write(cell.getFamilyArray(), cell.getFamilyOffset(), flength);
    }
  }

  /**
   * Writes the qualifier from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param qlength the qualifier length
   * @throws IOException
   */
  public static void writeQualifier(OutputStream out, Cell cell, int qlength)
      throws IOException {
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getQualifierByteBuffer(),
        ((ByteBufferCell) cell).getQualifierPosition(), qlength);
    } else {
      out.write(cell.getQualifierArray(), cell.getQualifierOffset(), qlength);
    }
  }

  /**
   * Writes the qualifier from the given cell to the output stream excluding the common prefix
   * @param out The dataoutputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param qlength the qualifier length
   * @throws IOException
   */
  public static void writeQualifierSkippingBytes(DataOutputStream out, Cell cell,
      int qlength, int commonPrefix) throws IOException {
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getQualifierByteBuffer(),
        ((ByteBufferCell) cell).getQualifierPosition() + commonPrefix, qlength - commonPrefix);
    } else {
      out.write(cell.getQualifierArray(), cell.getQualifierOffset() + commonPrefix,
        qlength - commonPrefix);
    }
  }

  /**
   * Writes the value from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param vlength the value length
   * @throws IOException
   */
  public static void writeValue(OutputStream out, Cell cell, int vlength) throws IOException {
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getValueByteBuffer(),
        ((ByteBufferCell) cell).getValuePosition(), vlength);
    } else {
      out.write(cell.getValueArray(), cell.getValueOffset(), vlength);
    }
  }

  /**
   * Writes the tag from the given cell to the output stream
   * @param out The outputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param tagsLength the tag length
   * @throws IOException
   */
  public static void writeTags(OutputStream out, Cell cell, int tagsLength) throws IOException {
    if (cell instanceof ByteBufferCell) {
      ByteBufferUtils.copyBufferToStream(out, ((ByteBufferCell) cell).getTagsByteBuffer(),
        ((ByteBufferCell) cell).getTagsPosition(), tagsLength);
    } else {
      out.write(cell.getTagsArray(), cell.getTagsOffset(), tagsLength);
    }
  }

  /**
   * @param cell
   * @return The Key portion of the passed <code>cell</code> as a String.
   */
  public static String getCellKeyAsString(Cell cell) {
    StringBuilder sb = new StringBuilder(Bytes.toStringBinary(
      cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    sb.append('/');
    sb.append(cell.getFamilyLength() == 0? "":
      Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    // KeyValue only added ':' if family is non-null.  Do same.
    if (cell.getFamilyLength() > 0) sb.append(':');
    sb.append(cell.getQualifierLength() == 0? "":
      Bytes.toStringBinary(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength()));
    sb.append('/');
    sb.append(KeyValue.humanReadableTimestamp(cell.getTimestamp()));
    sb.append('/');
    sb.append(Type.codeToType(cell.getTypeByte()));
    if (!(cell instanceof KeyValue.KeyOnlyKeyValue)) {
      sb.append("/vlen=");
      sb.append(cell.getValueLength());
    }
    sb.append("/seqid=");
    sb.append(cell.getSequenceId());
    return sb.toString();
  }

  /**
   * This method exists just to encapsulate how we serialize keys.  To be replaced by a factory
   * that we query to figure what the Cell implementation is and then, what serialization engine
   * to use and further, how to serialize the key for inclusion in hfile index. TODO.
   * @param cell
   * @return The key portion of the Cell serialized in the old-school KeyValue way or null if
   * passed a null <code>cell</code>
   */
  public static byte [] getCellKeySerializedAsKeyValueKey(final Cell cell) {
    if (cell == null) return null;
    byte [] b = new byte[KeyValueUtil.keyLength(cell)];
    KeyValueUtil.appendKeyTo(cell, b, 0);
    return b;
  }

  /**
   * Write rowkey excluding the common part.
   * @param cell
   * @param rLen
   * @param commonPrefix
   * @param out
   * @throws IOException
   */
  public static void writeRowKeyExcludingCommon(Cell cell, short rLen, int commonPrefix,
      DataOutputStream out) throws IOException {
    if (commonPrefix == 0) {
      out.writeShort(rLen);
    } else if (commonPrefix == 1) {
      out.writeByte((byte) rLen);
      commonPrefix--;
    } else {
      commonPrefix -= KeyValue.ROW_LENGTH_SIZE;
    }
    if (rLen > commonPrefix) {
      writeRowSkippingBytes(out, cell, rLen, commonPrefix);
    }
  }

  /**
   * Find length of common prefix in keys of the cells, considering key as byte[] if serialized in
   * {@link KeyValue}. The key format is &lt;2 bytes rk len&gt;&lt;rk&gt;&lt;1 byte cf
   * len&gt;&lt;cf&gt;&lt;qualifier&gt;&lt;8 bytes timestamp&gt;&lt;1 byte type&gt;
   * @param c1
   *          the cell
   * @param c2
   *          the cell
   * @param bypassFamilyCheck
   *          when true assume the family bytes same in both cells. Pass it as true when dealing
   *          with Cells in same CF so as to avoid some checks
   * @param withTsType
   *          when true check timestamp and type bytes also.
   * @return length of common prefix
   */
  public static int findCommonPrefixInFlatKey(Cell c1, Cell c2, boolean bypassFamilyCheck,
      boolean withTsType) {
    // Compare the 2 bytes in RK length part
    short rLen1 = c1.getRowLength();
    short rLen2 = c2.getRowLength();
    int commonPrefix = KeyValue.ROW_LENGTH_SIZE;
    if (rLen1 != rLen2) {
      // early out when the RK length itself is not matching
      return ByteBufferUtils.findCommonPrefix(Bytes.toBytes(rLen1), 0, KeyValue.ROW_LENGTH_SIZE,
          Bytes.toBytes(rLen2), 0, KeyValue.ROW_LENGTH_SIZE);
    }
    // Compare the RKs
    int rkCommonPrefix = 0;
    if (c1 instanceof ByteBufferCell && c2 instanceof ByteBufferCell) {
      rkCommonPrefix = ByteBufferUtils.findCommonPrefix(((ByteBufferCell) c1).getRowByteBuffer(),
        ((ByteBufferCell) c1).getRowPosition(), rLen1, ((ByteBufferCell) c2).getRowByteBuffer(),
        ((ByteBufferCell) c2).getRowPosition(), rLen2);
    } else {
      // There cannot be a case where one cell is BBCell and other is KeyValue. This flow comes either
      // in flush or compactions. In flushes both cells are KV and in case of compaction it will be either
      // KV or BBCell
      rkCommonPrefix = ByteBufferUtils.findCommonPrefix(c1.getRowArray(), c1.getRowOffset(),
        rLen1, c2.getRowArray(), c2.getRowOffset(), rLen2);
    }
    commonPrefix += rkCommonPrefix;
    if (rkCommonPrefix != rLen1) {
      // Early out when RK is not fully matching.
      return commonPrefix;
    }
    // Compare 1 byte CF length part
    byte fLen1 = c1.getFamilyLength();
    if (bypassFamilyCheck) {
      // This flag will be true when caller is sure that the family will be same for both the cells
      // Just make commonPrefix to increment by the family part
      commonPrefix += KeyValue.FAMILY_LENGTH_SIZE + fLen1;
    } else {
      byte fLen2 = c2.getFamilyLength();
      if (fLen1 != fLen2) {
        // early out when the CF length itself is not matching
        return commonPrefix;
      }
      // CF lengths are same so there is one more byte common in key part
      commonPrefix += KeyValue.FAMILY_LENGTH_SIZE;
      // Compare the CF names
      int fCommonPrefix;
      if (c1 instanceof ByteBufferCell && c2 instanceof ByteBufferCell) {
        fCommonPrefix =
            ByteBufferUtils.findCommonPrefix(((ByteBufferCell) c1).getFamilyByteBuffer(),
              ((ByteBufferCell) c1).getFamilyPosition(), fLen1,
              ((ByteBufferCell) c2).getFamilyByteBuffer(),
              ((ByteBufferCell) c2).getFamilyPosition(), fLen2);
      } else {
        fCommonPrefix = ByteBufferUtils.findCommonPrefix(c1.getFamilyArray(), c1.getFamilyOffset(),
          fLen1, c2.getFamilyArray(), c2.getFamilyOffset(), fLen2);
      }
      commonPrefix += fCommonPrefix;
      if (fCommonPrefix != fLen1) {
        return commonPrefix;
      }
    }
    // Compare the Qualifiers
    int qLen1 = c1.getQualifierLength();
    int qLen2 = c2.getQualifierLength();
    int qCommon;
    if (c1 instanceof ByteBufferCell && c2 instanceof ByteBufferCell) {
      qCommon = ByteBufferUtils.findCommonPrefix(((ByteBufferCell) c1).getQualifierByteBuffer(),
        ((ByteBufferCell) c1).getQualifierPosition(), qLen1,
        ((ByteBufferCell) c2).getQualifierByteBuffer(),
        ((ByteBufferCell) c2).getQualifierPosition(), qLen2);
    } else {
      qCommon = ByteBufferUtils.findCommonPrefix(c1.getQualifierArray(), c1.getQualifierOffset(),
        qLen1, c2.getQualifierArray(), c2.getQualifierOffset(), qLen2);
    }
    commonPrefix += qCommon;
    if (!withTsType || Math.max(qLen1, qLen2) != qCommon) {
      return commonPrefix;
    }
    // Compare the timestamp parts
    int tsCommonPrefix = ByteBufferUtils.findCommonPrefix(Bytes.toBytes(c1.getTimestamp()), 0,
        KeyValue.TIMESTAMP_SIZE, Bytes.toBytes(c2.getTimestamp()), 0, KeyValue.TIMESTAMP_SIZE);
    commonPrefix += tsCommonPrefix;
    if (tsCommonPrefix != KeyValue.TIMESTAMP_SIZE) {
      return commonPrefix;
    }
    // Compare the type
    if (c1.getTypeByte() == c2.getTypeByte()) {
      commonPrefix += KeyValue.TYPE_SIZE;
    }
    return commonPrefix;
  }

  /** Returns a string representation of the cell */
  public static String toString(Cell cell, boolean verbose) {
    if (cell == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    String keyStr = getCellKeyAsString(cell);

    String tag = null;
    String value = null;
    if (verbose) {
      // TODO: pretty print tags as well
      if (cell.getTagsLength() > 0) {
        tag = Bytes.toStringBinary(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
      }
      if (!(cell instanceof KeyValue.KeyOnlyKeyValue)) {
        value = Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(),
            cell.getValueLength());
      }
    }

    builder
      .append(keyStr);
    if (tag != null && !tag.isEmpty()) {
      builder.append("/").append(tag);
    }
    if (value != null) {
      builder.append("/").append(value);
    }

    return builder.toString();
  }

  /***************** special cases ****************************/

  /**
   * special case for Cell.equals
   */
  public static boolean equalsIgnoreMvccVersion(Cell a, Cell b) {
    // row
    boolean res = matchingRow(a, b);
    if (!res)
      return res;

    // family
    res = matchingColumn(a, b);
    if (!res)
      return res;

    // timestamp: later sorts first
    if (!matchingTimestamp(a, b))
      return false;

    // type
    int c = (0xff & b.getTypeByte()) - (0xff & a.getTypeByte());
    if (c != 0)
      return false;
    else return true;
  }

  /**************** equals ****************************/

  public static boolean equals(Cell a, Cell b) {
    return matchingRow(a, b) && matchingFamily(a, b) && matchingQualifier(a, b)
        && matchingTimestamp(a, b) && matchingType(a, b);
  }

  public static boolean matchingTimestamp(Cell a, Cell b) {
    return CellComparator.compareTimestamps(a.getTimestamp(), b.getTimestamp()) == 0;
  }

  public static boolean matchingType(Cell a, Cell b) {
    return a.getTypeByte() == b.getTypeByte();
  }

  /**
   * Compares the row of two keyvalues for equality
   *
   * @param left
   * @param right
   * @return True if rows match.
   */
  public static boolean matchingRows(final Cell left, final Cell right) {
    short lrowlength = left.getRowLength();
    short rrowlength = right.getRowLength();
    if (lrowlength != rrowlength) return false;
    if (left instanceof ByteBufferCell && right instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getRowByteBuffer(),
          ((ByteBufferCell) left).getRowPosition(), lrowlength,
          ((ByteBufferCell) right).getRowByteBuffer(),
          ((ByteBufferCell) right).getRowPosition(), rrowlength);
    }
    if (left instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) left).getRowByteBuffer(),
          ((ByteBufferCell) left).getRowPosition(), lrowlength, right.getRowArray(),
          right.getRowOffset(), rrowlength);
    }
    if (right instanceof ByteBufferCell) {
      return ByteBufferUtils.equals(((ByteBufferCell) right).getRowByteBuffer(),
          ((ByteBufferCell) right).getRowPosition(), rrowlength, left.getRowArray(),
          left.getRowOffset(), lrowlength);
    }
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), lrowlength,
        right.getRowArray(), right.getRowOffset(), rrowlength);
  }

  /**
   * Compares the row and column of two keyvalues for equality
   *
   * @param left
   * @param right
   * @return True if same row and column.
   */
  public static boolean matchingRowColumn(final Cell left, final Cell right) {
    if ((left.getRowLength() + left.getFamilyLength() + left.getQualifierLength()) != (right
        .getRowLength() + right.getFamilyLength() + right.getQualifierLength())) {
      return false;
    }

    if (!matchingRows(left, right)) {
      return false;
    }
    return matchingColumn(left, right);
  }

  /**
   * Converts the rowkey bytes of the given cell into an int value
   *
   * @param cell
   * @return rowkey as int
   */
  public static int getRowAsInt(Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return ByteBufferUtils.toInt(((ByteBufferCell) cell).getRowByteBuffer(),
          ((ByteBufferCell) cell).getRowPosition());
    }
    return Bytes.toInt(cell.getRowArray(), cell.getRowOffset());
  }

  /**
   * Converts the value bytes of the given cell into a long value
   *
   * @param cell
   * @return value as long
   */
  public static long getValueAsLong(Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return ByteBufferUtils.toLong(((ByteBufferCell) cell).getValueByteBuffer(),
          ((ByteBufferCell) cell).getValuePosition());
    }
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset());
  }

  /**
   * Converts the value bytes of the given cell into a int value
   *
   * @param cell
   * @return value as int
   */
  public static int getValueAsInt(Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return ByteBufferUtils.toInt(((ByteBufferCell) cell).getValueByteBuffer(),
          ((ByteBufferCell) cell).getValuePosition());
    }
    return Bytes.toInt(cell.getValueArray(), cell.getValueOffset());
  }

  /**
   * Converts the value bytes of the given cell into a double value
   *
   * @param cell
   * @return value as double
   */
  public static double getValueAsDouble(Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return ByteBufferUtils.toDouble(((ByteBufferCell) cell).getValueByteBuffer(),
          ((ByteBufferCell) cell).getValuePosition());
    }
    return Bytes.toDouble(cell.getValueArray(), cell.getValueOffset());
  }

  /**
   * Converts the value bytes of the given cell into a BigDecimal
   *
   * @param cell
   * @return value as BigDecimal
   */
  public static BigDecimal getValueAsBigDecimal(Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return ByteBufferUtils.toBigDecimal(((ByteBufferCell) cell).getValueByteBuffer(),
          ((ByteBufferCell) cell).getValuePosition(), cell.getValueLength());
    }
    return Bytes.toBigDecimal(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * Create a Cell that is smaller than all other possible Cells for the given Cell's row.
   *
   * @param cell
   * @return First possible Cell on passed Cell's row.
   */
  public static Cell createFirstOnRow(final Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return new FirstOnRowByteBufferCell(((ByteBufferCell) cell).getRowByteBuffer(),
        ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength());
    }
    return new FirstOnRowCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }
  
  public static Cell createFirstOnRow(final byte [] row, int roffset, short rlength) {
    return new FirstOnRowCell(row, roffset, rlength);
  }

  public static Cell createFirstOnRow(final byte[] row, final byte[] family, final byte[] col) {
    return createFirstOnRow(row, 0, (short)row.length,
        family, 0, (byte)family.length,
        col, 0, col.length);
  }

  public static Cell createFirstOnRow(final byte[] row, int roffset, short rlength,
                                      final byte[] family, int foffset, byte flength,
                                      final byte[] col, int coffset, int clength) {
    return new FirstOnRowColCell(row, roffset, rlength,
        family, foffset, flength,
        col, coffset, clength);
  }

  public static Cell createFirstOnRow(final byte[] row) {
    return createFirstOnRow(row, 0, (short)row.length);
  }

  /**
   * @return Cell that is smaller than all other possible Cells for the given Cell's row and passed
   *         family.
   */
  public static Cell createFirstOnRowFamily(Cell cell, byte[] fArray, int foff, int flen) {
    if (cell instanceof ByteBufferCell) {
      return new FirstOnRowColByteBufferCell(((ByteBufferCell) cell).getRowByteBuffer(),
          ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength(), ByteBuffer.wrap(fArray),
          foff, (byte) flen, HConstants.EMPTY_BYTE_BUFFER, 0, 0);
    }
    return new FirstOnRowColCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        fArray, foff, (byte) flen, HConstants.EMPTY_BYTE_ARRAY, 0, 0);
  }

  /**
   * Create a Cell that is smaller than all other possible Cells for the given Cell's row.
   * The family length is considered to be 0
   * @param cell
   * @return First possible Cell on passed Cell's row.
   */
  public static Cell createFirstOnRowCol(final Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return new FirstOnRowColByteBufferCell(((ByteBufferCell) cell).getRowByteBuffer(),
          ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength(),
          HConstants.EMPTY_BYTE_BUFFER, 0, (byte) 0,
          ((ByteBufferCell) cell).getQualifierByteBuffer(),
          ((ByteBufferCell) cell).getQualifierPosition(), cell.getQualifierLength());
    }
    return new FirstOnRowColCell(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowLength(), HConstants.EMPTY_BYTE_ARRAY, 0, (byte)0, cell.getQualifierArray(),
        cell.getQualifierOffset(), cell.getQualifierLength());
  }
  /**
   * Create a Cell that is smaller than all other possible Cells for the given Cell row's next row.
   * Makes the next row's rowkey by appending single byte 0x00 to the end of current row key.
   */
  public static Cell createFirstOnNextRow(final Cell cell) {
    byte[] nextRow = new byte[cell.getRowLength() + 1];
    copyRowTo(cell, nextRow, 0);
    nextRow[nextRow.length - 1] = 0;// maybe not necessary
    return new FirstOnRowCell(nextRow, 0, (short) nextRow.length);
  }

  /**
   * Create a Cell that is smaller than all other possible Cells for the given Cell's rk:cf and
   * passed qualifier.
   *
   * @param cell
   * @param qArray
   * @param qoffest
   * @param qlength
   * @return Last possible Cell on passed Cell's rk:cf and passed qualifier.
   */
  public static Cell createFirstOnRowCol(final Cell cell, byte[] qArray, int qoffest, int qlength) {
    if(cell instanceof ByteBufferCell) {
      return new FirstOnRowColByteBufferCell(((ByteBufferCell) cell).getRowByteBuffer(),
          ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength(),
          ((ByteBufferCell) cell).getFamilyByteBuffer(),
          ((ByteBufferCell) cell).getFamilyPosition(), cell.getFamilyLength(),
          ByteBuffer.wrap(qArray), qoffest, qlength);
    }
    return new FirstOnRowColCell(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowLength(), cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        qArray, qoffest, qlength);
  }

  /**
   * Creates the first cell with the row/family/qualifier of this cell and the given timestamp.
   * Uses the "maximum" type that guarantees that the new cell is the lowest possible for this
   * combination of row, family, qualifier, and timestamp. This cell's own timestamp is ignored.
   *
   * @param cell - cell
   * @param ts
   */
  public static Cell createFirstOnRowColTS(Cell cell, long ts) {
    if(cell instanceof ByteBufferCell) {
      return new FirstOnRowColTSByteBufferCell(((ByteBufferCell) cell).getRowByteBuffer(),
          ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength(),
          ((ByteBufferCell) cell).getFamilyByteBuffer(),
          ((ByteBufferCell) cell).getFamilyPosition(), cell.getFamilyLength(),
          ((ByteBufferCell) cell).getQualifierByteBuffer(),
          ((ByteBufferCell) cell).getQualifierPosition(), cell.getQualifierLength(),
          ts);
    }
    return new FirstOnRowColTSCell(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowLength(), cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength(), ts);
  }

  /**
   * Create a Cell that is larger than all other possible Cells for the given Cell's row.
   *
   * @param cell
   * @return Last possible Cell on passed Cell's row.
   */
  public static Cell createLastOnRow(final Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return new LastOnRowByteBufferCell(((ByteBufferCell) cell).getRowByteBuffer(),
        ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength());
    }
    return new LastOnRowCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
  }

  public static Cell createLastOnRow(final byte[] row) {
    return new LastOnRowCell(row, 0, (short)row.length);
  }

  /**
   * Create a Cell that is larger than all other possible Cells for the given Cell's rk:cf:q. Used
   * in creating "fake keys" for the multi-column Bloom filter optimization to skip the row/column
   * we already know is not in the file.
   *
   * @param cell
   * @return Last possible Cell on passed Cell's rk:cf:q.
   */
  public static Cell createLastOnRowCol(final Cell cell) {
    if (cell instanceof ByteBufferCell) {
      return new LastOnRowColByteBufferCell(((ByteBufferCell) cell).getRowByteBuffer(),
          ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength(),
          ((ByteBufferCell) cell).getFamilyByteBuffer(),
          ((ByteBufferCell) cell).getFamilyPosition(), cell.getFamilyLength(),
          ((ByteBufferCell) cell).getQualifierByteBuffer(),
          ((ByteBufferCell) cell).getQualifierPosition(), cell.getQualifierLength());
    }
    return new LastOnRowColCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
        cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
  }

  /**
   * Create a Delete Family Cell for the specified row and family that would
   * be smaller than all other possible Delete Family KeyValues that have the
   * same row and family.
   * Used for seeking.
   * @param row - row key (arbitrary byte array)
   * @param fam - family name
   * @return First Delete Family possible key on passed <code>row</code>.
   */
  public static Cell createFirstDeleteFamilyCellOnRow(final byte[] row, final byte[] fam) {
    return new FirstOnRowDeleteFamilyCell(row, fam);
  }

  /**
   * Compresses the tags to the given outputstream using the TagcompressionContext
   * @param out the outputstream to which the compression should happen
   * @param cell the cell which has tags
   * @param tagCompressionContext the TagCompressionContext
   * @throws IOException can throw IOException if the compression encounters issue
   */
  @InterfaceAudience.Private
  public static void compressTags(OutputStream out, Cell cell,
      TagCompressionContext tagCompressionContext) throws IOException {
    if (cell instanceof ByteBufferCell) {
      tagCompressionContext.compressTags(out, ((ByteBufferCell) cell).getTagsByteBuffer(),
          ((ByteBufferCell) cell).getTagsPosition(), cell.getTagsLength());
    } else {
      tagCompressionContext.compressTags(out, cell.getTagsArray(), cell.getTagsOffset(),
          cell.getTagsLength());
    }
  }

  @InterfaceAudience.Private
  public static void compressRow(OutputStream out, Cell cell, Dictionary dict) throws IOException {
    if (cell instanceof ByteBufferCell) {
      Dictionary.write(out, ((ByteBufferCell) cell).getRowByteBuffer(),
        ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength(), dict);
    } else {
      Dictionary.write(out, cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), dict);
    }
  }

  @InterfaceAudience.Private
  public static void compressFamily(OutputStream out, Cell cell, Dictionary dict)
      throws IOException {
    if (cell instanceof ByteBufferCell) {
      Dictionary.write(out, ((ByteBufferCell) cell).getFamilyByteBuffer(),
        ((ByteBufferCell) cell).getFamilyPosition(), cell.getFamilyLength(), dict);
    } else {
      Dictionary.write(out, cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
        dict);
    }
  }

  @InterfaceAudience.Private
  public static void compressQualifier(OutputStream out, Cell cell, Dictionary dict)
      throws IOException {
    if (cell instanceof ByteBufferCell) {
      Dictionary.write(out, ((ByteBufferCell) cell).getQualifierByteBuffer(),
        ((ByteBufferCell) cell).getQualifierPosition(), cell.getQualifierLength(), dict);
    } else {
      Dictionary.write(out, cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength(), dict);
    }
  }

  @InterfaceAudience.Private
  /**
   * These cells are used in reseeks/seeks to improve the read performance.
   * They are not real cells that are returned back to the clients
   */
  private static abstract class EmptyCell implements Cell, SettableSequenceId {

    @Override
    public void setSequenceId(long seqId) {
      // Fake cells don't need seqId, so leaving it as a noop.
    }
    @Override
    public byte[] getRowArray() {
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getRowOffset() {
      return 0;
    }

    @Override
    public short getRowLength() {
      return 0;
    }

    @Override
    public byte[] getFamilyArray() {
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getFamilyOffset() {
      return 0;
    }

    @Override
    public byte getFamilyLength() {
      return 0;
    }

    @Override
    public byte[] getQualifierArray() {
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getQualifierOffset() {
      return 0;
    }

    @Override
    public int getQualifierLength() {
      return 0;
    }

    @Override
    public long getSequenceId() {
      return 0;
    }

    @Override
    public byte[] getValueArray() {
      return EMPTY_BYTE_ARRAY;
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
      return EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public int getTagsLength() {
      return 0;
    }
  }

  @InterfaceAudience.Private
  /**
   * These cells are used in reseeks/seeks to improve the read performance.
   * They are not real cells that are returned back to the clients
   */
  private static abstract class EmptyByteBufferCell extends ByteBufferCell
      implements SettableSequenceId {

    @Override
    public void setSequenceId(long seqId) {
      // Fake cells don't need seqId, so leaving it as a noop.
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
      return 0;
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
      return 0;
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
      return 0;
    }

    @Override
    public long getSequenceId() {
      return 0;
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
      return 0;
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
      return 0;
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getRowPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getFamilyPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getQualifierPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getTagsByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getTagsPosition() {
      return 0;
    }

    @Override
    public ByteBuffer getValueByteBuffer() {
      return HConstants.EMPTY_BYTE_BUFFER;
    }

    @Override
    public int getValuePosition() {
      return 0;
    }
  }

  @InterfaceAudience.Private
  private static class FirstOnRowCell extends EmptyCell {
    private final byte[] rowArray;
    private final int roffset;
    private final short rlength;

    public FirstOnRowCell(final byte[] row, int roffset, short rlength) {
      this.rowArray = row;
      this.roffset = roffset;
      this.rlength = rlength;
    }

    @Override
    public byte[] getRowArray() {
      return this.rowArray;
    }

    @Override
    public int getRowOffset() {
      return this.roffset;
    }

    @Override
    public short getRowLength() {
      return this.rlength;
    }

    @Override
    public long getTimestamp() {
      return HConstants.LATEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return Type.Maximum.getCode();
    }
  }

  @InterfaceAudience.Private
  private static class FirstOnRowByteBufferCell extends EmptyByteBufferCell {
    private final ByteBuffer rowBuff;
    private final int roffset;
    private final short rlength;

    public FirstOnRowByteBufferCell(final ByteBuffer row, int roffset, short rlength) {
      this.rowBuff = row;
      this.roffset = roffset;
      this.rlength = rlength;
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return this.rowBuff;
    }

    @Override
    public int getRowPosition() {
      return this.roffset;
    }

    @Override
    public short getRowLength() {
      return this.rlength;
    }

    @Override
    public long getTimestamp() {
      return HConstants.LATEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return Type.Maximum.getCode();
    }
  }

  @InterfaceAudience.Private
  private static class LastOnRowByteBufferCell extends EmptyByteBufferCell {
    private final ByteBuffer rowBuff;
    private final int roffset;
    private final short rlength;

    public LastOnRowByteBufferCell(final ByteBuffer row, int roffset, short rlength) {
      this.rowBuff = row;
      this.roffset = roffset;
      this.rlength = rlength;
    }

    @Override
    public ByteBuffer getRowByteBuffer() {
      return this.rowBuff;
    }

    @Override
    public int getRowPosition() {
      return this.roffset;
    }

    @Override
    public short getRowLength() {
      return this.rlength;
    }

    @Override
    public long getTimestamp() {
      return HConstants.OLDEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return Type.Minimum.getCode();
    }
  }

  @InterfaceAudience.Private
  private static class FirstOnRowColByteBufferCell extends FirstOnRowByteBufferCell {
    private final ByteBuffer famBuff;
    private final int famOffset;
    private final byte famLength;
    private final ByteBuffer colBuff;
    private final int colOffset;
    private final int colLength;

    public FirstOnRowColByteBufferCell(final ByteBuffer row, int roffset, short rlength,
        final ByteBuffer famBuff, final int famOffset, final byte famLength, final ByteBuffer col,
        final int colOffset, final int colLength) {
      super(row, roffset, rlength);
      this.famBuff = famBuff;
      this.famOffset = famOffset;
      this.famLength = famLength;
      this.colBuff = col;
      this.colOffset = colOffset;
      this.colLength = colLength;
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return this.famBuff;
    }

    @Override
    public int getFamilyPosition() {
      return this.famOffset;
    }

    @Override
    public byte getFamilyLength() {
      return famLength;
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return this.colBuff;
    }

    @Override
    public int getQualifierPosition() {
      return this.colOffset;
    }

    @Override
    public int getQualifierLength() {
      return this.colLength;
    }
  }

  @InterfaceAudience.Private
  private static class FirstOnRowColCell extends FirstOnRowCell {
    private final byte[] fArray;
    private final int foffset;
    private final byte flength;
    private final byte[] qArray;
    private final int qoffset;
    private final int qlength;

    public FirstOnRowColCell(byte[] rArray, int roffset, short rlength, byte[] fArray,
        int foffset, byte flength, byte[] qArray, int qoffset, int qlength) {
      super(rArray, roffset, rlength);
      this.fArray = fArray;
      this.foffset = foffset;
      this.flength = flength;
      this.qArray = qArray;
      this.qoffset = qoffset;
      this.qlength = qlength;
    }

    @Override
    public byte[] getFamilyArray() {
      return this.fArray;
    }

    @Override
    public int getFamilyOffset() {
      return this.foffset;
    }

    @Override
    public byte getFamilyLength() {
      return this.flength;
    }

    @Override
    public byte[] getQualifierArray() {
      return this.qArray;
    }

    @Override
    public int getQualifierOffset() {
      return this.qoffset;
    }

    @Override
    public int getQualifierLength() {
      return this.qlength;
    }
  }

  @InterfaceAudience.Private
  private static class FirstOnRowColTSCell extends FirstOnRowColCell {

    private long ts;

    public FirstOnRowColTSCell(byte[] rArray, int roffset, short rlength, byte[] fArray,
        int foffset, byte flength, byte[] qArray, int qoffset, int qlength, long ts) {
      super(rArray, roffset, rlength, fArray, foffset, flength, qArray, qoffset, qlength);
      this.ts = ts;
    }

    @Override
    public long getTimestamp() {
      return this.ts;
    }
  }

  @InterfaceAudience.Private
  private static class FirstOnRowColTSByteBufferCell extends FirstOnRowColByteBufferCell {

    private long ts;

    public FirstOnRowColTSByteBufferCell(ByteBuffer rBuffer, int roffset, short rlength,
        ByteBuffer fBuffer, int foffset, byte flength, ByteBuffer qBuffer, int qoffset, int qlength,
        long ts) {
      super(rBuffer, roffset, rlength, fBuffer, foffset, flength, qBuffer, qoffset, qlength);
      this.ts = ts;
    }

    @Override
    public long getTimestamp() {
      return this.ts;
    }
  }

  @InterfaceAudience.Private
  private static class LastOnRowCell extends EmptyCell {
    private final byte[] rowArray;
    private final int roffset;
    private final short rlength;

    public LastOnRowCell(byte[] row, int roffset, short rlength) {
      this.rowArray = row;
      this.roffset = roffset;
      this.rlength = rlength;
    }

    @Override
    public byte[] getRowArray() {
      return this.rowArray;
    }

    @Override
    public int getRowOffset() {
      return this.roffset;
    }

    @Override
    public short getRowLength() {
      return this.rlength;
    }

    @Override
    public long getTimestamp() {
      return HConstants.OLDEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return Type.Minimum.getCode();
    }
  }

  @InterfaceAudience.Private
  private static class LastOnRowColCell extends LastOnRowCell {
    private final byte[] fArray;
    private final int foffset;
    private final byte flength;
    private final byte[] qArray;
    private final int qoffset;
    private final int qlength;

    public LastOnRowColCell(byte[] rArray, int roffset, short rlength, byte[] fArray,
        int foffset, byte flength, byte[] qArray, int qoffset, int qlength) {
      super(rArray, roffset, rlength);
      this.fArray = fArray;
      this.foffset = foffset;
      this.flength = flength;
      this.qArray = qArray;
      this.qoffset = qoffset;
      this.qlength = qlength;
    }

    @Override
    public byte[] getFamilyArray() {
      return this.fArray;
    }

    @Override
    public int getFamilyOffset() {
      return this.foffset;
    }

    @Override
    public byte getFamilyLength() {
      return this.flength;
    }

    @Override
    public byte[] getQualifierArray() {
      return this.qArray;
    }

    @Override
    public int getQualifierOffset() {
      return this.qoffset;
    }

    @Override
    public int getQualifierLength() {
      return this.qlength;
    }
  }

  @InterfaceAudience.Private
  private static class LastOnRowColByteBufferCell extends LastOnRowByteBufferCell {
    private final ByteBuffer fBuffer;
    private final int foffset;
    private final byte flength;
    private final ByteBuffer qBuffer;
    private final int qoffset;
    private final int qlength;

    public LastOnRowColByteBufferCell(ByteBuffer rBuffer, int roffset, short rlength,
        ByteBuffer fBuffer, int foffset, byte flength, ByteBuffer qBuffer, int qoffset,
        int qlength) {
      super(rBuffer, roffset, rlength);
      this.fBuffer = fBuffer;
      this.foffset = foffset;
      this.flength = flength;
      this.qBuffer = qBuffer;
      this.qoffset = qoffset;
      this.qlength = qlength;
    }

    @Override
    public ByteBuffer getFamilyByteBuffer() {
      return this.fBuffer;
    }

    @Override
    public int getFamilyPosition() {
      return this.foffset;
    }

    @Override
    public byte getFamilyLength() {
      return this.flength;
    }

    @Override
    public ByteBuffer getQualifierByteBuffer() {
      return this.qBuffer;
    }

    @Override
    public int getQualifierPosition() {
      return this.qoffset;
    }

    @Override
    public int getQualifierLength() {
      return this.qlength;
    }
  }

  @InterfaceAudience.Private
  private static class FirstOnRowDeleteFamilyCell extends EmptyCell {
    private final byte[] row;
    private final byte[] fam;

    public FirstOnRowDeleteFamilyCell(byte[] row, byte[] fam) {
      this.row = row;
      this.fam = fam;
    }

    @Override
    public byte[] getRowArray() {
      return this.row;
    }

    @Override
    public short getRowLength() {
      return (short) this.row.length;
    }

    @Override
    public byte[] getFamilyArray() {
      return this.fam;
    }

    @Override
    public byte getFamilyLength() {
      return (byte) this.fam.length;
    }

    @Override
    public long getTimestamp() {
      return HConstants.LATEST_TIMESTAMP;
    }

    @Override
    public byte getTypeByte() {
      return Type.DeleteFamily.getCode();
    }
  }

  /**
   * @return An new cell is located following input cell. If both of type and timestamp are
   *         minimum, the input cell will be returned directly.
   */
  @InterfaceAudience.Private
  public static Cell createNextOnRowCol(Cell cell) {
    long ts = cell.getTimestamp();
    byte type = cell.getTypeByte();
    if (type != Type.Minimum.getCode()) {
      type = KeyValue.Type.values()[KeyValue.Type.codeToType(type).ordinal() - 1].getCode();
    } else if (ts != HConstants.OLDEST_TIMESTAMP) {
      ts = ts - 1;
      type = Type.Maximum.getCode();
    } else {
      return cell;
    }
    return createNextOnRowCol(cell, ts, type);
  }

  private static Cell createNextOnRowCol(Cell cell, long ts, byte type) {
    if (cell instanceof ByteBufferCell) {
      return new LastOnRowColByteBufferCell(((ByteBufferCell) cell).getRowByteBuffer(),
              ((ByteBufferCell) cell).getRowPosition(), cell.getRowLength(),
              ((ByteBufferCell) cell).getFamilyByteBuffer(),
              ((ByteBufferCell) cell).getFamilyPosition(), cell.getFamilyLength(),
              ((ByteBufferCell) cell).getQualifierByteBuffer(),
              ((ByteBufferCell) cell).getQualifierPosition(), cell.getQualifierLength()) {
        @Override
        public long getTimestamp() {
          return ts;
        }

        @Override
        public byte getTypeByte() {
          return type;
        }
      };
    }
    return new LastOnRowColCell(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
            cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
            cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) {
      @Override
      public long getTimestamp() {
        return ts;
      }

      @Override
      public byte getTypeByte() {
        return type;
      }
    };
  }
}
