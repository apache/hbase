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

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility methods helpful slinging {@link Cell} instances.
 * Some methods below are for internal use only and are marked InterfaceAudience.Private at the
 * method level.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class CellUtil {

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

  /**
   * Returns tag value in a new byte array. If server-side, use
   * {@link Tag#getBuffer()} with appropriate {@link Tag#getTagOffset()} and
   * {@link Tag#getTagLength()} instead to save on allocations.
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
    System.arraycopy(cell.getRowArray(), cell.getRowOffset(), destination, destinationOffset,
      cell.getRowLength());
    return destinationOffset + cell.getRowLength();
  }

  public static int copyFamilyTo(Cell cell, byte[] destination, int destinationOffset) {
    System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(), destination, destinationOffset,
      cell.getFamilyLength());
    return destinationOffset + cell.getFamilyLength();
  }

  public static int copyQualifierTo(Cell cell, byte[] destination, int destinationOffset) {
    System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), destination,
      destinationOffset, cell.getQualifierLength());
    return destinationOffset + cell.getQualifierLength();
  }

  public static int copyValueTo(Cell cell, byte[] destination, int destinationOffset) {
    System.arraycopy(cell.getValueArray(), cell.getValueOffset(), destination, destinationOffset,
        cell.getValueLength());
    return destinationOffset + cell.getValueLength();
  }

  /**
   * Copies the tags info into the tag portion of the cell
   * @param cell
   * @param destination
   * @param destinationOffset
   * @return position after tags
   */
  public static int copyTagTo(Cell cell, byte[] destination, int destinationOffset) {
    System.arraycopy(cell.getTagsArray(), cell.getTagsOffset(), destination, destinationOffset,
        cell.getTagsLength());
    return destinationOffset + cell.getTagsLength();
  }

  /********************* misc *************************************/

  public static byte getRowByte(Cell cell, int index) {
    return cell.getRowArray()[cell.getRowOffset() + index];
  }

  public static ByteBuffer getValueBufferShallowCopy(Cell cell) {
    ByteBuffer buffer = ByteBuffer.wrap(cell.getValueArray(), cell.getValueOffset(),
      cell.getValueLength());
    return buffer;
  }

  public static ByteBuffer getQualifierBufferShallowCopy(Cell cell) {
    ByteBuffer buffer = ByteBuffer.wrap(cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
    return buffer;
  }

  public static Cell createCell(final byte [] row, final byte [] family, final byte [] qualifier,
      final long timestamp, final byte type, final byte [] value) {
    // I need a Cell Factory here.  Using KeyValue for now. TODO.
    // TODO: Make a new Cell implementation that just carries these
    // byte arrays.
    // TODO: Call factory to create Cell
    return new KeyValue(row, family, qualifier, timestamp, KeyValue.Type.codeToType(type), value);
  }

  public static Cell createCell(final byte [] rowArray, final int rowOffset, final int rowLength,
      final byte [] familyArray, final int familyOffset, final int familyLength,
      final byte [] qualifierArray, final int qualifierOffset, final int qualifierLength) {
    // See createCell(final byte [] row, final byte [] value) for why we default Maximum type.
    return new KeyValue(rowArray, rowOffset, rowLength,
        familyArray, familyOffset, familyLength,
        qualifierArray, qualifierOffset, qualifierLength,
        HConstants.LATEST_TIMESTAMP,
        KeyValue.Type.Maximum,
        HConstants.EMPTY_BYTE_ARRAY, 0, HConstants.EMPTY_BYTE_ARRAY.length);
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with a memstoreTS/mvcc is an internal implementation detail not for
   * public use.
   */
  @InterfaceAudience.Private
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, final byte type, final byte[] value, final long memstoreTS) {
    KeyValue keyValue = new KeyValue(row, family, qualifier, timestamp,
        KeyValue.Type.codeToType(type), value);
    keyValue.setSequenceId(memstoreTS);
    return keyValue;
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with tags and a memstoreTS/mvcc is an internal implementation detail not for
   * public use.
   */
  @InterfaceAudience.Private
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, final byte type, final byte[] value, byte[] tags, final long memstoreTS) {
    KeyValue keyValue = new KeyValue(row, family, qualifier, timestamp,
        KeyValue.Type.codeToType(type), value, tags);
    keyValue.setSequenceId(memstoreTS);
    return keyValue;
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with tags is an internal implementation detail not for
   * public use.
   */
  @InterfaceAudience.Private
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, Type type, final byte[] value, byte[] tags) {
    KeyValue keyValue = new KeyValue(row, family, qualifier, timestamp, type, value, tags);
    return keyValue;
  }

  /**
   * Create a Cell with specific row.  Other fields defaulted.
   * @param row
   * @return Cell with passed row but all other fields are arbitrary
   */
  public static Cell createCell(final byte [] row) {
    return createCell(row, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Create a Cell with specific row and value.  Other fields are defaulted.
   * @param row
   * @param value
   * @return Cell with passed row and value but all other fields are arbitrary
   */
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
   */
  public static Cell createCell(final byte [] row, final byte [] family, final byte [] qualifier) {
    // See above in createCell(final byte [] row, final byte [] value) why we set type to Maximum.
    return createCell(row, family, qualifier,
        HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * @param cellScannerables
   * @return CellScanner interface over <code>cellIterables</code>
   */
  public static CellScanner createCellScanner(final List<? extends CellScannable> cellScannerables) {
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
   */
  public static boolean matchingRow(final Cell left, final Cell right) {
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
        right.getRowArray(), right.getRowOffset(), right.getRowLength());
  }

  public static boolean matchingRow(final Cell left, final byte[] buf) {
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), left.getRowLength(), buf, 0,
        buf.length);
  }

  public static boolean matchingRow(final Cell left, final byte[] buf, final int offset,
      final int length) {
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), left.getRowLength(), buf, offset,
        length);
  }

  public static boolean matchingFamily(final Cell left, final Cell right) {
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
        right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
  }

  public static boolean matchingFamily(final Cell left, final byte[] buf) {
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), buf,
        0, buf.length);
  }

  public static boolean matchingFamily(final Cell left, final byte[] buf, final int offset,
      final int length) {
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), buf,
        offset, length);
  }

  public static boolean matchingQualifier(final Cell left, final Cell right) {
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
        right.getQualifierLength());
  }

  public static boolean matchingQualifier(final Cell left, final byte[] buf) {
    if (buf == null) {
      return left.getQualifierLength() == 0;
    }
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), buf, 0, buf.length);
  }

  public static boolean matchingQualifier(final Cell left, final byte[] buf, final int offset,
      final int length) {
    if (buf == null) {
      return left.getQualifierLength() == 0;
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
    return Bytes.equals(left.getValueArray(), left.getValueOffset(), left.getValueLength(),
        right.getValueArray(), right.getValueOffset(), right.getValueLength());
  }

  public static boolean matchingValue(final Cell left, final byte[] buf) {
    return Bytes.equals(left.getValueArray(), left.getValueOffset(), left.getValueLength(), buf, 0,
        buf.length);
  }

  public static boolean matchingTimestamp(Cell a, Cell b) {
    return CellComparator.compareTimestamps(a, b) == 0;
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
   * @param cell
   * @return Estimate of the <code>cell</code> size in bytes.
   * @deprecated please use estimatedSerializedSizeOf(Cell)
   */
  @Deprecated
  public static int estimatedSizeOf(final Cell cell) {
    return estimatedSerializedSizeOf(cell);
  }

  /**
   * @param cell
   * @return Estimate of the <code>cell</code> size in bytes.
   */
  public static int estimatedSerializedSizeOf(final Cell cell) {
    // If a KeyValue, we can give a good estimate of size.
    if (cell instanceof KeyValue) {
      return ((KeyValue)cell).getLength() + Bytes.SIZEOF_INT;
    }
    // TODO: Should we add to Cell a sizeOf?  Would it help? Does it make sense if Cell is
    // prefix encoded or compressed?
    return getSumOfCellElementLengths(cell) +
      // Use the KeyValue's infrastructure size presuming that another implementation would have
      // same basic cost.
      KeyValue.KEY_INFRASTRUCTURE_SIZE +
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

  public static int estimatedSerializedSizeOfKey(final Cell cell) {
    if (cell instanceof KeyValue) return ((KeyValue)cell).getKeyLength();
    // This will be a low estimate.  Will do for now.
    return getSumOfCellKeyElementLengths(cell);
  }

  /**
   * This is an estimate of the heap space occupied by a cell. When the cell is of type
   * {@link HeapSize} we call {@link HeapSize#heapSize()} so cell can give a correct value. In other
   * cases we just consider the byte occupied by the cell components ie. row, CF, qualifier,
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

  /**
   * This is a hack that should be removed once we don't care about matching
   * up client- and server-side estimations of cell size. It needed to be
   * backwards compatible with estimations done by older clients. We need to
   * pretend that tags never exist and cells aren't serialized with tag
   * length included. See HBASE-13262 and HBASE-13303
   */
  @Deprecated
  public static long estimatedHeapSizeOfWithoutTags(final Cell cell) {
    if (cell instanceof KeyValue) {
      return ((KeyValue)cell).heapSizeWithoutTags();
    }
    return getSumOfCellKeyElementLengths(cell) + cell.getValueLength();
  }

  /********************* tags *************************************/
  /**
   * Util method to iterate through the tags
   *
   * @param tags
   * @param offset
   * @param length
   * @return iterator for the tags
   */
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
          Tag tag = new Tag(tags, pos, curTagLen + Tag.TAG_LENGTH_SIZE);
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
    out.writeShort(rowLen);
    out.write(cell.getRowArray(), cell.getRowOffset(), rowLen);
    byte fLen = cell.getFamilyLength();
    out.writeByte(fLen);
    out.write(cell.getFamilyArray(), cell.getFamilyOffset(), fLen);
    out.write(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
    out.writeLong(cell.getTimestamp());
    out.writeByte(cell.getTypeByte());
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
    sb.append("/vlen=");
    sb.append(cell.getValueLength());
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
      out.write(cell.getRowArray(), cell.getRowOffset() + commonPrefix, rLen - commonPrefix);
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
    int rkCommonPrefix = ByteBufferUtils.findCommonPrefix(c1.getRowArray(), c1.getRowOffset(),
        rLen1, c2.getRowArray(), c2.getRowOffset(), rLen2);
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
      int fCommonPrefix = ByteBufferUtils.findCommonPrefix(c1.getFamilyArray(),
          c1.getFamilyOffset(), fLen1, c2.getFamilyArray(), c2.getFamilyOffset(), fLen2);
      commonPrefix += fCommonPrefix;
      if (fCommonPrefix != fLen1) {
        return commonPrefix;
      }
    }
    // Compare the Qualifiers
    int qLen1 = c1.getQualifierLength();
    int qLen2 = c2.getQualifierLength();
    int qCommon = ByteBufferUtils.findCommonPrefix(c1.getQualifierArray(), c1.getQualifierOffset(),
        qLen1, c2.getQualifierArray(), c2.getQualifierOffset(), qLen2);
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
      tag = Bytes.toStringBinary(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
      value = Bytes.toStringBinary(cell.getValueArray(), cell.getValueOffset(),
        cell.getValueLength());
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

  /**************** equals ****************************/

  public static boolean equals(Cell a, Cell b) {
    return matchingRow(a, b) && matchingFamily(a, b) && matchingQualifier(a, b)
        && matchingTimestamp(a, b) && matchingType(a, b);
  }

  public static boolean matchingType(Cell a, Cell b) {
    return a.getTypeByte() == b.getTypeByte();
  }
}
