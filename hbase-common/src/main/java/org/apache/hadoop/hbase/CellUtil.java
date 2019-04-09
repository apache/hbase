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

import static org.apache.hadoop.hbase.KeyValue.COLUMN_FAMILY_DELIMITER;
import static org.apache.hadoop.hbase.KeyValue.COLUMN_FAMILY_DELIM_ARRAY;
import static org.apache.hadoop.hbase.KeyValue.getDelimiter;
import static org.apache.hadoop.hbase.Tag.TAG_LENGTH_SIZE;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience.Private;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Utility methods helpful for slinging {@link Cell} instances. Some methods below are for internal
 * use only and are marked InterfaceAudience.Private at the method level. Note that all such methods
 * have been marked deprecated in HBase-2.0 which will be subsequently removed in HBase-3.0
 */
@InterfaceAudience.Public
public final class CellUtil {

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private CellUtil() {
  }

  /******************* ByteRange *******************************/

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillRowRange(Cell cell, ByteRange range) {
    return PrivateCellUtil.fillRowRange(cell, range);
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillFamilyRange(Cell cell, ByteRange range) {
    return PrivateCellUtil.fillFamilyRange(cell, range);
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillQualifierRange(Cell cell, ByteRange range) {
    return PrivateCellUtil.fillQualifierRange(cell, range);
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillValueRange(Cell cell, ByteRange range) {
    return PrivateCellUtil.fillValueRange(cell, range);
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static ByteRange fillTagRange(Cell cell, ByteRange range) {
    return PrivateCellUtil.fillTagRange(cell, range);
  }

  /***************** get individual arrays for tests ************/

  public static byte[] cloneRow(Cell cell) {
    byte[] output = new byte[cell.getRowLength()];
    copyRowTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneFamily(Cell cell) {
    byte[] output = new byte[cell.getFamilyLength()];
    copyFamilyTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneQualifier(Cell cell) {
    byte[] output = new byte[cell.getQualifierLength()];
    copyQualifierTo(cell, output, 0);
    return output;
  }

  public static byte[] cloneValue(Cell cell) {
    byte[] output = new byte[cell.getValueLength()];
    copyValueTo(cell, output, 0);
    return output;
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   *             Use {@link RawCell#cloneTags()}
   */
  @Deprecated
  public static byte[] cloneTags(Cell cell) {
    return PrivateCellUtil.cloneTags(cell);
  }

  /**
   * Returns tag value in a new byte array. If server-side, use {@link Tag#getValueArray()} with
   * appropriate {@link Tag#getValueOffset()} and {@link Tag#getValueLength()} instead to save on
   * allocations.
   * @param cell
   * @return tag value in a new byte array.
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static byte[] getTagArray(Cell cell) {
    return PrivateCellUtil.cloneTags(cell);
  }

  /**
   * Makes a column in family:qualifier form from separate byte arrays.
   * <p>
   * Not recommended for usage as this is old-style API.
   * @param family
   * @param qualifier
   * @return family:qualifier
   */
  public static byte[] makeColumn(byte[] family, byte[] qualifier) {
    return Bytes.add(family, COLUMN_FAMILY_DELIM_ARRAY, qualifier);
  }

  /**
   * Splits a column in {@code family:qualifier} form into separate byte arrays. An empty qualifier
   * (ie, {@code fam:}) is parsed as <code>{ fam, EMPTY_BYTE_ARRAY }</code> while no delimiter (ie,
   * {@code fam}) is parsed as an array of one element, <code>{ fam }</code>.
   * <p>
   * Don't forget, HBase DOES support empty qualifiers. (see HBASE-9549)
   * </p>
   * <p>
   * Not recommend to be used as this is old-style API.
   * </p>
   * @param c The column.
   * @return The parsed column.
   */
  public static byte[][] parseColumn(byte[] c) {
    final int index = getDelimiter(c, 0, c.length, COLUMN_FAMILY_DELIMITER);
    if (index == -1) {
      // If no delimiter, return array of size 1
      return new byte[][] { c };
    } else if (index == c.length - 1) {
      // family with empty qualifier, return array size 2
      byte[] family = new byte[c.length - 1];
      System.arraycopy(c, 0, family, 0, family.length);
      return new byte[][] { family, HConstants.EMPTY_BYTE_ARRAY };
    }
    // Family and column, return array size 2
    final byte[][] result = new byte[2][];
    result[0] = new byte[index];
    System.arraycopy(c, 0, result[0], 0, index);
    final int len = c.length - (index + 1);
    result[1] = new byte[len];
    System.arraycopy(c, index + 1 /* Skip delimiter */, result[1], 0, len);
    return result;
  }

  /******************** copyTo **********************************/

  /**
   * Copies the row to the given byte[]
   * @param cell the cell whose row has to be copied
   * @param destination the destination byte[] to which the row has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the byte[] after the copy has happened
   */
  public static int copyRowTo(Cell cell, byte[] destination, int destinationOffset) {
    short rowLen = cell.getRowLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferExtendedCell) cell).getRowByteBuffer(),
          ((ByteBufferExtendedCell) cell).getRowPosition(), destinationOffset, rowLen);
    } else {
      System.arraycopy(cell.getRowArray(), cell.getRowOffset(), destination, destinationOffset,
        rowLen);
    }
    return destinationOffset + rowLen;
  }

  /**
   * Copies the row to the given bytebuffer
   * @param cell cell the cell whose row has to be copied
   * @param destination the destination bytebuffer to which the row has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the bytebuffer after the copy has happened
   */
  public static int copyRowTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    short rowLen = cell.getRowLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferExtendedCell) cell).getRowByteBuffer(),
        destination, ((ByteBufferExtendedCell) cell).getRowPosition(), destinationOffset, rowLen);
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
    if (cell instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.copyOfRange(((ByteBufferExtendedCell) cell).getRowByteBuffer(),
        ((ByteBufferExtendedCell) cell).getRowPosition(),
        ((ByteBufferExtendedCell) cell).getRowPosition() + cell.getRowLength());
    } else {
      return Arrays.copyOfRange(cell.getRowArray(), cell.getRowOffset(),
        cell.getRowOffset() + cell.getRowLength());
    }
  }

  /**
   * Copies the family to the given byte[]
   * @param cell the cell whose family has to be copied
   * @param destination the destination byte[] to which the family has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the byte[] after the copy has happened
   */
  public static int copyFamilyTo(Cell cell, byte[] destination, int destinationOffset) {
    byte fLen = cell.getFamilyLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) cell).getFamilyPosition(), destinationOffset, fLen);
    } else {
      System.arraycopy(cell.getFamilyArray(), cell.getFamilyOffset(), destination,
        destinationOffset, fLen);
    }
    return destinationOffset + fLen;
  }

  /**
   * Copies the family to the given bytebuffer
   * @param cell the cell whose family has to be copied
   * @param destination the destination bytebuffer to which the family has to be copied
   * @param destinationOffset the offset in the destination bytebuffer
   * @return the offset of the bytebuffer after the copy has happened
   */
  public static int copyFamilyTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    byte fLen = cell.getFamilyLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferExtendedCell) cell).getFamilyByteBuffer(),
        destination, ((ByteBufferExtendedCell) cell).getFamilyPosition(), destinationOffset, fLen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset, cell.getFamilyArray(),
        cell.getFamilyOffset(), fLen);
    }
    return destinationOffset + fLen;
  }

  /**
   * Copies the qualifier to the given byte[]
   * @param cell the cell whose qualifier has to be copied
   * @param destination the destination byte[] to which the qualifier has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the byte[] after the copy has happened
   */
  public static int copyQualifierTo(Cell cell, byte[] destination, int destinationOffset) {
    int qlen = cell.getQualifierLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
        ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
        ((ByteBufferExtendedCell) cell).getQualifierPosition(), destinationOffset, qlen);
    } else {
      System.arraycopy(cell.getQualifierArray(), cell.getQualifierOffset(), destination,
        destinationOffset, qlen);
    }
    return destinationOffset + qlen;
  }

  /**
   * Copies the qualifier to the given bytebuffer
   * @param cell the cell whose qualifier has to be copied
   * @param destination the destination bytebuffer to which the qualifier has to be copied
   * @param destinationOffset the offset in the destination bytebuffer
   * @return the offset of the bytebuffer after the copy has happened
   */
  public static int copyQualifierTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    int qlen = cell.getQualifierLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          destination, ((ByteBufferExtendedCell) cell).getQualifierPosition(),
          destinationOffset, qlen);
    } else {
      ByteBufferUtils.copyFromArrayToBuffer(destination, destinationOffset,
        cell.getQualifierArray(), cell.getQualifierOffset(), qlen);
    }
    return destinationOffset + qlen;
  }

  /**
   * Copies the value to the given byte[]
   * @param cell the cell whose value has to be copied
   * @param destination the destination byte[] to which the value has to be copied
   * @param destinationOffset the offset in the destination byte[]
   * @return the offset of the byte[] after the copy has happened
   */
  public static int copyValueTo(Cell cell, byte[] destination, int destinationOffset) {
    int vlen = cell.getValueLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToArray(destination,
          ((ByteBufferExtendedCell) cell).getValueByteBuffer(),
          ((ByteBufferExtendedCell) cell).getValuePosition(), destinationOffset, vlen);
    } else {
      System.arraycopy(cell.getValueArray(), cell.getValueOffset(), destination, destinationOffset,
        vlen);
    }
    return destinationOffset + vlen;
  }

  /**
   * Copies the value to the given bytebuffer
   * @param cell the cell whose value has to be copied
   * @param destination the destination bytebuffer to which the value has to be copied
   * @param destinationOffset the offset in the destination bytebuffer
   * @return the offset of the bytebuffer after the copy has happened
   */
  public static int copyValueTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    int vlen = cell.getValueLength();
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyFromBufferToBuffer(((ByteBufferExtendedCell) cell).getValueByteBuffer(),
        destination, ((ByteBufferExtendedCell) cell).getValuePosition(), destinationOffset, vlen);
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
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static int copyTagTo(Cell cell, byte[] destination, int destinationOffset) {
    return PrivateCellUtil.copyTagsTo(cell, destination, destinationOffset);
  }

  /**
   * Copies the tags info into the tag portion of the cell
   * @param cell
   * @param destination
   * @param destinationOffset
   * @return position after tags
   * @deprecated As of HBase-2.0. Will be removed in 3.0.
   */
  @Deprecated
  public static int copyTagTo(Cell cell, ByteBuffer destination, int destinationOffset) {
    return PrivateCellUtil.copyTagsTo(cell, destination, destinationOffset);
  }

  /********************* misc *************************************/

  @Private
  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0.
   */
  @Deprecated
  public static byte getRowByte(Cell cell, int index) {
    return PrivateCellUtil.getRowByte(cell, index);
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in 3.0.
   */
  @Deprecated
  public static ByteBuffer getValueBufferShallowCopy(Cell cell) {
    return PrivateCellUtil.getValueBufferShallowCopy(cell);
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, final byte type, final byte[] value) {
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] rowArray, final int rowOffset, final int rowLength,
      final byte[] familyArray, final int familyOffset, final int familyLength,
      final byte[] qualifierArray, final int qualifierOffset, final int qualifierLength) {
    // See createCell(final byte [] row, final byte [] value) for why we default Maximum type.
    return ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY)
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
   * Creating a Cell with a memstoreTS/mvcc is an internal
   * implementation detail not for public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use
   *             {@link ExtendedCellBuilder} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, final byte type, final byte[] value, final long memstoreTS) {
    return createCell(row, family, qualifier, timestamp, type, value, null, memstoreTS);
  }

  /**
   * Marked as audience Private as of 1.2.0.
   * Creating a Cell with tags and a memstoreTS/mvcc is an
   * internal implementation detail not for public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use
   *             {@link ExtendedCellBuilder} instead
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
   * Creating a Cell with tags is an internal implementation detail not for public use.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use
   *             {@link ExtendedCellBuilder} instead
   */
  @InterfaceAudience.Private
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] family, final byte[] qualifier,
      final long timestamp, Type type, final byte[] value, byte[] tags) {
    return createCell(row, family, qualifier, timestamp, type.getCode(), value, tags, 0);
  }

  /**
   * Create a Cell with specific row. Other fields defaulted.
   * @param row
   * @return Cell with passed row but all other fields are arbitrary
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] row) {
    return createCell(row, HConstants.EMPTY_BYTE_ARRAY);
  }

  /**
   * Create a Cell with specific row and value. Other fields are defaulted.
   * @param row
   * @param value
   * @return Cell with passed row and value but all other fields are arbitrary
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0. Use {@link CellBuilder}
   *             instead
   */
  @Deprecated
  public static Cell createCell(final byte[] row, final byte[] value) {
    // An empty family + empty qualifier + Type.Minimum is used as flag to indicate last on row.
    // See the CellComparator and KeyValue comparator. Search for compareWithoutRow.
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
   * Note : Now only CPs can create cell with tags using the CP environment
   * @return A new cell which is having the extra tags also added to it.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *
   */
  @Deprecated
  public static Cell createCell(Cell cell, List<Tag> tags) {
    return PrivateCellUtil.createCell(cell, tags);
  }

  /**
   * Now only CPs can create cell with tags using the CP environment
   * @return A new cell which is having the extra tags also added to it.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static Cell createCell(Cell cell, byte[] tags) {
    return PrivateCellUtil.createCell(cell, tags);
  }

  /**
   * Now only CPs can create cell with tags using the CP environment
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static Cell createCell(Cell cell, byte[] value, byte[] tags) {
    return PrivateCellUtil.createCell(cell, value, tags);
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

  /**
   *  @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Instead use {@link #matchingRows(Cell, byte[])}
   */
  @Deprecated
  public static boolean matchingRow(final Cell left, final byte[] buf) {
    return matchingRows(left, buf);
  }

  public static boolean matchingRows(final Cell left, final byte[] buf) {
    if (buf == null) {
      return left.getRowLength() == 0;
    }
    return PrivateCellUtil.matchingRows(left, buf, 0, buf.length);
  }

  public static boolean matchingRow(final Cell left, final byte[] buf, final int offset,
      final int length) {
    return PrivateCellUtil.matchingRows(left, buf, offset, length);
  }

  public static boolean matchingFamily(final Cell left, final Cell right) {
    byte lfamlength = left.getFamilyLength();
    byte rfamlength = right.getFamilyLength();
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), lfamlength,
          ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), rfamlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) left).getFamilyPosition(), lfamlength,
          right.getFamilyArray(), right.getFamilyOffset(), rfamlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
          ((ByteBufferExtendedCell) right).getFamilyPosition(), rfamlength,
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

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean matchingFamily(final Cell left, final byte[] buf, final int offset,
      final int length) {
    return PrivateCellUtil.matchingFamily(left, buf, offset, length);
  }

  public static boolean matchingQualifier(final Cell left, final Cell right) {
    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), lqlength,
          ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) right).getQualifierPosition(), rqlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(), lqlength,
          right.getQualifierArray(), right.getQualifierOffset(), rqlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) right).getQualifierPosition(), rqlength,
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean matchingQualifier(final Cell left, final byte[] buf, final int offset,
      final int length) {
    return PrivateCellUtil.matchingQualifier(left, buf, offset, length);
  }

  public static boolean matchingColumn(final Cell left, final byte[] fam, final byte[] qual) {
    if (!matchingFamily(left, fam))
      return false;
    return matchingQualifier(left, qual);
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean matchingColumn(final Cell left, final byte[] fam, final int foffset,
      final int flength, final byte[] qual, final int qoffset, final int qlength) {
    return PrivateCellUtil.matchingColumn(left, fam, foffset, flength, qual, qoffset, qlength);
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
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getValueByteBuffer(),
        ((ByteBufferExtendedCell) left).getValuePosition(), lvlength,
        ((ByteBufferExtendedCell) right).getValueByteBuffer(),
        ((ByteBufferExtendedCell) right).getValuePosition(), rvlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getValueByteBuffer(),
        ((ByteBufferExtendedCell) left).getValuePosition(), lvlength, right.getValueArray(),
        right.getValueOffset(), rvlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) right).getValueByteBuffer(),
        ((ByteBufferExtendedCell) right).getValuePosition(), rvlength, left.getValueArray(),
        left.getValueOffset(), lvlength);
    }
    return Bytes.equals(left.getValueArray(), left.getValueOffset(), lvlength,
      right.getValueArray(), right.getValueOffset(), rvlength);
  }

  public static boolean matchingValue(final Cell left, final byte[] buf) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getValueByteBuffer(),
          ((ByteBufferExtendedCell) left).getValuePosition(), left.getValueLength(), buf, 0,
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
  @SuppressWarnings("deprecation")
  public static boolean isDelete(final Cell cell) {
    return PrivateCellUtil.isDelete(cell.getTypeByte());
  }

  /**
   * @return True if a delete type, a {@link KeyValue.Type#Delete} or a
   *         {KeyValue.Type#DeleteFamily} or a
   *         {@link KeyValue.Type#DeleteColumn} KeyValue type.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDelete(final byte type) {
    return Type.Delete.getCode() <= type
        && type <= Type.DeleteFamily.getCode();
  }

  /**
   * @return True if this cell is a {@link KeyValue.Type#Delete} type.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteType(Cell cell) {
    return cell.getTypeByte() == Type.Delete.getCode();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteFamily(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteFamily.getCode();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteFamilyVersion(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteFamilyVersion.getCode();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteColumns(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteColumn.getCode();
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteColumnVersion(final Cell cell) {
    return cell.getTypeByte() == Type.Delete.getCode();
  }

  /**
   *
   * @return True if this cell is a delete family or column type.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean isDeleteColumnOrFamily(Cell cell) {
    int t = cell.getTypeByte();
    return t == Type.DeleteColumn.getCode() || t == Type.DeleteFamily.getCode();
  }

  /**
   * @return True if this cell is a Put.
   */
  @SuppressWarnings("deprecation")
  public static boolean isPut(Cell cell) {
    return cell.getTypeByte() == Type.Put.getCode();
  }

  /**
   * Estimate based on keyvalue's serialization format in the RPC layer. Note that there is an extra
   * SIZEOF_INT added to the size here that indicates the actual length of the cell for cases where
   * cell's are serialized in a contiguous format (For eg in RPCs).
   * @param cell
   * @return Estimate of the <code>cell</code> size in bytes plus an extra SIZEOF_INT indicating the
   *         actual cell length.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int estimatedSerializedSizeOf(final Cell cell) {
    return PrivateCellUtil.estimatedSerializedSizeOf(cell);
  }

  /**
   * Calculates the serialized key size. We always serialize in the KeyValue's serialization
   * format.
   * @param cell the cell for which the key size has to be calculated.
   * @return the key size
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int estimatedSerializedSizeOfKey(final Cell cell) {
    return PrivateCellUtil.estimatedSerializedSizeOfKey(cell);
  }

  /**
   * This is an estimate of the heap space occupied by a cell. When the cell is of type
   * {@link HeapSize} we call {@link HeapSize#heapSize()} so cell can give a correct value. In other
   * cases we just consider the bytes occupied by the cell components ie. row, CF, qualifier,
   * timestamp, type, value and tags.
   * @param cell
   * @return estimate of the heap space
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static long estimatedHeapSizeOf(final Cell cell) {
    return cell.heapSize();
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

  /**
   * @param cell The Cell
   * @return Tags in the given Cell as a List
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link RawCell#getTags()}
   */
  @Deprecated
  public static List<Tag> getTags(Cell cell) {
    return PrivateCellUtil.getTags(cell);
  }

  /**
   * Retrieve Cell's first tag, matching the passed in type
   *
   * @param cell The Cell
   * @param type Type of the Tag to retrieve
   * @return null if there is no tag of the passed in tag type
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   *             Use {@link RawCell#getTag(byte)}
   */
  @Deprecated
  public static Tag getTag(Cell cell, byte type) {
    Optional<Tag> tag = PrivateCellUtil.getTag(cell, type);
    if (tag.isPresent()) {
      return tag.get();
    } else {
      return null;
    }
  }

  /**
   * Returns true if the first range start1...end1 overlaps with the second range
   * start2...end2, assuming the byte arrays represent row keys
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean overlappingKeys(final byte[] start1, final byte[] end1,
      final byte[] start2, final byte[] end2) {
    return PrivateCellUtil.overlappingKeys(start1, end1, start2, end2);
  }

  /**
   * Sets the given seqId to the cell.
   * Marked as audience Private as of 1.2.0.
   * Setting a Cell sequenceid is an internal implementation detail not for general public use.
   * @param cell
   * @param seqId
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static void setSequenceId(Cell cell, long seqId) throws IOException {
    PrivateCellUtil.setSequenceId(cell, seqId);
  }

  /**
   * Sets the given timestamp to the cell.
   * @param cell
   * @param ts
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static void setTimestamp(Cell cell, long ts) throws IOException {
    PrivateCellUtil.setTimestamp(cell, ts);
  }

  /**
   * Sets the given timestamp to the cell.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @param tsOffset offset to the new timestamp
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static void setTimestamp(Cell cell, byte[] ts, int tsOffset) throws IOException {
    PrivateCellUtil.setTimestamp(cell, Bytes.toLong(ts, tsOffset));
  }

  /**
   * Sets the given timestamp to the cell iff current timestamp is
   * {@link HConstants#LATEST_TIMESTAMP}.
   * @param cell
   * @param ts
   * @return True if cell timestamp is modified.
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean updateLatestStamp(Cell cell, long ts) throws IOException {
    return PrivateCellUtil.updateLatestStamp(cell, ts);
  }

  /**
   * Sets the given timestamp to the cell iff current timestamp is
   * {@link HConstants#LATEST_TIMESTAMP}.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @param tsOffset offset to the new timestamp
   * @return True if cell timestamp is modified.
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static boolean updateLatestStamp(Cell cell, byte[] ts, int tsOffset) throws IOException {
    return PrivateCellUtil.updateLatestStamp(cell, Bytes.toLong(ts, tsOffset));
  }


  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static int writeFlatKey(Cell cell, OutputStream out) throws IOException {
    return PrivateCellUtil.writeFlatKey(cell, out);
  }

  /**
   * Writes the row from the given cell to the output stream excluding the common prefix
   * @param out The dataoutputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param rlength the row length
   * @throws IOException
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static void writeRowSkippingBytes(DataOutputStream out, Cell cell, short rlength,
      int commonPrefix) throws IOException {
    PrivateCellUtil.writeRowSkippingBytes(out, cell, rlength, commonPrefix);
  }

  /**
   * Writes the qualifier from the given cell to the output stream excluding the common prefix
   * @param out The dataoutputstream to which the data has to be written
   * @param cell The cell whose contents has to be written
   * @param qlength the qualifier length
   * @throws IOException
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static void writeQualifierSkippingBytes(DataOutputStream out, Cell cell,
      int qlength, int commonPrefix) throws IOException {
    if (cell instanceof ByteBufferExtendedCell) {
      ByteBufferUtils.copyBufferToStream((DataOutput)out,
          ((ByteBufferExtendedCell) cell).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) cell).getQualifierPosition() + commonPrefix,
          qlength - commonPrefix);
    } else {
      out.write(cell.getQualifierArray(), cell.getQualifierOffset() + commonPrefix,
        qlength - commonPrefix);
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static byte [] getCellKeySerializedAsKeyValueKey(final Cell cell) {
    return PrivateCellUtil.getCellKeySerializedAsKeyValueKey(cell);
  }

  /**
   * Write rowkey excluding the common part.
   * @param cell
   * @param rLen
   * @param commonPrefix
   * @param out
   * @throws IOException
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
   */
  @Deprecated
  public static void writeRowKeyExcludingCommon(Cell cell, short rLen, int commonPrefix,
      DataOutputStream out) throws IOException {
    PrivateCellUtil.writeRowKeyExcludingCommon(cell, rLen, commonPrefix, out);
  }

  /**
   * Find length of common prefix in keys of the cells, considering key as byte[] if serialized in
   * {@link KeyValue}. The key format is &lt;2 bytes rk len&gt;&lt;rk&gt;&lt;1 byte cf
   * len&gt;&lt;cf&gt;&lt;qualifier&gt;&lt;8 bytes timestamp&gt;&lt;1 byte type&gt;
   * @param c1 the cell
   * @param c2 the cell
   * @param bypassFamilyCheck when true assume the family bytes same in both cells. Pass it as true
   *          when dealing with Cells in same CF so as to avoid some checks
   * @param withTsType when true check timestamp and type bytes also.
   * @return length of common prefix
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static int findCommonPrefixInFlatKey(Cell c1, Cell c2, boolean bypassFamilyCheck,
      boolean withTsType) {
    return PrivateCellUtil.findCommonPrefixInFlatKey(c1, c2, bypassFamilyCheck, withTsType);
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

    builder.append(keyStr);
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
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static boolean equalsIgnoreMvccVersion(Cell a, Cell b) {
    return PrivateCellUtil.equalsIgnoreMvccVersion(a, b);
  }

  /**************** equals ****************************/

  public static boolean equals(Cell a, Cell b) {
    return matchingRows(a, b) && matchingFamily(a, b) && matchingQualifier(a, b)
        && matchingTimestamp(a, b) && PrivateCellUtil.matchingType(a, b);
  }

  public static boolean matchingTimestamp(Cell a, Cell b) {
    return CellComparator.getInstance().compareTimestamps(a.getTimestamp(), b.getTimestamp()) == 0;
  }

  /**
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @Deprecated
  public static boolean matchingType(Cell a, Cell b) {
    return PrivateCellUtil.matchingType(a, b);
  }

  /**
   * Compares the row of two keyvalues for equality
   * @param left
   * @param right
   * @return True if rows match.
   */
  public static boolean matchingRows(final Cell left, final Cell right) {
    short lrowlength = left.getRowLength();
    short rrowlength = right.getRowLength();
    if (lrowlength != rrowlength) return false;
    if (left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), lrowlength,
          ((ByteBufferExtendedCell) right).getRowByteBuffer(),
          ((ByteBufferExtendedCell) right).getRowPosition(), rrowlength);
    }
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) left).getRowByteBuffer(),
          ((ByteBufferExtendedCell) left).getRowPosition(), lrowlength, right.getRowArray(),
          right.getRowOffset(), rrowlength);
    }
    if (right instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.equals(((ByteBufferExtendedCell) right).getRowByteBuffer(),
          ((ByteBufferExtendedCell) right).getRowPosition(), rrowlength, left.getRowArray(),
          left.getRowOffset(), lrowlength);
    }
    return Bytes.equals(left.getRowArray(), left.getRowOffset(), lrowlength, right.getRowArray(),
        right.getRowOffset(), rrowlength);
  }

  /**
   * Compares the row and column of two keyvalues for equality
   * @param left
   * @param right
   * @return True if same row and column.
   */
  public static boolean matchingRowColumn(final Cell left, final Cell right) {
    if ((left.getRowLength() + left.getFamilyLength()
        + left.getQualifierLength()) != (right.getRowLength() + right.getFamilyLength()
            + right.getQualifierLength())) {
      return false;
    }

    if (!matchingRows(left, right)) {
      return false;
    }
    return matchingColumn(left, right);
  }

  public static boolean matchingRowColumnBytes(final Cell left, final Cell right) {
    int lrowlength = left.getRowLength();
    int rrowlength = right.getRowLength();
    int lfamlength = left.getFamilyLength();
    int rfamlength = right.getFamilyLength();
    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();
    // match length
    if ((lrowlength + lfamlength + lqlength) !=
        (rrowlength + rfamlength + rqlength)) {
      return false;
    }

    // match row
    if (!Bytes.equals(left.getRowArray(), left.getRowOffset(), lrowlength, right.getRowArray(),
        right.getRowOffset(), rrowlength)) {
      return false;
    }
    //match family
    if (!Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), lfamlength,
        right.getFamilyArray(), right.getFamilyOffset(), rfamlength)) {
      return false;
    }
    //match qualifier
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(),
        lqlength, right.getQualifierArray(), right.getQualifierOffset(),
        rqlength);
  }

  /**
   * Compares the cell's qualifier with the given byte[]
   * @param left the cell for which the qualifier has to be compared
   * @param right the byte[] having the qualifier
   * @param rOffset the offset of the qualifier
   * @param rLength the length of the qualifier
   * @return greater than 0 if left cell's qualifier is bigger than byte[], lesser than 0 if left
   *         cell's qualifier is lesser than byte[] and 0 otherwise
   */
  public final static int compareQualifiers(Cell left, byte[] right, int rOffset, int rLength) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
          ((ByteBufferExtendedCell) left).getQualifierPosition(),
          left.getQualifierLength(), right, rOffset, rLength);
    }
    return Bytes.compareTo(left.getQualifierArray(), left.getQualifierOffset(),
      left.getQualifierLength(), right, rOffset, rLength);
  }

  /**
   * Used when a cell needs to be compared with a key byte[] such as cases of finding the index from
   * the index block, bloom keys from the bloom blocks This byte[] is expected to be serialized in
   * the KeyValue serialization format If the KeyValue (Cell's) serialization format changes this
   * method cannot be used.
   * @param comparator the cell comparator
   * @param left the cell to be compared
   * @param key the serialized key part of a KeyValue
   * @param offset the offset in the key byte[]
   * @param length the length of the key byte[]
   * @return an int greater than 0 if left is greater than right lesser than 0 if left is lesser
   *         than right equal to 0 if left is equal to right
   * @deprecated As of HBase-2.0. Will be removed in HBase-3.0
   */
  @VisibleForTesting
  @Deprecated
  public static final int compare(CellComparator comparator, Cell left, byte[] key, int offset,
      int length) {
    // row
    short rrowlength = Bytes.toShort(key, offset);
    int c = comparator.compareRows(left, key, offset + Bytes.SIZEOF_SHORT, rrowlength);
    if (c != 0) return c;

    // Compare the rest of the two KVs without making any assumptions about
    // the common prefix. This function will not compare rows anyway, so we
    // don't need to tell it that the common prefix includes the row.
    return PrivateCellUtil.compareWithoutRow(comparator, left, key, offset, length, rrowlength);
  }

  /**
   * Compares the cell's family with the given byte[]
   * @param left the cell for which the family has to be compared
   * @param right the byte[] having the family
   * @param roffset the offset of the family
   * @param rlength the length of the family
   * @return greater than 0 if left cell's family is bigger than byte[], lesser than 0 if left
   *         cell's family is lesser than byte[] and 0 otherwise
   */
  public final static int compareFamilies(Cell left, byte[] right, int roffset, int rlength) {
    if (left instanceof ByteBufferExtendedCell) {
      return ByteBufferUtils.compareTo(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
        ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(), right, roffset,
        rlength);
    }
    return Bytes.compareTo(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
      right, roffset, rlength);
  }

  /**
   * Compares the cell's column (family and qualifier) with the given byte[]
   * @param left the cell for which the column has to be compared
   * @param right the byte[] having the column
   * @param rfoffset the offset of the family
   * @param rflength the length of the family
   * @param rqoffset the offset of the qualifier
   * @param rqlength the length of the qualifier
   * @return greater than 0 if left cell's column is bigger than byte[], lesser than 0 if left
   *         cell's column is lesser than byte[] and 0 otherwise
   */
  public final static int compareColumns(Cell left, byte[] right, int rfoffset, int rflength,
      int rqoffset, int rqlength) {
    int diff = compareFamilies(left, right, rfoffset, rflength);
    if (diff != 0) return diff;
    return compareQualifiers(left, right, rqoffset, rqlength);
  }
}
