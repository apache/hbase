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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.function.Function;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

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
    return matchingFamily(left, lfamlength, right, rfamlength);
  }

  public static boolean matchingFamily(final Cell left, final byte lfamlength, final Cell right,
      final byte rfamlength) {
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
    return PrivateCellUtil.matchingFamily(left, buf, 0, buf.length);
  }

  public static boolean matchingQualifier(final Cell left, final Cell right) {
    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();
    return matchingQualifier(left, lqlength, right, rqlength);
  }

  private static boolean matchingQualifier(final Cell left, final int lqlength, final Cell right,
      final int rqlength) {
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
    return PrivateCellUtil.matchingQualifier(left, buf, 0, buf.length);
  }

  public static boolean matchingColumn(final Cell left, final byte[] fam, final byte[] qual) {
    return matchingFamily(left, fam) && matchingQualifier(left, qual);
  }

  /**
   * @return True if matching column family and the qualifier starts with <code>qual</code>
   */
  public static boolean matchingColumnFamilyAndQualifierPrefix(final Cell left, final byte[] fam,
      final byte[] qual) {
    return matchingFamily(left, fam) && PrivateCellUtil.qualifierStartsWith(left, qual);
  }

  public static boolean matchingColumn(final Cell left, final Cell right) {
    if (!matchingFamily(left, right))
      return false;
    return matchingQualifier(left, right);
  }

  private static boolean matchingColumn(final Cell left, final byte lFamLen, final int lQualLength,
      final Cell right, final byte rFamLen, final int rQualLength) {
    if (!matchingFamily(left, lFamLen, right, rFamLen)) {
      return false;
    }
    return matchingQualifier(left, lQualLength, right, rQualLength);
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

  public static boolean matchingTags(final Cell left, final Cell right) {
    return PrivateCellUtil.matchingTags(left, right, left.getTagsLength(), right.getTagsLength());
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
   * @return True if this cell is a Put.
   */
  @SuppressWarnings("deprecation")
  public static boolean isPut(Cell cell) {
    return cell.getTypeByte() == Type.Put.getCode();
  }

  /**
   * Sets the given timestamp to the cell.
   *
   * Note that this method is a LimitedPrivate API and may change between minor releases.
   * @param cell
   * @param ts
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
  public static void setTimestamp(Cell cell, long ts) throws IOException {
    PrivateCellUtil.setTimestamp(cell, ts);
  }

  /**
   * Sets the given timestamp to the cell.
   *
   * Note that this method is a LimitedPrivate API and may change between minor releases.
   * @param cell
   * @param ts buffer containing the timestamp value
   * @param tsOffset offset to the new timestamp
   * @throws IOException when the passed cell is not of type {@link ExtendedCell}
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
  public static void setTimestamp(Cell cell, byte[] ts, int tsOffset) throws IOException {
    PrivateCellUtil.setTimestamp(cell, Bytes.toLong(ts, tsOffset));
  }

  /**
   * @return The Key portion of the passed <code>cell</code> as a String.
   */
  public static String getCellKeyAsString(Cell cell) {
    return getCellKeyAsString(cell,
      c -> Bytes.toStringBinary(c.getRowArray(), c.getRowOffset(), c.getRowLength()));
  }

  /**
   * @param cell the cell to convert
   * @param rowConverter used to convert the row of the cell to a string
   * @return The Key portion of the passed <code>cell</code> as a String.
   */
  public static String getCellKeyAsString(Cell cell, Function<Cell, String> rowConverter) {
    StringBuilder sb = new StringBuilder(rowConverter.apply(cell));
    sb.append('/');
    sb.append(cell.getFamilyLength() == 0 ? "" :
      Bytes.toStringBinary(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
    // KeyValue only added ':' if family is non-null. Do same.
    if (cell.getFamilyLength() > 0) sb.append(':');
    sb.append(cell.getQualifierLength() == 0 ? "" :
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

  /**************** equals ****************************/

  public static boolean equals(Cell a, Cell b) {
    return matchingRows(a, b) && matchingFamily(a, b) && matchingQualifier(a, b)
        && matchingTimestamp(a, b) && PrivateCellUtil.matchingType(a, b);
  }

  public static boolean matchingTimestamp(Cell a, Cell b) {
    return CellComparator.getInstance().compareTimestamps(a.getTimestamp(), b.getTimestamp()) == 0;
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
    return matchingRows(left, lrowlength, right, rrowlength);
  }

  public static boolean matchingRows(final Cell left, final short lrowlength, final Cell right,
      final short rrowlength) {
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
    short lrowlength = left.getRowLength();
    short rrowlength = right.getRowLength();
    // match length
    if (lrowlength != rrowlength) {
      return false;
    }

    byte lfamlength = left.getFamilyLength();
    byte rfamlength = right.getFamilyLength();
    if (lfamlength != rfamlength) {
      return false;
    }

    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();
    if (lqlength != rqlength) {
      return false;
    }

    if (!matchingRows(left, lrowlength, right, rrowlength)) {
      return false;
    }
    return matchingColumn(left, lfamlength, lqlength, right, rfamlength, rqlength);
  }

  public static boolean matchingRowColumnBytes(final Cell left, final Cell right) {
    int lrowlength = left.getRowLength();
    int rrowlength = right.getRowLength();
    int lfamlength = left.getFamilyLength();
    int rfamlength = right.getFamilyLength();
    int lqlength = left.getQualifierLength();
    int rqlength = right.getQualifierLength();

    // match length
    if ((lrowlength != rrowlength) || (lfamlength != rfamlength) || (lqlength != rqlength)) {
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

  public static void cloneIfNecessary(ArrayList<Cell> cells) {
    if (cells == null || cells.isEmpty()) {
      return;
    }
    for (int i = 0; i < cells.size(); i++) {
      Cell cell = cells.get(i);
      cells.set(i, cloneIfNecessary(cell));
    }
  }

  public static Cell cloneIfNecessary(Cell cell) {
    return (cell instanceof ByteBufferExtendedCell ? KeyValueUtil.copyToNewKeyValue(cell) : cell);
  }
}
