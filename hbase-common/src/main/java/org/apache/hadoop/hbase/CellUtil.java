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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.util.ByteRange;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility methods helpful slinging {@link Cell} instances.
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
    return new KeyValue(row, family, qualifier, timestamp,
      KeyValue.Type.codeToType(type), value);
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
        if (this.cellScanner == null) {
          if (!this.iterator.hasNext()) return false;
          this.cellScanner = this.iterator.next().cellScanner();
        }
        if (this.cellScanner.advance()) return true;
        this.cellScanner = null;
        return advance();
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
  public static CellScanner createCellScanner(final NavigableMap<byte [],
      List<Cell>> map) {
    return new CellScanner() {
      private final Iterator<Entry<byte[], List<Cell>>> entries =
          map.entrySet().iterator();
      private Iterator<Cell> currentIterator = null;
      private Cell currentCell;

      @Override
      public Cell current() {
        return this.currentCell;
      }

      @Override
      public boolean advance() {
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
        return advance();
      }
    };
  }

  /**
   * @param left
   * @param right
   * @return True if the rows in <code>left</code> and <code>right</code> Cells match
   */
  public static boolean matchingRow(final Cell left, final Cell right) {
    return Bytes.equals(left.getRowArray(),  left.getRowOffset(), left.getRowLength(),
      right.getRowArray(), right.getRowOffset(), right.getRowLength());
  }

  public static boolean matchingRow(final Cell left, final byte[] buf) {
    return Bytes.equals(left.getRowArray(),  left.getRowOffset(), left.getRowLength(),
      buf, 0, buf.length);
  }

  public static boolean matchingFamily(final Cell left, final Cell right) {
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
        right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
  }

  public static boolean matchingFamily(final Cell left, final byte[] buf) {
    return Bytes.equals(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(),
        buf, 0, buf.length);
  }

  public static boolean matchingQualifier(final Cell left, final Cell right) {
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(), left.getQualifierLength(),
        right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
  }

  public static boolean matchingQualifier(final Cell left, final byte[] buf) {
    return Bytes.equals(left.getQualifierArray(), left.getQualifierOffset(), left.getQualifierLength(),
        buf, 0, buf.length);
  }


  public static boolean matchingValue(final Cell left, final Cell right) {
    return Bytes.equals(left.getValueArray(), left.getValueOffset(), left.getValueLength(),
        right.getValueArray(), right.getValueOffset(), right.getValueLength());
  }

  public static boolean matchingValue(final Cell left, final byte[] buf) {
    return Bytes.equals(left.getValueArray(), left.getValueOffset(), left.getValueLength(),
        buf, 0, buf.length);
  }
  /**
   * @return True if a delete type, a {@link KeyValue.Type#Delete} or
   * a {KeyValue.Type#DeleteFamily} or a {@link KeyValue.Type#DeleteColumn}
   * KeyValue type.
   */
  public static boolean isDelete(final Cell cell) {
    return KeyValue.isDelete(cell.getTypeByte());
  }

  public static boolean isDeleteFamily(final Cell cell) {
    return cell.getTypeByte() == Type.DeleteFamily.getCode();
  }

  /**
   * @param cell
   * @return Estimate of the <code>cell</code> size in bytes.
   */
  public static int estimatedSizeOf(final Cell cell) {
    // If a KeyValue, we can give a good estimate of size.
    if (cell instanceof KeyValue) {
      return ((KeyValue)cell).getLength() + Bytes.SIZEOF_INT;
    }
    // TODO: Should we add to Cell a sizeOf?  Would it help? Does it make sense if Cell is
    // prefix encoded or compressed?
    return cell.getRowLength() + cell.getFamilyLength() +
      cell.getQualifierLength() +
      cell.getValueLength() +
      // Use the KeyValue's infrastructure size presuming that another implementation would have
      // same basic cost.
      KeyValue.KEY_INFRASTRUCTURE_SIZE +
      // Serialization is probably preceded by a length (it is in the KeyValueCodec at least).
      Bytes.SIZEOF_INT;
  }
}
