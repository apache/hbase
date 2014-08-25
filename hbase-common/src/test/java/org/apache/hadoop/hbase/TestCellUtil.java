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
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCellUtil {
  /**
   * CellScannable used in test. Returns a {@link TestCellScanner}
   */
  private class TestCellScannable implements CellScannable {
    private final int cellsCount;
    TestCellScannable(final int cellsCount) {
      this.cellsCount = cellsCount;
    }
    @Override
    public CellScanner cellScanner() {
      return new TestCellScanner(this.cellsCount);
    }
  };

  /**
   * CellScanner used in test.
   */
  private class TestCellScanner implements CellScanner {
    private int count = 0;
    private Cell current = null;
    private final int cellsCount;

    TestCellScanner(final int cellsCount) {
      this.cellsCount = cellsCount;
    }

    @Override
    public Cell current() {
      return this.current;
    }

    @Override
    public boolean advance() throws IOException {
      if (this.count < cellsCount) {
        this.current = new TestCell(this.count);
        this.count++;
        return true;
      }
      return false;
    }
  }

  /**
   * Cell used in test. Has row only.
   */
  private class TestCell implements Cell {
    private final byte [] row;

    TestCell(final int i) {
      this.row = Bytes.toBytes(i);
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
      return (short)this.row.length;
    }

    @Override
    public byte[] getFamilyArray() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getFamilyOffset() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public byte getFamilyLength() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public byte[] getQualifierArray() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getQualifierOffset() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int getQualifierLength() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getTimestamp() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public byte getTypeByte() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public long getMvccVersion() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public byte[] getValueArray() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getValueOffset() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int getValueLength() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public byte[] getTagsArray() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public int getTagsOffset() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public short getTagsLength() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int getTagsLengthUnsigned() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public byte[] getValue() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public byte[] getFamily() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public byte[] getQualifier() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public byte[] getRow() {
      // TODO Auto-generated method stub
      return null;
    }
  };

  /**
   * Was overflowing if 100k or so lists of cellscanners to return.
   * @throws IOException
   */
  @Test
  public void testCreateCellScannerOverflow() throws IOException {
    consume(doCreateCellScanner(1, 1), 1 * 1);
    consume(doCreateCellScanner(3, 0), 3 * 0);
    consume(doCreateCellScanner(3, 3), 3 * 3);
    consume(doCreateCellScanner(0, 1), 0 * 1);
    // Do big number. See HBASE-11813 for why.
    final int hundredK = 100000;
    consume(doCreateCellScanner(hundredK, 0), hundredK * 0);
    consume(doCreateCellArray(1), 1);
    consume(doCreateCellArray(0), 0);
    consume(doCreateCellArray(3), 3);
    List<CellScannable> cells = new ArrayList<CellScannable>(hundredK);
    for (int i = 0; i < hundredK; i++) {
      cells.add(new TestCellScannable(1));
    }
    consume(CellUtil.createCellScanner(cells), hundredK * 1);
    NavigableMap<byte [], List<Cell>> m = new TreeMap<byte [], List<Cell>>(Bytes.BYTES_COMPARATOR);
    List<Cell> cellArray = new ArrayList<Cell>(hundredK);
    for (int i = 0; i < hundredK; i++) cellArray.add(new TestCell(i));
    m.put(new byte [] {'f'}, cellArray);
    consume(CellUtil.createCellScanner(m), hundredK * 1);
  }

  private CellScanner doCreateCellArray(final int itemsPerList) {
    Cell [] cells = new Cell [itemsPerList];
    for (int i = 0; i < itemsPerList; i++) {
      cells[i] = new TestCell(i);
    }
    return CellUtil.createCellScanner(cells);
  }

  private CellScanner doCreateCellScanner(final int listsCount, final int itemsPerList)
  throws IOException {
    List<CellScannable> cells = new ArrayList<CellScannable>(listsCount);
    for (int i = 0; i < listsCount; i++) {
      CellScannable cs = new CellScannable() {
        @Override
        public CellScanner cellScanner() {
          return new TestCellScanner(itemsPerList);
        }
      };
      cells.add(cs);
    }
    return CellUtil.createCellScanner(cells);
  }

  private void consume(final CellScanner scanner, final int expected) throws IOException {
    int count = 0;
    while (scanner.advance()) count++;
    Assert.assertEquals(expected, count);
  }

  @Test
  public void testOverlappingKeys() {
    byte[] empty = HConstants.EMPTY_BYTE_ARRAY;
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    byte[] c = Bytes.toBytes("c");
    byte[] d = Bytes.toBytes("d");

    // overlaps
    Assert.assertTrue(CellUtil.overlappingKeys(a, b, a, b));
    Assert.assertTrue(CellUtil.overlappingKeys(a, c, a, b));
    Assert.assertTrue(CellUtil.overlappingKeys(a, b, a, c));
    Assert.assertTrue(CellUtil.overlappingKeys(b, c, a, c));
    Assert.assertTrue(CellUtil.overlappingKeys(a, c, b, c));
    Assert.assertTrue(CellUtil.overlappingKeys(a, d, b, c));
    Assert.assertTrue(CellUtil.overlappingKeys(b, c, a, d));

    Assert.assertTrue(CellUtil.overlappingKeys(empty, b, a, b));
    Assert.assertTrue(CellUtil.overlappingKeys(empty, b, a, c));

    Assert.assertTrue(CellUtil.overlappingKeys(a, b, empty, b));
    Assert.assertTrue(CellUtil.overlappingKeys(a, b, empty, c));

    Assert.assertTrue(CellUtil.overlappingKeys(a, empty, a, b));
    Assert.assertTrue(CellUtil.overlappingKeys(a, empty, a, c));

    Assert.assertTrue(CellUtil.overlappingKeys(a, b, empty, empty));
    Assert.assertTrue(CellUtil.overlappingKeys(empty, empty, a, b));

    // non overlaps
    Assert.assertFalse(CellUtil.overlappingKeys(a, b, c, d));
    Assert.assertFalse(CellUtil.overlappingKeys(c, d, a, b));

    Assert.assertFalse(CellUtil.overlappingKeys(b, c, c, d));
    Assert.assertFalse(CellUtil.overlappingKeys(b, c, c, empty));
    Assert.assertFalse(CellUtil.overlappingKeys(b, c, d, empty));
    Assert.assertFalse(CellUtil.overlappingKeys(c, d, b, c));
    Assert.assertFalse(CellUtil.overlappingKeys(c, empty, b, c));
    Assert.assertFalse(CellUtil.overlappingKeys(d, empty, b, c));

    Assert.assertFalse(CellUtil.overlappingKeys(b, c, a, b));
    Assert.assertFalse(CellUtil.overlappingKeys(b, c, empty, b));
    Assert.assertFalse(CellUtil.overlappingKeys(b, c, empty, a));
    Assert.assertFalse(CellUtil.overlappingKeys(a,b, b, c));
    Assert.assertFalse(CellUtil.overlappingKeys(empty, b, b, c));
    Assert.assertFalse(CellUtil.overlappingKeys(empty, a, b, c));
  }
}
