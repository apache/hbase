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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
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

    @Override
    public long getSequenceId() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public int getTagsLength() {
      // TODO Auto-generated method stub
      return 0;
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

  @Test
  public void testFindCommonPrefixInFlatKey() {
    // The whole key matching case
    KeyValue kv1 = new KeyValue("r1".getBytes(), "f1".getBytes(), "q1".getBytes(), null);
    Assert.assertEquals(kv1.getKeyLength(),
        CellUtil.findCommonPrefixInFlatKey(kv1, kv1, true, true));
    Assert.assertEquals(kv1.getKeyLength(),
        CellUtil.findCommonPrefixInFlatKey(kv1, kv1, false, true));
    Assert.assertEquals(kv1.getKeyLength() - KeyValue.TIMESTAMP_TYPE_SIZE,
        CellUtil.findCommonPrefixInFlatKey(kv1, kv1, true, false));
    // The rk length itself mismatch
    KeyValue kv2 = new KeyValue("r12".getBytes(), "f1".getBytes(), "q1".getBytes(), null);
    Assert.assertEquals(1, CellUtil.findCommonPrefixInFlatKey(kv1, kv2, true, true));
    // part of rk is same
    KeyValue kv3 = new KeyValue("r14".getBytes(), "f1".getBytes(), "q1".getBytes(), null);
    Assert.assertEquals(KeyValue.ROW_LENGTH_SIZE + "r1".getBytes().length,
        CellUtil.findCommonPrefixInFlatKey(kv2, kv3, true, true));
    // entire rk is same but different cf name
    KeyValue kv4 = new KeyValue("r14".getBytes(), "f2".getBytes(), "q1".getBytes(), null);
    Assert.assertEquals(KeyValue.ROW_LENGTH_SIZE + kv3.getRowLength() + KeyValue.FAMILY_LENGTH_SIZE
        + "f".getBytes().length, CellUtil.findCommonPrefixInFlatKey(kv3, kv4, false, true));
    // rk and family are same and part of qualifier
    KeyValue kv5 = new KeyValue("r14".getBytes(), "f2".getBytes(), "q123".getBytes(), null);
    Assert.assertEquals(KeyValue.ROW_LENGTH_SIZE + kv3.getRowLength() + KeyValue.FAMILY_LENGTH_SIZE
        + kv4.getFamilyLength() + kv4.getQualifierLength(),
        CellUtil.findCommonPrefixInFlatKey(kv4, kv5, true, true));
    // rk, cf and q are same. ts differs
    KeyValue kv6 = new KeyValue("rk".getBytes(), 1234L);
    KeyValue kv7 = new KeyValue("rk".getBytes(), 1235L);
    // only last byte out of 8 ts bytes in ts part differs
    Assert.assertEquals(KeyValue.ROW_LENGTH_SIZE + kv6.getRowLength() + KeyValue.FAMILY_LENGTH_SIZE
        + kv6.getFamilyLength() + kv6.getQualifierLength() + 7,
        CellUtil.findCommonPrefixInFlatKey(kv6, kv7, true, true));
    // rk, cf, q and ts are same. Only type differs
    KeyValue kv8 = new KeyValue("rk".getBytes(), 1234L, Type.Delete);
    Assert.assertEquals(KeyValue.ROW_LENGTH_SIZE + kv6.getRowLength() + KeyValue.FAMILY_LENGTH_SIZE
        + kv6.getFamilyLength() + kv6.getQualifierLength() + KeyValue.TIMESTAMP_SIZE,
        CellUtil.findCommonPrefixInFlatKey(kv6, kv8, true, true));
    // With out TS_TYPE check
    Assert.assertEquals(KeyValue.ROW_LENGTH_SIZE + kv6.getRowLength() + KeyValue.FAMILY_LENGTH_SIZE
        + kv6.getFamilyLength() + kv6.getQualifierLength(),
        CellUtil.findCommonPrefixInFlatKey(kv6, kv8, true, false));
  }

  /**
   * Assert CellUtil makes Cell toStrings same way we do KeyValue toStrings.
   */
  @Test
  public void testToString() {
    byte [] row = Bytes.toBytes("row");
    long ts = 123l;
    // Make a KeyValue and a Cell and see if same toString result.
    KeyValue kv = new KeyValue(row, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
        ts, KeyValue.Type.Minimum, HConstants.EMPTY_BYTE_ARRAY);
    Cell cell = CellUtil.createCell(row, HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY,
        ts, KeyValue.Type.Minimum.getCode(), HConstants.EMPTY_BYTE_ARRAY);
    String cellToString = CellUtil.getCellKeyAsString(cell);
    assertEquals(kv.toString(), cellToString);
    // Do another w/ non-null family.
    byte [] f = new byte [] {'f'};
    byte [] q = new byte [] {'q'};
    kv = new KeyValue(row, f, q, ts, KeyValue.Type.Minimum, HConstants.EMPTY_BYTE_ARRAY);
    cell = CellUtil.createCell(row, f, q, ts, KeyValue.Type.Minimum.getCode(),
        HConstants.EMPTY_BYTE_ARRAY);
    cellToString = CellUtil.getCellKeyAsString(cell);
    assertEquals(kv.toString(), cellToString);
    
  }

  @Test
  public void testToString1() {
    String row = "test.row";
    String family = "test.family";
    String qualifier = "test.qualifier";
    long timestamp = 42;
    Type type = Type.Put;
    String value = "test.value";
    long seqId = 1042;

    Cell cell = CellUtil.createCell(Bytes.toBytes(row), Bytes.toBytes(family),
      Bytes.toBytes(qualifier), timestamp, type.getCode(), Bytes.toBytes(value), seqId);

    String nonVerbose = CellUtil.toString(cell, false);
    String verbose = CellUtil.toString(cell, true);

    System.out.println("nonVerbose=" + nonVerbose);
    System.out.println("verbose=" + verbose);

    Assert.assertEquals(
        String.format("%s/%s:%s/%d/%s/vlen=%s/seqid=%s",
          row, family, qualifier, timestamp, type.toString(),
          Bytes.toBytes(value).length, seqId),
        nonVerbose);

    Assert.assertEquals(
      String.format("%s/%s:%s/%d/%s/vlen=%s/seqid=%s/%s",
        row, family, qualifier, timestamp, type.toString(), Bytes.toBytes(value).length,
        seqId, value),
      verbose);

    // TODO: test with tags
  }
}