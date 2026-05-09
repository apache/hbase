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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.TestTemplate;

public class FromClientSideTestReverseScan extends FromClientSideTestBase {

  protected FromClientSideTestReverseScan(Class<? extends ConnectionRegistry> registryImpl,
    int numHedgedReqs) {
    super(registryImpl, numHedgedReqs);
  }

  @TestTemplate
  public void testSuperSimpleWithReverseScan() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Put put = new Put(Bytes.toBytes("0-b11111-0000000000000000000"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000002"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000004"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000006"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b11111-0000000000000000008"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000001"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000003"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000005"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000007"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      put = new Put(Bytes.toBytes("0-b22222-0000000000000000009"));
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);
      Scan scan = new Scan().withStartRow(Bytes.toBytes("0-b11111-9223372036854775807"))
        .withStopRow(Bytes.toBytes("0-b11111-0000000000000000000"), true);
      scan.setReversed(true);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        Result result = scanner.next();
        assertTrue(Bytes.equals(result.getRow(), Bytes.toBytes("0-b11111-0000000000000000008")));
      }
    }
  }

  @TestTemplate
  public void testFiltersWithReverseScan() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS =
        { Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
          Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
          Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
          Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
          Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>") };
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(FAMILY, QUALIFIERS[i], VALUE);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      Filter filter =
        new QualifierFilter(CompareOperator.EQUAL, new RegexStringComparator("col[1-5]"));
      scan.setFilter(filter);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        int expectedIndex = 5;
        for (Result result : scanner) {
          assertEquals(1, result.size());
          Cell c = result.rawCells()[0];
          assertTrue(Bytes.equals(c.getRowArray(), c.getRowOffset(), c.getRowLength(),
            ROWS[expectedIndex], 0, ROWS[expectedIndex].length));
          assertTrue(
            Bytes.equals(c.getQualifierArray(), c.getQualifierOffset(), c.getQualifierLength(),
              QUALIFIERS[expectedIndex], 0, QUALIFIERS[expectedIndex].length));
          expectedIndex--;
        }
        assertEquals(0, expectedIndex);
      }
    }
  }

  @TestTemplate
  public void testKeyOnlyFilterWithReverseScan() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      byte[][] ROWS = makeN(ROW, 10);
      byte[][] QUALIFIERS =
        { Bytes.toBytes("col0-<d2v1>-<d3v2>"), Bytes.toBytes("col1-<d2v1>-<d3v2>"),
          Bytes.toBytes("col2-<d2v1>-<d3v2>"), Bytes.toBytes("col3-<d2v1>-<d3v2>"),
          Bytes.toBytes("col4-<d2v1>-<d3v2>"), Bytes.toBytes("col5-<d2v1>-<d3v2>"),
          Bytes.toBytes("col6-<d2v1>-<d3v2>"), Bytes.toBytes("col7-<d2v1>-<d3v2>"),
          Bytes.toBytes("col8-<d2v1>-<d3v2>"), Bytes.toBytes("col9-<d2v1>-<d3v2>") };
      for (int i = 0; i < 10; i++) {
        Put put = new Put(ROWS[i]);
        put.addColumn(FAMILY, QUALIFIERS[i], VALUE);
        ht.put(put);
      }
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      Filter filter = new KeyOnlyFilter(true);
      scan.setFilter(filter);
      try (ResultScanner ignored = ht.getScanner(scan)) {
        int count = 0;
        for (Result result : ht.getScanner(scan)) {
          assertEquals(1, result.size());
          assertEquals(Bytes.SIZEOF_INT, result.rawCells()[0].getValueLength());
          assertEquals(VALUE.length, Bytes.toInt(CellUtil.cloneValue(result.rawCells()[0])));
          count++;
        }
        assertEquals(10, count);
      }
    }
  }

  /**
   * Test simple table and non-existent row cases.
   */
  @TestTemplate
  public void testSimpleMissingWithReverseScan() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      byte[][] ROWS = makeN(ROW, 4);

      // Try to get a row on an empty table
      Scan scan = new Scan();
      scan.setReversed(true);
      Result result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan().withStartRow(ROWS[0]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan().withStartRow(ROWS[0]).withStopRow(ROWS[1], true);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan();
      scan.setReversed(true);
      scan.addFamily(FAMILY);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      scan = new Scan();
      scan.setReversed(true);
      scan.addColumn(FAMILY, QUALIFIER);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);

      // Insert a row
      Put put = new Put(ROWS[2]);
      put.addColumn(FAMILY, QUALIFIER, VALUE);
      ht.put(put);

      // Make sure we can scan the row
      scan = new Scan();
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      scan = new Scan().withStartRow(ROWS[3]).withStopRow(ROWS[0], true);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      scan = new Scan().withStartRow(ROWS[2]).withStopRow(ROWS[1], true);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROWS[2], FAMILY, QUALIFIER, VALUE);

      // Try to scan empty rows around it
      // Introduced MemStore#shouldSeekForReverseScan to fix the following
      scan = new Scan().withStartRow(ROWS[1]);
      scan.setReversed(true);
      result = getSingleScanResult(ht, scan);
      assertNullResult(result);
    }
  }

  @TestTemplate
  public void testNullWithReverseScan() throws Exception {
    TEST_UTIL.createTable(tableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      // Null qualifier (should work)
      Put put = new Put(ROW);
      put.addColumn(FAMILY, null, VALUE);
      ht.put(put);
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, null);
      ht.delete(delete);
    }

    // Use a new table
    TableName newTableName = TableName.valueOf(tableName.toString() + "2");
    TEST_UTIL.createTable(newTableName, FAMILY);
    try (Connection conn = getConnection(); Table ht = conn.getTable(newTableName)) {
      // Empty qualifier, byte[0] instead of null (should work)
      Put put = new Put(ROW);
      put.addColumn(FAMILY, HConstants.EMPTY_BYTE_ARRAY, VALUE);
      ht.put(put);
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      TEST_UTIL.flush();
      scanTestNull(ht, ROW, FAMILY, VALUE, true);
      Delete delete = new Delete(ROW);
      delete.addColumns(FAMILY, HConstants.EMPTY_BYTE_ARRAY);
      ht.delete(delete);
      // Null value
      put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, null);
      ht.put(put);
      Scan scan = new Scan();
      scan.setReversed(true);
      scan.addColumn(FAMILY, QUALIFIER);
      Result result = getSingleScanResult(ht, scan);
      assertSingleResult(result, ROW, FAMILY, QUALIFIER, null);
    }
  }

  @TestTemplate
  @SuppressWarnings("checkstyle:MethodLength")
  public void testDeletesWithReverseScan() throws Exception {
    byte[][] ROWS = makeNAscii(ROW, 6);
    byte[][] FAMILIES = makeNAscii(FAMILY, 3);
    byte[][] VALUES = makeN(VALUE, 5);
    long[] ts = { 1000, 2000, 3000, 4000, 5000 };
    TEST_UTIL.createTable(tableName, FAMILIES, 3);
    try (Connection conn = getConnection(); Table ht = conn.getTable(tableName)) {
      Put put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[1], VALUES[1]);
      ht.put(put);

      Delete delete = new Delete(ROW);
      delete.addFamily(FAMILIES[0], ts[0]);
      ht.delete(delete);

      Scan scan = new Scan().withStartRow(ROW);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[0]);
      scan.readVersions(Integer.MAX_VALUE);
      Result result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1] },
        new byte[][] { VALUES[1] }, 0, 0);

      // Test delete latest version
      put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[3], VALUES[3]);
      put.addColumn(FAMILIES[0], null, ts[4], VALUES[4]);
      put.addColumn(FAMILIES[0], null, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[0], null, ts[3], VALUES[3]);
      ht.put(put);

      delete = new Delete(ROW);
      delete.addColumn(FAMILIES[0], QUALIFIER); // ts[4]
      ht.delete(delete);

      scan = new Scan().withStartRow(ROW);
      scan.setReversed(true);
      scan.addColumn(FAMILIES[0], QUALIFIER);
      scan.readVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1], ts[2], ts[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      // Test for HBASE-1847
      delete = new Delete(ROW);
      delete.addColumn(FAMILIES[0], null);
      ht.delete(delete);

      // Cleanup null qualifier
      delete = new Delete(ROW);
      delete.addColumns(FAMILIES[0], null);
      ht.delete(delete);

      // Expected client behavior might be that you can re-put deleted values
      // But alas, this is not to be. We can't put them back in either case.

      put = new Put(ROW);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[0], QUALIFIER, ts[4], VALUES[4]);
      ht.put(put);

      // The Scanner returns the previous values, the expected-naive-unexpected
      // behavior

      scan = new Scan().withStartRow(ROW);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[0]);
      scan.readVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertNResult(result, ROW, FAMILIES[0], QUALIFIER, new long[] { ts[1], ts[2], ts[3] },
        new byte[][] { VALUES[1], VALUES[2], VALUES[3] }, 0, 2);

      // Test deleting an entire family from one row but not the other various
      // ways

      put = new Put(ROWS[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      put = new Put(ROWS[1]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      put = new Put(ROWS[2]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[0], VALUES[0]);
      put.addColumn(FAMILIES[1], QUALIFIER, ts[1], VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[2], VALUES[2]);
      put.addColumn(FAMILIES[2], QUALIFIER, ts[3], VALUES[3]);
      ht.put(put);

      delete = new Delete(ROWS[0]);
      delete.addFamily(FAMILIES[2]);
      ht.delete(delete);

      delete = new Delete(ROWS[1]);
      delete.addColumns(FAMILIES[1], QUALIFIER);
      ht.delete(delete);

      delete = new Delete(ROWS[2]);
      delete.addColumn(FAMILIES[1], QUALIFIER);
      delete.addColumn(FAMILIES[1], QUALIFIER);
      delete.addColumn(FAMILIES[2], QUALIFIER);
      ht.delete(delete);

      scan = new Scan().withStartRow(ROWS[0]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.readVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals(2, result.size(), "Expected 2 keys but received " + result.size());
      assertNResult(result, ROWS[0], FAMILIES[1], QUALIFIER, new long[] { ts[0], ts[1] },
        new byte[][] { VALUES[0], VALUES[1] }, 0, 1);

      scan = new Scan().withStartRow(ROWS[1]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.readVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals(2, result.size(), "Expected 2 keys but received " + result.size());

      scan = new Scan().withStartRow(ROWS[2]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.readVersions(Integer.MAX_VALUE);
      result = getSingleScanResult(ht, scan);
      assertEquals(1, result.size());
      assertNResult(result, ROWS[2], FAMILIES[2], QUALIFIER, new long[] { ts[2] },
        new byte[][] { VALUES[2] }, 0, 0);

      // Test if we delete the family first in one row (HBASE-1541)

      delete = new Delete(ROWS[3]);
      delete.addFamily(FAMILIES[1]);
      ht.delete(delete);

      put = new Put(ROWS[3]);
      put.addColumn(FAMILIES[2], QUALIFIER, VALUES[0]);
      ht.put(put);

      put = new Put(ROWS[4]);
      put.addColumn(FAMILIES[1], QUALIFIER, VALUES[1]);
      put.addColumn(FAMILIES[2], QUALIFIER, VALUES[2]);
      ht.put(put);

      scan = new Scan().withStartRow(ROWS[4]);
      scan.setReversed(true);
      scan.addFamily(FAMILIES[1]);
      scan.addFamily(FAMILIES[2]);
      scan.readVersions(Integer.MAX_VALUE);
      try (ResultScanner scanner = ht.getScanner(scan)) {
        result = scanner.next();
        assertEquals(2, result.size(), "Expected 2 keys but received " + result.size());
        assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[4]));
        assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[1]), ROWS[4]));
        assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[1]));
        assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[1]), VALUES[2]));
        result = scanner.next();
        assertEquals(1, result.size(), "Expected 1 key but received " + result.size());
        assertTrue(Bytes.equals(CellUtil.cloneRow(result.rawCells()[0]), ROWS[3]));
        assertTrue(Bytes.equals(CellUtil.cloneValue(result.rawCells()[0]), VALUES[0]));
      }
    }
  }

  /**
   * Tests reversed scan under multi regions
   */
  @TestTemplate
  public void testReversedScanUnderMultiRegions() throws Exception {
    // Test Initialization.
    byte[] maxByteArray = ConnectionUtils.MAX_BYTE_ARRAY;
    byte[][] splitRows = new byte[][] { Bytes.toBytes("005"),
      Bytes.add(Bytes.toBytes("005"), Bytes.multiple(maxByteArray, 16)), Bytes.toBytes("006"),
      Bytes.add(Bytes.toBytes("006"), Bytes.multiple(maxByteArray, 8)), Bytes.toBytes("007"),
      Bytes.add(Bytes.toBytes("007"), Bytes.multiple(maxByteArray, 4)), Bytes.toBytes("008"),
      Bytes.multiple(maxByteArray, 2) };
    TEST_UTIL.createTable(tableName, FAMILY, splitRows);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      try (RegionLocator l = conn.getRegionLocator(tableName)) {
        assertEquals(splitRows.length + 1, l.getAllRegionLocations().size());
      }
      // Insert one row each region
      int insertNum = splitRows.length;
      for (byte[] splitRow : splitRows) {
        Put put = new Put(splitRow);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // scan forward
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertFalse(r.isEmpty());
          count++;
        }
        assertEquals(insertNum, count);
      }

      // scan backward
      Scan scan = new Scan();
      scan.setReversed(true);
      try (ResultScanner scanner = table.getScanner(scan)) {
        int count = 0;
        byte[] lastRow = null;
        for (Result r : scanner) {
          assertFalse(r.isEmpty());
          count++;
          byte[] thisRow = r.getRow();
          if (lastRow != null) {
            assertTrue(Bytes.compareTo(thisRow, lastRow) < 0, "Error scan order, last row= "
              + Bytes.toString(lastRow) + ",this row=" + Bytes.toString(thisRow));
          }
          lastRow = thisRow;
        }
        assertEquals(insertNum, count);
      }
    }
  }

  /**
   * Tests reversed scan under multi regions
   */
  @TestTemplate
  public void testSmallReversedScanUnderMultiRegions() throws Exception {
    // Test Initialization.
    byte[][] splitRows = new byte[][] { Bytes.toBytes("000"), Bytes.toBytes("002"),
      Bytes.toBytes("004"), Bytes.toBytes("006"), Bytes.toBytes("008"), Bytes.toBytes("010") };
    TEST_UTIL.createTable(tableName, FAMILY, splitRows);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
    try (Connection conn = getConnection(); Table table = conn.getTable(tableName)) {
      try (RegionLocator l = conn.getRegionLocator(tableName)) {
        assertEquals(splitRows.length + 1, l.getAllRegionLocations().size());
      }
      for (byte[] splitRow : splitRows) {
        Put put = new Put(splitRow);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);

        byte[] nextRow = Bytes.copy(splitRow);
        nextRow[nextRow.length - 1]++;

        put = new Put(nextRow);
        put.addColumn(FAMILY, QUALIFIER, VALUE);
        table.put(put);
      }

      // scan forward
      try (ResultScanner scanner = table.getScanner(new Scan())) {
        int count = 0;
        for (Result r : scanner) {
          assertTrue(!r.isEmpty());
          count++;
        }
        assertEquals(12, count);
      }

      reverseScanTest(table, ReadType.STREAM);
      reverseScanTest(table, ReadType.PREAD);
      reverseScanTest(table, ReadType.DEFAULT);
    }
  }

  private void reverseScanTest(Table table, ReadType readType) throws IOException {
    // scan backward
    Scan scan = new Scan();
    scan.setReversed(true);
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue(Bytes.compareTo(thisRow, lastRow) < 0, "Error scan order, last row= "
            + Bytes.toString(lastRow) + ",this row=" + Bytes.toString(thisRow));
        }
        lastRow = thisRow;
      }
      assertEquals(12, count);
    }

    scan = new Scan();
    scan.setReadType(readType);
    scan.setReversed(true);
    scan.withStartRow(Bytes.toBytes("002"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertTrue(!r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue(Bytes.compareTo(thisRow, lastRow) < 0, "Error scan order, last row= "
            + Bytes.toString(lastRow) + ",this row=" + Bytes.toString(thisRow));
        }
        lastRow = thisRow;
      }
      assertEquals(3, count); // 000 001 002
    }

    scan = new Scan();
    scan.setReadType(readType);
    scan.setReversed(true);
    scan.withStartRow(Bytes.toBytes("002"));
    scan.withStopRow(Bytes.toBytes("000"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertFalse(r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue(Bytes.compareTo(thisRow, lastRow) < 0, "Error scan order, last row= "
            + Bytes.toString(lastRow) + ",this row=" + Bytes.toString(thisRow));
        }
        lastRow = thisRow;
      }
      assertEquals(2, count); // 001 002
    }

    scan = new Scan();
    scan.setReadType(readType);
    scan.setReversed(true);
    scan.withStartRow(Bytes.toBytes("001"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertFalse(r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue(Bytes.compareTo(thisRow, lastRow) < 0, "Error scan order, last row= "
            + Bytes.toString(lastRow) + ",this row=" + Bytes.toString(thisRow));
        }
        lastRow = thisRow;
      }
      assertEquals(2, count); // 000 001
    }

    scan = new Scan();
    scan.setReadType(readType);
    scan.setReversed(true);
    scan.withStartRow(Bytes.toBytes("000"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertFalse(r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue(Bytes.compareTo(thisRow, lastRow) < 0, "Error scan order, last row= "
            + Bytes.toString(lastRow) + ",this row=" + Bytes.toString(thisRow));
        }
        lastRow = thisRow;
      }
      assertEquals(1, count); // 000
    }

    scan = new Scan();
    scan.setReadType(readType);
    scan.setReversed(true);
    scan.withStartRow(Bytes.toBytes("006"));
    scan.withStopRow(Bytes.toBytes("002"));
    try (ResultScanner scanner = table.getScanner(scan)) {
      int count = 0;
      byte[] lastRow = null;
      for (Result r : scanner) {
        assertFalse(r.isEmpty());
        count++;
        byte[] thisRow = r.getRow();
        if (lastRow != null) {
          assertTrue(Bytes.compareTo(thisRow, lastRow) < 0, "Error scan order, last row= "
            + Bytes.toString(lastRow) + ",this row=" + Bytes.toString(thisRow));
        }
        lastRow = thisRow;
      }
      assertEquals(4, count); // 003 004 005 006
    }
  }
}
