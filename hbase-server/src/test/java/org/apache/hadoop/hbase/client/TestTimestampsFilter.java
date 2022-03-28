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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Run tests related to {@link TimestampsFilter} using HBase client APIs.
 * Sets up the HBase mini cluster once at start. Each creates a table
 * named for the method and does its stuff against that.
 */
@Category({MediumTests.class, ClientTests.class})
public class TestTimestampsFilter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimestampsFilter.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @Rule
  public TestName name = new TestName();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Nothing to do.
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  /**
   * Test from client side for TimestampsFilter.
   *
   * The TimestampsFilter provides the ability to request cells (KeyValues)
   * whose timestamp/version is in the specified list of timestamps/version.
   *
   * @throws Exception
   */
  @Test
  public void testTimestampsFilter() throws Exception {
    final byte [] TABLE = Bytes.toBytes(name.getMethodName());
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };
    Cell kvs[];

    // create table; set versions to max...
    Table ht = TEST_UTIL.createTable(TableName.valueOf(TABLE), FAMILIES, Integer.MAX_VALUE);

    for (int rowIdx = 0; rowIdx < 5; rowIdx++) {
      for (int colIdx = 0; colIdx < 5; colIdx++) {
        // insert versions 201..300
        putNVersions(ht, FAMILY, rowIdx, colIdx, 201, 300);
        // insert versions 1..100
        putNVersions(ht, FAMILY, rowIdx, colIdx, 1, 100);
      }
    }

    // do some verification before flush
    verifyInsertedValues(ht, FAMILY);

    TEST_UTIL.flush();

    // do some verification after flush
    verifyInsertedValues(ht, FAMILY);

    // Insert some more versions after flush. These should be in memstore.
    // After this we should have data in both memstore & HFiles.
    for (int rowIdx = 0; rowIdx < 5; rowIdx++) {
      for (int colIdx = 0; colIdx < 5; colIdx++) {
        putNVersions(ht, FAMILY, rowIdx, colIdx, 301, 400);
        putNVersions(ht, FAMILY, rowIdx, colIdx, 101, 200);
      }
    }

    for (int rowIdx = 0; rowIdx < 5; rowIdx++) {
      for (int colIdx = 0; colIdx < 5; colIdx++) {
        kvs = getNVersions(ht, FAMILY, rowIdx, colIdx,
                           Arrays.asList(505L, 5L, 105L, 305L, 205L));
        assertEquals(4, kvs.length);
        checkOneCell(kvs[0], FAMILY, rowIdx, colIdx, 305);
        checkOneCell(kvs[1], FAMILY, rowIdx, colIdx, 205);
        checkOneCell(kvs[2], FAMILY, rowIdx, colIdx, 105);
        checkOneCell(kvs[3], FAMILY, rowIdx, colIdx, 5);
      }
    }

    // Request an empty list of versions using the Timestamps filter;
    // Should return none.
    kvs = getNVersions(ht, FAMILY, 2, 2, new ArrayList<>());
    assertEquals(0, kvs == null? 0: kvs.length);

    //
    // Test the filter using a Scan operation
    // Scan rows 0..4. For each row, get all its columns, but only
    // those versions of the columns with the specified timestamps.
    Result[] results = scanNVersions(ht, FAMILY, 0, 4,
                                     Arrays.asList(6L, 106L, 306L));
    assertEquals("# of rows returned from scan", 5, results.length);
    for (int rowIdx = 0; rowIdx < 5; rowIdx++) {
      kvs = results[rowIdx].rawCells();
      // each row should have 5 columns.
      // And we have requested 3 versions for each.
      assertEquals("Number of KeyValues in result for row:" + rowIdx,
                   3*5, kvs.length);
      for (int colIdx = 0; colIdx < 5; colIdx++) {
        int offset = colIdx * 3;
        checkOneCell(kvs[offset + 0], FAMILY, rowIdx, colIdx, 306);
        checkOneCell(kvs[offset + 1], FAMILY, rowIdx, colIdx, 106);
        checkOneCell(kvs[offset + 2], FAMILY, rowIdx, colIdx, 6);
      }
    }
    ht.close();
  }

  @Test
  public void testMultiColumns() throws Exception {
    final byte [] TABLE = Bytes.toBytes(name.getMethodName());
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    Table ht = TEST_UTIL.createTable(TableName.valueOf(TABLE), FAMILIES, Integer.MAX_VALUE);

    Put p = new Put(Bytes.toBytes("row"));
    p.addColumn(FAMILY, Bytes.toBytes("column0"), 3L, Bytes.toBytes("value0-3"));
    p.addColumn(FAMILY, Bytes.toBytes("column1"), 3L, Bytes.toBytes("value1-3"));
    p.addColumn(FAMILY, Bytes.toBytes("column2"), 1L, Bytes.toBytes("value2-1"));
    p.addColumn(FAMILY, Bytes.toBytes("column2"), 2L, Bytes.toBytes("value2-2"));
    p.addColumn(FAMILY, Bytes.toBytes("column2"), 3L, Bytes.toBytes("value2-3"));
    p.addColumn(FAMILY, Bytes.toBytes("column3"), 2L, Bytes.toBytes("value3-2"));
    p.addColumn(FAMILY, Bytes.toBytes("column4"), 1L, Bytes.toBytes("value4-1"));
    p.addColumn(FAMILY, Bytes.toBytes("column4"), 2L, Bytes.toBytes("value4-2"));
    p.addColumn(FAMILY, Bytes.toBytes("column4"), 3L, Bytes.toBytes("value4-3"));
    ht.put(p);

    ArrayList<Long> timestamps = new ArrayList<>();
    timestamps.add(new Long(3));
    TimestampsFilter filter = new TimestampsFilter(timestamps);

    Get g = new Get(Bytes.toBytes("row"));
    g.setFilter(filter);
    g.readAllVersions();
    g.addColumn(FAMILY, Bytes.toBytes("column2"));
    g.addColumn(FAMILY, Bytes.toBytes("column4"));

    Result result = ht.get(g);
    for (Cell kv : result.listCells()) {
      System.out.println("found row " + Bytes.toString(CellUtil.cloneRow(kv)) +
          ", column " + Bytes.toString(CellUtil.cloneQualifier(kv)) + ", value "
          + Bytes.toString(CellUtil.cloneValue(kv)));
    }

    assertEquals(2, result.listCells().size());
    assertTrue(CellUtil.matchingValue(result.listCells().get(0), Bytes.toBytes("value2-3")));
    assertTrue(CellUtil.matchingValue(result.listCells().get(1), Bytes.toBytes("value4-3")));

    ht.close();
  }

  /**
   * Test TimestampsFilter in the presence of version deletes.
   *
   * @throws Exception
   */
  @Test
  public void testWithVersionDeletes() throws Exception {

    // first test from memstore (without flushing).
    testWithVersionDeletes(false);

    // run same test against HFiles (by forcing a flush).
    testWithVersionDeletes(true);
  }

  private void testWithVersionDeletes(boolean flushTables) throws IOException {
    final byte [] TABLE = Bytes.toBytes(name.getMethodName() + "_" +
                                   (flushTables ? "flush" : "noflush"));
    byte [] FAMILY = Bytes.toBytes("event_log");
    byte [][] FAMILIES = new byte[][] { FAMILY };

    // create table; set versions to max...
    Table ht = TEST_UTIL.createTable(TableName.valueOf(TABLE), FAMILIES, Integer.MAX_VALUE);

    // For row:0, col:0: insert versions 1 through 5.
    putNVersions(ht, FAMILY, 0, 0, 1, 5);

    // delete version 4.
    deleteOneVersion(ht, FAMILY, 0, 0, 4);

    if (flushTables) {
      TEST_UTIL.flush();
    }

    // request a bunch of versions including the deleted version. We should
    // only get back entries for the versions that exist.
    Cell kvs[] = getNVersions(ht, FAMILY, 0, 0, Arrays.asList(2L, 3L, 4L, 5L));
    assertEquals(3, kvs.length);
    checkOneCell(kvs[0], FAMILY, 0, 0, 5);
    checkOneCell(kvs[1], FAMILY, 0, 0, 3);
    checkOneCell(kvs[2], FAMILY, 0, 0, 2);

    ht.close();
  }

  private void verifyInsertedValues(Table ht, byte[] cf) throws IOException {
    for (int rowIdx = 0; rowIdx < 5; rowIdx++) {
      for (int colIdx = 0; colIdx < 5; colIdx++) {
        // ask for versions that exist.
        Cell[] kvs = getNVersions(ht, cf, rowIdx, colIdx,
                                      Arrays.asList(5L, 300L, 6L, 80L));
        assertEquals(4, kvs.length);
        checkOneCell(kvs[0], cf, rowIdx, colIdx, 300);
        checkOneCell(kvs[1], cf, rowIdx, colIdx, 80);
        checkOneCell(kvs[2], cf, rowIdx, colIdx, 6);
        checkOneCell(kvs[3], cf, rowIdx, colIdx, 5);

        // ask for versions that do not exist.
        kvs = getNVersions(ht, cf, rowIdx, colIdx,
                           Arrays.asList(101L, 102L));
        assertEquals(0, kvs == null? 0: kvs.length);

        // ask for some versions that exist and some that do not.
        kvs = getNVersions(ht, cf, rowIdx, colIdx,
                           Arrays.asList(1L, 300L, 105L, 70L, 115L));
        assertEquals(3, kvs.length);
        checkOneCell(kvs[0], cf, rowIdx, colIdx, 300);
        checkOneCell(kvs[1], cf, rowIdx, colIdx, 70);
        checkOneCell(kvs[2], cf, rowIdx, colIdx, 1);
      }
    }
  }

  /**
   * Assert that the passed in KeyValue has expected contents for the
   * specified row, column & timestamp.
   */
  private void checkOneCell(Cell kv, byte[] cf,
                             int rowIdx, int colIdx, long ts) {

    String ctx = "rowIdx=" + rowIdx + "; colIdx=" + colIdx + "; ts=" + ts;

    assertEquals("Row mismatch which checking: " + ctx,
                 "row:"+ rowIdx, Bytes.toString(CellUtil.cloneRow(kv)));

    assertEquals("ColumnFamily mismatch while checking: " + ctx,
                 Bytes.toString(cf), Bytes.toString(CellUtil.cloneFamily(kv)));

    assertEquals("Column qualifier mismatch while checking: " + ctx,
                 "column:" + colIdx,
                  Bytes.toString(CellUtil.cloneQualifier(kv)));

    assertEquals("Timestamp mismatch while checking: " + ctx,
                 ts, kv.getTimestamp());

    assertEquals("Value mismatch while checking: " + ctx,
                 "value-version-" + ts, Bytes.toString(CellUtil.cloneValue(kv)));
  }

  /**
   * Uses the TimestampFilter on a Get to request a specified list of
   * versions for the row/column specified by rowIdx & colIdx.
   *
   */
  private  Cell[] getNVersions(Table ht, byte[] cf, int rowIdx,
                                   int colIdx, List<Long> versions)
    throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    byte column[] = Bytes.toBytes("column:" + colIdx);
    Filter filter = new TimestampsFilter(versions);
    Get get = new Get(row);
    get.addColumn(cf, column);
    get.setFilter(filter);
    get.readAllVersions();
    Result result = ht.get(get);

    return result.rawCells();
  }

  /**
   * Uses the TimestampFilter on a Scan to request a specified list of
   * versions for the rows from startRowIdx to endRowIdx (both inclusive).
   */
  private Result[] scanNVersions(Table ht, byte[] cf, int startRowIdx,
                                 int endRowIdx, List<Long> versions)
    throws IOException {
    byte startRow[] = Bytes.toBytes("row:" + startRowIdx);
    byte endRow[] = Bytes.toBytes("row:" + endRowIdx + 1); // exclusive
    Filter filter = new TimestampsFilter(versions);
    Scan scan = new Scan().withStartRow(startRow).withStopRow(endRow);
    scan.setFilter(filter);
    scan.readAllVersions();
    ResultScanner scanner = ht.getScanner(scan);
    return scanner.next(endRowIdx - startRowIdx + 1);
  }

  /**
   * Insert in specific row/column versions with timestamps
   * versionStart..versionEnd.
   */
  private void putNVersions(Table ht, byte[] cf, int rowIdx, int colIdx,
                            long versionStart, long versionEnd)
      throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    byte column[] = Bytes.toBytes("column:" + colIdx);
    Put put = new Put(row);
    put.setDurability(Durability.SKIP_WAL);

    for (long idx = versionStart; idx <= versionEnd; idx++) {
      put.addColumn(cf, column, idx, Bytes.toBytes("value-version-" + idx));
    }

    ht.put(put);
  }

  /**
   * For row/column specified by rowIdx/colIdx, delete the cell
   * corresponding to the specified version.
   */
  private void deleteOneVersion(Table ht, byte[] cf, int rowIdx,
                                int colIdx, long version)
    throws IOException {
    byte row[] = Bytes.toBytes("row:" + rowIdx);
    byte column[] = Bytes.toBytes("column:" + colIdx);
    Delete del = new Delete(row);
    del.addColumn(cf, column, version);
    ht.delete(del);
  }

}


