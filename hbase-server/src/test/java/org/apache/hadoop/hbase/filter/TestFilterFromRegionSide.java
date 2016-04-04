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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * To test behavior of filters at server from region side.
 */
@Category(SmallTests.class)
public class TestFilterFromRegionSide {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HRegion REGION;

  private static TableName TABLE_NAME = TableName.valueOf("TestFilterFromRegionSide");

  private static int NUM_ROWS = 5;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  // Should keep this value below 10 to keep generation of expected kv's simple. If above 10 then
  // table/row/cf1/... will be followed by table/row/cf10/... instead of table/row/cf2/... which
  // breaks the simple generation of expected kv's
  private static int NUM_FAMILIES = 5;
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  private static int NUM_QUALIFIERS = 5;
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  private static int VALUE_SIZE = 1024;
  private static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);

  private static int NUM_COLS = NUM_FAMILIES * NUM_QUALIFIERS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    for (byte[] family : FAMILIES) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      htd.addFamily(hcd);
    }
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    REGION = HBaseTestingUtility
        .createRegionAndWAL(info, TEST_UTIL.getDataTestDir(), TEST_UTIL.getConfiguration(), htd);
    for(Put put:createPuts(ROWS, FAMILIES, QUALIFIERS, VALUE)){
      REGION.put(put);
    }
  }

  private static ArrayList<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
      byte[] value) throws IOException {
    Put put;
    ArrayList<Put> puts = new ArrayList<>();

    for (byte[] row1 : rows) {
      put = new Put(row1);
      for (byte[] family : families) {
        for (int qual = 0; qual < qualifiers.length; qual++) {
          KeyValue kv = new KeyValue(row1, family, qualifiers[qual], qual, value);
          put.add(kv);
        }
      }
      puts.add(put);
    }

    return puts;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    REGION.close();
  }

  @Test
  public void testFirstKeyOnlyFilterAndBatch() throws IOException {
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    scan.setBatch(1);
    InternalScanner scanner = REGION.getScanner(scan);
    List<Cell> results = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      results.clear();
      scanner.next(results);
      assertEquals(1, results.size());
      Cell cell = results.get(0);
      assertArrayEquals(ROWS[i],
          Bytes.copy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
    }
    assertFalse(scanner.next(results));
    scanner.close();
  }

  public static class FirstSeveralCellsFilter extends FilterBase{
    private int count = 0;

    public void reset() {
      count = 0;
    }

    @Override
    public boolean filterRowKey(Cell cell) throws IOException {
      return false;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
      if (count++ < NUM_COLS) {
        return ReturnCode.INCLUDE;
      }
      return ReturnCode.SKIP;
    }

    public static Filter parseFrom(final byte [] pbBytes){
      return new FirstSeveralCellsFilter();
    }
  }

  @Test
  public void testFirstSeveralCellsFilterAndBatch() throws IOException {
    Scan scan = new Scan();
    scan.setFilter(new FirstSeveralCellsFilter());
    scan.setBatch(NUM_COLS);
    InternalScanner scanner = REGION.getScanner(scan);
    List<Cell> results = new ArrayList<>();
    for (int i = 0; i < NUM_ROWS; i++) {
      results.clear();
      scanner.next(results);
      assertEquals(NUM_COLS, results.size());
      Cell cell = results.get(0);
      assertArrayEquals(ROWS[i],
          Bytes.copy(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength()));
      assertArrayEquals(FAMILIES[0],
          Bytes.copy(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
      assertArrayEquals(QUALIFIERS[0], Bytes.copy(cell.getQualifierArray(),
          cell.getQualifierOffset(), cell.getQualifierLength()));
    }
    assertFalse(scanner.next(results));
    scanner.close();
  }
}
