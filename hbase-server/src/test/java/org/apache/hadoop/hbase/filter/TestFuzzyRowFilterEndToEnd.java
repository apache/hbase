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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ FilterTests.class, LargeTests.class })
public class TestFuzzyRowFilterEndToEnd {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestFuzzyRowFilterEndToEnd.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte fuzzyValue = (byte) 63;
  private static final Logger LOG = LoggerFactory.getLogger(TestFuzzyRowFilterEndToEnd.class);

  private static int firstPartCardinality = 50;
  private static int secondPartCardinality = 50;
  private static int thirdPartCardinality = 50;
  private static int colQualifiersTotal = 5;
  private static int totalFuzzyKeys = thirdPartCardinality / 2;

  private static String table = "TestFuzzyRowFilterEndToEnd";

  @Rule
  public TestName name = new TestName();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.client.scanner.caching", 1000);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    // set no splits
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, (1024L) * 1024 * 1024 * 10);

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

  // HBASE-15676 Test that fuzzy info of all fixed bits (0s) finds matching row.
  @Test
  public void testAllFixedBits() throws IOException {
    String cf = "f";
    String cq = "q";

    Table ht =
        TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), Bytes.toBytes(cf), Integer.MAX_VALUE);
    // Load data
    String[] rows = new String[] { "\\x9C\\x00\\x044\\x00\\x00\\x00\\x00",
        "\\x9C\\x00\\x044\\x01\\x00\\x00\\x00", "\\x9C\\x00\\x044\\x00\\x01\\x00\\x00",
        "\\x9B\\x00\\x044e\\x9B\\x02\\xBB", "\\x9C\\x00\\x044\\x00\\x00\\x01\\x00",
        "\\x9C\\x00\\x044\\x00\\x01\\x00\\x01", "\\x9B\\x00\\x044e\\xBB\\xB2\\xBB", };

    for (int i = 0; i < rows.length; i++) {
      Put p = new Put(Bytes.toBytesBinary(rows[i]));
      p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes("value"));
      ht.put(p);
    }

    TEST_UTIL.flush();

    List<Pair<byte[], byte[]>> data = new ArrayList<>();
    byte[] fuzzyKey = Bytes.toBytesBinary("\\x9B\\x00\\x044e");
    byte[] mask = new byte[] { 0, 0, 0, 0, 0 };

    // copy the fuzzy key and mask to test HBASE-18617
    byte[] copyFuzzyKey = Arrays.copyOf(fuzzyKey, fuzzyKey.length);
    byte[] copyMask = Arrays.copyOf(mask, mask.length);

    data.add(new Pair<>(fuzzyKey, mask));
    FuzzyRowFilter filter = new FuzzyRowFilter(data);

    Scan scan = new Scan();
    scan.setFilter(filter);

    ResultScanner scanner = ht.getScanner(scan);
    int total = 0;
    while (scanner.next() != null) {
      total++;
    }
    assertEquals(2, total);

    assertEquals(true, Arrays.equals(copyFuzzyKey, fuzzyKey));
    assertEquals(true, Arrays.equals(copyMask, mask));

    TEST_UTIL.deleteTable(TableName.valueOf(name.getMethodName()));
  }

  @Test
  public void testHBASE14782() throws IOException
  {
    String cf = "f";
    String cq = "q";

    Table ht =
        TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), Bytes.toBytes(cf), Integer.MAX_VALUE);
    // Load data
    String[] rows = new String[] {
      "\\x9C\\x00\\x044\\x00\\x00\\x00\\x00",
      "\\x9C\\x00\\x044\\x01\\x00\\x00\\x00",
      "\\x9C\\x00\\x044\\x00\\x01\\x00\\x00",
      "\\x9C\\x00\\x044\\x00\\x00\\x01\\x00",
      "\\x9C\\x00\\x044\\x00\\x01\\x00\\x01",
      "\\x9B\\x00\\x044e\\xBB\\xB2\\xBB",
    };

    String badRow = "\\x9C\\x00\\x03\\xE9e\\xBB{X\\x1Fwts\\x1F\\x15vRX";

    for(int i=0; i < rows.length; i++){
      Put p = new Put(Bytes.toBytesBinary(rows[i]));
      p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes("value"));
      ht.put(p);
    }

    Put p = new Put(Bytes.toBytesBinary(badRow));
    p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq), Bytes.toBytes("value"));
    ht.put(p);

    TEST_UTIL.flush();

    List<Pair<byte[], byte[]>> data =  new ArrayList<>();
    byte[] fuzzyKey = Bytes.toBytesBinary("\\x00\\x00\\x044");
    byte[] mask = new byte[] { 1,0,0,0};
    data.add(new Pair<>(fuzzyKey, mask));
    FuzzyRowFilter filter = new FuzzyRowFilter(data);

    Scan scan = new Scan();
    scan.setFilter(filter);

    ResultScanner scanner = ht.getScanner(scan);
    int total = 0;
    while(scanner.next() != null){
      total++;
    }
    assertEquals(rows.length, total);
    TEST_UTIL.deleteTable(TableName.valueOf(name.getMethodName()));
  }

  @Test
  public void testEndToEnd() throws Exception {
    String cf = "f";

    Table ht =
        TEST_UTIL.createTable(TableName.valueOf(table), Bytes.toBytes(cf), Integer.MAX_VALUE);

    // 10 byte row key - (2 bytes 4 bytes 4 bytes)
    // 4 byte qualifier
    // 4 byte value

    for (int i0 = 0; i0 < firstPartCardinality; i0++) {

      for (int i1 = 0; i1 < secondPartCardinality; i1++) {

        for (int i2 = 0; i2 < thirdPartCardinality; i2++) {
          byte[] rk = new byte[10];

          ByteBuffer buf = ByteBuffer.wrap(rk);
          buf.clear();
          buf.putShort((short) i0);
          buf.putInt(i1);
          buf.putInt(i2);
          for (int c = 0; c < colQualifiersTotal; c++) {
            byte[] cq = new byte[4];
            Bytes.putBytes(cq, 0, Bytes.toBytes(c), 0, 4);

            Put p = new Put(rk);
            p.setDurability(Durability.SKIP_WAL);
            p.addColumn(Bytes.toBytes(cf), cq, Bytes.toBytes(c));
            ht.put(p);
          }
        }
      }
    }

    TEST_UTIL.flush();

    // test passes
    runTest1(ht);
    runTest2(ht);

  }

  private void runTest1(Table hTable) throws IOException {
    // [0, 2, ?, ?, ?, ?, 0, 0, 0, 1]

    byte[] mask = new byte[] { 0, 0, 1, 1, 1, 1, 0, 0, 0, 0 };

    List<Pair<byte[], byte[]>> list = new ArrayList<>();
    for (int i = 0; i < totalFuzzyKeys; i++) {
      byte[] fuzzyKey = new byte[10];
      ByteBuffer buf = ByteBuffer.wrap(fuzzyKey);
      buf.clear();
      buf.putShort((short) 2);
      for (int j = 0; j < 4; j++) {
        buf.put(fuzzyValue);
      }
      buf.putInt(i);

      Pair<byte[], byte[]> pair = new Pair<>(fuzzyKey, mask);
      list.add(pair);
    }

    int expectedSize = secondPartCardinality * totalFuzzyKeys * colQualifiersTotal;
    FuzzyRowFilter fuzzyRowFilter0 = new FuzzyRowFilter(list);
    // Filters are not stateless - we can't reuse them
    FuzzyRowFilter fuzzyRowFilter1 = new FuzzyRowFilter(list);

    // regular test
    runScanner(hTable, expectedSize, fuzzyRowFilter0);
    // optimized from block cache
    runScanner(hTable, expectedSize, fuzzyRowFilter1);

  }

  private void runTest2(Table hTable) throws IOException {
    // [0, 0, ?, ?, ?, ?, 0, 0, 0, 0] , [0, 1, ?, ?, ?, ?, 0, 0, 0, 1]...

    byte[] mask = new byte[] { 0, 0, 1, 1, 1, 1, 0, 0, 0, 0 };

    List<Pair<byte[], byte[]>> list = new ArrayList<>();

    for (int i = 0; i < totalFuzzyKeys; i++) {
      byte[] fuzzyKey = new byte[10];
      ByteBuffer buf = ByteBuffer.wrap(fuzzyKey);
      buf.clear();
      buf.putShort((short) (i * 2));
      for (int j = 0; j < 4; j++) {
        buf.put(fuzzyValue);
      }
      buf.putInt(i * 2);

      Pair<byte[], byte[]> pair = new Pair<>(fuzzyKey, mask);
      list.add(pair);
    }

    int expectedSize = totalFuzzyKeys * secondPartCardinality * colQualifiersTotal;

    FuzzyRowFilter fuzzyRowFilter0 = new FuzzyRowFilter(list);
    // Filters are not stateless - we can't reuse them
    FuzzyRowFilter fuzzyRowFilter1 = new FuzzyRowFilter(list);

    // regular test
    runScanner(hTable, expectedSize, fuzzyRowFilter0);
    // optimized from block cache
    runScanner(hTable, expectedSize, fuzzyRowFilter1);

  }

  private void runScanner(Table hTable, int expectedSize, Filter filter) throws IOException {

    String cf = "f";
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(cf));
    scan.setFilter(filter);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TableName.valueOf(table));
    HRegion first = regions.get(0);
    first.getScanner(scan);
    RegionScanner scanner = first.getScanner(scan);
    List<Cell> results = new ArrayList<>();
    // Result result;
    long timeBeforeScan = System.currentTimeMillis();
    int found = 0;
    while (scanner.next(results)) {
      found += results.size();
      results.clear();
    }
    found += results.size();
    long scanTime = System.currentTimeMillis() - timeBeforeScan;
    scanner.close();

    LOG.info("\nscan time = " + scanTime + "ms");
    LOG.info("found " + found + " results\n");

    assertEquals(expectedSize, found);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testFilterList() throws Exception {
    String cf = "f";
    Table ht =
        TEST_UTIL.createTable(TableName.valueOf(name.getMethodName()), Bytes.toBytes(cf), Integer.MAX_VALUE);

    // 10 byte row key - (2 bytes 4 bytes 4 bytes)
    // 4 byte qualifier
    // 4 byte value

    for (int i1 = 0; i1 < 5; i1++) {
      for (int i2 = 0; i2 < 5; i2++) {
        byte[] rk = new byte[10];

        ByteBuffer buf = ByteBuffer.wrap(rk);
        buf.clear();
        buf.putShort((short) 2);
        buf.putInt(i1);
        buf.putInt(i2);

        // Each row contains 5 columns
        for (int c = 0; c < 5; c++) {
          byte[] cq = new byte[4];
          Bytes.putBytes(cq, 0, Bytes.toBytes(c), 0, 4);

          Put p = new Put(rk);
          p.setDurability(Durability.SKIP_WAL);
          p.addColumn(Bytes.toBytes(cf), cq, Bytes.toBytes(c));
          ht.put(p);
          LOG.info("Inserting: rk: " + Bytes.toStringBinary(rk) + " cq: "
              + Bytes.toStringBinary(cq));
        }
      }
    }

    TEST_UTIL.flush();

    // test passes if we get back 5 KV's (1 row)
    runTest(ht, 5);

  }

  @SuppressWarnings("unchecked")
  private void runTest(Table hTable, int expectedSize) throws IOException {
    // [0, 2, ?, ?, ?, ?, 0, 0, 0, 1]
    byte[] fuzzyKey1 = new byte[10];
    ByteBuffer buf = ByteBuffer.wrap(fuzzyKey1);
    buf.clear();
    buf.putShort((short) 2);
    for (int i = 0; i < 4; i++)
      buf.put(fuzzyValue);
    buf.putInt((short) 1);
    byte[] mask1 = new byte[] { 0, 0, 1, 1, 1, 1, 0, 0, 0, 0 };

    byte[] fuzzyKey2 = new byte[10];
    buf = ByteBuffer.wrap(fuzzyKey2);
    buf.clear();
    buf.putShort((short) 2);
    buf.putInt((short) 2);
    for (int i = 0; i < 4; i++)
      buf.put(fuzzyValue);

    byte[] mask2 = new byte[] { 0, 0, 0, 0, 0, 0, 1, 1, 1, 1 };

    Pair<byte[], byte[]> pair1 = new Pair<>(fuzzyKey1, mask1);
    Pair<byte[], byte[]> pair2 = new Pair<>(fuzzyKey2, mask2);

    FuzzyRowFilter fuzzyRowFilter1 = new FuzzyRowFilter(Lists.newArrayList(pair1));
    FuzzyRowFilter fuzzyRowFilter2 = new FuzzyRowFilter(Lists.newArrayList(pair2));
    // regular test - we expect 1 row back (5 KVs)
    runScanner(hTable, expectedSize, fuzzyRowFilter1, fuzzyRowFilter2);
  }

  private void runScanner(Table hTable, int expectedSize, Filter filter1, Filter filter2)
      throws IOException {
    String cf = "f";
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(cf));
    FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, filter1, filter2);
    scan.setFilter(filterList);

    ResultScanner scanner = hTable.getScanner(scan);
    List<Cell> results = new ArrayList<>();
    Result result;
    long timeBeforeScan = System.currentTimeMillis();
    while ((result = scanner.next()) != null) {
      for (Cell kv : result.listCells()) {
        LOG.info("Got rk: " + Bytes.toStringBinary(CellUtil.cloneRow(kv)) + " cq: "
            + Bytes.toStringBinary(CellUtil.cloneQualifier(kv)));
        results.add(kv);
      }
    }
    long scanTime = System.currentTimeMillis() - timeBeforeScan;
    scanner.close();

    LOG.info("scan time = " + scanTime + "ms");
    LOG.info("found " + results.size() + " results");

    assertEquals(expectedSize, results.size());
  }
}
