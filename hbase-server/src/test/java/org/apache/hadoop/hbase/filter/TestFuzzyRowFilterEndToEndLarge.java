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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ FilterTests.class, LargeTests.class })
public class TestFuzzyRowFilterEndToEndLarge {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFuzzyRowFilterEndToEndLarge.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestFuzzyRowFilterEndToEndLarge.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private final static byte fuzzyValue = (byte) 63;

  private static int firstPartCardinality = 30;
  private static int secondPartCardinality = 30;
  private static int thirdPartCardinality = 30;
  private static int colQualifiersTotal = 5;
  private static int totalFuzzyKeys = thirdPartCardinality / 2;

  private static String table = "TestFuzzyRowFilterEndToEndLarge";

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

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
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
    long timeBeforeScan = EnvironmentEdgeManager.currentTime();
    int found = 0;
    while (scanner.next(results)) {
      found += results.size();
      results.clear();
    }
    found += results.size();
    long scanTime = EnvironmentEdgeManager.currentTime() - timeBeforeScan;
    scanner.close();

    LOG.info("\nscan time = " + scanTime + "ms");
    LOG.info("found " + found + " results\n");

    assertEquals(expectedSize, found);
  }
}
