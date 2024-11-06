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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

// TODO: Move this into TestFuzzyRowFilterEndToEnd?!
@Category({ FilterTests.class, MediumTests.class })
public class TestFuzzyRowFilterWithMiniCluster {

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final String CF = "f";
  private static final String CQ = "name";
  private static final String TABLE = "abcd";
  private static Table ht;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFuzzyRowFilterWithMiniCluster.class);

  @Rule
  public TestName name = new TestName();

  private static final byte[][] ROWS_ONE = { Bytes.toBytes("111311"), Bytes.toBytes("111444"),
    Bytes.toBytes("111511"), Bytes.toBytes("111611"), Bytes.toBytes("111446"),
    Bytes.toBytes("111777"), Bytes.toBytes("111777"), };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.client.scanner.caching", 1000);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    // set no splits
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, (1024L) * 1024 * 1024 * 10);

    TEST_UTIL.startMiniCluster();

    // load the mini cluster with a single table with 20 rows, with rowkeys of a single byte, 0-19.
    ht = TEST_UTIL.createTable(TableName.valueOf(TABLE), Bytes.toBytes(CF), Integer.MAX_VALUE);
    // Insert first half
    for (byte[] ROW : ROWS_ONE) {
      Put p = new Put(ROW);
      p.setDurability(Durability.SKIP_WAL);
      p.addColumn(Bytes.toBytes(CF), Bytes.toBytes(CQ), Bytes.toBytes("aaaaa"));
      ht.put(p);
    }
    TEST_UTIL.flush();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNormalScanForwardsSeek() throws IOException {
    LinkedList<Pair<byte[], byte[]>> fuzzyList = new LinkedList<>();
    byte[] fuzzyRowKey = Bytes.toBytes("111433");
    byte[] mask = Bytes.toBytesBinary("\\xFF\\xFF\\xFF\\xFF\\x02\\x02");
    fuzzyList.add(new Pair<>(fuzzyRowKey, mask));
    FuzzyRowFilter filter = new FuzzyRowFilter(fuzzyList);

    Scan scan = new Scan();
    scan.setFilter(filter);

    ResultScanner scanner = ht.getScanner(scan);
    List<byte[]> actualRowsList = new ArrayList<>();
    for (Result result : scanner) {
      byte[] row = result.getRow();
      actualRowsList.add(row);
    }

    assertEquals(2, actualRowsList.size());
  }

  @Test
  public void testReversedScanBackwardsSeek() throws IOException {
    LinkedList<Pair<byte[], byte[]>> fuzzyList = new LinkedList<>();
    byte[] fuzzyRowKey = Bytes.toBytes("111433");
    byte[] mask = Bytes.toBytesBinary("\\xFF\\xFF\\xFF\\xFF\\x02\\x02");
    fuzzyList.add(new Pair<>(fuzzyRowKey, mask));
    FuzzyRowFilter filter = new FuzzyRowFilter(fuzzyList);

    Scan scan = new Scan();
    scan.setFilter(filter);
    scan.setReversed(true);

    ResultScanner scanner = ht.getScanner(scan);
    List<byte[]> actualRowsList = new ArrayList<>();
    for (Result result : scanner) {
      byte[] row = result.getRow();
      actualRowsList.add(row);
    }

    assertEquals(2, actualRowsList.size());
  }
}
