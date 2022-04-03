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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.TimestampTestBase.FlushCache;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Port of old TestScanMultipleVersions, TestTimestamp and TestGetRowVersions
 * from old testing framework to {@link HBaseTestingUtility}.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestMultiVersions {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMultiVersions.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMultiVersions.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private Admin admin;

  private static final int NUM_SLAVES = 3;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(NUM_SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before()
  throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    this.admin = UTIL.getAdmin();
  }

  /**
  * Tests user specifiable time stamps putting, getting and scanning.  Also
   * tests same in presence of deletes.  Test cores are written so can be
   * run against an HRegion and against an HTable: i.e. both local and remote.
   *
   * <p>Port of old TestTimestamp test to here so can better utilize the spun
   * up cluster running more than a single test per spin up.  Keep old tests'
   * crazyness.
   */
  @Test
  public void testTimestamps() throws Exception {
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    HColumnDescriptor hcd = new HColumnDescriptor(TimestampTestBase.FAMILY_NAME);
    hcd.setMaxVersions(3);
    desc.addFamily(hcd);
    this.admin.createTable(desc);
    Table table = UTIL.getConnection().getTable(desc.getTableName());
    // TODO: Remove these deprecated classes or pull them in here if this is
    // only test using them.
    TimestampTestBase.doTestDelete(table, new FlushCache() {
      @Override
      public void flushcache() throws IOException {
        UTIL.getHBaseCluster().flushcache();
      }
     });

    // Perhaps drop and readd the table between tests so the former does
    // not pollute this latter?  Or put into separate tests.
    TimestampTestBase.doTestTimestampScanning(table, new FlushCache() {
      @Override
      public void flushcache() throws IOException {
        UTIL.getMiniHBaseCluster().flushcache();
      }
    });

    table.close();
  }

  /**
   * Verifies versions across a cluster restart.
   * <p/>
   * Port of old TestGetRowVersions test to here so can better utilize the spun
   * up cluster running more than a single test per spin up.  Keep old tests'
   * crazyness.
   */
  @Test
  public void testGetRowVersions() throws Exception {
    final byte [] contents = Bytes.toBytes("contents");
    final byte [] row = Bytes.toBytes("row");
    final byte [] value1 = Bytes.toBytes("value1");
    final byte [] value2 = Bytes.toBytes("value2");
    final long timestamp1 = 100L;
    final long timestamp2 = 200L;
    final HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    HColumnDescriptor hcd = new HColumnDescriptor(contents);
    hcd.setMaxVersions(3);
    desc.addFamily(hcd);
    this.admin.createTable(desc);
    Put put = new Put(row, timestamp1);
    put.addColumn(contents, contents, value1);
    Table table = UTIL.getConnection().getTable(desc.getTableName());
    table.put(put);
    // Shut down and restart the HBase cluster
    table.close();
    UTIL.shutdownMiniHBaseCluster();
    LOG.debug("HBase cluster shut down -- restarting");
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numRegionServers(NUM_SLAVES).build();
    UTIL.startMiniHBaseCluster(option);
    // Make a new connection.
    table = UTIL.getConnection().getTable(desc.getTableName());
    // Overwrite previous value
    put = new Put(row, timestamp2);
    put.addColumn(contents, contents, value2);
    table.put(put);
    // Now verify that getRow(row, column, latest) works
    Get get = new Get(row);
    // Should get one version by default
    Result r = table.get(get);
    assertNotNull(r);
    assertFalse(r.isEmpty());
    assertEquals(1, r.size());
    byte [] value = r.getValue(contents, contents);
    assertNotEquals(0, value.length);
    assertTrue(Bytes.equals(value, value2));
    // Now check getRow with multiple versions
    get = new Get(row);
    get.setMaxVersions();
    r = table.get(get);
    assertEquals(2, r.size());
    value = r.getValue(contents, contents);
    assertNotEquals(0, value.length);
    assertArrayEquals(value, value2);
    NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map =
      r.getMap();
    NavigableMap<byte[], NavigableMap<Long, byte[]>> familyMap =
      map.get(contents);
    NavigableMap<Long, byte[]> versionMap = familyMap.get(contents);
    assertEquals(2, versionMap.size());
    assertArrayEquals(value1, versionMap.get(timestamp1));
    assertArrayEquals(value2, versionMap.get(timestamp2));
    table.close();
  }

  /**
   * Port of old TestScanMultipleVersions test here so can better utilize the
   * spun up cluster running more than just a single test.  Keep old tests
   * crazyness.
   *
   * <p>Tests five cases of scans and timestamps.
   * @throws Exception
   */
  @Test
  public void testScanMultipleVersions() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));
    final byte [][] rows = new byte[][] {
      Bytes.toBytes("row_0200"),
      Bytes.toBytes("row_0800")
    };
    final byte [][] splitRows = new byte[][] {Bytes.toBytes("row_0500")};
    final long [] timestamp = new long[] {100L, 1000L};
    this.admin.createTable(desc, splitRows);
    Table table = UTIL.getConnection().getTable(tableName);
    // Assert we got the region layout wanted.
    Pair<byte[][], byte[][]> keys = UTIL.getConnection()
        .getRegionLocator(tableName).getStartEndKeys();
    assertEquals(2, keys.getFirst().length);
    byte[][] startKeys = keys.getFirst();
    byte[][] endKeys = keys.getSecond();

    for (int i = 0; i < startKeys.length; i++) {
      if (i == 0) {
        assertArrayEquals(HConstants.EMPTY_START_ROW, startKeys[i]);
        assertArrayEquals(endKeys[i], splitRows[0]);
      } else if (i == 1) {
        assertArrayEquals(splitRows[0], startKeys[i]);
        assertArrayEquals(endKeys[i], HConstants.EMPTY_END_ROW);
      }
    }
    // Insert data
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < startKeys.length; i++) {
      for (int j = 0; j < timestamp.length; j++) {
        Put put = new Put(rows[i], timestamp[j]);
        put.addColumn(HConstants.CATALOG_FAMILY, null, timestamp[j], Bytes.toBytes(timestamp[j]));
        puts.add(put);
      }
    }
    table.put(puts);
    // There are 5 cases we have to test. Each is described below.
    for (int i = 0; i < rows.length; i++) {
      for (int j = 0; j < timestamp.length; j++) {
        Get get = new Get(rows[i]);
        get.addFamily(HConstants.CATALOG_FAMILY);
        get.setTimestamp(timestamp[j]);
        Result result = table.get(get);
        int cellCount = result.rawCells().length;
        assertEquals(1, cellCount);
      }
    }

    // Case 1: scan with LATEST_TIMESTAMP. Should get two rows
    int count;
    Scan scan = new Scan();
    scan.addFamily(HConstants.CATALOG_FAMILY);
    try (ResultScanner s = table.getScanner(scan)) {
      count = Iterables.size(s);
    }
    assertEquals("Number of rows should be 2", 2, count);

    // Case 2: Scan with a timestamp greater than most recent timestamp
    // (in this case > 1000 and < LATEST_TIMESTAMP. Should get 2 rows.
    scan = new Scan();
    scan.setTimeRange(1000L, Long.MAX_VALUE);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    try (ResultScanner s = table.getScanner(scan)) {
      count = Iterables.size(s);
    }
    assertEquals("Number of rows should be 2", 2, count);

    // Case 3: scan with timestamp equal to most recent timestamp
    // (in this case == 1000. Should get 2 rows.
    scan = new Scan();
    scan.setTimestamp(1000L);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    try (ResultScanner s = table.getScanner(scan)) {
      count = Iterables.size(s);
    }
    assertEquals("Number of rows should be 2", 2, count);

    // Case 4: scan with timestamp greater than first timestamp but less than
    // second timestamp (100 < timestamp < 1000). Should get 2 rows.
    scan = new Scan();
    scan.setTimeRange(100L, 1000L);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    try (ResultScanner s = table.getScanner(scan)) {
      count = Iterables.size(s);
    }
    assertEquals("Number of rows should be 2", 2, count);

    // Case 5: scan with timestamp equal to first timestamp (100)
    // Should get 2 rows.
    scan = new Scan();
    scan.setTimestamp(100L);
    scan.addFamily(HConstants.CATALOG_FAMILY);
    try (ResultScanner s = table.getScanner(scan)) {
      count = Iterables.size(s);
    }
    assertEquals("Number of rows should be 2", 2, count);
  }

}

