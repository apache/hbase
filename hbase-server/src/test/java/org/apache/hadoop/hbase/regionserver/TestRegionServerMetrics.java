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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Category({RegionServerTests.class, LargeTests.class})
public class TestRegionServerMetrics {
  private static final Log LOG = LogFactory.getLog(TestRegionServerMetrics.class);

  @Rule
  public TestName testName = new TestName();

  @ClassRule
  public static TestRule timeout = CategoryBasedTimeout.forClass(TestRegionServerMetrics.class);

  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  private static MetricsAssertHelper metricsHelper;
  private static MiniHBaseCluster cluster;
  private static HRegionServer rs;
  private static Configuration conf;
  private static HBaseTestingUtility TEST_UTIL;
  private static Connection connection;
  private static MetricsRegionServer metricsRegionServer;
  private static MetricsRegionServerSource serverSource;
  private static final int NUM_SCAN_NEXT = 30;
  private static int numScanNext = 0;
  private static byte[] cf = Bytes.toBytes("cf");
  private static byte[] row = Bytes.toBytes("row");
  private static byte[] qualifier = Bytes.toBytes("qual");
  private static byte[] val = Bytes.toBytes("val");
  private static Admin admin;

  @BeforeClass
  public static void startCluster() throws Exception {
    metricsHelper = CompatibilityFactory.getInstance(MetricsAssertHelper.class);
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    // testMobMetrics creates few hfiles and manages compaction manually.
    conf.setInt("hbase.hstore.compactionThreshold", 100);
    conf.setInt("hbase.hstore.compaction.max", 100);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);

    TEST_UTIL.startMiniCluster(1, 1);
    cluster = TEST_UTIL.getHBaseCluster();
    cluster.waitForActiveAndReadyMaster();
    admin = TEST_UTIL.getHBaseAdmin();
    connection = TEST_UTIL.getConnection();

    while (cluster.getLiveRegionServerThreads().size() < 1) {
      Threads.sleep(100);
    }

    rs = cluster.getRegionServer(0);
    metricsRegionServer = rs.getRegionServerMetrics();
    serverSource = metricsRegionServer.getMetricsSource();
  }

  @AfterClass
  public static void after() throws Exception {
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  TableName tableName;
  Table table;

  @Before
  public void beforeTestMethod() throws Exception {
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    tableName = TableName.valueOf(testName.getMethodName());
    table = TEST_UTIL.createTable(tableName, cf);
  }

  @After
  public void afterTestMethod() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  public void waitTableDeleted(TableName name, long timeoutInMillis) throws Exception {
    long start = System.currentTimeMillis();
    while (true) {
      HTableDescriptor[] tables = admin.listTables();
      for (HTableDescriptor htd : tables) {
        if (htd.getNameAsString() == name.getNameAsString())
          return;
      }
      if (System.currentTimeMillis() - start > timeoutInMillis)
        return;
      Thread.sleep(1000);
    }
  }

  public void assertCounter(String metric, long expectedValue) {
    metricsHelper.assertCounter(metric, expectedValue, serverSource);
  }

  public void assertGauge(String metric, long expectedValue) {
    metricsHelper.assertGauge(metric, expectedValue, serverSource);
  }

  // Aggregates metrics from regions and assert given list of metrics and expected values.
  public void assertRegionMetrics(String metric, long expectedValue) throws Exception {
    try (RegionLocator locator = connection.getRegionLocator(tableName)) {
      for ( HRegionLocation location: locator.getAllRegionLocations()) {
        HRegionInfo hri = location.getRegionInfo();
        MetricsRegionAggregateSource agg =
            rs.getRegion(hri.getRegionName()).getMetrics().getSource().getAggregateSource();
        String prefix = "namespace_" + NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR +
            "_table_" + tableName.getNameAsString() +
            "_region_" + hri.getEncodedName()+
            "_metric_";
        metricsHelper.assertCounter(prefix + metric, expectedValue, agg);
      }
    }
  }

  public void doNPuts(int n, boolean batch) throws Exception {
    if (batch) {
      List<Put> puts = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        Put p = new Put(Bytes.toBytes("" + i + "row")).addColumn(cf, qualifier, val);
        puts.add(p);
      }
      table.put(puts);
    } else {
      for (int i = 0; i < n; i++) {
        Put p = new Put(row).addColumn(cf, qualifier, val);
        table.put(p);
      }
    }
  }

  public void doNGets(int n, boolean batch) throws Exception {
    if (batch) {
      List<Get> gets = new ArrayList<>();
      for (int i = 0; i < n; i++) {
        gets.add(new Get(row));
      }
      table.get(gets);
    } else {
      for (int i = 0; i < n; i++) {
        table.get(new Get(row));
      }
    }
  }

  @Test
  public void testRegionCount() throws Exception {
    metricsHelper.assertGauge("regionCount", 1, serverSource);
  }

  @Test
  public void testLocalFiles() throws Exception {
    assertGauge("percentFilesLocal", 0);
    assertGauge("percentFilesLocalSecondaryRegions", 0);
  }

  @Test
  public void testRequestCount() throws Exception {
    // Do a first put to be sure that the connection is established, meta is there and so on.
    doNPuts(1, false);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    long requests = metricsHelper.getCounter("totalRequestCount", serverSource);
    long readRequests = metricsHelper.getCounter("readRequestCount", serverSource);
    long writeRequests = metricsHelper.getCounter("writeRequestCount", serverSource);

    doNPuts(30, false);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("totalRequestCount", requests + 30);
    assertCounter("readRequestCount", readRequests);
    assertCounter("writeRequestCount", writeRequests + 30);

    doNGets(10, false);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("totalRequestCount", requests + 40);
    assertCounter("readRequestCount", readRequests + 10);
    assertCounter("writeRequestCount", writeRequests + 30);

    assertRegionMetrics("getNumOps", 10);
    assertRegionMetrics("mutateCount", 31);

    doNGets(10, true);  // true = batch

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("totalRequestCount", requests + 50);
    assertCounter("readRequestCount", readRequests + 20);
    assertCounter("writeRequestCount", writeRequests + 30);

    doNPuts(30, true);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("totalRequestCount", requests + 80);
    assertCounter("readRequestCount", readRequests + 20);
    assertCounter("writeRequestCount", writeRequests + 60);
  }

  @Test
  public void testGet() throws Exception {
    // Do a first put to be sure that the connection is established, meta is there and so on.
    doNPuts(1, false);
    doNGets(10, false);
    assertRegionMetrics("getNumOps", 10);
    assertRegionMetrics("getSizeNumOps", 10);
    metricsHelper.assertCounterGt("Get_num_ops", 10, serverSource);
  }

  @Test
  public void testMutationsWithoutWal() throws Exception {
    Put p = new Put(row).addColumn(cf, qualifier, val)
        .setDurability(Durability.SKIP_WAL);
    table.put(p);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertGauge("mutationsWithoutWALCount", 1);
    long minLength = row.length + cf.length + qualifier.length + val.length;
    metricsHelper.assertGaugeGt("mutationsWithoutWALSize", minLength, serverSource);
  }

  @Test
  public void testStoreCount() throws Exception {
    //Force a hfile.
    doNPuts(1, false);
    TEST_UTIL.getHBaseAdmin().flush(tableName);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertGauge("storeCount", 1);
    assertGauge("storeFileCount", 1);
  }

  @Test
  public void testStoreFileAge() throws Exception {
    //Force a hfile.
    doNPuts(1, false);
    TEST_UTIL.getHBaseAdmin().flush(tableName);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertTrue(metricsHelper.getGaugeLong("maxStoreFileAge", serverSource) > 0);
    assertTrue(metricsHelper.getGaugeLong("minStoreFileAge", serverSource) > 0);
    assertTrue(metricsHelper.getGaugeLong("avgStoreFileAge", serverSource) > 0);
  }

  @Test
  public void testCheckAndPutCount() throws Exception {
    byte[] valOne = Bytes.toBytes("Value");
    byte[] valTwo = Bytes.toBytes("ValueTwo");
    byte[] valThree = Bytes.toBytes("ValueThree");

    Put p = new Put(row);
    p.addColumn(cf, qualifier, valOne);
    table.put(p);

    Put pTwo = new Put(row);
    pTwo.addColumn(cf, qualifier, valTwo);
    table.checkAndPut(row, cf, qualifier, valOne, pTwo);

    Put pThree = new Put(row);
    pThree.addColumn(cf, qualifier, valThree);
    table.checkAndPut(row, cf, qualifier, valOne, pThree);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("checkMutateFailedCount", 1);
    assertCounter("checkMutatePassedCount", 1);
  }

  @Test
  public void testIncrement() throws Exception {
    Put p = new Put(row).addColumn(cf, qualifier, Bytes.toBytes(0L));
    table.put(p);

    for(int count = 0; count < 13; count++) {
      Increment inc = new Increment(row);
      inc.addColumn(cf, qualifier, 100);
      table.increment(inc);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("incrementNumOps", 13);
  }

  @Test
  public void testAppend() throws Exception {
    doNPuts(1, false);

    for(int count = 0; count< 73; count++) {
      Append append = new Append(row);
      append.add(cf, qualifier, Bytes.toBytes(",Test"));
      table.append(append);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("appendNumOps", 73);
  }

  @Test
  public void testScanSize() throws Exception {
    doNPuts(100, true);  // batch put
    Scan s = new Scan();
    s.setBatch(1);
    s.setCaching(1);
    ResultScanner resultScanners = table.getScanner(s);

    for (int nextCount = 0; nextCount < NUM_SCAN_NEXT; nextCount++) {
      Result result = resultScanners.next();
      assertNotNull(result);
      assertEquals(1, result.size());
    }
    numScanNext += NUM_SCAN_NEXT;
    assertRegionMetrics("scanSizeNumOps", NUM_SCAN_NEXT);
    assertCounter("ScanSize_num_ops", numScanNext);
  }

  @Test
  public void testScanTime() throws Exception {
    doNPuts(100, true);
    Scan s = new Scan();
    s.setBatch(1);
    s.setCaching(1);
    ResultScanner resultScanners = table.getScanner(s);

    for (int nextCount = 0; nextCount < NUM_SCAN_NEXT; nextCount++) {
      Result result = resultScanners.next();
      assertNotNull(result);
      assertEquals(1, result.size());
    }
    numScanNext += NUM_SCAN_NEXT;
    assertRegionMetrics("scanTimeNumOps", NUM_SCAN_NEXT);
    assertCounter("ScanTime_num_ops", numScanNext);
  }

  @Test
  public void testScanSizeForSmallScan() throws Exception {
    doNPuts(100, true);
    Scan s = new Scan();
    s.setSmall(true);
    s.setCaching(1);
    ResultScanner resultScanners = table.getScanner(s);

    for (int nextCount = 0; nextCount < NUM_SCAN_NEXT; nextCount++) {
      Result result = resultScanners.next();
      assertNotNull(result);
      assertEquals(1, result.size());
    }
    numScanNext += NUM_SCAN_NEXT;
    assertRegionMetrics("scanSizeNumOps", NUM_SCAN_NEXT);
    assertCounter("ScanSize_num_ops", numScanNext);
  }

  @Test
  public void testMobMetrics() throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf("testMobMetricsLocal");
    int numHfiles = 5;
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(cf);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(0);
    htd.addFamily(hcd);
    byte[] val = Bytes.toBytes("mobdata");
    try {
      Table table = TEST_UTIL.createTable(htd, new byte[0][0], conf);
      Region region = rs.getOnlineRegions(tableName).get(0);
      for (int insertCount = 0; insertCount < numHfiles; insertCount++) {
        Put p = new Put(Bytes.toBytes(insertCount));
        p.addColumn(cf, qualifier, val);
        table.put(p);
        admin.flush(tableName);
      }
      metricsRegionServer.getRegionServerWrapper().forceRecompute();
      assertCounter("mobFlushCount", numHfiles);

      Scan scan = new Scan(Bytes.toBytes(0), Bytes.toBytes(numHfiles));
      ResultScanner scanner = table.getScanner(scan);
      scanner.next(100);
      numScanNext++;  // this is an ugly construct
      scanner.close();
      metricsRegionServer.getRegionServerWrapper().forceRecompute();
      assertCounter("mobScanCellsCount", numHfiles);

      region.getTableDesc().getFamily(cf).setMobThreshold(100);
      // metrics are reset by the region initialization
      ((HRegion) region).initialize();
      region.compact(true);
      metricsRegionServer.getRegionServerWrapper().forceRecompute();
      assertCounter("cellsCountCompactedFromMob", numHfiles);
      assertCounter("cellsCountCompactedToMob", 0);

      scanner = table.getScanner(scan);
      scanner.next(100);
      numScanNext++;  // this is an ugly construct
      metricsRegionServer.getRegionServerWrapper().forceRecompute();
      assertCounter("mobScanCellsCount", 0);

      for (int insertCount = numHfiles; insertCount < 2 * numHfiles; insertCount++) {
        Put p = new Put(Bytes.toBytes(insertCount));
        p.addColumn(cf, qualifier, val);
        table.put(p);
        admin.flush(tableName);
      }
      region.getTableDesc().getFamily(cf).setMobThreshold(0);

      // closing the region forces the compaction.discharger to archive the compacted hfiles
      ((HRegion) region).close();

      // metrics are reset by the region initialization
      ((HRegion) region).initialize();
      region.compact(true);
      metricsRegionServer.getRegionServerWrapper().forceRecompute();
      // metrics are reset by the region initialization
      assertCounter("cellsCountCompactedFromMob", 0);
      assertCounter("cellsCountCompactedToMob", 2 * numHfiles);
    } finally {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  @Test
  @Ignore
  public void testRangeCountMetrics() throws Exception {
    final long[] timeranges =
        { 1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 60000, 120000, 300000, 600000 };
    final String timeRangeType = "TimeRangeCount";
    final String timeRangeMetricName = "Mutate";
    boolean timeRangeCountUpdated = false;

    // Do a first put to be sure that the connection is established, meta is there and so on.
    Put p = new Put(row);
    p.addColumn(cf, qualifier, val);
    table.put(p);

    // do some puts and gets
    for (int i = 0; i < 10; i++) {
      table.put(p);
    }

    Get g = new Get(row);
    for (int i = 0; i < 10; i++) {
      table.get(g);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();

    // Check some time range counters were updated
    long prior = 0;

    String dynamicMetricName;
    for (int i = 0; i < timeranges.length; i++) {
      dynamicMetricName =
          timeRangeMetricName + "_" + timeRangeType + "_" + prior + "-" + timeranges[i];
      if (metricsHelper.checkCounterExists(dynamicMetricName, serverSource)) {
        long count = metricsHelper.getGaugeLong(dynamicMetricName, serverSource);
        if (count > 0) {
          timeRangeCountUpdated = true;
          break;
        }
      }
      prior = timeranges[i];
    }
    dynamicMetricName =
        timeRangeMetricName + "_" + timeRangeType + "_" + timeranges[timeranges.length - 1] + "-inf";
    if (metricsHelper.checkCounterExists(dynamicMetricName, serverSource)) {
      long count = metricsHelper.getCounter(dynamicMetricName, serverSource);
      if (count > 0) {
        timeRangeCountUpdated = true;
      }
    }
    assertEquals(true, timeRangeCountUpdated);
  }

  @Test
  public void testAverageRegionSize() throws Exception {
    //Force a hfile.
    doNPuts(1, false);
    TEST_UTIL.getHBaseAdmin().flush(tableName);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertTrue(metricsHelper.getGaugeDouble("averageRegionSize", serverSource) > 0.0);
  }
}
