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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RegionServerTests.class, LargeTests.class})
public class TestRegionServerMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionServerMetrics.class);

  @Rule
  public TestName testName = new TestName();

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
  private static boolean TABLES_ON_MASTER;

  @BeforeClass
  public static void startCluster() throws Exception {
    metricsHelper = CompatibilityFactory.getInstance(MetricsAssertHelper.class);
    TEST_UTIL = new HBaseTestingUtility();
    TABLES_ON_MASTER = LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration());
    conf = TEST_UTIL.getConfiguration();
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    // testMobMetrics creates few hfiles and manages compaction manually.
    conf.setInt("hbase.hstore.compactionThreshold", 100);
    conf.setInt("hbase.hstore.compaction.max", 100);
    conf.setInt("hbase.regionserver.periodicmemstoreflusher.rangeofdelayseconds", 4*60);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);

    TEST_UTIL.startMiniCluster();
    cluster = TEST_UTIL.getHBaseCluster();
    cluster.waitForActiveAndReadyMaster();
    admin = TEST_UTIL.getAdmin();
    connection = TEST_UTIL.getConnection();

    while (cluster.getLiveRegionServerThreads().isEmpty() &&
        cluster.getRegionServer(0) == null &&
        rs.getMetrics() == null) {
      Threads.sleep(100);
    }
    rs = cluster.getRegionServer(0);
    metricsRegionServer = rs.getMetrics();
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

  public void doScan(int n, boolean caching) throws IOException {
    Scan scan = new Scan();
    if (caching) {
      scan.setCaching(n);
    } else {
      scan.setCaching(1);
    }
    ResultScanner scanner = table.getScanner(scan);
    for (int i = 0; i < n; i++) {
      Result res = scanner.next();
      LOG.debug("Result row: " + Bytes.toString(res.getRow()) + ", value: " +
          Bytes.toString(res.getValue(cf, qualifier)));
    }
  }

  @Test
  public void testRegionCount() throws Exception {
    metricsHelper.assertGauge("regionCount", TABLES_ON_MASTER? 1: 3, serverSource);
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
    long rowActionRequests = metricsHelper.getCounter("totalRowActionRequestCount", serverSource);
    long readRequests = metricsHelper.getCounter("readRequestCount", serverSource);
    long writeRequests = metricsHelper.getCounter("writeRequestCount", serverSource);

    doNPuts(30, false);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("totalRequestCount", requests + 30);
    assertCounter("totalRowActionRequestCount", rowActionRequests + 30);
    assertCounter("readRequestCount", readRequests);
    assertCounter("writeRequestCount", writeRequests + 30);

    doNGets(10, false);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertCounter("totalRequestCount", requests + 40);
    assertCounter("totalRowActionRequestCount", rowActionRequests + 40);
    assertCounter("readRequestCount", readRequests + 10);
    assertCounter("writeRequestCount", writeRequests + 30);

    assertRegionMetrics("getCount", 10);
    assertRegionMetrics("putCount", 31);

    doNGets(10, true);  // true = batch

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    if (TABLES_ON_MASTER) {
      assertCounter("totalRequestCount", requests + 41);
      assertCounter("totalRowActionRequestCount", rowActionRequests + 50);
      assertCounter("readRequestCount", readRequests + 20);
    }


    assertCounter("writeRequestCount", writeRequests + 30);

    doNPuts(30, true);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    if (TABLES_ON_MASTER) {
      assertCounter("totalRequestCount", requests + 42);
      assertCounter("totalRowActionRequestCount", rowActionRequests + 80);
      assertCounter("readRequestCount", readRequests + 20);
    }
    assertCounter("writeRequestCount", writeRequests + 60);

    doScan(10, false); // test after batch put so we have enough lines
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    if (TABLES_ON_MASTER) {
      assertCounter("totalRequestCount", requests + 52);
      assertCounter("totalRowActionRequestCount", rowActionRequests + 90);
      assertCounter("readRequestCount", readRequests + 30);
    }
    assertCounter("writeRequestCount", writeRequests + 60);
    numScanNext += 10;

    doScan(10, true); // true = caching
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    if (TABLES_ON_MASTER) {
      assertCounter("totalRequestCount", requests + 53);
      assertCounter("totalRowActionRequestCount", rowActionRequests + 100);
      assertCounter("readRequestCount", readRequests + 40);
    }
    assertCounter("writeRequestCount", writeRequests + 60);
    numScanNext += 1;
  }

  @Test
  public void testGet() throws Exception {
    // Do a first put to be sure that the connection is established, meta is there and so on.
    doNPuts(1, false);
    doNGets(10, false);
    assertRegionMetrics("getCount", 10);
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
    // Force a hfile.
    doNPuts(1, false);
    TEST_UTIL.getAdmin().flush(tableName);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertGauge("storeCount", TABLES_ON_MASTER ? 1 : 5);
    assertGauge("storeFileCount", 1);
  }

  @Test
  public void testStoreFileAge() throws Exception {
    //Force a hfile.
    doNPuts(1, false);
    TEST_UTIL.getAdmin().flush(tableName);

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
    table.checkAndMutate(row, cf).qualifier(qualifier).ifEquals(valOne).thenPut(pTwo);

    Put pThree = new Put(row);
    pThree.addColumn(cf, qualifier, valThree);
    table.checkAndMutate(row, cf).qualifier(qualifier).ifEquals(valOne).thenPut(pThree);

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
      append.addColumn(cf, qualifier, Bytes.toBytes(",Test"));
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
    assertRegionMetrics("scanCount", NUM_SCAN_NEXT);
    if (TABLES_ON_MASTER) {
      assertCounter("ScanSize_num_ops", numScanNext);
    }
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
    assertRegionMetrics("scanCount", NUM_SCAN_NEXT);
    if (TABLES_ON_MASTER) {
      assertCounter("ScanTime_num_ops", numScanNext);
    }
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
      if (TABLES_ON_MASTER) {
        assertEquals(1, result.size());
      }
    }
    numScanNext += NUM_SCAN_NEXT;
    assertRegionMetrics("scanCount", NUM_SCAN_NEXT);
    if (TABLES_ON_MASTER) {
      assertCounter("ScanSize_num_ops", numScanNext);
    }
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
      HRegion region = rs.getRegions(tableName).get(0);
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

      setMobThreshold(region, cf, 100);
      // metrics are reset by the region initialization
      region.initialize();
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
      setMobThreshold(region, cf, 0);

      // closing the region forces the compaction.discharger to archive the compacted hfiles
      region.close();

      // metrics are reset by the region initialization
      region.initialize();
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

  private static Region setMobThreshold(Region region, byte[] cfName, long modThreshold) {
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder
            .newBuilder(region.getTableDescriptor().getColumnFamily(cfName))
            .setMobThreshold(modThreshold)
            .build();
    TableDescriptor td = TableDescriptorBuilder
            .newBuilder(region.getTableDescriptor())
            .removeColumnFamily(cfName)
            .setColumnFamily(cfd)
            .build();
    ((HRegion)region).setTableDescriptor(td);
    return region;
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
    TEST_UTIL.getAdmin().flush(tableName);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertTrue(metricsHelper.getGaugeDouble("averageRegionSize", serverSource) > 0.0);
  }

  @Test
  public void testReadBytes() throws Exception {
    // Do a first put to be sure that the connection is established, meta is there and so on.
    doNPuts(1, false);
    doNGets(10, false);
    TEST_UTIL.getAdmin().flush(tableName);
    metricsRegionServer.getRegionServerWrapper().forceRecompute();

    assertTrue("Total read bytes should be larger than 0",
        metricsRegionServer.getRegionServerWrapper().getTotalBytesRead() > 0);
    assertTrue("Total local read bytes should be larger than 0",
        metricsRegionServer.getRegionServerWrapper().getLocalBytesRead() > 0);
    assertEquals("Total short circuit read bytes should be equal to 0", 0,
        metricsRegionServer.getRegionServerWrapper().getShortCircuitBytesRead());
    assertEquals("Total zero-byte read bytes should be equal to 0", 0,
        metricsRegionServer.getRegionServerWrapper().getZeroCopyBytesRead());
  }
}
