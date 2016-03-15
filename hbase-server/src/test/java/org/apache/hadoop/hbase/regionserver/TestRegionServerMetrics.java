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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


@Category({RegionServerTests.class, MediumTests.class})
public class TestRegionServerMetrics {
  private static MetricsAssertHelper metricsHelper;

  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  private static MiniHBaseCluster cluster;
  private static HRegionServer rs;
  private static Configuration conf;
  private static HBaseTestingUtility TEST_UTIL;
  private static MetricsRegionServer metricsRegionServer;
  private static MetricsRegionServerSource serverSource;
  private static final int NUM_SCAN_NEXT = 30;
  private static int numScanNext = 0;

  @BeforeClass
  public static void startCluster() throws Exception {
    metricsHelper = CompatibilityFactory.getInstance(MetricsAssertHelper.class);
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);

    TEST_UTIL.startMiniCluster(1, 1);
    cluster = TEST_UTIL.getHBaseCluster();

    cluster.waitForActiveAndReadyMaster();

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

  @Test(timeout = 300000)
  public void testRegionCount() throws Exception {
    String regionMetricsKey = "regionCount";
    long regions = metricsHelper.getGaugeLong(regionMetricsKey, serverSource);
    // Creating a table should add one region
    TEST_UTIL.createTable(TableName.valueOf("table"), Bytes.toBytes("cf"));
    metricsHelper.assertGaugeGt(regionMetricsKey, regions, serverSource);
  }

  @Test
  public void testLocalFiles() throws Exception {
    metricsHelper.assertGauge("percentFilesLocal", 0, serverSource);
    metricsHelper.assertGauge("percentFilesLocalSecondaryRegions", 0, serverSource);
  }

  @Test
  public void testRequestCount() throws Exception {
    String tableNameString = "testRequestCount";
    TableName tName = TableName.valueOf(tableNameString);
    byte[] cfName = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] initValue = Bytes.toBytes("Value");

    TEST_UTIL.createTable(tName, cfName);

    Connection connection = TEST_UTIL.getConnection();
    connection.getTable(tName).close(); //wait for the table to come up.

    // Do a first put to be sure that the connection is established, meta is there and so on.
    Table table = connection.getTable(tName);
    Put p = new Put(row);
    p.addColumn(cfName, qualifier, initValue);
    table.put(p);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    long requests = metricsHelper.getCounter("totalRequestCount", serverSource);
    long readRequests = metricsHelper.getCounter("readRequestCount", serverSource);
    long writeRequests = metricsHelper.getCounter("writeRequestCount", serverSource);

    for (int i=0; i< 30; i++) {
      table.put(p);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 30, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests, serverSource);
    metricsHelper.assertCounter("writeRequestCount", writeRequests + 30, serverSource);

    Get g = new Get(row);
    for (int i=0; i< 10; i++) {
      table.get(g);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 40, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests + 10, serverSource);
    metricsHelper.assertCounter("writeRequestCount", writeRequests + 30, serverSource);

    try (RegionLocator locator = connection.getRegionLocator(tName)) {
      for ( HRegionLocation location: locator.getAllRegionLocations()) {
        HRegionInfo i = location.getRegionInfo();
        MetricsRegionAggregateSource agg = rs.getRegion(i.getRegionName())
            .getMetrics()
            .getSource()
            .getAggregateSource();
        String prefix = "namespace_"+NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR+
            "_table_"+tableNameString +
            "_region_" + i.getEncodedName()+
            "_metric";
        metricsHelper.assertCounter(prefix + "_getNumOps", 10, agg);
        metricsHelper.assertCounter(prefix + "_mutateCount", 31, agg);
      }
    }
    List<Get> gets = new ArrayList<Get>();
    for (int i=0; i< 10; i++) {
      gets.add(new Get(row));
    }
    table.get(gets);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 50, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests + 20, serverSource);
    metricsHelper.assertCounter("writeRequestCount", writeRequests + 30, serverSource);

    List<Put> puts = new ArrayList<>();
    for (int i=0; i< 30; i++) {
      puts.add(p);
    }
    table.put(puts);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 80, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests + 20, serverSource);
    metricsHelper.assertCounter("writeRequestCount", writeRequests + 60, serverSource);

    table.close();
  }

  @Test
  public void testGet() throws Exception {
    String tableNameString = "testGet";
    TableName tName = TableName.valueOf(tableNameString);
    byte[] cfName = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] initValue = Bytes.toBytes("Value");

    TEST_UTIL.createTable(tName, cfName);

    Connection connection = TEST_UTIL.getConnection();
    connection.getTable(tName).close(); //wait for the table to come up.

    // Do a first put to be sure that the connection is established, meta is there and so on.
    Table table = connection.getTable(tName);
    Put p = new Put(row);
    p.addColumn(cfName, qualifier, initValue);
    table.put(p);

    Get g = new Get(row);
    for (int i=0; i< 10; i++) {
      table.get(g);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();

    try (RegionLocator locator = connection.getRegionLocator(tName)) {
      for ( HRegionLocation location: locator.getAllRegionLocations()) {
        HRegionInfo i = location.getRegionInfo();
        MetricsRegionAggregateSource agg = rs.getRegion(i.getRegionName())
          .getMetrics()
          .getSource()
          .getAggregateSource();
        String prefix = "namespace_"+NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR+
          "_table_"+tableNameString +
          "_region_" + i.getEncodedName()+
          "_metric";
        metricsHelper.assertCounter(prefix + "_getSizeNumOps", 10, agg);
        metricsHelper.assertCounter(prefix + "_getNumOps", 10, agg);
      }
      metricsHelper.assertCounterGt("Get_num_ops", 10, serverSource);
    }
    table.close();
  }

  @Test
  public void testMutationsWithoutWal() throws Exception {
    TableName tableName = TableName.valueOf("testMutationsWithoutWal");
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("Value");

    metricsRegionServer.getRegionServerWrapper().forceRecompute();

    Table t = TEST_UTIL.createTable(tableName, cf);

    Put p = new Put(row);
    p.addColumn(cf, qualifier, val);
    p.setDurability(Durability.SKIP_WAL);

    t.put(p);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertGauge("mutationsWithoutWALCount", 1, serverSource);
    long minLength = row.length + cf.length + qualifier.length + val.length;
    metricsHelper.assertGaugeGt("mutationsWithoutWALSize", minLength, serverSource);

    t.close();
  }

  @Test
  public void testStoreCount() throws Exception {
    TableName tableName = TableName.valueOf("testStoreCount");
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("Value");

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    long stores = metricsHelper.getGaugeLong("storeCount", serverSource);
    long storeFiles = metricsHelper.getGaugeLong("storeFileCount", serverSource);

    //Force a hfile.
    Table t = TEST_UTIL.createTable(tableName, cf);
    Put p = new Put(row);
    p.addColumn(cf, qualifier, val);
    t.put(p);
    TEST_UTIL.getHBaseAdmin().flush(tableName);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertGauge("storeCount", stores +1, serverSource);
    metricsHelper.assertGauge("storeFileCount", storeFiles + 1, serverSource);

    t.close();
  }

  @Test
  public void testStoreFileAge() throws Exception {
    TableName tableName = TableName.valueOf("testStoreFileAge");
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("Value");

    //Force a hfile.
    Table t = TEST_UTIL.createTable(tableName, cf);
    Put p = new Put(row);
    p.addColumn(cf, qualifier, val);
    t.put(p);
    TEST_UTIL.getHBaseAdmin().flush(tableName);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    assertTrue(metricsHelper.getGaugeLong("maxStoreFileAge", serverSource) > 0);
    assertTrue(metricsHelper.getGaugeLong("minStoreFileAge", serverSource) > 0);
    assertTrue(metricsHelper.getGaugeLong("avgStoreFileAge", serverSource) > 0);

    t.close();
  }

  @Test
  public void testCheckAndPutCount() throws Exception {
    String tableNameString = "testCheckAndPutCount";
    TableName tableName = TableName.valueOf(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] valOne = Bytes.toBytes("Value");
    byte[] valTwo = Bytes.toBytes("ValueTwo");
    byte[] valThree = Bytes.toBytes("ValueThree");

    Table t = TEST_UTIL.createTable(tableName, cf);
    Put p = new Put(row);
    p.addColumn(cf, qualifier, valOne);
    t.put(p);

    Put pTwo = new Put(row);
    pTwo.addColumn(cf, qualifier, valTwo);
    t.checkAndPut(row, cf, qualifier, valOne, pTwo);

    Put pThree = new Put(row);
    pThree.addColumn(cf, qualifier, valThree);
    t.checkAndPut(row, cf, qualifier, valOne, pThree);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("checkMutateFailedCount", 1, serverSource);
    metricsHelper.assertCounter("checkMutatePassedCount", 1, serverSource);

    t.close();
  }

  @Test
  public void testIncrement() throws Exception {
    String tableNameString = "testIncrement";
    TableName tableName = TableName.valueOf(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes(0l);


    Table t = TEST_UTIL.createTable(tableName, cf);
    Put p = new Put(row);
    p.addColumn(cf, qualifier, val);
    t.put(p);

    for(int count = 0; count< 13; count++) {
      Increment inc = new Increment(row);
      inc.addColumn(cf, qualifier, 100);
      t.increment(inc);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("incrementNumOps", 13, serverSource);

    t.close();
  }

  @Test
  public void testAppend() throws Exception {
    String tableNameString = "testAppend";
    TableName tableName = TableName.valueOf(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("One");


    Table t = TEST_UTIL.createTable(tableName, cf);
    Put p = new Put(row);
    p.addColumn(cf, qualifier, val);
    t.put(p);

    for(int count = 0; count< 73; count++) {
      Append append = new Append(row);
      append.add(cf, qualifier, Bytes.toBytes(",Test"));
      t.append(append);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("appendNumOps", 73, serverSource);

    t.close();
  }

  @Test
  public void testScanSize() throws IOException {
    String tableNameString = "testScanSize";
    TableName tableName = TableName.valueOf(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("One");

    List<Put> puts = new ArrayList<>();
    for (int insertCount =0; insertCount < 100; insertCount++) {
      Put p = new Put(Bytes.toBytes("" + insertCount + "row"));
      p.addColumn(cf, qualifier, val);
      puts.add(p);
    }
    try (Table t = TEST_UTIL.createTable(tableName, cf)) {
      t.put(puts);

      Scan s = new Scan();
      s.setBatch(1);
      s.setCaching(1);
      ResultScanner resultScanners = t.getScanner(s);

      for (int nextCount = 0; nextCount < NUM_SCAN_NEXT; nextCount++) {
        Result result = resultScanners.next();
        assertNotNull(result);
        assertEquals(1, result.size());
      }
    }
    numScanNext += NUM_SCAN_NEXT;
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      for ( HRegionLocation location: locator.getAllRegionLocations()) {
        HRegionInfo i = location.getRegionInfo();
        MetricsRegionAggregateSource agg = rs.getRegion(i.getRegionName())
            .getMetrics()
            .getSource()
            .getAggregateSource();
        String prefix = "namespace_"+NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR+
            "_table_"+tableNameString +
            "_region_" + i.getEncodedName()+
            "_metric";
        metricsHelper.assertCounter(prefix + "_scanSizeNumOps", NUM_SCAN_NEXT, agg);
      }
      metricsHelper.assertCounter("ScanSize_num_ops", numScanNext, serverSource);
    }
    try (Admin admin = TEST_UTIL.getHBaseAdmin()) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  @Test
  public void testScanTime() throws IOException {
    String tableNameString = "testScanTime";
    TableName tableName = TableName.valueOf(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("One");

    List<Put> puts = new ArrayList<>();
    for (int insertCount =0; insertCount < 100; insertCount++) {
      Put p = new Put(Bytes.toBytes("" + insertCount + "row"));
      p.addColumn(cf, qualifier, val);
      puts.add(p);
    }
    try (Table t = TEST_UTIL.createTable(tableName, cf)) {
      t.put(puts);

      Scan s = new Scan();
      s.setBatch(1);
      s.setCaching(1);
      ResultScanner resultScanners = t.getScanner(s);

      for (int nextCount = 0; nextCount < NUM_SCAN_NEXT; nextCount++) {
        Result result = resultScanners.next();
        assertNotNull(result);
        assertEquals(1, result.size());
      }
    }
    numScanNext += NUM_SCAN_NEXT;
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      for ( HRegionLocation location: locator.getAllRegionLocations()) {
        HRegionInfo i = location.getRegionInfo();
        MetricsRegionAggregateSource agg = rs.getRegion(i.getRegionName())
          .getMetrics()
          .getSource()
          .getAggregateSource();
        String prefix = "namespace_"+NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR+
          "_table_"+tableNameString +
          "_region_" + i.getEncodedName()+
          "_metric";
        metricsHelper.assertCounter(prefix + "_scanTimeNumOps", NUM_SCAN_NEXT, agg);
      }
      metricsHelper.assertCounter("ScanTime_num_ops", numScanNext, serverSource);
    }
    try (Admin admin = TEST_UTIL.getHBaseAdmin()) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  @Test
  public void testScanSizeForSmallScan() throws IOException {
    String tableNameString = "testScanSizeSmall";
    TableName tableName = TableName.valueOf(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("One");

    List<Put> puts = new ArrayList<>();
    for (int insertCount =0; insertCount < 100; insertCount++) {
      Put p = new Put(Bytes.toBytes("" + insertCount + "row"));
      p.addColumn(cf, qualifier, val);
      puts.add(p);
    }
    try (Table t = TEST_UTIL.createTable(tableName, cf)) {
      t.put(puts);

      Scan s = new Scan();
      s.setSmall(true);
      s.setCaching(1);
      ResultScanner resultScanners = t.getScanner(s);

      for (int nextCount = 0; nextCount < NUM_SCAN_NEXT; nextCount++) {
        Result result = resultScanners.next();
        assertNotNull(result);
        assertEquals(1, result.size());
      }
    }
    numScanNext += NUM_SCAN_NEXT;
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
      for ( HRegionLocation location: locator.getAllRegionLocations()) {
        HRegionInfo i = location.getRegionInfo();
        MetricsRegionAggregateSource agg = rs.getRegion(i.getRegionName())
            .getMetrics()
            .getSource()
            .getAggregateSource();
        String prefix = "namespace_"+NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR+
            "_table_"+tableNameString +
            "_region_" + i.getEncodedName()+
            "_metric";
        metricsHelper.assertCounter(prefix + "_scanSizeNumOps", NUM_SCAN_NEXT, agg);
      }
      metricsHelper.assertCounter("ScanSize_num_ops", numScanNext, serverSource);
    }
    try (Admin admin = TEST_UTIL.getHBaseAdmin()) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  @Test
  public void testMobMetrics() throws IOException, InterruptedException {
    String tableNameString = "testMobMetrics";
    TableName tableName = TableName.valueOf(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("mobdata");
    int numHfiles = conf.getInt("hbase.hstore.compactionThreshold", 3) - 1;
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(cf);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(0);
    htd.addFamily(hcd);
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    HTable t = TEST_UTIL.createTable(htd, new byte[0][0], conf);
    Region region = rs.getOnlineRegions(tableName).get(0);
    t.setAutoFlush(true, true);
    for (int insertCount = 0; insertCount < numHfiles; insertCount++) {
      Put p = new Put(Bytes.toBytes(insertCount));
      p.addColumn(cf, qualifier, val);
      t.put(p);
      admin.flush(tableName);
    }
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("mobFlushCount", numHfiles, serverSource);
    Scan scan = new Scan(Bytes.toBytes(0), Bytes.toBytes(2));
    ResultScanner scanner = t.getScanner(scan);
    scanner.next(100);
    numScanNext++;  // this is an ugly construct
    scanner.close();
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("mobScanCellsCount", 2, serverSource);
    region.getTableDesc().getFamily(cf).setMobThreshold(100);
    ((HRegion)region).initialize();
    region.compact(true);
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("cellsCountCompactedFromMob", numHfiles,
        serverSource);
    metricsHelper.assertCounter("cellsCountCompactedToMob", 0, serverSource);
    scanner = t.getScanner(scan);
    scanner.next(100);
    numScanNext++;  // this is an ugly construct
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    // metrics are reset by the region initialization
    metricsHelper.assertCounter("mobScanCellsCount", 0, serverSource);
    for (int insertCount = numHfiles;
        insertCount < 2 * numHfiles - 1; insertCount++) {
      Put p = new Put(Bytes.toBytes(insertCount));
      p.addColumn(cf, qualifier, val);
      t.put(p);
      admin.flush(tableName);
    }
    region.getTableDesc().getFamily(cf).setMobThreshold(0);
    ((HRegion)region).initialize();
    region.compact(true);
    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    // metrics are reset by the region initialization
    metricsHelper.assertCounter("cellsCountCompactedFromMob", 0, serverSource);
    metricsHelper.assertCounter("cellsCountCompactedToMob", 2 * numHfiles - 1,
        serverSource);
    t.close();
    admin.close();
    connection.close();
  }
  
  @Test
  @Ignore
  public void testRangeCountMetrics() throws Exception {
    String tableNameString = "testRangeCountMetrics";
    final long[] timeranges =
        { 1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000, 60000, 120000, 300000, 600000 };
    final String timeRangeType = "TimeRangeCount";
    final String timeRangeMetricName = "Mutate";
    boolean timeRangeCountUpdated = false;

    TableName tName = TableName.valueOf(tableNameString);
    byte[] cfName = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] initValue = Bytes.toBytes("Value");

    TEST_UTIL.createTable(tName, cfName);

    Connection connection = TEST_UTIL.getConnection();
    connection.getTable(tName).close(); // wait for the table to come up.

    // Do a first put to be sure that the connection is established, meta is there and so on.
    Table table = connection.getTable(tName);
    Put p = new Put(row);
    p.addColumn(cfName, qualifier, initValue);
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

    table.close();
  }
}
