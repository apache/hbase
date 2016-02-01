/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestFlushWithThroughputController {

  private static final Log LOG = LogFactory.getLog(TestFlushWithThroughputController.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final double EPSILON = 1E-6;

  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());

  private final byte[] family = Bytes.toBytes("f");

  private final byte[] qualifier = Bytes.toBytes("q");

  private Store getStoreWithName(TableName tableName) {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region region : hrs.getOnlineRegions(tableName)) {
        return region.getStores().iterator().next();
      }
    }
    return null;
  }

  private Store generateAndFlushData() throws IOException {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTable table = TEST_UTIL.createTable(tableName, family);
    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[256 * 1024];
        rand.nextBytes(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
      }
      admin.flush(tableName);
    }
    return getStoreWithName(tableName);
  }

  private long testFlushWithThroughputLimit() throws Exception {
    long throughputLimit = 1L * 1024 * 1024;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(FlushThroughputControllerFactory.HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
      PressureAwareFlushThroughputController.class.getName());
    conf.setLong(
      PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND,
      throughputLimit);
    conf.setLong(
      PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_UPPER_BOUND,
      throughputLimit);
    conf.setLong(
      PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_THROUGHPUT_CONTROL_CHECK_INTERVAL,
      throughputLimit);
    TEST_UTIL.startMiniCluster(1);
    try {
      long startTime = System.nanoTime();
      Store store = generateAndFlushData();
      assertEquals(10, store.getStorefilesCount());
      long duration = System.nanoTime() - startTime;
      double throughput = (double) store.getStorefilesSize() / duration * 1000 * 1000 * 1000;
      LOG.debug("Throughput is: " + (throughput / 1024 / 1024) + " MB/s");
      // confirm that the speed limit work properly(not too fast, and also not too slow)
      // 20% is the max acceptable error rate.
      assertTrue(throughput < throughputLimit * 1.2);
      assertTrue(throughput > throughputLimit * 0.8);
      return duration;
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private long testFlushWithoutThroughputLimit() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(FlushThroughputControllerFactory.HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
      NoLimitThroughputController.class.getName());
    TEST_UTIL.startMiniCluster(1);
    try {
      long startTime = System.nanoTime();
      Store store = generateAndFlushData();
      assertEquals(10, store.getStorefilesCount());
      long duration = System.nanoTime() - startTime;
      double throughput = (double) store.getStorefilesSize() / duration * 1000 * 1000 * 1000;
      LOG.debug("Throughput w/o limit is: " + (throughput / 1024 / 1024) + " MB/s");
      return duration;
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testFlushControl() throws Exception {
    long limitTime = testFlushWithThroughputLimit();
    long noLimitTime = testFlushWithoutThroughputLimit();
    LOG.info("With 1M/s limit, flush use " + (limitTime / 1000000)
        + "ms; without limit, flush use " + (noLimitTime / 1000000) + "ms");
    // Commonly if multiple region flush at the same time, the throughput could be very high
    // but flush in this test is in serial, so we use a weak assumption.
    assertTrue(limitTime > 2 * noLimitTime);
  }

  /**
   * Test the tuning task of {@link PressureAwareFlushThroughputController}
   */
  @Test
  public void testFlushThroughputTuning() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    conf.setLong(
      PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_UPPER_BOUND,
      20L * 1024 * 1024);
    conf.setLong(
      PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_MAX_THROUGHPUT_LOWER_BOUND,
      10L * 1024 * 1024);
    conf.set(FlushThroughputControllerFactory.HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
      PressureAwareFlushThroughputController.class.getName());
    conf.setInt(PressureAwareFlushThroughputController.HBASE_HSTORE_FLUSH_THROUGHPUT_TUNE_PERIOD,
      3000);
    TEST_UTIL.startMiniCluster(1);
    Connection conn = ConnectionFactory.createConnection(conf);
    try {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(family));
      htd.setCompactionEnabled(false);
      TEST_UTIL.getHBaseAdmin().createTable(htd);
      TEST_UTIL.waitTableAvailable(tableName);
      HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      PressureAwareFlushThroughputController throughputController =
          (PressureAwareFlushThroughputController) regionServer.getFlushThroughputController();
      for (Region region : regionServer.getOnlineRegions()) {
        region.flush(true);
      }
      assertEquals(0.0, regionServer.getFlushPressure(), EPSILON);
      Thread.sleep(5000);
      assertEquals(10L * 1024 * 1024, throughputController.getMaxThroughput(), EPSILON);
      Table table = conn.getTable(tableName);
      Random rand = new Random();
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
          byte[] value = new byte[256 * 1024];
          rand.nextBytes(value);
          table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
        }
      }
      Thread.sleep(5000);
      double expectedThroughPut = 10L * 1024 * 1024 * (1 + regionServer.getFlushPressure());
      assertEquals(expectedThroughPut, throughputController.getMaxThroughput(), EPSILON);

      conf.set(FlushThroughputControllerFactory.HBASE_FLUSH_THROUGHPUT_CONTROLLER_KEY,
        NoLimitThroughputController.class.getName());
      regionServer.onConfigurationChange(conf);
      assertTrue(throughputController.isStopped());
      assertTrue(regionServer.getFlushThroughputController() instanceof NoLimitThroughputController);
    } finally {
      conn.close();
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  /**
   * Test the logic for striped store.
   */
  @Test
  public void testFlushControlForStripedStore() throws Exception {
    TEST_UTIL.getConfiguration().set(StoreEngine.STORE_ENGINE_CLASS_KEY,
      StripeStoreEngine.class.getName());
    testFlushControl();
  }
}
