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
package org.apache.hadoop.hbase.regionserver.compactions;

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
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCompactionWithThroughputController {

  private static final Log LOG = LogFactory.getLog(TestCompactionWithThroughputController.class);

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

  private Store prepareData() throws IOException {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    HTable table = TEST_UTIL.createTable(tableName, family);
    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[128 * 1024];
        rand.nextBytes(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).add(family, qualifier, value));
      }
      admin.flush(tableName);
    }
    return getStoreWithName(tableName);
  }

  private long testCompactionWithThroughputLimit() throws Exception {
    long throughputLimit = 1024L * 1024;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 200);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    conf.setLong(
      PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
      throughputLimit);
    conf.setLong(
      PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
      throughputLimit);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      PressureAwareCompactionThroughputController.class.getName());
    TEST_UTIL.startMiniCluster(1);
    try {
      Store store = prepareData();
      assertEquals(10, store.getStorefilesCount());
      long startTime = System.currentTimeMillis();
      TEST_UTIL.getHBaseAdmin().majorCompact(tableName);
      while (store.getStorefilesCount() != 1) {
        Thread.sleep(20);
      }
      long duration = System.currentTimeMillis() - startTime;
      double throughput = (double) store.getStorefilesSize() / duration * 1000;
      // confirm that the speed limit work properly(not too fast, and also not too slow)
      // 20% is the max acceptable error rate.
      assertTrue(throughput < throughputLimit * 1.2);
      assertTrue(throughput > throughputLimit * 0.8);
      return System.currentTimeMillis() - startTime;
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private long testCompactionWithoutThroughputLimit() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 200);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      NoLimitCompactionThroughputController.class.getName());
    TEST_UTIL.startMiniCluster(1);
    try {
      Store store = prepareData();
      assertEquals(10, store.getStorefilesCount());
      long startTime = System.currentTimeMillis();
      TEST_UTIL.getHBaseAdmin().majorCompact(tableName);
      while (store.getStorefilesCount() != 1) {
        Thread.sleep(20);
      }
      return System.currentTimeMillis() - startTime;
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test
  public void testCompaction() throws Exception {
    long limitTime = testCompactionWithThroughputLimit();
    long noLimitTime = testCompactionWithoutThroughputLimit();
    LOG.info("With 1M/s limit, compaction use " + limitTime + "ms; without limit, compaction use "
        + noLimitTime + "ms");
    // usually the throughput of a compaction without limitation is about 40MB/sec at least, so this
    // is a very weak assumption.
    assertTrue(limitTime > noLimitTime * 2);
  }

  /**
   * Test the tuning task of {@link PressureAwareCompactionThroughputController}
   */
  @Test
  public void testThroughputTuning() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    conf.setLong(
      PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
      20L * 1024 * 1024);
    conf.setLong(
      PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
      10L * 1024 * 1024);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 4);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 6);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      PressureAwareCompactionThroughputController.class.getName());
    conf.setInt(
      PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD,
      1000);
    TEST_UTIL.startMiniCluster(1);
    Connection conn = ConnectionFactory.createConnection(conf);
    try {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(family));
      htd.setCompactionEnabled(false);
      TEST_UTIL.getHBaseAdmin().createTable(htd);
      TEST_UTIL.waitTableAvailable(tableName);
      HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      PressureAwareCompactionThroughputController throughputController =
          (PressureAwareCompactionThroughputController) regionServer.compactSplitThread
              .getCompactionThroughputController();
      assertEquals(10L * 1024 * 1024, throughputController.maxThroughput, EPSILON);
      Table table = conn.getTable(tableName);
      for (int i = 0; i < 5; i++) {
        table.put(new Put(Bytes.toBytes(i)).add(family, qualifier, new byte[0]));
        TEST_UTIL.flush(tableName);
      }
      Thread.sleep(2000);
      assertEquals(15L * 1024 * 1024, throughputController.maxThroughput, EPSILON);

      table.put(new Put(Bytes.toBytes(5)).add(family, qualifier, new byte[0]));
      TEST_UTIL.flush(tableName);
      Thread.sleep(2000);
      assertEquals(20L * 1024 * 1024, throughputController.maxThroughput, EPSILON);

      table.put(new Put(Bytes.toBytes(6)).add(family, qualifier, new byte[0]));
      TEST_UTIL.flush(tableName);
      Thread.sleep(2000);
      assertEquals(Double.MAX_VALUE, throughputController.maxThroughput, EPSILON);

      conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
        NoLimitCompactionThroughputController.class.getName());
      regionServer.compactSplitThread.onConfigurationChange(conf);
      assertTrue(throughputController.isStopped());
      assertTrue(regionServer.compactSplitThread.getCompactionThroughputController() instanceof NoLimitCompactionThroughputController);
    } finally {
      conn.close();
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  /**
   * Test the logic that we calculate compaction pressure for a striped store.
   */
  @Test
  public void testGetCompactionPressureForStripedStore() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, StripeStoreEngine.class.getName());
    conf.setBoolean(StripeStoreConfig.FLUSH_TO_L0_KEY, false);
    conf.setInt(StripeStoreConfig.INITIAL_STRIPE_COUNT_KEY, 2);
    conf.setInt(StripeStoreConfig.MIN_FILES_KEY, 4);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 12);
    TEST_UTIL.startMiniCluster(1);
    Connection conn = ConnectionFactory.createConnection(conf);
    try {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      htd.addFamily(new HColumnDescriptor(family));
      htd.setCompactionEnabled(false);
      TEST_UTIL.getHBaseAdmin().createTable(htd);
      TEST_UTIL.waitTableAvailable(tableName);
      HStore store = (HStore) getStoreWithName(tableName);
      assertEquals(0, store.getStorefilesCount());
      assertEquals(0.0, store.getCompactionPressure(), EPSILON);
      Table table = conn.getTable(tableName);
      for (int i = 0; i < 4; i++) {
        table.put(new Put(Bytes.toBytes(i)).add(family, qualifier, new byte[0]));
        table.put(new Put(Bytes.toBytes(100 + i)).add(family, qualifier, new byte[0]));
        TEST_UTIL.flush(tableName);
      }
      assertEquals(8, store.getStorefilesCount());
      assertEquals(0.0, store.getCompactionPressure(), EPSILON);

      table.put(new Put(Bytes.toBytes(4)).add(family, qualifier, new byte[0]));
      table.put(new Put(Bytes.toBytes(104)).add(family, qualifier, new byte[0]));
      TEST_UTIL.flush(tableName);
      assertEquals(10, store.getStorefilesCount());
      assertEquals(0.5, store.getCompactionPressure(), EPSILON);

      table.put(new Put(Bytes.toBytes(5)).add(family, qualifier, new byte[0]));
      table.put(new Put(Bytes.toBytes(105)).add(family, qualifier, new byte[0]));
      TEST_UTIL.flush(tableName);
      assertEquals(12, store.getStorefilesCount());
      assertEquals(1.0, store.getCompactionPressure(), EPSILON);

      table.put(new Put(Bytes.toBytes(6)).add(family, qualifier, new byte[0]));
      table.put(new Put(Bytes.toBytes(106)).add(family, qualifier, new byte[0]));
      TEST_UTIL.flush(tableName);
      assertEquals(14, store.getStorefilesCount());
      assertEquals(2.0, store.getCompactionPressure(), EPSILON);
    } finally {
      conn.close();
      TEST_UTIL.shutdownMiniCluster();
    }
  }
}
