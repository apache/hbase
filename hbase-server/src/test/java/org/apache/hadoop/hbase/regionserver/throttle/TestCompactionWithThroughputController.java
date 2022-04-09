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
package org.apache.hadoop.hbase.regionserver.throttle;

import static org.apache.hadoop.hbase.regionserver.throttle.PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND;
import static org.apache.hadoop.hbase.regionserver.throttle.PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND;
import static org.apache.hadoop.hbase.regionserver.throttle.PressureAwareCompactionThroughputController.HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StripeStoreConfig;
import org.apache.hadoop.hbase.regionserver.StripeStoreEngine;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestCompactionWithThroughputController {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionWithThroughputController.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestCompactionWithThroughputController.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final double EPSILON = 1E-6;

  private final TableName tableName = TableName.valueOf(getClass().getSimpleName());

  private final byte[] family = Bytes.toBytes("f");

  private final byte[] qualifier = Bytes.toBytes("q");

  private HStore getStoreWithName(TableName tableName) {
    SingleProcessHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region region : hrs.getRegions(tableName)) {
        return ((HRegion) region).getStores().iterator().next();
      }
    }
    return null;
  }

  private HStore prepareData() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    Table table = TEST_UTIL.createTable(tableName, family);
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[128 * 1024];
        Bytes.random(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
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
    conf.setLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND, throughputLimit);
    conf.setLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND, throughputLimit);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      PressureAwareCompactionThroughputController.class.getName());
    TEST_UTIL.startMiniCluster(1);
    try {
      HStore store = prepareData();
      assertEquals(10, store.getStorefilesCount());
      long startTime = EnvironmentEdgeManager.currentTime();
      TEST_UTIL.getAdmin().majorCompact(tableName);
      while (store.getStorefilesCount() != 1) {
        Thread.sleep(20);
      }
      long duration = EnvironmentEdgeManager.currentTime() - startTime;
      double throughput = (double) store.getStorefilesSize() / duration * 1000;
      // confirm that the speed limit work properly(not too fast, and also not too slow)
      // 20% is the max acceptable error rate.
      assertTrue(throughput < throughputLimit * 1.2);
      assertTrue(throughput > throughputLimit * 0.8);
      return EnvironmentEdgeManager.currentTime() - startTime;
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
      NoLimitThroughputController.class.getName());
    TEST_UTIL.startMiniCluster(1);
    try {
      HStore store = prepareData();
      assertEquals(10, store.getStorefilesCount());
      long startTime = EnvironmentEdgeManager.currentTime();
      TEST_UTIL.getAdmin().majorCompact(tableName);
      while (store.getStorefilesCount() != 1) {
        Thread.sleep(20);
      }
      return EnvironmentEdgeManager.currentTime() - startTime;
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
    conf.setLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND, 20L * 1024 * 1024);
    conf.setLong(HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND, 10L * 1024 * 1024);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 4);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 6);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      PressureAwareCompactionThroughputController.class.getName());
    conf.setInt(HBASE_HSTORE_COMPACTION_THROUGHPUT_TUNE_PERIOD, 1000);
    TEST_UTIL.startMiniCluster(1);
    Connection conn = ConnectionFactory.createConnection(conf);
    try {
      TEST_UTIL.getAdmin()
          .createTable(TableDescriptorBuilder.newBuilder(tableName)
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).setCompactionEnabled(false)
              .build());
      TEST_UTIL.waitTableAvailable(tableName);
      HRegionServer regionServer = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      PressureAwareCompactionThroughputController throughputController =
          (PressureAwareCompactionThroughputController) regionServer.getCompactSplitThread()
              .getCompactionThroughputController();
      assertEquals(10L * 1024 * 1024, throughputController.getMaxThroughput(), EPSILON);
      Table table = conn.getTable(tableName);
      for (int i = 0; i < 5; i++) {
        byte[] value = new byte[0];
        table.put(new Put(Bytes.toBytes(i)).addColumn(family, qualifier, value));
        TEST_UTIL.flush(tableName);
      }
      Thread.sleep(2000);
      assertEquals(15L * 1024 * 1024, throughputController.getMaxThroughput(), EPSILON);

      byte[] value1 = new byte[0];
      table.put(new Put(Bytes.toBytes(5)).addColumn(family, qualifier, value1));
      TEST_UTIL.flush(tableName);
      Thread.sleep(2000);
      assertEquals(20L * 1024 * 1024, throughputController.getMaxThroughput(), EPSILON);

      byte[] value = new byte[0];
      table.put(new Put(Bytes.toBytes(6)).addColumn(family, qualifier, value));
      TEST_UTIL.flush(tableName);
      Thread.sleep(2000);
      assertEquals(Double.MAX_VALUE, throughputController.getMaxThroughput(), EPSILON);

      conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
        NoLimitThroughputController.class.getName());
      regionServer.getCompactSplitThread().onConfigurationChange(conf);
      assertTrue(throughputController.isStopped());
      assertTrue(regionServer.getCompactSplitThread().getCompactionThroughputController()
        instanceof NoLimitThroughputController);
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
      TEST_UTIL.getAdmin()
          .createTable(TableDescriptorBuilder.newBuilder(tableName)
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).setCompactionEnabled(false)
              .build());
      TEST_UTIL.waitTableAvailable(tableName);
      HStore store = getStoreWithName(tableName);
      assertEquals(0, store.getStorefilesCount());
      assertEquals(0.0, store.getCompactionPressure(), EPSILON);
      Table table = conn.getTable(tableName);
      for (int i = 0; i < 4; i++) {
        byte[] value1 = new byte[0];
        table.put(new Put(Bytes.toBytes(i)).addColumn(family, qualifier, value1));
        byte[] value = new byte[0];
        table.put(new Put(Bytes.toBytes(100 + i)).addColumn(family, qualifier, value));
        TEST_UTIL.flush(tableName);
      }
      assertEquals(8, store.getStorefilesCount());
      assertEquals(0.0, store.getCompactionPressure(), EPSILON);

      byte[] value5 = new byte[0];
      table.put(new Put(Bytes.toBytes(4)).addColumn(family, qualifier, value5));
      byte[] value4 = new byte[0];
      table.put(new Put(Bytes.toBytes(104)).addColumn(family, qualifier, value4));
      TEST_UTIL.flush(tableName);
      assertEquals(10, store.getStorefilesCount());
      assertEquals(0.5, store.getCompactionPressure(), EPSILON);

      byte[] value3 = new byte[0];
      table.put(new Put(Bytes.toBytes(5)).addColumn(family, qualifier, value3));
      byte[] value2 = new byte[0];
      table.put(new Put(Bytes.toBytes(105)).addColumn(family, qualifier, value2));
      TEST_UTIL.flush(tableName);
      assertEquals(12, store.getStorefilesCount());
      assertEquals(1.0, store.getCompactionPressure(), EPSILON);

      byte[] value1 = new byte[0];
      table.put(new Put(Bytes.toBytes(6)).addColumn(family, qualifier, value1));
      byte[] value = new byte[0];
      table.put(new Put(Bytes.toBytes(106)).addColumn(family, qualifier, value));
      TEST_UTIL.flush(tableName);
      assertEquals(14, store.getStorefilesCount());
      assertEquals(2.0, store.getCompactionPressure(), EPSILON);
    } finally {
      conn.close();
      TEST_UTIL.shutdownMiniCluster();
    }
  }
}
