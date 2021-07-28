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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.DefaultStoreEngine;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;


public class TestCompactionWithThroughputControllerBase {
  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static final double EPSILON = 1E-6;

  protected final TableName tableName = TableName.valueOf(getClass().getSimpleName());

  protected final byte[] family = Bytes.toBytes("f");

  protected final byte[] qualifier = Bytes.toBytes("q");

  protected HStore getStoreWithName(TableName tableName) {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region region : hrs.getRegions(tableName)) {
        return ((HRegion) region).getStores().iterator().next();
      }
    }
    return null;
  }

  protected Table createTable() throws IOException {
    return TEST_UTIL.createTable(tableName, family);
  }

  protected HStore prepareData() throws IOException {
    Admin admin = TEST_UTIL.getAdmin();
    if (admin.tableExists(tableName)) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
    Table table = createTable();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        byte[] value = new byte[128 * 1024];
        ThreadLocalRandom.current().nextBytes(value);
        table.put(new Put(Bytes.toBytes(i * 10 + j)).addColumn(family, qualifier, value));
      }
      admin.flush(tableName);
    }
    return getStoreWithName(tableName);
  }

  protected void startMiniCluster() throws Exception{
    TEST_UTIL.startMiniCluster(1);
  }

  protected void shutdownMiniCluster() throws Exception{
    TEST_UTIL.shutdownMiniCluster();
  }
  protected void setThroughputLimitConf(long throughputLimit){
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setLong(
      PressureAwareCompactionThroughputController
        .HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_HIGHER_BOUND,
      throughputLimit);
    conf.setLong(
      PressureAwareCompactionThroughputController
        .HBASE_HSTORE_COMPACTION_MAX_THROUGHPUT_LOWER_BOUND,
      throughputLimit);
  }

  protected long testCompactionWithThroughputLimit() throws Exception {
    long throughputLimit = 1024L * 1024;
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 200);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    setThroughputLimitConf(throughputLimit);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      PressureAwareCompactionThroughputController.class.getName());
    startMiniCluster();
    try {
      HStore store = prepareData();
      assertEquals(10, store.getStorefilesCount());
      long startTime = System.currentTimeMillis();
      TEST_UTIL.getAdmin().majorCompact(tableName);
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
      shutdownMiniCluster();
    }
  }


  protected long testCompactionWithoutThroughputLimit() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DefaultStoreEngine.class.getName());
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 100);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 200);
    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 10000);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      NoLimitThroughputController.class.getName());
    startMiniCluster();
    try {
      HStore store = prepareData();
      assertEquals(10, store.getStorefilesCount());
      long startTime = System.currentTimeMillis();
      TEST_UTIL.getAdmin().majorCompact(tableName);
      while (store.getStorefilesCount() != 1) {
        Thread.sleep(20);
      }
      return System.currentTimeMillis() - startTime;
    } finally {
      shutdownMiniCluster();
    }
  }

}
