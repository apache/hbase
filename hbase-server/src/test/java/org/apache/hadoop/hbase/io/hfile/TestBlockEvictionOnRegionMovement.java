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
package org.apache.hadoop.hbase.io.hfile;

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_IOENGINE_KEY;
import static org.apache.hadoop.hbase.io.hfile.CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, MediumTests.class })
public class TestBlockEvictionOnRegionMovement {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBlockEvictionOnRegionMovement.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestBlockEvictionOnRegionMovement.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Configuration conf;
  Path testDir;
  MiniZooKeeperCluster zkCluster;
  MiniHBaseCluster cluster;
  StartMiniClusterOption option = StartMiniClusterOption.builder().numRegionServers(2).build();

  @Before
  public void setup() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    conf.set(BUCKET_CACHE_IOENGINE_KEY, "file:" + testDir + "/bucket.cache");
    conf.setInt("hbase.bucketcache.size", 400);
    conf.set("hbase.bucketcache.persistent.path", testDir + "/bucket.persistence");
    conf.setLong(CacheConfig.BUCKETCACHE_PERSIST_INTERVAL_KEY, 100);
    conf.setBoolean(CacheConfig.EVICT_BLOCKS_ON_CLOSE_KEY, true);
    conf.setBoolean(CACHE_BLOCKS_ON_WRITE_KEY, true);
    zkCluster = TEST_UTIL.startMiniZKCluster();
    cluster = TEST_UTIL.startMiniHBaseCluster(option);
    cluster.setConf(conf);
  }

  @Test
  public void testBlockEvictionOnRegionMove() throws Exception {
    // Write to table and flush
    TableName tableRegionMove = writeDataToTable("testBlockEvictionOnRegionMove");

    HRegionServer regionServingRS =
      cluster.getRegionServer(1).getRegions(tableRegionMove).size() == 1
        ? cluster.getRegionServer(1)
        : cluster.getRegionServer(0);
    assertTrue(regionServingRS.getBlockCache().isPresent());

    // wait for running prefetch threads to be completed.
    Waiter.waitFor(this.conf, 200, () -> PrefetchExecutor.getPrefetchFutures().isEmpty());

    long oldUsedCacheSize =
      regionServingRS.getBlockCache().get().getBlockCaches()[1].getCurrentSize();
    assertNotEquals(0, oldUsedCacheSize);

    Admin admin = TEST_UTIL.getAdmin();
    RegionInfo regionToMove = regionServingRS.getRegions(tableRegionMove).get(0).getRegionInfo();
    admin.move(regionToMove.getEncodedNameAsBytes(),
      TEST_UTIL.getOtherRegionServer(regionServingRS).getServerName());
    assertEquals(0, regionServingRS.getRegions(tableRegionMove).size());

    long newUsedCacheSize =
      regionServingRS.getBlockCache().get().getBlockCaches()[1].getCurrentSize();
    assertTrue(oldUsedCacheSize > newUsedCacheSize);
    assertEquals(0, regionServingRS.getBlockCache().get().getBlockCaches()[1].getBlockCount());
  }

  @Test
  public void testBlockEvictionOnGracefulStop() throws Exception {
    // Write to table and flush
    TableName tableRegionClose = writeDataToTable("testBlockEvictionOnGracefulStop");

    HRegionServer regionServingRS =
      cluster.getRegionServer(1).getRegions(tableRegionClose).size() == 1
        ? cluster.getRegionServer(1)
        : cluster.getRegionServer(0);

    assertTrue(regionServingRS.getBlockCache().isPresent());
    long oldUsedCacheSize =
      regionServingRS.getBlockCache().get().getBlockCaches()[1].getCurrentSize();
    assertNotEquals(0, regionServingRS.getBlockCache().get().getBlockCaches()[1].getBlockCount());

    cluster.stopRegionServer(regionServingRS.getServerName());
    Thread.sleep(500);
    cluster.startRegionServer();
    Thread.sleep(500);

    long newUsedCacheSize =
      regionServingRS.getBlockCache().get().getBlockCaches()[1].getCurrentSize();
    assertEquals(oldUsedCacheSize, newUsedCacheSize);
    assertNotEquals(0, regionServingRS.getBlockCache().get().getBlockCaches()[1].getBlockCount());
  }

  public TableName writeDataToTable(String testName) throws IOException, InterruptedException {
    TableName tableName = TableName.valueOf(testName + EnvironmentEdgeManager.currentTime());
    byte[] row0 = Bytes.toBytes("row1");
    byte[] row1 = Bytes.toBytes("row2");
    byte[] family = Bytes.toBytes("family");
    byte[] qf1 = Bytes.toBytes("qf1");
    byte[] qf2 = Bytes.toBytes("qf2");
    byte[] value1 = Bytes.toBytes("value1");
    byte[] value2 = Bytes.toBytes("value2");

    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    Table table = TEST_UTIL.createTable(td, null);
    try {
      // put data
      Put put0 = new Put(row0);
      put0.addColumn(family, qf1, 1, value1);
      table.put(put0);
      Put put1 = new Put(row1);
      put1.addColumn(family, qf2, 1, value2);
      table.put(put1);
      TEST_UTIL.flush(tableName);
    } finally {
      Thread.sleep(1000);
    }
    assertEquals(1, cluster.getRegions(tableName).size());
    return tableName;
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupDataTestDirOnTestFS(String.valueOf(testDir));
    if (zkCluster != null) {
      zkCluster.shutdown();
    }
  }
}
