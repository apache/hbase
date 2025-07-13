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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ IOTests.class, LargeTests.class })
public class TestPrefetchRSClose {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPrefetchRSClose.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestPrefetchRSClose.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private Configuration conf;
  Path testDir;
  MiniZooKeeperCluster zkCluster;
  SingleProcessHBaseCluster cluster;
  StartTestingClusterOption option =
    StartTestingClusterOption.builder().numRegionServers(1).build();

  @Before
  public void setup() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    testDir = TEST_UTIL.getDataTestDir();
    TEST_UTIL.getTestFileSystem().mkdirs(testDir);

    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    conf.set(BUCKET_CACHE_IOENGINE_KEY, "file:" + testDir + "/bucket.cache");
    conf.setInt("hbase.bucketcache.size", 400);
    conf.set("hbase.bucketcache.persistent.path", testDir + "/bucket.persistence");
    zkCluster = TEST_UTIL.startMiniZKCluster();
    cluster = TEST_UTIL.startMiniHBaseCluster(option);
    cluster.setConf(conf);
  }

  @Test
  public void testPrefetchPersistence() throws Exception {

    // Write to table and flush
    TableName tableName = TableName.valueOf("table1");
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
      Thread.sleep(2000);
    }

    // Default interval for cache persistence is 1000ms. So after 1000ms, both the persistence files
    // should exist.
    HRegionServer regionServingRS = cluster.getRegionServer(0);
    Admin admin = TEST_UTIL.getAdmin();
    List<String> cachedFilesList = new ArrayList<>();
    Waiter.waitFor(conf, 5000, () -> {
      try {
        cachedFilesList.addAll(admin.getCachedFilesList(regionServingRS.getServerName()));
      } catch (IOException e) {
        // let the test try again
      }
      return cachedFilesList.size() > 0;
    });
    assertEquals(1, cachedFilesList.size());
    for (HStoreFile h : regionServingRS.getRegions().get(0).getStores().get(0).getStorefiles()) {
      assertTrue(cachedFilesList.contains(h.getPath().getName()));
    }

    // Stop the RS
    cluster.stopRegionServer(0);
    LOG.info("Stopped Region Server 0.");
    Thread.sleep(1000);
    assertTrue(new File(testDir + "/bucket.persistence").exists());
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
