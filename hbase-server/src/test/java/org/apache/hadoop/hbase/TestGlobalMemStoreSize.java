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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;

/**
 * Test HBASE-3694 whether the GlobalMemStoreSize is the same as the summary
 * of all the online region's MemStoreSize
 */
@Category({MiscTests.class, MediumTests.class})
public class TestGlobalMemStoreSize {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGlobalMemStoreSize.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestGlobalMemStoreSize.class);
  private static int regionServerNum = 4;
  private static int regionNum = 16;
  // total region num = region num + root and meta regions
  private static int totalRegionNum = regionNum+2;

  private HBaseTestingUtility TEST_UTIL;
  private MiniHBaseCluster cluster;

  @Rule
  public TestName name = new TestName();

  /**
   * Test the global mem store size in the region server is equal to sum of each
   * region's mem store size
   * @throws Exception
   */
  @Test
  public void testGlobalMemStore() throws Exception {
    // Start the cluster
    LOG.info("Starting cluster");
    Configuration conf = HBaseConfiguration.create();
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniCluster(regionServerNum);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();

    // Create a table with regions
    final TableName table = TableName.valueOf(name.getMethodName());
    byte [] family = Bytes.toBytes("family");
    LOG.info("Creating table with " + regionNum + " regions");
    Table ht = TEST_UTIL.createMultiRegionTable(table, family, regionNum);
    int numRegions = -1;
    try (RegionLocator r = TEST_UTIL.getConnection().getRegionLocator(table)) {
      numRegions = r.getStartKeys().length;
    }
    assertEquals(regionNum,numRegions);
    waitForAllRegionsAssigned();

    for (HRegionServer server : getOnlineRegionServers()) {
      long globalMemStoreSize = 0;
      for (RegionInfo regionInfo :
          ProtobufUtil.getOnlineRegions(null, server.getRSRpcServices())) {
        globalMemStoreSize += server.getRegion(regionInfo.getEncodedName()).getMemStoreDataSize();
      }
      assertEquals(server.getRegionServerAccounting().getGlobalMemStoreDataSize(),
        globalMemStoreSize);
    }

    // check the global memstore size after flush
    int i = 0;
    for (HRegionServer server : getOnlineRegionServers()) {
      LOG.info("Starting flushes on " + server.getServerName() +
        ", size=" + server.getRegionServerAccounting().getGlobalMemStoreDataSize());

      for (RegionInfo regionInfo :
          ProtobufUtil.getOnlineRegions(null, server.getRSRpcServices())) {
        HRegion r = server.getRegion(regionInfo.getEncodedName());
        flush(r, server);
      }
      LOG.info("Post flush on " + server.getServerName());
      long now = System.currentTimeMillis();
      long timeout = now + 1000;
      while(server.getRegionServerAccounting().getGlobalMemStoreDataSize() != 0 &&
          timeout < System.currentTimeMillis()) {
        Threads.sleep(10);
      }
      long size = server.getRegionServerAccounting().getGlobalMemStoreDataSize();
      if (size > 0) {
        // If size > 0, see if its because the meta region got edits while
        // our test was running....
        for (RegionInfo regionInfo :
            ProtobufUtil.getOnlineRegions(null, server.getRSRpcServices())) {
          HRegion r = server.getRegion(regionInfo.getEncodedName());
          long l = r.getMemStoreDataSize();
          if (l > 0) {
            // Only meta could have edits at this stage.  Give it another flush
            // clear them.
            assertTrue(regionInfo.isMetaRegion());
            LOG.info(r.toString() + " " + l + ", reflushing");
            r.flush(true);
          }
        }
      }
      size = server.getRegionServerAccounting().getGlobalMemStoreDataSize();
      assertEquals("Server=" + server.getServerName() + ", i=" + i++, 0, size);
    }

    ht.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Flush and log stats on flush
   * @param r
   * @param server
   * @throws IOException
   */
  private void flush(final HRegion r, final HRegionServer server)
  throws IOException {
    LOG.info("Flush " + r.toString() + " on " + server.getServerName() +
      ", " +  r.flush(true) + ", size=" +
      server.getRegionServerAccounting().getGlobalMemStoreDataSize());
  }

  private List<HRegionServer> getOnlineRegionServers() {
    List<HRegionServer> list = new ArrayList<>();
    for (JVMClusterUtil.RegionServerThread rst :
          cluster.getRegionServerThreads()) {
      if (rst.getRegionServer().isOnline()) {
        list.add(rst.getRegionServer());
      }
    }
    return list;
  }

  /**
   * Wait until all the regions are assigned.
   */
  private void waitForAllRegionsAssigned() throws IOException {
    while (true) {
      int regionCount = HBaseTestingUtility.getAllOnlineRegions(cluster).size();
      if (regionCount >= totalRegionNum) break;
      LOG.debug("Waiting for there to be "+ totalRegionNum
        +" regions, but there are " + regionCount + " right now.");
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {}
    }
  }
}
