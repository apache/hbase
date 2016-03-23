/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional infomation
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test HBASE-3694 whether the GlobalMemStoreSize is the same as the summary
 * of all the online region's MemStoreSize
 */
@Category(MediumTests.class)
public class TestGlobalMemStoreSize {
  private static final Log LOG = LogFactory.getLog(TestGlobalMemStoreSize.class);
  private static int regionServerNum = 4;
  private static int regionNum = 16;
  // total region num = region num + root and meta regions
  private static int totalRegionNum = regionNum+2;

  private HBaseTestingUtility TEST_UTIL;
  private MiniHBaseCluster cluster;

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
    TEST_UTIL.startMiniCluster(1, regionServerNum);
    cluster = TEST_UTIL.getHBaseCluster();
    LOG.info("Waiting for active/ready master");
    cluster.waitForActiveAndReadyMaster();

    // Create a table with regions
    byte [] table = Bytes.toBytes("TestGlobalMemStoreSize");
    byte [] family = Bytes.toBytes("family");
    LOG.info("Creating table with " + regionNum + " regions");
    HTable ht = TEST_UTIL.createMultiRegionTable(TableName.valueOf(table), family, regionNum);
    int numRegions = -1;
    try (RegionLocator r = ht.getRegionLocator()) {
      numRegions = r.getStartKeys().length;
    }
    assertEquals(regionNum,numRegions);
    waitForAllRegionsAssigned();

    for (HRegionServer server : getOnlineRegionServers()) {
      long globalMemStoreSize = 0;
      for (HRegionInfo regionInfo :
          ProtobufUtil.getOnlineRegions(null, server.getRSRpcServices())) {
        globalMemStoreSize +=
          server.getFromOnlineRegions(regionInfo.getEncodedName()).
          getMemstoreSize();
      }
      assertEquals(server.getRegionServerAccounting().getGlobalMemstoreSize(),
        globalMemStoreSize);
    }

    // check the global memstore size after flush
    int i = 0;
    for (HRegionServer server : getOnlineRegionServers()) {
      LOG.info("Starting flushes on " + server.getServerName() +
        ", size=" + server.getRegionServerAccounting().getGlobalMemstoreSize());

      for (HRegionInfo regionInfo :
          ProtobufUtil.getOnlineRegions(null, server.getRSRpcServices())) {
        Region r = server.getFromOnlineRegions(regionInfo.getEncodedName());
        flush(r, server);
      }
      LOG.info("Post flush on " + server.getServerName());
      long now = System.currentTimeMillis();
      long timeout = now + 1000;
      while(server.getRegionServerAccounting().getGlobalMemstoreSize() != 0 &&
          timeout < System.currentTimeMillis()) {
        Threads.sleep(10);
      }
      long size = server.getRegionServerAccounting().getGlobalMemstoreSize();
      if (size > 0) {
        // If size > 0, see if its because the meta region got edits while
        // our test was running....
        for (HRegionInfo regionInfo :
            ProtobufUtil.getOnlineRegions(null, server.getRSRpcServices())) {
          Region r = server.getFromOnlineRegions(regionInfo.getEncodedName());
          long l = r.getMemstoreSize();
          if (l > 0) {
            // Only meta could have edits at this stage.  Give it another flush
            // clear them.
            assertTrue(regionInfo.isMetaRegion());
            LOG.info(r.toString() + " " + l + ", reflushing");
            r.flush(true);
          }
        }
      }
      size = server.getRegionServerAccounting().getGlobalMemstoreSize();
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
  private void flush(final Region r, final HRegionServer server)
  throws IOException {
    LOG.info("Flush " + r.toString() + " on " + server.getServerName() +
      ", " +  r.flush(true) + ", size=" +
      server.getRegionServerAccounting().getGlobalMemstoreSize());
  }

  private List<HRegionServer> getOnlineRegionServers() {
    List<HRegionServer> list = new ArrayList<HRegionServer>();
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
