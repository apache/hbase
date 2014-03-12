/**
 * Copyright 2008 The Apache Software Foundation
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

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;

/**
 * Test whether region rebalancing works. (HBASE-71)
 * Test HBASE-3663 whether region rebalancing works after a new server booted
 * especially when no server has more regions than the ceils of avg load
 */
public class TestGlobalMemStoreSize extends HBaseClusterTestCase {
  final Log LOG = LogFactory.getLog(this.getClass().getName());

  HTable table;

  HTableDescriptor desc;

  final byte[] FIVE_HUNDRED_KBYTES;

  final byte [] FAMILY_NAME = Bytes.toBytes("col");
  
  private static int regionServerNum =4;
  private static int regionNum = 16;
  // total region num = region num + root and meta region
  private static int totalRegionNum = regionNum +2;
  
  /** constructor */
  public TestGlobalMemStoreSize() {
    super(regionServerNum);
    
    FIVE_HUNDRED_KBYTES = new byte[500 * 1024];
    for (int i = 0; i < 500 * 1024; i++) {
      FIVE_HUNDRED_KBYTES[i] = 'x';
    }

    desc = new HTableDescriptor("test");
    desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
  }

  /**
   * Before the hbase cluster starts up, create some dummy regions.
   */
  @Override
  public void preHBaseClusterSetup() throws IOException {
    // create a 20-region table by writing directly to disk
    List<byte []> startKeys = new ArrayList<byte []>();
    startKeys.add(null);
    for (int i = 10; i < regionNum+10; i++) {
      startKeys.add(Bytes.toBytes("row_" + i));
    }
    startKeys.add(null);
    LOG.debug(startKeys.size() + " start keys generated");
    
    List<HRegion> regions = new ArrayList<HRegion>();
    for (int i = 0; i < regionNum; i++) {
      regions.add(createAregion(startKeys.get(i), startKeys.get(i+1)));
    }

    // Now create the root and meta regions and insert the data regions
    // created above into the meta

    createRootAndMetaRegions();
    for (HRegion region : regions) {
      HRegion.addRegionToMETA(meta, region);
    }
    closeRootAndMeta();
  }
  
  /**
   * Test the global mem store size in the region server is equal to sum of each
   * region's mem store size
   * @throws IOException
   */

  public void testGlobalMemStore() throws IOException {
    waitForAllRegionsAssigned();
    assertEquals(getOnlineRegionServers().size(), regionServerNum);
    
    int totalRegionNum = 0;
    for (HRegionServer server : getOnlineRegionServers()) {
      long globalMemStoreSize = 0;
      totalRegionNum += server.getOnlineRegions().size();
      for(HRegion region : server.getOnlineRegions()) {
        globalMemStoreSize += region.getMemstoreSize().get();
      }
      assertEquals(server.getGlobalMemstoreSize().get(),globalMemStoreSize);
    }
    assertEquals(totalRegionNum,totalRegionNum);
    
    for (HRegionServer server : getOnlineRegionServers()) {
      for(HRegion region : server.getOnlineRegions()) {
        region.flushcache();
      }
      assertEquals(server.getGlobalMemstoreSize().get(),0);
    }
  }

  /** figure out how many regions are currently being served. */
  private int getRegionCount() {
    int total = 0;
    System.out.println("getOnlineRegionServers "+ getOnlineRegionServers().size());
    for (HRegionServer server : getOnlineRegionServers()) {
      total += server.getOnlineRegions().size();
    }
    return total;
  }
  
  private List<HRegionServer> getOnlineRegionServers() {
    List<HRegionServer> list = new ArrayList<HRegionServer>();
    for (JVMClusterUtil.RegionServerThread rst : cluster.getRegionServerThreads()) {
      if (rst.getRegionServer().isOnline()) {
        list.add(rst.getRegionServer());
      }
    }
    return list;
  }

  /**
   * Wait until all the regions are assigned.
   */
  private void waitForAllRegionsAssigned() {
    while (getRegionCount() < totalRegionNum) {
      LOG.debug("Waiting for there to be "+totalRegionNum+" regions, but there are " + getRegionCount() + " right now.");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
  }

  /**
   * create a region with the specified start and end key and exactly one row
   * inside.
   */
  private HRegion createAregion(byte [] startKey, byte [] endKey)
  throws IOException {
    HRegion region = createNewHRegion(desc, startKey, endKey);
    byte [] keyToWrite = startKey == null ? Bytes.toBytes("row_0000") : startKey;
    Put put = new Put(keyToWrite);
    put.add(FAMILY_NAME, null, Bytes.toBytes("test"));
    region.put(put);
    region.close();
    region.getLog().closeAndDelete();
    return region;
  }
}

