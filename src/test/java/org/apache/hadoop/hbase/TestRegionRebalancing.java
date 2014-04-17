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
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DuplicateZKNotificationInjectionHandler;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Test;

/**
 * Test whether region rebalancing works. (HBASE-71)
 * Test HBASE-3663 whether region rebalancing works after a new server booted
 * especially when no server has more regions than the ceils of avg load
 */
public class TestRegionRebalancing extends HBaseClusterTestCase {
  final Log LOG = LogFactory.getLog(this.getClass().getName());

  HTable table;

  HTableDescriptor desc;

  final byte[] FIVE_HUNDRED_KBYTES;

  final byte [] FAMILY_NAME = Bytes.toBytes("col");

  final Random rand = new Random(57473);

  /** constructor */
  public TestRegionRebalancing() {
    super(1);
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
    for (int i = 10; i < 70; i++) {
      startKeys.add(Bytes.toBytes("row_" + i));
    }
    startKeys.add(null);
    LOG.debug(startKeys.size() + " start keys generated");
    
    List<HRegion> regions = new ArrayList<HRegion>();
    for (int i = 0; i < 60; i++) {
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
   * In this case, create 16 servers here, there will be 17 servers and 62 regions totally.
   * When one of the server shuts down, the avg load is 3.875.
   * When this server comes back, the avg load will be 3.64
   * Set the slot number near 0, so no server's load will large than 4.
   * The load balance algorithm should handle this case properly. 
   */
  public void testRebalancing() throws IOException {

    for (int i = 1; i <= 16; i++){
      LOG.debug("Adding region server #"+i);
      cluster.startRegionServer();
      checkingServerStatus();
    }
   
    setInjectionHandler();

    LOG.debug("Restart: killing 1 region server.");
    cluster.stopRegionServer(2, false);
    cluster.waitOnRegionServer(2);
    assertRegionsAreBalanced();
    
    LOG.debug("Restart: adding that region server back");
    cluster.startRegionServer();
    assertRegionsAreBalanced();
  }

  private void setInjectionHandler() {
    InjectionHandler.set(
        new InjectionHandler() {
        protected boolean _falseCondition(InjectionEvent event, Object... args) {
          if (event.equals(InjectionEvent.HREGIONSERVER_REPORT_RESPONSE)) {
            double num = rand.nextDouble();
            HMsg msgs[] = (HMsg[]) args;
            if (msgs.length > 0 && num < 0.45) {
              StringBuilder sb = new StringBuilder(
              "Causing a false condition to be true. "
                  + " rand prob = " + num + " msgs.length is " + msgs.length +
              ". Messages received from the master that are ignored : \t" );
              for (int i = 0; i < msgs.length; i++) {
                sb.append(msgs[i].toString());
                sb.append("\t");
              }
              LOG.debug(sb.toString());
              return true;
            }
          }
          return false;
        }});
  }

  private void checkingServerStatus() {
    List<HRegionServer> servers = getOnlineRegionServers();
    double avg = cluster.getMaster().getAverageLoad();
    for (HRegionServer server : servers) {
      int serverLoad = server.getOnlineRegions().size();      
      LOG.debug(server.hashCode() + " Avg: " + avg + " actual: " + serverLoad);
    }
  }

  /** figure out how many regions are currently being served. */
  private int getRegionCount() {
    int total = 0;
    for (HRegionServer server : getOnlineRegionServers()) {
      total += server.getOnlineRegions().size();
    }
    return total;
  }

  /**
   * Determine if regions are balanced. Figure out the total, divide by the
   * number of online servers, then test if each server is +/- 1 of average
   * rounded up.
   */
  private void assertRegionsAreBalanced() {
    boolean success = false;
    float slop = conf.getFloat("hbase.regions.slop", (float)0.1);
    if (slop <= 0) slop = 1;
    
    // waiting for load balancer running
    try {
      Thread.sleep(MiniHBaseCluster.WAIT_FOR_LOADBALANCER);
    } catch (InterruptedException e1) {
      LOG.error("Got InterruptedException when waiting for load balance " + e1);
    }
    
    for (int i = 0; i < 5; i++) {
      success = true;
      // make sure all the regions are reassigned before we test balance
      waitForAllRegionsAssigned();

      int regionCount = getRegionCount();
      List<HRegionServer> servers = getOnlineRegionServers();
      double avg = cluster.getMaster().getAverageLoad();
      int avgLoadPlusSlop = (int)Math.ceil(avg * (1 + slop));
      int avgLoadMinusSlop = (int)Math.floor(avg * (1 - slop)) - 1;
     
      LOG.debug("There are " + servers.size() + " servers and " + regionCount
        + " regions. Load Average: " + avg + " low border: " + avgLoadMinusSlop
        + ", up border: " + avgLoadPlusSlop + "; attempt: " + i);
     
      for (HRegionServer server : servers) {
        int serverLoad = server.getOnlineRegions().size(); 
        LOG.debug(server.hashCode() + " Avg: " + avg + " actual: " + serverLoad);      
        if (!(avg > 2.0 && serverLoad <= avgLoadPlusSlop
            && serverLoad >= avgLoadMinusSlop)) { 
          LOG.debug(server.hashCode() + " Isn't balanced!!! Avg: " + avg +
              " actual: " + serverLoad + " slop: " + slop);   
          success = false;
        }
      }

      if (!success) {
        // one or more servers are not balanced. sleep a little to give it a
        // chance to catch up. then, go back to the retry loop.
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {}

        continue;
      }

      // if we get here, all servers were balanced, so we should just return.
      return;
    }
    // if we get here, we tried 5 times and never got to short circuit out of
    // the retry loop, so this is a failure.
    fail("After 5 attempts, region assignments were not balanced.");
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
    while (getRegionCount() < 62) {
      LOG.debug("Waiting for there to be 62 regions, but there are " + getRegionCount() + " right now.");
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

