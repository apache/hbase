/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HMsg;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.io.MapWritable;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class TestMasterSettingRegionServerAddress {
  private static final Log LOG =
    LogFactory.getLog(TestMasterSettingRegionServerAddress.class);
  private static final HBaseTestingUtility TESTING_UTIL =
    new HBaseTestingUtility();
  private static final String WHAT_TO_USE_INSTEAD = "example.org:10000";

  @BeforeClass public static void beforeClass() throws Exception {
    TESTING_UTIL.startMiniZKCluster();
  }

  @AfterClass public static void afterClass() throws IOException {
    TESTING_UTIL.shutdownMiniZKCluster();
  }

  @Test public void testDifferentAddress()
  throws IOException, KeeperException, InterruptedException {
    HMaster master = new HMaster(TESTING_UTIL.getConfiguration()) {
      @Override
      public HMsg[] regionServerReport(HServerInfo serverInfo, HMsg[] msgs,
          HRegionInfo[] mostLoadedRegions)
      throws IOException {
        LOG.info("Heartbeat: " + serverInfo.getServerName());
        Assert.assertEquals(WHAT_TO_USE_INSTEAD, serverInfo.getHostnamePort());
        return super.regionServerReport(serverInfo, msgs, mostLoadedRegions);
      }
      @Override
      public MapWritable regionServerStartup(HServerInfo serverInfo,
          long serverCurrentTime) throws IOException {
        // TODO Auto-generated method stub
        return super.regionServerStartup(serverInfo, serverCurrentTime);
      }
      /*

      @Override
      public MapWritable regionServerStartup(HServerInfo serverInfo,
          long serverCurrentTime)
      throws IOException {
        LOG.info("INSIDE");
        MapWritable mw =
          super.regionServerStartup(serverInfo, serverCurrentTime);
        HServerAddress hsa =
          (HServerAddress)mw.get(HConstants.HBASE_REGIONSERVER_ADDRESS);
        HServerAddress instead = new HServerAddress(WHAT_TO_USE_INSTEAD);
        LOG.info("reportForDuty returning " + hsa.toString() +
          " but changed to " + instead.toString());
        mw.put(HConstants.HBASE_REGIONSERVER_ADDRESS, instead);
        return mw;
      }*/
    };
    master.start();
    while(!master.isMasterRunning()) Thread.sleep(100);
    HRegionServer hrs = new HRegionServer(TESTING_UTIL.getConfiguration());
    Thread t = new Thread(hrs);
    t.start();
    while(!master.isInitialized()) Thread.sleep(100);
  }
}
