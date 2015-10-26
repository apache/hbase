/**
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests region server termination during startup.
 */
@Category(LargeTests.class)
public class TestRSKilledWhenInitializing {
  private static boolean masterActive = false;
  private static AtomicBoolean firstRS = new AtomicBoolean(true);

  /**
   * Test verifies whether a region server is removing from online servers list in master if it went
   * down after registering with master.
   * @throws Exception
   */
  @Test(timeout = 180000)
  public void testRSTermnationAfterRegisteringToMasterBeforeCreatingEphemeralNod() throws Exception {

    final int NUM_MASTERS = 1;
    final int NUM_RS = 2;
    firstRS.set(true);
    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);

    // Start the cluster
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniDFSCluster(3);
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.createRootDir();
    final LocalHBaseCluster cluster =
        new LocalHBaseCluster(conf, NUM_MASTERS, NUM_RS, HMaster.class, MockedRegionServer.class);
    final MasterThread master = cluster.getMasters().get(0);
    master.start();
    try {
      long startTime = System.currentTimeMillis();
      while (!master.getMaster().isActiveMaster()) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
        if (System.currentTimeMillis() > startTime + 30000) {
          throw new RuntimeException("Master not active after 30 seconds");
        }
      }
      masterActive = true;
      cluster.getRegionServers().get(0).start();
      cluster.getRegionServers().get(1).start();
      Thread.sleep(10000);
      List<ServerName> onlineServersList =
          master.getMaster().getServerManager().getOnlineServersList();
      while (onlineServersList.size() > 1) {
        Thread.sleep(100);
        onlineServersList = master.getMaster().getServerManager().getOnlineServersList();
      }
      assertEquals(onlineServersList.size(), 1);
      cluster.shutdown();
    } finally {
      masterActive = false;
      firstRS.set(true);
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  public static class MockedRegionServer extends MiniHBaseCluster.MiniHBaseClusterRegionServer {

    public MockedRegionServer(Configuration conf, CoordinatedStateManager cp)
      throws IOException, InterruptedException {
      super(conf, cp);
    }

    @Override
    protected void handleReportForDutyResponse(RegionServerStartupResponse c) throws IOException {
      if (firstRS.getAndSet(false)) {
        InetSocketAddress address = super.getRpcServer().getListenerAddress();
        if (address == null) {
          throw new IOException("Listener channel is closed");
        }
        for (NameStringPair e : c.getMapEntriesList()) {
          String key = e.getName();
          // The hostname the master sees us as.
          if (key.equals(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
            String hostnameFromMasterPOV = e.getValue();
            assertEquals(address.getHostName(), hostnameFromMasterPOV);
          }
        }
        while (!masterActive) {
          Threads.sleep(100);
        }
        super.kill();
      } else {
        super.handleReportForDutyResponse(c);
      }
    }
  }
}
