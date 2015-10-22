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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestRegionServerReportForDuty {

  private static final Log LOG = LogFactory.getLog(TestRegionServerReportForDuty.class);

  private static final long SLEEP_INTERVAL = 500;

  private HBaseTestingUtility testUtil;
  private LocalHBaseCluster cluster;
  private RegionServerThread rs;
  private RegionServerThread rs2;
  private MasterThread master;
  private MasterThread backupMaster;

  @Before
  public void setUp() throws Exception {
    testUtil = new HBaseTestingUtility();
    testUtil.startMiniDFSCluster(1);
    testUtil.startMiniZKCluster(1);
    testUtil.createRootDir();
    cluster = new LocalHBaseCluster(testUtil.getConfiguration(), 0, 0);
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
    cluster.join();
    testUtil.shutdownMiniZKCluster();
    testUtil.shutdownMiniDFSCluster();
  }

  /**
   * Tests region sever reportForDuty with backup master becomes primary master after
   * the first master goes away.
   */
  @Test (timeout=180000)
  public void testReportForDutyWithMasterChange() throws Exception {

    // Start a master and wait for it to become the active/primary master.
    // Use a random unique port
    cluster.getConfiguration().setInt(HConstants.MASTER_PORT, HBaseTestingUtility.randomFreePort());
    // master has a rs. defaultMinToStart = 2
    cluster.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 2);
    cluster.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 2);
    master = cluster.addMaster();
    rs = cluster.addRegionServer();
    LOG.debug("Starting master: " + master.getMaster().getServerName());
    master.start();
    rs.start();

    waitForClusterOnline(master);

    // Add a 2nd region server
    cluster.getConfiguration().set(HConstants.REGION_SERVER_IMPL, MyRegionServer.class.getName());
    rs2 = cluster.addRegionServer();
    // Start the region server. This region server will refresh RPC connection
    // from the current active master to the next active master before completing
    // reportForDuty
    LOG.debug("Starting 2nd region server: " + rs2.getRegionServer().getServerName());
    rs2.start();

    waitForSecondRsStarted();

    // Stop the current master.
    master.getMaster().stop("Stopping master");

    // Start a new master and use another random unique port
    // Also let it wait for exactly 2 region severs to report in.
    cluster.getConfiguration().setInt(HConstants.MASTER_PORT, HBaseTestingUtility.randomFreePort());
    cluster.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 3);
    cluster.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 3);
    backupMaster = cluster.addMaster();
    LOG.debug("Starting new master: " + backupMaster.getMaster().getServerName());
    backupMaster.start();

    waitForClusterOnline(backupMaster);

    // Do some checking/asserts here.
    assertTrue(backupMaster.getMaster().isActiveMaster());
    assertTrue(backupMaster.getMaster().isInitialized());
    assertEquals(backupMaster.getMaster().getServerManager().getOnlineServersList().size(), 3);

  }

  private void waitForClusterOnline(MasterThread master) throws InterruptedException {
    while (true) {
      if (master.getMaster().isInitialized()) {
        break;
      }
      Thread.sleep(SLEEP_INTERVAL);
      LOG.debug("Waiting for master to come online ...");
    }
    rs.waitForServerOnline();
  }

  private void waitForSecondRsStarted() throws InterruptedException {
    while (true) {
      if (((MyRegionServer) rs2.getRegionServer()).getRpcStubCreatedFlag() == true) {
        break;
      }
      Thread.sleep(SLEEP_INTERVAL);
      LOG.debug("Waiting 2nd RS to be started ...");
    }
  }

  // Create a Region Server that provide a hook so that we can wait for the master switch over
  // before continuing reportForDuty to the mater.
  // The idea is that we get a RPC connection to the first active master, then we wait.
  // The first master goes down, the second master becomes the active master. The region
  // server continues reportForDuty. It should succeed with the new master.
  public static class MyRegionServer extends MiniHBaseClusterRegionServer {

    private ServerName sn;
    // This flag is to make sure this rs has obtained the rpcStub to the first master.
    // The first master will go down after this.
    private boolean rpcStubCreatedFlag = false;
    private boolean masterChanged = false;

    public MyRegionServer(Configuration conf, CoordinatedStateManager cp)
      throws IOException, KeeperException,
        InterruptedException {
      super(conf, cp);
    }

    @Override
    protected synchronized ServerName createRegionServerStatusStub() {
      sn = super.createRegionServerStatusStub();
      rpcStubCreatedFlag = true;

      // Wait for master switch over. Only do this for the second region server.
      while (!masterChanged) {
        ServerName newSn = super.getMasterAddressTracker().getMasterAddress(true);
        if (newSn != null && !newSn.equals(sn)) {
          masterChanged = true;
          break;
        }
        try {
          Thread.sleep(SLEEP_INTERVAL);
        } catch (InterruptedException e) {
          return null;
        }
        LOG.debug("Waiting for master switch over ... ");
      }
      return sn;
    }

    public boolean getRpcStubCreatedFlag() {
      return rpcStubCreatedFlag;
    }
  }
}
