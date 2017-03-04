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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

/**
 * Tests that a regionserver that dies after reporting for duty gets removed
 * from list of online regions. See HBASE-9593.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestRSKilledWhenInitializing {
  private static final Log LOG = LogFactory.getLog(TestRSKilledWhenInitializing.class);
  @Rule public TestName testName = new TestName();
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
    withTimeout(this.getClass()).withLookingForStuckThread(true).build();

  // This boolean needs to be globally available. It is used below in our
  // mocked up regionserver so it knows when to die.
  private static AtomicBoolean masterActive = new AtomicBoolean(false);
  // Ditto for this variable. It also is used in the mocked regionserver class.
  private static final AtomicReference<ServerName> killedRS = new AtomicReference<ServerName>();

  private static final int NUM_MASTERS = 1;
  private static final int NUM_RS = 2;

  /**
   * Test verifies whether a region server is removing from online servers list in master if it went
   * down after registering with master. Test will TIMEOUT if an error!!!!
   * @throws Exception
   */
  @Test
  public void testRSTerminationAfterRegisteringToMasterBeforeCreatingEphemeralNode()
  throws Exception {
    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    // Start the cluster
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniDFSCluster(3);
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.createRootDir();
    final LocalHBaseCluster cluster =
        new LocalHBaseCluster(conf, NUM_MASTERS, NUM_RS, HMaster.class,
            RegisterAndDieRegionServer.class);
    final MasterThread master = startMaster(cluster.getMasters().get(0));
    try {
      masterActive.set(true);
      // Now start regionservers.
      // First RS to report for duty will kill itself when it gets a response.
      // See below in the RegisterAndDieRegionServer handleReportForDutyResponse.
      for (int i = 0; i < NUM_RS; i++) {
        cluster.getRegionServers().get(i).start();
      }
      // Now wait on master to see NUM_RS + 1 servers as being online, NUM_RS and itself.
      // Then wait until the killed RS gets removed from zk and triggers Master to remove
      // it from list of online RS.
      List<ServerName> onlineServersList =
          master.getMaster().getServerManager().getOnlineServersList();
      while (onlineServersList.size() < NUM_RS + 1) {
        // Spin till we see NUM_RS + Master in online servers list.
        onlineServersList = master.getMaster().getServerManager().getOnlineServersList();
      }
      LOG.info(onlineServersList);
      assertEquals(NUM_RS + 1, onlineServersList.size());
      // Steady state. How many regions open?
      // Wait until killedRS is set
      while (killedRS.get() == null) {
        Threads.sleep(10);
      }
      final int regionsOpenCount = master.getMaster().getAssignmentManager().getNumRegionsOpened();
      // Find non-meta region (namespace?) and assign to the killed server. That'll trigger cleanup.
      Map<HRegionInfo, ServerName> assigments =
          master.getMaster().getAssignmentManager().getRegionStates().getRegionAssignments();
      HRegionInfo hri = null;
      for (Map.Entry<HRegionInfo, ServerName> e: assigments.entrySet()) {
        if (e.getKey().isMetaRegion()) continue;
        hri = e.getKey();
        break;
      }
      // Try moving region to the killed server. It will fail. As by-product, we will
      // remove the RS from Master online list because no corresponding znode.
      LOG.info("Move " + hri.getEncodedName() + " to " + killedRS.get());
      master.getMaster().move(hri.getEncodedNameAsBytes(),
          Bytes.toBytes(killedRS.get().toString()));
      while (onlineServersList.size() > NUM_RS) {
        Thread.sleep(100);
        onlineServersList = master.getMaster().getServerManager().getOnlineServersList();
      }
      // Just for kicks, ensure namespace was put back on the old server after above failed move.
      assertEquals(regionsOpenCount,
          master.getMaster().getAssignmentManager().getNumRegionsOpened());
    } finally {
      cluster.shutdown();
      cluster.join();
      TEST_UTIL.shutdownMiniDFSCluster();
      TEST_UTIL.shutdownMiniZKCluster();
      TEST_UTIL.cleanupTestDir();
    }
  }

  private MasterThread startMaster(MasterThread master) {
    master.start();
    long startTime = System.currentTimeMillis();
    while (!master.getMaster().isInitialized()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
        LOG.info("Interrupted: ignoring");
      }
      if (System.currentTimeMillis() > startTime + 30000) {
        throw new RuntimeException("Master not active after 30 seconds");
      }
    }
    return master;
  }

  /**
   * A RegionServer that reports for duty and then immediately dies if it is the first to receive
   * the response to a reportForDuty. When it dies, it clears its ephemeral znode which the master
   * notices and so removes the region from its set of online regionservers.
   */
  static class RegisterAndDieRegionServer extends MiniHBaseCluster.MiniHBaseClusterRegionServer {
    public RegisterAndDieRegionServer(Configuration conf, CoordinatedStateManager cp)
    throws IOException, InterruptedException {
      super(conf, cp);
    }

    @Override
    protected void handleReportForDutyResponse(RegionServerStartupResponse c)
    throws IOException {
      if (killedRS.compareAndSet(null, getServerName())) {
        // Make sure Master is up so it will see the removal of the ephemeral znode for this RS.
        while (!masterActive.get()) {
          Threads.sleep(100);
        }
        super.kill();
      } else {
        super.handleReportForDutyResponse(c);
      }
    }
  }
}