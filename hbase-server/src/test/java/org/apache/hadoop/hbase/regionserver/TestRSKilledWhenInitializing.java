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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;

/**
 * Tests that a regionserver that dies after reporting for duty gets removed
 * from list of online regions. See HBASE-9593.
 */
@Category({RegionServerTests.class, MediumTests.class})
@Ignore("See HBASE-19515")
public class TestRSKilledWhenInitializing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRSKilledWhenInitializing.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRSKilledWhenInitializing.class);

  @Rule
  public TestName testName = new TestName();

  // This boolean needs to be globally available. It is used below in our
  // mocked up regionserver so it knows when to die.
  private static AtomicBoolean masterActive = new AtomicBoolean(false);
  // Ditto for this variable. It also is used in the mocked regionserver class.
  private static final AtomicReference<ServerName> killedRS = new AtomicReference<ServerName>();

  private static final int NUM_MASTERS = 1;
  private static final int NUM_RS = 2;

  /**
   * Test verifies whether a region server is removed from online servers list in master if it went
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
    final LocalHBaseCluster cluster = new LocalHBaseCluster(conf, NUM_MASTERS, NUM_RS,
        HMaster.class, RegisterAndDieRegionServer.class);
    final MasterThread master = startMaster(cluster.getMasters().get(0));
    try {
      // Master is up waiting on RegionServers to check in. Now start RegionServers.
      for (int i = 0; i < NUM_RS; i++) {
        cluster.getRegionServers().get(i).start();
      }
      // Expected total regionservers depends on whether Master can host regions or not.
      int expectedTotalRegionServers = NUM_RS + (LoadBalancer.isTablesOnMaster(conf)? 1: 0);
      List<ServerName> onlineServersList = null;
      do {
        onlineServersList = master.getMaster().getServerManager().getOnlineServersList();
      } while (onlineServersList.size() < expectedTotalRegionServers);
      // Wait until killedRS is set. Means RegionServer is starting to go down.
      while (killedRS.get() == null) {
        Threads.sleep(1);
      }
      // Wait on the RegionServer to fully die.
      while (cluster.getLiveRegionServers().size() >= expectedTotalRegionServers) {
        Threads.sleep(1);
      }
      // Make sure Master is fully up before progressing. Could take a while if regions
      // being reassigned.
      while (!master.getMaster().isInitialized()) {
        Threads.sleep(1);
      }

      // Now in steady state. How many regions open? Master should have too many regionservers
      // showing still. The downed RegionServer should still be showing as registered.
      assertTrue(master.getMaster().getServerManager().isServerOnline(killedRS.get()));
      // Find non-meta region (namespace?) and assign to the killed server. That'll trigger cleanup.
      Map<RegionInfo, ServerName> assignments = null;
      do {
        assignments = master.getMaster().getAssignmentManager().getRegionStates().getRegionAssignments();
      } while (assignments == null || assignments.size() < 2);
      RegionInfo hri = null;
      for (Map.Entry<RegionInfo, ServerName> e: assignments.entrySet()) {
        if (e.getKey().isMetaRegion()) continue;
        hri = e.getKey();
        break;
      }
      // Try moving region to the killed server. It will fail. As by-product, we will
      // remove the RS from Master online list because no corresponding znode.
      assertEquals(expectedTotalRegionServers,
        master.getMaster().getServerManager().getOnlineServersList().size());
      LOG.info("Move " + hri.getEncodedName() + " to " + killedRS.get());
      master.getMaster().move(hri.getEncodedNameAsBytes(),
          Bytes.toBytes(killedRS.get().toString()));

      // TODO: This test could do more to verify fix. It could create a table
      // and do round-robin assign. It should fail if zombie RS. HBASE-19515.

      // Wait until the RS no longer shows as registered in Master.
      while (onlineServersList.size() > (NUM_RS + 1)) {
        Thread.sleep(100);
        onlineServersList = master.getMaster().getServerManager().getOnlineServersList();
      }
    } finally {
      // Shutdown is messy with complaints about fs being closed. Why? TODO.
      cluster.shutdown();
      cluster.join();
      TEST_UTIL.shutdownMiniDFSCluster();
      TEST_UTIL.shutdownMiniZKCluster();
      TEST_UTIL.cleanupTestDir();
    }
  }

  /**
   * Start Master. Get as far as the state where Master is waiting on
   * RegionServers to check in, then return.
   */
  private MasterThread startMaster(MasterThread master) {
    master.start();
    // It takes a while until ServerManager creation to happen inside Master startup.
    while (master.getMaster().getServerManager() == null) {
      continue;
    }
    // Set a listener for the waiting-on-RegionServers state. We want to wait
    // until this condition before we leave this method and start regionservers.
    final AtomicBoolean waiting = new AtomicBoolean(false);
    if (master.getMaster().getServerManager() == null) throw new NullPointerException("SM");
    master.getMaster().getServerManager().registerListener(new ServerListener() {
      @Override
      public void waiting() {
        waiting.set(true);
      }
    });
    // Wait until the Master gets to place where it is waiting on RegionServers to check in.
    while (!waiting.get()) {
      continue;
    }
    // Set the global master-is-active; gets picked up by regionservers later.
    masterActive.set(true);
    return master;
  }

  /**
   * A RegionServer that reports for duty and then immediately dies if it is the first to receive
   * the response to a reportForDuty. When it dies, it clears its ephemeral znode which the master
   * notices and so removes the region from its set of online regionservers.
   */
  static class RegisterAndDieRegionServer extends MiniHBaseCluster.MiniHBaseClusterRegionServer {
    public RegisterAndDieRegionServer(Configuration conf)
    throws IOException, InterruptedException {
      super(conf);
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
