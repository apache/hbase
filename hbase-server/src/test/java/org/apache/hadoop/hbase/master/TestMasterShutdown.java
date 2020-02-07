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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterShutdown {
  private static final Logger LOG = LoggerFactory.getLogger(TestMasterShutdown.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterShutdown.class);

  private HBaseTestingUtility htu;

  @Before
  public void shutdownCluster() throws IOException {
    if (htu != null) {
      LOG.warn("found non-null TestingUtility -- previous test did not terminate cleanly.");
      htu.shutdownMiniCluster();
    }
  }

  /**
   * Simple test of shutdown.
   * <p>
   * Starts with three masters.  Tells the active master to shutdown the cluster.
   * Verifies that all masters are properly shutdown.
   */
  @Test
  public void testMasterShutdown() throws Exception {
    final int NUM_MASTERS = 3;
    final int NUM_RS = 3;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();

    // Start the cluster
    try {
      htu = new HBaseTestingUtility(conf);
      StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(NUM_MASTERS)
        .numRegionServers(NUM_RS)
        .numDataNodes(NUM_RS)
        .build();
      final MiniHBaseCluster cluster = htu.startMiniCluster(option);

      // wait for all master thread to spawn and start their run loop.
      final long thirtySeconds = TimeUnit.SECONDS.toMillis(30);
      final long oneSecond = TimeUnit.SECONDS.toMillis(1);
      assertNotEquals(-1, htu.waitFor(thirtySeconds, oneSecond, () -> {
        final List<MasterThread> masterThreads = cluster.getMasterThreads();
        return CollectionUtils.isNotEmpty(masterThreads)
          && masterThreads.size() >= 3
          && masterThreads.stream().allMatch(Thread::isAlive);
      }));

      // find the active master
      final HMaster active = cluster.getMaster();
      assertNotNull(active);

      // make sure the other two are backup masters
      ClusterMetrics status = active.getClusterMetrics();
      assertEquals(2, status.getBackupMasterNames().size());

      // tell the active master to shutdown the cluster
      active.shutdown();
      assertNotEquals(-1, htu.waitFor(thirtySeconds, oneSecond,
        () -> CollectionUtils.isEmpty(cluster.getLiveMasterThreads())));
      assertNotEquals(-1, htu.waitFor(thirtySeconds, oneSecond,
        () -> CollectionUtils.isEmpty(cluster.getLiveRegionServerThreads())));
    } finally {
      if (htu != null) {
        htu.shutdownMiniCluster();
        htu = null;
      }
    }
  }

  private Connection createConnection(HBaseTestingUtility util) throws InterruptedException {
    // the cluster may have not been initialized yet which means we can not get the cluster id thus
    // an exception will be thrown. So here we need to retry.
    for (;;) {
      try {
        return ConnectionFactory.createConnection(util.getConfiguration());
      } catch (Exception e) {
        Thread.sleep(10);
      }
    }
  }

  @Test
  public void testMasterShutdownBeforeStartingAnyRegionServer() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 0;

    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.ipc.client.failed.servers.expiry", 200);
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);

    // Start the cluster
    LocalHBaseCluster cluster = null;
    try {
      htu = new HBaseTestingUtility(conf);
      htu.startMiniDFSCluster(3);
      htu.startMiniZKCluster();
      htu.createRootDir();
      cluster = new LocalHBaseCluster(conf, NUM_MASTERS, NUM_RS, HMaster.class,
        MiniHBaseCluster.MiniHBaseClusterRegionServer.class);
      final int MASTER_INDEX = 0;
      final MasterThread master = cluster.getMasters().get(MASTER_INDEX);
      master.start();
      LOG.info("Called master start on " + master.getName());
      final LocalHBaseCluster finalCluster = cluster;
      Thread shutdownThread = new Thread("Shutdown-Thread") {
        @Override
        public void run() {
          LOG.info("Before call to shutdown master");
          try (Connection connection = createConnection(htu); Admin admin = connection.getAdmin()) {
            admin.shutdown();
          } catch (Exception e) {
            LOG.info("Error while calling Admin.shutdown, which is expected: " + e.getMessage());
          }
          LOG.info("After call to shutdown master");
          finalCluster.waitOnMaster(MASTER_INDEX);
        }
      };
      shutdownThread.start();
      LOG.info("Called master join on " + master.getName());
      master.join();
      shutdownThread.join();

      List<MasterThread> masterThreads = cluster.getMasters();
      // make sure all the masters properly shutdown
      assertEquals(0, masterThreads.size());
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (htu != null) {
        htu.shutdownMiniZKCluster();
        htu.shutdownMiniDFSCluster();
        htu.cleanupTestDir();
        htu = null;
      }
    }
  }
}
