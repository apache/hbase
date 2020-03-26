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
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

@Category({MasterTests.class, LargeTests.class})
public class TestMasterShutdown {
  private static final Logger LOG = LoggerFactory.getLogger(TestMasterShutdown.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterShutdown.class);

  private HBaseTestingUtility htu;

  @Before
  public void shutdownCluster() throws IOException {
    if (htu != null) {
      // an extra check in case the test cluster was not terminated after HBaseClassTestRule's
      // Timeout interrupted the test thread.
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
    // Create config to use for this cluster
    Configuration conf = HBaseConfiguration.create();

    // Start the cluster
    try {
      htu = new HBaseTestingUtility(conf);
      StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(3)
        .numRegionServers(1)
        .numDataNodes(1)
        .build();
      final MiniHBaseCluster cluster = htu.startMiniCluster(option);

      // wait for all master thread to spawn and start their run loop.
      final long thirtySeconds = TimeUnit.SECONDS.toMillis(30);
      final long oneSecond = TimeUnit.SECONDS.toMillis(1);
      assertNotEquals(-1, htu.waitFor(thirtySeconds, oneSecond, () -> {
        final List<MasterThread> masterThreads = cluster.getMasterThreads();
        return masterThreads != null
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

  /**
   * This test appears to be an intentional race between a thread that issues a shutdown RPC to the
   * master, while the master is concurrently realizing it cannot initialize because there are no
   * region servers available to it. The expected behavior is that master initialization is
   * interruptable via said shutdown RPC.
   */
  @Test
  public void testMasterShutdownBeforeStartingAnyRegionServer() throws Exception {
    LocalHBaseCluster hbaseCluster = null;
    try {
      htu =  new HBaseTestingUtility(
        createMasterShutdownBeforeStartingAnyRegionServerConfiguration());

      // configure a cluster with
      final StartMiniClusterOption options = StartMiniClusterOption.builder()
        .numDataNodes(1)
        .numMasters(1)
        .numRegionServers(0)
        .masterClass(HMaster.class)
        .rsClass(MiniHBaseCluster.MiniHBaseClusterRegionServer.class)
        .createRootDir(true)
        .build();

      // Can't simply `htu.startMiniCluster(options)` because that method waits for the master to
      // start completely. However, this test's premise is that a partially started master should
      // still respond to a shutdown RPC. So instead, we manage each component lifecycle
      // independently.
      // I think it's not worth refactoring HTU's helper methods just for this class.
      htu.startMiniDFSCluster(options.getNumDataNodes());
      htu.startMiniZKCluster(options.getNumZkServers());
      htu.createRootDir();
      hbaseCluster = new LocalHBaseCluster(htu.getConfiguration(), options.getNumMasters(),
        options.getNumRegionServers(), options.getMasterClass(), options.getRsClass());
      final MasterThread masterThread = hbaseCluster.getMasters().get(0);
      masterThread.start();
      // Switching to master registry exacerbated a race in the master bootstrap that can result
      // in a lost shutdown command (HBASE-8422, HBASE-23836). The race is essentially because
      // the server manager in HMaster is not initialized by the time shutdown() RPC (below) is
      // made to the master. The suspected reason as to why it was uncommon before HBASE-18095
      // is because the connection creation with ZK registry is so slow that by then the server
      // manager is usually init'ed in time for the RPC to be made. For now, adding an explicit
      // wait() in the test, waiting for the server manager to become available.
      final long timeout = TimeUnit.MINUTES.toMillis(10);
      assertNotEquals("Timeout waiting for server manager to become available.",
        -1, Waiter.waitFor(htu.getConfiguration(), timeout,
          () -> masterThread.getMaster().getServerManager() != null));
      htu.getConnection().getAdmin().shutdown();
      masterThread.join();
    } finally {
      if (hbaseCluster != null) {
        hbaseCluster.shutdown();
      }
      if (htu != null) {
        htu.shutdownMiniCluster();
        htu = null;
      }
    }
  }

  /**
   * Create a cluster configuration suitable for
   * {@link #testMasterShutdownBeforeStartingAnyRegionServer()}.
   */
  private static Configuration createMasterShutdownBeforeStartingAnyRegionServerConfiguration() {
    final Configuration conf = HBaseConfiguration.create();
    // make sure the master will wait forever in the absence of a RS.
    conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    // don't need a long write pipeline for this test.
    conf.setInt("dfs.replication", 1);
    return conf;
  }

  /**
   * Create a new {@link Configuration} based on {@code baseConf} that has ZooKeeper connection
   * settings tuned very aggressively. The resulting client is used within a retry loop, so there's
   * no value in having the client itself do the retries. We want to iterate on the base
   * configuration because we're waiting for the mini-cluster to start and set it's ZK client port.
   *
   * @return a new, configured {@link Configuration} instance.
   */
  private static Configuration createResponsiveZkConfig(final Configuration baseConf) {
    final Configuration conf = HBaseConfiguration.create(baseConf);
    conf.setInt(ReadOnlyZKClient.RECOVERY_RETRY, 3);
    conf.setInt(ReadOnlyZKClient.RECOVERY_RETRY_INTERVAL_MILLIS, 100);
    return conf;
  }
}
