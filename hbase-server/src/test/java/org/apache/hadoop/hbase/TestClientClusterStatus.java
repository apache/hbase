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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the ClusterStatus.
 */
@Category(MediumTests.class)
public class TestClientClusterStatus {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClientClusterStatus.class);

  private static HBaseTestingUtility UTIL;
  private static Admin ADMIN;
  private final static int SLAVES = 5;
  private final static int MASTERS = 3;
  private static MiniHBaseCluster CLUSTER;
  private static HRegionServer DEAD;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, MyObserver.class.getName());
    UTIL = new HBaseTestingUtility(conf);
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(MASTERS).numRegionServers(SLAVES).numDataNodes(SLAVES).build();
    UTIL.startMiniCluster(option);
    CLUSTER = UTIL.getHBaseCluster();
    CLUSTER.waitForActiveAndReadyMaster();
    ADMIN = UTIL.getAdmin();
    // Kill one region server
    List<RegionServerThread> rsts = CLUSTER.getLiveRegionServerThreads();
    RegionServerThread rst = rsts.get(rsts.size() - 1);
    DEAD = rst.getRegionServer();
    DEAD.stop("Test dead servers status");
    while (rst.isAlive()) {
      Thread.sleep(500);
    }
  }

  @Test
  public void testDefaults() throws Exception {
    ClusterStatus origin = ADMIN.getClusterStatus();
    ClusterStatus defaults
        = new ClusterStatus(ADMIN.getClusterMetrics(EnumSet.allOf(Option.class)));
    checkPbObjectNotNull(origin);
    checkPbObjectNotNull(defaults);
    Assert.assertEquals(origin.getHBaseVersion(), defaults.getHBaseVersion());
    Assert.assertEquals(origin.getClusterId(), defaults.getClusterId());
    Assert.assertTrue(origin.getAverageLoad() == defaults.getAverageLoad());
    Assert.assertTrue(origin.getBackupMastersSize() == defaults.getBackupMastersSize());
    Assert.assertTrue(origin.getDeadServersSize() == defaults.getDeadServersSize());
    Assert.assertTrue(origin.getRegionsCount() == defaults.getRegionsCount());
    Assert.assertTrue(origin.getServersSize() == defaults.getServersSize());
    Assert.assertTrue(origin.getMasterInfoPort() == defaults.getMasterInfoPort());
    Assert.assertTrue(origin.equals(defaults));
    Assert.assertTrue(origin.getServersName().size() == defaults.getServersName().size());
  }

  @Test
  public void testNone() throws Exception {
    ClusterMetrics status0 = ADMIN.getClusterMetrics(EnumSet.allOf(Option.class));
    ClusterMetrics status1 = ADMIN.getClusterMetrics(EnumSet.noneOf(Option.class));
    // Do a rough compare. More specific compares can fail because all regions not deployed yet
    // or more requests than expected.
    Assert.assertEquals(status0.getLiveServerMetrics().size(),
        status1.getLiveServerMetrics().size());
    checkPbObjectNotNull(new ClusterStatus(status0));
    checkPbObjectNotNull(new ClusterStatus(status1));
  }

  @Test
  public void testLiveAndDeadServersStatus() throws Exception {
    // Count the number of live regionservers
    List<RegionServerThread> regionserverThreads = CLUSTER.getLiveRegionServerThreads();
    int numRs = 0;
    int len = regionserverThreads.size();
    for (int i = 0; i < len; i++) {
      if (regionserverThreads.get(i).isAlive()) {
        numRs++;
      }
    }
    // Depending on the (random) order of unit execution we may run this unit before the
    // minicluster is fully up and recovered from the RS shutdown done during test init.
    Waiter.waitFor(CLUSTER.getConfiguration(), 10 * 1000, 100, new Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        ClusterStatus status
          = new ClusterStatus(ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)));
        Assert.assertNotNull(status);
        return status.getRegionsCount() > 0;
      }
    });
    // Retrieve live servers and dead servers info.
    EnumSet<Option> options =
        EnumSet.of(Option.LIVE_SERVERS, Option.DEAD_SERVERS, Option.SERVERS_NAME);
    ClusterStatus status = new ClusterStatus(ADMIN.getClusterMetrics(options));
    checkPbObjectNotNull(status);
    Assert.assertNotNull(status);
    Assert.assertNotNull(status.getServers());
    // exclude a dead region server
    Assert.assertEquals(SLAVES -1, numRs);
    // live servers = nums of regionservers
    // By default, HMaster don't carry any regions so it won't report its load.
    // Hence, it won't be in the server list.
    Assert.assertEquals(status.getServers().size(), numRs);
    Assert.assertTrue(status.getRegionsCount() > 0);
    Assert.assertNotNull(status.getDeadServerNames());
    Assert.assertEquals(1, status.getDeadServersSize());
    ServerName deadServerName = status.getDeadServerNames().iterator().next();
    Assert.assertEquals(DEAD.getServerName(), deadServerName);
    Assert.assertNotNull(status.getServersName());
    Assert.assertEquals(numRs, status.getServersName().size());
  }

  @Test
  public void testMasterAndBackupMastersStatus() throws Exception {
    // get all the master threads
    List<MasterThread> masterThreads = CLUSTER.getMasterThreads();
    int numActive = 0;
    int activeIndex = 0;
    ServerName activeName = null;
    HMaster active = null;
    for (int i = 0; i < masterThreads.size(); i++) {
      if (masterThreads.get(i).getMaster().isActiveMaster()) {
        numActive++;
        activeIndex = i;
        active = masterThreads.get(activeIndex).getMaster();
        activeName = active.getServerName();
      }
    }
    Assert.assertNotNull(active);
    Assert.assertEquals(1, numActive);
    Assert.assertEquals(MASTERS, masterThreads.size());
    // Retrieve master and backup masters infos only.
    EnumSet<Option> options = EnumSet.of(Option.MASTER, Option.BACKUP_MASTERS);
    ClusterStatus status = new ClusterStatus(ADMIN.getClusterMetrics(options));
    Assert.assertTrue(status.getMaster().equals(activeName));
    Assert.assertEquals(MASTERS - 1, status.getBackupMastersSize());
  }

  @Test
  public void testOtherStatusInfos() throws Exception {
    EnumSet<Option> options =
        EnumSet.of(Option.MASTER_COPROCESSORS, Option.HBASE_VERSION,
                   Option.CLUSTER_ID, Option.BALANCER_ON);
    ClusterStatus status = new ClusterStatus(ADMIN.getClusterMetrics(options));
    Assert.assertTrue(status.getMasterCoprocessors().length == 1);
    Assert.assertNotNull(status.getHBaseVersion());
    Assert.assertNotNull(status.getClusterId());
    Assert.assertTrue(status.getAverageLoad() == 0.0);
    Assert.assertNotNull(status.getBalancerOn());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (ADMIN != null) ADMIN.close();
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testObserver() throws IOException {
    int preCount = MyObserver.PRE_COUNT.get();
    int postCount = MyObserver.POST_COUNT.get();
    Assert.assertTrue(Stream.of(ADMIN.getClusterStatus().getMasterCoprocessors())
        .anyMatch(s -> s.equals(MyObserver.class.getSimpleName())));
    Assert.assertEquals(preCount + 1, MyObserver.PRE_COUNT.get());
    Assert.assertEquals(postCount + 1, MyObserver.POST_COUNT.get());
  }

  /**
   * HBASE-19496 do the refactor for ServerLoad and RegionLoad so the inner pb object is useless
   * now. However, they are Public classes, and consequently we must make sure the all pb objects
   * have initialized.
   */
  private static void checkPbObjectNotNull(ClusterStatus status) {
    for (ServerName name : status.getLiveServerMetrics().keySet()) {
      ServerLoad load = status.getLoad(name);
      Assert.assertNotNull(load.obtainServerLoadPB());
      for (RegionLoad rl : load.getRegionsLoad().values()) {
        Assert.assertNotNull(rl.regionLoadPB);
      }
    }
  }

  public static class MyObserver implements MasterCoprocessor, MasterObserver {
    private static final AtomicInteger PRE_COUNT = new AtomicInteger(0);
    private static final AtomicInteger POST_COUNT = new AtomicInteger(0);

    @Override public Optional<MasterObserver> getMasterObserver() {
      return Optional.of(this);
    }

    @Override public void preGetClusterMetrics(ObserverContext<MasterCoprocessorEnvironment> ctx)
        throws IOException {
      PRE_COUNT.incrementAndGet();
    }

    @Override public void postGetClusterMetrics(ObserverContext<MasterCoprocessorEnvironment> ctx,
      ClusterMetrics status) throws IOException {
      POST_COUNT.incrementAndGet();
    }
  }
}
