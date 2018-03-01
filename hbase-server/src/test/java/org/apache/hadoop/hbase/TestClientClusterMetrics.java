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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncAdmin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestClientClusterMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClientClusterMetrics.class);

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
    UTIL.startMiniCluster(MASTERS, SLAVES);
    CLUSTER = UTIL.getHBaseCluster();
    CLUSTER.waitForActiveAndReadyMaster();
    ADMIN = UTIL.getAdmin();
    // Kill one region server
    List<RegionServerThread> rsts = CLUSTER.getLiveRegionServerThreads();
    RegionServerThread rst = rsts.get(rsts.size() - 1);
    DEAD = rst.getRegionServer();
    DEAD.stop("Test dead servers metrics");
    while (rst.isAlive()) {
      Thread.sleep(500);
    }
  }

  @Test
  public void testDefaults() throws Exception {
    ClusterMetrics origin = ADMIN.getClusterMetrics();
    ClusterMetrics defaults = ADMIN.getClusterMetrics(EnumSet.allOf(Option.class));
    Assert.assertEquals(origin.getHBaseVersion(), defaults.getHBaseVersion());
    Assert.assertEquals(origin.getClusterId(), defaults.getClusterId());
    Assert.assertEquals(origin.getAverageLoad(), defaults.getAverageLoad(), 0);
    Assert.assertEquals(origin.getBackupMasterNames().size(),
        defaults.getBackupMasterNames().size());
    Assert.assertEquals(origin.getDeadServerNames().size(), defaults.getDeadServerNames().size());
    Assert.assertEquals(origin.getRegionCount(), defaults.getRegionCount());
    Assert.assertEquals(origin.getLiveServerMetrics().size(),
        defaults.getLiveServerMetrics().size());
    Assert.assertEquals(origin.getMasterInfoPort(), defaults.getMasterInfoPort());
  }

  @Test
  public void testAsyncClient() throws Exception {
    try (AsyncConnection asyncConnect = ConnectionFactory.createAsyncConnection(
      UTIL.getConfiguration()).get()) {
      AsyncAdmin asyncAdmin = asyncConnect.getAdmin();
      CompletableFuture<ClusterMetrics> originFuture =
        asyncAdmin.getClusterMetrics();
      CompletableFuture<ClusterMetrics> defaultsFuture =
        asyncAdmin.getClusterMetrics(EnumSet.allOf(Option.class));
      ClusterMetrics origin = originFuture.get();
      ClusterMetrics defaults = defaultsFuture.get();
      Assert.assertEquals(origin.getHBaseVersion(), defaults.getHBaseVersion());
      Assert.assertEquals(origin.getClusterId(), defaults.getClusterId());
      Assert.assertEquals(origin.getHBaseVersion(), defaults.getHBaseVersion());
      Assert.assertEquals(origin.getClusterId(), defaults.getClusterId());
      Assert.assertEquals(origin.getAverageLoad(), defaults.getAverageLoad(), 0);
      Assert.assertEquals(origin.getBackupMasterNames().size(),
        defaults.getBackupMasterNames().size());
      Assert.assertEquals(origin.getDeadServerNames().size(), defaults.getDeadServerNames().size());
      Assert.assertEquals(origin.getRegionCount(), defaults.getRegionCount());
      Assert.assertEquals(origin.getLiveServerMetrics().size(),
        defaults.getLiveServerMetrics().size());
      Assert.assertEquals(origin.getMasterInfoPort(), defaults.getMasterInfoPort());
    }
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
        ClusterMetrics metrics = ADMIN.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS));
        Assert.assertNotNull(metrics);
        return metrics.getRegionCount() > 0;
      }
    });
    // Retrieve live servers and dead servers info.
    EnumSet<Option> options = EnumSet.of(Option.LIVE_SERVERS, Option.DEAD_SERVERS);
    ClusterMetrics metrics = ADMIN.getClusterMetrics(options);
    Assert.assertNotNull(metrics);
    // exclude a dead region server
    Assert.assertEquals(SLAVES -1, numRs);
    Assert.assertEquals(numRs + 1 /*Master*/, metrics.getLiveServerMetrics().size());
    Assert.assertTrue(metrics.getRegionCount() > 0);
    Assert.assertNotNull(metrics.getDeadServerNames());
    Assert.assertEquals(1, metrics.getDeadServerNames().size());
    ServerName deadServerName = metrics.getDeadServerNames().iterator().next();
    Assert.assertEquals(DEAD.getServerName(), deadServerName);
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
    ClusterMetrics metrics = ADMIN.getClusterMetrics(options);
    Assert.assertTrue(metrics.getMasterName().equals(activeName));
    Assert.assertEquals(MASTERS - 1, metrics.getBackupMasterNames().size());
  }

  @Test
  public void testOtherStatusInfos() throws Exception {
    EnumSet<Option> options =
        EnumSet.of(Option.MASTER_COPROCESSORS, Option.HBASE_VERSION,
                   Option.CLUSTER_ID, Option.BALANCER_ON);
    ClusterMetrics metrics = ADMIN.getClusterMetrics(options);
    Assert.assertEquals(1, metrics.getMasterCoprocessorNames().size());
    Assert.assertNotNull(metrics.getHBaseVersion());
    Assert.assertNotNull(metrics.getClusterId());
    Assert.assertTrue(metrics.getAverageLoad() == 0.0);
    Assert.assertNotNull(metrics.getBalancerOn());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (ADMIN != null) {
      ADMIN.close();
    }
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testObserver() throws IOException {
    int preCount = MyObserver.PRE_COUNT.get();
    int postCount = MyObserver.POST_COUNT.get();
    Assert.assertTrue(ADMIN.getClusterMetrics().getMasterCoprocessorNames().stream()
        .anyMatch(s -> s.equals(MyObserver.class.getSimpleName())));
    Assert.assertEquals(preCount + 1, MyObserver.PRE_COUNT.get());
    Assert.assertEquals(postCount + 1, MyObserver.POST_COUNT.get());
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
        ClusterMetrics metrics) throws IOException {
      POST_COUNT.incrementAndGet();
    }
  }
}
