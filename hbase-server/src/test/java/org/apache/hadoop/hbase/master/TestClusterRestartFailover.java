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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.ServerState;
import org.apache.hadoop.hbase.master.assignment.ServerStateNode;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.region.MasterRegion;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestClusterRestartFailover extends AbstractTestRestartCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClusterRestartFailover.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestClusterRestartFailover.class);
  private static final MetricsAssertHelper metricsHelper =
    CompatibilityFactory.getInstance(MetricsAssertHelper.class);

  private volatile static CountDownLatch SCP_LATCH;
  private static ServerName SERVER_FOR_TEST;

  @Override
  protected boolean splitWALCoordinatedByZk() {
    return true;
  }

  private ServerStateNode getServerStateNode(ServerName serverName) {
    return UTIL.getHBaseCluster().getMaster().getAssignmentManager().getRegionStates()
      .getServerNode(serverName);
  }

  /**
   * Test for HBASE-22964
   */
  @Test
  public void test() throws Exception {
    setupCluster();
    setupTable();

    SERVER_FOR_TEST = UTIL.getHBaseCluster().getRegionServer(0).getServerName();
    UTIL.waitFor(60000, () -> getServerStateNode(SERVER_FOR_TEST) != null);
    ServerStateNode serverNode = getServerStateNode(SERVER_FOR_TEST);
    assertNotNull(serverNode);
    assertTrue("serverNode should be ONLINE when cluster runs normally",
      serverNode.isInState(ServerState.ONLINE));

    SCP_LATCH = new CountDownLatch(1);

    // Shutdown cluster and restart
    List<Integer> ports =
      UTIL.getHBaseCluster().getMaster().getServerManager().getOnlineServersList().stream()
        .map(serverName -> serverName.getPort()).collect(Collectors.toList());
    LOG.info("Shutting down cluster");
    UTIL.getHBaseCluster().killAll();
    UTIL.getHBaseCluster().waitUntilShutDown();
    LOG.info("Restarting cluster");
    UTIL.restartHBaseCluster(StartTestingClusterOption.builder().masterClass(HMasterForTest.class)
      .numMasters(1).numRegionServers(3).rsPorts(ports).build());
    LOG.info("Started cluster");
    UTIL.waitFor(60000, () -> UTIL.getHBaseCluster().getMaster().isInitialized());
    LOG.info("Started cluster master, waiting for {}", SERVER_FOR_TEST);
    UTIL.waitFor(60000, () -> getServerStateNode(SERVER_FOR_TEST) != null);
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return !getServerStateNode(SERVER_FOR_TEST).isInState(ServerState.ONLINE);
      }

      @Override
      public String explainFailure() throws Exception {
        return "serverNode should not be ONLINE during SCP processing";
      }
    });
    Optional<Procedure<?>> procedure = UTIL.getHBaseCluster().getMaster().getProcedures().stream()
      .filter(p -> (p instanceof ServerCrashProcedure)
        && ((ServerCrashProcedure) p).getServerName().equals(SERVER_FOR_TEST))
      .findAny();
    assertTrue("Should have one SCP for " + SERVER_FOR_TEST, procedure.isPresent());
    assertEquals("Submit the SCP for the same serverName " + SERVER_FOR_TEST + " which should fail",
      Procedure.NO_PROC_ID,
      UTIL.getHBaseCluster().getMaster().getServerManager().expireServer(SERVER_FOR_TEST));

    // Wait the SCP to finish
    LOG.info("Waiting on latch");
    SCP_LATCH.countDown();
    UTIL.waitFor(60000, () -> procedure.get().isFinished());
    assertNull("serverNode should be deleted after SCP finished",
      getServerStateNode(SERVER_FOR_TEST));

    assertEquals(
      "Even when the SCP is finished, the duplicate SCP should not be scheduled for "
        + SERVER_FOR_TEST,
      Procedure.NO_PROC_ID,
      UTIL.getHBaseCluster().getMaster().getServerManager().expireServer(SERVER_FOR_TEST));

    MetricsMasterSource masterSource =
      UTIL.getHBaseCluster().getMaster().getMasterMetrics().getMetricsSource();
    metricsHelper.assertCounter(MetricsMasterSource.SERVER_CRASH_METRIC_PREFIX + "SubmittedCount",
      3, masterSource);
  }

  private void setupCluster() throws Exception {
    LOG.info("Setup cluster");
    UTIL.startMiniCluster(StartTestingClusterOption.builder().masterClass(HMasterForTest.class)
      .numMasters(1).numRegionServers(3).build());
    // this test has been flaky. When it is rerun by surefire, the underlying minicluster isn't
    // completely cleaned. specifically, the metrics system isn't reset. The result is an otherwise
    // successful re-run is failed because there's 8 or 12 SCPcounts instead of the 4 that a
    // single run of the test would otherwise produce. Thus, explicitly reset the metrics source
    // each time we setup the cluster.
    UTIL.getMiniHBaseCluster().getMaster().getMasterMetrics().getMetricsSource().init();
    LOG.info("Cluster is up");
    UTIL.waitFor(60000, () -> UTIL.getMiniHBaseCluster().getMaster().isInitialized());
    LOG.info("Master is up");
    // wait for all SCPs finished
    UTIL.waitFor(60000, () -> UTIL.getHBaseCluster().getMaster().getProcedures().stream()
      .noneMatch(p -> p instanceof ServerCrashProcedure));
    LOG.info("No SCPs");
  }

  private void setupTable() throws Exception {
    TableName tableName = TABLES[0];
    UTIL.createMultiRegionTable(tableName, FAMILY);
    UTIL.waitTableAvailable(tableName);
    Table table = UTIL.getConnection().getTable(tableName);
    for (int i = 0; i < 100; i++) {
      UTIL.loadTable(table, FAMILY);
    }
  }

  public static final class HMasterForTest extends HMaster {

    public HMasterForTest(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    protected AssignmentManager createAssignmentManager(MasterServices master,
      MasterRegion masterRegion) {
      return new AssignmentManagerForTest(master, masterRegion);
    }
  }

  private static final class AssignmentManagerForTest extends AssignmentManager {

    public AssignmentManagerForTest(MasterServices master, MasterRegion masterRegion) {
      super(master, masterRegion);
    }

    @Override
    public List<RegionInfo> getRegionsOnServer(ServerName serverName) {
      List<RegionInfo> regions = super.getRegionsOnServer(serverName);
      // ServerCrashProcedure will call this method, so wait the CountDownLatch here
      if (SCP_LATCH != null && SERVER_FOR_TEST != null && serverName.equals(SERVER_FOR_TEST)) {
        try {
          LOG.info("ServerCrashProcedure wait the CountDownLatch here");
          SCP_LATCH.await();
          LOG.info("Continue the ServerCrashProcedure");
          SCP_LATCH = null;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return regions;
    }
  }
}
