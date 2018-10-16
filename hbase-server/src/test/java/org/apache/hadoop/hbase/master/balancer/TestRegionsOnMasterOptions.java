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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test options for regions on master; none, system, or any (i.e. master is like any other
 * regionserver). Checks how regions are deployed when each of the options are enabled.
 * It then does kill combinations to make sure the distribution is more than just for startup.
 * NOTE: Regions on Master does not work well. See HBASE-19828. Until addressed, disabling this
 * test.
 */
@Ignore
@Category({MediumTests.class})
public class TestRegionsOnMasterOptions {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionsOnMasterOptions.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionsOnMasterOptions.class);
  @Rule public TestName name = new TestName();
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Configuration c;
  private String tablesOnMasterOldValue;
  private String systemTablesOnMasterOldValue;
  private static final int SLAVES = 3;
  private static final int MASTERS = 2;
  // Make the count of REGIONS high enough so I can distingush case where master is only carrying
  // system regions from the case where it is carrying any region; i.e. 2 system regions vs more
  // if user + system.
  private static final int REGIONS = 12;
  private static final int SYSTEM_REGIONS = 2; // ns and meta -- no acl unless enabled.

  @Before
  public void setup() {
    this.c = TEST_UTIL.getConfiguration();
    this.tablesOnMasterOldValue = c.get(LoadBalancer.TABLES_ON_MASTER);
    this.systemTablesOnMasterOldValue = c.get(LoadBalancer.SYSTEM_TABLES_ON_MASTER);
  }

  @After
  public void tearDown() {
    unset(LoadBalancer.TABLES_ON_MASTER, this.tablesOnMasterOldValue);
    unset(LoadBalancer.SYSTEM_TABLES_ON_MASTER, this.systemTablesOnMasterOldValue);
  }

  private void unset(final String key, final String value) {
    if (value == null) {
      c.unset(key);
    } else {
      c.set(key, value);
    }
  }

  @Test
  public void testRegionsOnAllServers() throws Exception {
    c.setBoolean(LoadBalancer.TABLES_ON_MASTER, true);
    c.setBoolean(LoadBalancer.SYSTEM_TABLES_ON_MASTER, false);
    int rsCount = (REGIONS + SYSTEM_REGIONS)/(SLAVES + 1/*Master*/);
    checkBalance(rsCount, rsCount);
  }

  @Test
  public void testNoRegionOnMaster() throws Exception {
    c.setBoolean(LoadBalancer.TABLES_ON_MASTER, false);
    c.setBoolean(LoadBalancer.SYSTEM_TABLES_ON_MASTER, false);
    int rsCount = (REGIONS + SYSTEM_REGIONS)/SLAVES;
    checkBalance(0, rsCount);
  }

  @Ignore // Fix this. The Master startup doesn't allow Master reporting as a RegionServer, not
  // until way late after the Master startup finishes. Needs more work.
  @Test
  public void testSystemTablesOnMaster() throws Exception {
    c.setBoolean(LoadBalancer.TABLES_ON_MASTER, true);
    c.setBoolean(LoadBalancer.SYSTEM_TABLES_ON_MASTER, true);
    // IS THIS SHORT-CIRCUIT RPC? Yes. Here is how it looks currently if I have an exception
    // thrown in doBatchMutate inside a Region.
    //
    //    java.lang.Exception
    //    at org.apache.hadoop.hbase.regionserver.HRegion.doBatchMutate(HRegion.java:3845)
    //    at org.apache.hadoop.hbase.regionserver.HRegion.put(HRegion.java:2972)
    //    at org.apache.hadoop.hbase.regionserver.RSRpcServices.mutate(RSRpcServices.java:2751)
    //    at org.apache.hadoop.hbase.client.ClientServiceCallable.doMutate(ClientServiceCallable.java:55)
    //    at org.apache.hadoop.hbase.client.HTable$3.rpcCall(HTable.java:585)
    //    at org.apache.hadoop.hbase.client.HTable$3.rpcCall(HTable.java:579)
    //    at org.apache.hadoop.hbase.client.RegionServerCallable.call(RegionServerCallable.java:126)
    //    at org.apache.hadoop.hbase.client.RpcRetryingCallerImpl.callWithRetries(RpcRetryingCallerImpl.java:106)
    //    at org.apache.hadoop.hbase.client.HTable.put(HTable.java:589)
    //    at org.apache.hadoop.hbase.master.TableNamespaceManager.insertIntoNSTable(TableNamespaceManager.java:156)
    //    at org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure.insertIntoNSTable(CreateNamespaceProcedure.java:222)
    //    at org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure.executeFromState(CreateNamespaceProcedure.java:76)
    //    at org.apache.hadoop.hbase.master.procedure.CreateNamespaceProcedure.executeFromState(CreateNamespaceProcedure.java:40)
    //    at org.apache.hadoop.hbase.procedure2.StateMachineProcedure.execute(StateMachineProcedure.java:181)
    //    at org.apache.hadoop.hbase.procedure2.Procedure.doExecute(Procedure.java:847)
    //    at org.apache.hadoop.hbase.procedure2.ProcedureExecutor.execProcedure(ProcedureExecutor.java:1440)
    //    at org.apache.hadoop.hbase.procedure2.ProcedureExecutor.executeProcedure(ProcedureExecutor.java:1209)
    //    at org.apache.hadoop.hbase.procedure2.ProcedureExecutor.access$800(ProcedureExecutor.java:79)
    //    at org.apache.hadoop.hbase.procedure2.ProcedureExecutor$WorkerThread.run(ProcedureExecutor.java:1719)
    //
    // If I comment out the ConnectionUtils ConnectionImplementation content, I see this:
    //
    //    java.lang.Exception
    //    at org.apache.hadoop.hbase.regionserver.HRegion.doBatchMutate(HRegion.java:3845)
    //    at org.apache.hadoop.hbase.regionserver.HRegion.put(HRegion.java:2972)
    //    at org.apache.hadoop.hbase.regionserver.RSRpcServices.mutate(RSRpcServices.java:2751)
    //    at org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos$ClientService$2.callBlockingMethod(ClientProtos.java:41546)
    //    at org.apache.hadoop.hbase.ipc.RpcServer.call(RpcServer.java:406)
    //    at org.apache.hadoop.hbase.ipc.CallRunner.run(CallRunner.java:133)
    //    at org.apache.hadoop.hbase.ipc.RpcExecutor$Handler.run(RpcExecutor.java:278)
    //    at org.apache.hadoop.hbase.ipc.RpcExecutor$Handler.run(RpcExecutor.java:258)

    checkBalance(SYSTEM_REGIONS, REGIONS/SLAVES);
  }

  private void checkBalance(int masterCount, int rsCount) throws Exception {
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(MASTERS).numRegionServers(SLAVES).numDataNodes(SLAVES).build();
    MiniHBaseCluster cluster = TEST_UTIL.startMiniCluster(option);
    TableName tn = TableName.valueOf(this.name.getMethodName());
    try {
      Table t = TEST_UTIL.createMultiRegionTable(tn, HConstants.CATALOG_FAMILY, REGIONS);
      LOG.info("Server: " + cluster.getMaster().getServerManager().getOnlineServersList());
      List<HRegion> regions = cluster.getMaster().getRegions();
      int mActualCount = regions.size();
      if (masterCount == 0 || masterCount == SYSTEM_REGIONS) {
        // 0 means no regions on master.
        assertEquals(masterCount, mActualCount);
      } else {
        // This is master as a regionserver scenario.
        checkCount(masterCount, mActualCount);
      }
      // Allow that balance is not exact. FYI, getRegionServerThreads does not include master
      // thread though it is a regionserver so we have to check master and then below the
      // regionservers.
      for (JVMClusterUtil.RegionServerThread rst: cluster.getRegionServerThreads()) {
        regions = rst.getRegionServer().getRegions();
        int rsActualCount = regions.size();
        checkCount(rsActualCount, rsCount);
      }
      HMaster oldMaster = cluster.getMaster();
      cluster.killMaster(oldMaster.getServerName());
      oldMaster.join();
      while (cluster.getMaster() == null ||
          cluster.getMaster().getServerName().equals(oldMaster.getServerName())) {
        Threads.sleep(10);
      }
      while (!cluster.getMaster().isInitialized()) {
        Threads.sleep(10);
      }
      while (cluster.getMaster().getAssignmentManager().
          computeRegionInTransitionStat().getTotalRITs() > 0) {
        Threads.sleep(100);
        LOG.info("Waiting on RIT to go to zero before calling balancer...");
      }
      LOG.info("Cluster is up; running balancer");
      cluster.getMaster().balance();
      regions = cluster.getMaster().getRegions();
      int mNewActualCount = regions.size();
      if (masterCount == 0 || masterCount == SYSTEM_REGIONS) {
        // 0 means no regions on master. After crash, should still be no regions on master.
        // If masterCount == SYSTEM_REGIONS, means master only carrying system regions and should
        // still only carry system regions post crash.
        assertEquals(masterCount, mNewActualCount);
      }
    } finally {
      LOG.info("Running shutdown of cluster");
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  private void checkCount(int actual, int expected) {
    assertTrue("Actual=" + actual + ", expected=" + expected,
    actual >= (expected - 2) && actual <= (expected + 2)); // Lots of slop +/- 2
  }
}
