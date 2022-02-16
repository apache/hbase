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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.rsgroup.RSGroupBasedLoadBalancer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterBalancerNPE {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterBalancerNPE.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  @Rule
  public TestName name = new TestName();

  @Before
  public void setupConfiguration() {
    /**
     * Make {@link BalancerChore} not run,so does not disrupt the test.
     */
    HMaster.setDisableBalancerChoreForTest(true);
  }

  @After
  public void shutdown() throws Exception {
    HMaster.setDisableBalancerChoreForTest(false);
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * This test is for HBASE-26712, to make the region is unassigned just before
   * {@link AssignmentManager#balance} is invoked on the region.
   */
  @Test
  public void testBalancerNPE() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    TableName tableName = createTable(name.getMethodName());
    final HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    List<RegionInfo> regionInfos = TEST_UTIL.getAdmin().getRegions(tableName);
    assertTrue(regionInfos.size() == 1);
    final ServerName serverName1 =
        TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
    final ServerName serverName2 =
        TEST_UTIL.getMiniHBaseCluster().getRegionServer(1).getServerName();

    final RegionInfo regionInfo = regionInfos.get(0);

    RSGroupBasedLoadBalancer loadBalancer = master.getLoadBalancer();
    RSGroupBasedLoadBalancer spiedLoadBalancer = Mockito.spy(loadBalancer);
    final AtomicReference<RegionPlan> regionPlanRef = new AtomicReference<RegionPlan>();

    /**
     * Mock {@link RSGroupBasedLoadBalancer#balanceCluster} to return the {@link RegionPlan} to move
     * the only region to the other RegionServer.
     */
    Mockito.doAnswer((InvocationOnMock invocation) -> {
      @SuppressWarnings("unchecked")
      Map<TableName, Map<ServerName, List<RegionInfo>>> tableNameToRegionServerNameToRegionInfos =
          (Map<TableName, Map<ServerName, List<RegionInfo>>>) invocation.getArgument(0);
      Map<ServerName, List<RegionInfo>> regionServerNameToRegionInfos =
          tableNameToRegionServerNameToRegionInfos.get(tableName);
      assertTrue(regionServerNameToRegionInfos.size() == 2);
      List<ServerName> assignedRegionServerNames = new ArrayList<ServerName>();
      for (Map.Entry<ServerName, List<RegionInfo>> entry : regionServerNameToRegionInfos
          .entrySet()) {
        if (entry.getValue().size() > 0) {
          assignedRegionServerNames.add(entry.getKey());
        }
      }
      assertTrue(assignedRegionServerNames.size() == 1);
      ServerName assignedRegionServerName = assignedRegionServerNames.get(0);
      ServerName notAssignedRegionServerName =
          assignedRegionServerName.equals(serverName1) ? serverName2 : serverName1;
      RegionPlan regionPlan =
          new RegionPlan(regionInfo, assignedRegionServerName, notAssignedRegionServerName);
      regionPlanRef.set(regionPlan);
      return Arrays.asList(regionPlan);
    }).when(spiedLoadBalancer).balanceCluster(Mockito.anyMap());

    AssignmentManager assignmentManager = master.getAssignmentManager();
    final AssignmentManager spiedAssignmentManager = Mockito.spy(assignmentManager);
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);

    /**
     * Override {@link AssignmentManager#balance} to invoke real {@link AssignmentManager#balance}
     * after the region is successfully unassigned.
     */
    Mockito.doAnswer((InvocationOnMock invocation) -> {
      RegionPlan regionPlan = invocation.getArgument(0);
      RegionPlan referedRegionPlan = regionPlanRef.get();
      assertTrue(referedRegionPlan != null);
      if (referedRegionPlan.equals(regionPlan)) {
        /**
         * To make {@link AssignmentManager#unassign} could be invoked just before
         * {@link AssignmentManager#balance} is invoked.
         */
        cyclicBarrier.await();
        /**
         * After {@link AssignmentManager#unassign} is completed,we could invoke
         * {@link AssignmentManager#balance}.
         */
        cyclicBarrier.await();
      }
      /**
       * Before HBASE-26712,here may throw NPE.
       */
      return invocation.callRealMethod();
    }).when(spiedAssignmentManager).balance(Mockito.any());


    try {
      final AtomicReference<Throwable> exceptionRef = new AtomicReference<Throwable>(null);
      Thread unassignThread = new Thread(() -> {
        try {
          /**
           * To invoke {@link AssignmentManager#unassign} just before
           * {@link AssignmentManager#balance} is invoked.
           */
          cyclicBarrier.await();
          spiedAssignmentManager.unassign(regionInfo);
          assertTrue(spiedAssignmentManager.getRegionStates().getRegionAssignments()
              .get(regionInfo) == null);
          /**
           * After {@link AssignmentManager#unassign} is completed,we could invoke
           * {@link AssignmentManager#balance}.
           */
          cyclicBarrier.await();
        } catch (Exception e) {
          exceptionRef.set(e);
        }
      });
      unassignThread.setName("UnassignThread");
      unassignThread.start();

      master.setLoadBalancer(spiedLoadBalancer);
      master.setAssignmentManager(spiedAssignmentManager);
      /**
       * enable balance
       */
      TEST_UTIL.getAdmin().balancerSwitch(true, false);
      /**
       * Before HBASE-26712,here invokes {@link AssignmentManager#balance(RegionPlan)}
       * which may throw NPE.
       */
      master.balanceOrUpdateMetrics();

      unassignThread.join();
      assertTrue(exceptionRef.get() == null);
    } finally {
      master.setLoadBalancer(loadBalancer);
      master.setAssignmentManager(assignmentManager);
    }
  }

  private TableName createTable(String table) throws IOException {
    TableName tableName = TableName.valueOf(table);
    TEST_UTIL.createTable(tableName, FAMILYNAME);
    return tableName;
  }

}
