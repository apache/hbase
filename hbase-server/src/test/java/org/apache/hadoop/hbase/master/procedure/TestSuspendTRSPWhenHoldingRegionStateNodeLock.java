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
package org.apache.hadoop.hbase.master.procedure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Testcase for HBASE-28240.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestSuspendTRSPWhenHoldingRegionStateNodeLock {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSuspendTRSPWhenHoldingRegionStateNodeLock.class);

  private static final HBaseTestingUtil HBTU = new HBaseTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("test");

  private static byte[] FAMILY = Bytes.toBytes("family");

  @BeforeClass
  public static void setUp() throws Exception {
    HBTU.startMiniCluster(2);
    HBTU.createTable(TABLE_NAME, FAMILY);
    HBTU.waitTableAvailable(TABLE_NAME);
    HBTU.getAdmin().balancerSwitch(false, true);
    HBTU.waitUntilNoRegionsInTransition();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    HBTU.shutdownMiniCluster();
  }

  private <T> Matcher<Procedure<T>> notChildOf(long procId) {
    return new BaseMatcher<Procedure<T>>() {

      @Override
      public boolean matches(Object item) {
        if (!(item instanceof Procedure)) {
          return false;
        }
        Procedure<?> proc = (Procedure<?>) item;
        return !proc.hasParent() || proc.getRootProcId() != procId;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("not a child of pid=").appendValue(procId);
      }
    };
  }

  @Test
  public void testSuspend() throws Exception {
    HMaster master = HBTU.getMiniHBaseCluster().getMaster();
    AssignmentManager am = master.getAssignmentManager();
    RegionInfo ri = Iterables.getOnlyElement(am.getTableRegions(TABLE_NAME, true));
    RegionStateNode rsn = am.getRegionStates().getRegionStateNode(ri);

    ServerName src = rsn.getRegionLocation();
    ServerName dst = HBTU.getMiniHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer().getServerName()).filter(sn -> !sn.equals(src)).findFirst()
      .get();
    TransitRegionStateProcedure proc = am.createMoveRegionProcedure(ri, dst);
    // lock the region state node manually, so later TRSP can not lock it
    rsn.lock();
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    long procId = procExec.submitProcedure(proc);
    // sleep several seconds to let the procedure be scheduled
    Thread.sleep(2000);
    // wait until no active procedures
    HBTU.waitFor(30000, () -> procExec.getActiveExecutorCount() == 0);
    // the procedure should have not finished yet
    assertFalse(proc.isFinished());
    // the TRSP should have not scheduled any sub procedures yet
    assertThat(procExec.getProcedures(), everyItem(notChildOf(procId)));
    // make sure the region is still on the src region server
    assertEquals(src, HBTU.getRSForFirstRegionInTable(TABLE_NAME).getServerName());

    // unlock the region state node lock, the TRSP should be woken up and finish the execution
    rsn.unlock();
    HBTU.waitFor(30000, () -> proc.isFinished());
    // make sure the region is on the dst region server
    assertEquals(dst, HBTU.getRSForFirstRegionInTable(TABLE_NAME).getServerName());
  }
}
