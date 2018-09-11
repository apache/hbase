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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

/**
 * Confirm that we will do backoff when retrying on reopening table regions, to avoid consuming all
 * the CPUs.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestReopenTableRegionsProcedureBackoff {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReopenTableRegionsProcedureBackoff.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestReopenTableRegionsProcedureBackoff.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("Backoff");

  private static byte[] CF = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    UTIL.startMiniCluster(1);
    UTIL.createTable(TABLE_NAME, CF);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRetryBackoff() throws IOException, InterruptedException {
    AssignmentManager am = UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    ProcedureExecutor<MasterProcedureEnv> procExec =
      UTIL.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor();
    RegionInfo regionInfo = UTIL.getAdmin().getRegions(TABLE_NAME).get(0);
    RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(regionInfo);
    // just a dummy one
    TransitRegionStateProcedure trsp =
      TransitRegionStateProcedure.unassign(procExec.getEnvironment(), regionInfo);
    long openSeqNum;
    regionNode.lock();
    try {
      openSeqNum = regionNode.getOpenSeqNum();
      // make a fake state to let the procedure wait.
      regionNode.setState(State.OPENING);
      regionNode.setOpenSeqNum(-1L);
      regionNode.setProcedure(trsp);
    } finally {
      regionNode.unlock();
    }
    ReopenTableRegionsProcedure proc = new ReopenTableRegionsProcedure(TABLE_NAME);
    procExec.submitProcedure(proc);
    UTIL.waitFor(10000, () -> proc.getState() == ProcedureState.WAITING_TIMEOUT);
    long oldTimeout = 0;
    int timeoutIncrements = 0;
    for (;;) {
      long timeout = proc.getTimeout();
      if (timeout > oldTimeout) {
        LOG.info("Timeout incremented, was {}, now is {}, increments={}", timeout, oldTimeout,
          timeoutIncrements);
        oldTimeout = timeout;
        timeoutIncrements++;
        if (timeoutIncrements > 3) {
          // If we incremented at least twice, break; the backoff is working.
          break;
        }
      }
      Thread.sleep(1000);
    }
    regionNode.lock();
    try {
      // reset to the correct state
      regionNode.setState(State.OPEN);
      regionNode.setOpenSeqNum(openSeqNum);
      regionNode.unsetProcedure(trsp);
    } finally {
      regionNode.unlock();
    }
    ProcedureSyncWait.waitForProcedureToComplete(procExec, proc, 60000);
    assertTrue(regionNode.getOpenSeqNum() > openSeqNum);
  }
}
