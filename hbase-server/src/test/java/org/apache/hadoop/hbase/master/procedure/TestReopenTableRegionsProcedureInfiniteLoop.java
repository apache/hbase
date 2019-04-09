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

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.HMaster;
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

/**
 * Testcase for HBASE-21330.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestReopenTableRegionsProcedureInfiniteLoop {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReopenTableRegionsProcedureInfiniteLoop.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("InfiniteLoop");

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
  public void testInfiniteLoop() throws IOException {
    HMaster master = UTIL.getMiniHBaseCluster().getMaster();
    AssignmentManager am = master.getAssignmentManager();
    ProcedureExecutor<MasterProcedureEnv> exec = master.getMasterProcedureExecutor();
    RegionInfo regionInfo = UTIL.getAdmin().getRegions(TABLE_NAME).get(0);
    RegionStateNode regionNode = am.getRegionStates().getRegionStateNode(regionInfo);
    long procId;
    ReopenTableRegionsProcedure proc = new ReopenTableRegionsProcedure(TABLE_NAME);
    regionNode.lock();
    try {
      procId = exec.submitProcedure(proc);
      UTIL.waitFor(30000, () -> proc.hasLock());
      TransitRegionStateProcedure trsp =
        TransitRegionStateProcedure.reopen(exec.getEnvironment(), regionInfo);
      regionNode.setProcedure(trsp);
      exec.submitProcedure(trsp);
    } finally {
      regionNode.unlock();
    }
    UTIL.waitFor(60000, () -> exec.isFinished(procId));
  }
}
