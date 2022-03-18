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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedureRSCrashes extends TestSnapshotProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedureRSCrashes.class);

  @Test
  public void testRegionServerCrashWhileTakingSnapshot() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();
    SnapshotProcedure sp = new SnapshotProcedure(env, snapshotProto);
    long procId = procExec.submitProcedure(sp);

    SnapshotRegionProcedure snp = waitProcedureRunnableAndGetFirst(
      SnapshotRegionProcedure.class, 60000);
    ServerName targetServer = env.getAssignmentManager().getRegionStates()
      .getRegionStateNode(snp.getRegion()).getRegionLocation();
    TEST_UTIL.getHBaseCluster().killRegionServer(targetServer);

    TEST_UTIL.waitFor(60000, () -> snp.inRetrying());
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    SnapshotTestingUtils.assertOneSnapshotThatMatches(TEST_UTIL.getAdmin(), snapshotProto);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }

  @Test
  public void testRegionServerCrashWhileVerifyingSnapshot() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    MasterProcedureEnv env = procExec.getEnvironment();
    SnapshotProcedure sp = new SnapshotProcedure(env, snapshotProto);
    long procId = procExec.submitProcedure(sp);

    SnapshotVerifyProcedure svp = waitProcedureRunnableAndGetFirst(
      SnapshotVerifyProcedure.class, 60000);
    TEST_UTIL.waitFor(10000, () -> svp.getServerName() != null);
    ServerName previousTargetServer = svp.getServerName();

    HRegionServer rs = TEST_UTIL.getHBaseCluster().getRegionServer(previousTargetServer);
    TEST_UTIL.getHBaseCluster().killRegionServer(rs.getServerName());
    TEST_UTIL.waitFor(60000, () -> svp.getServerName() != null
      && !svp.getServerName().equals(previousTargetServer));
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    SnapshotTestingUtils.assertOneSnapshotThatMatches(TEST_UTIL.getAdmin(), snapshotProto);
    SnapshotTestingUtils.confirmSnapshotValid(TEST_UTIL, snapshotProto, TABLE_NAME, CF);
  }
}
