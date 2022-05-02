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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DeleteTableState;

@Category({ MasterTests.class, MediumTests.class })
public class TestDeleteTableWithMasterFailover extends MasterFailoverWithProceduresTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDeleteTableWithMasterFailover.class);

  // ==========================================================================
  // Test Delete Table
  // ==========================================================================
  @Test
  public void testDeleteWithFailover() throws Exception {
    // TODO: Should we try every step? (master failover takes long time)
    // It is already covered by TestDeleteTableProcedure
    // but without the master restart, only the executor/store is restarted.
    // Without Master restart we may not find bug in the procedure code
    // like missing "wait" for resources to be available (e.g. RS)
    testDeleteWithFailoverAtStep(DeleteTableState.DELETE_TABLE_UNASSIGN_REGIONS.ordinal());
  }

  private void testDeleteWithFailoverAtStep(final int step) throws Exception {
    final TableName tableName = TableName.valueOf("testDeleteWithFailoverAtStep" + step);

    // create the table
    byte[][] splitKeys = null;
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, "f1", "f2");
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "f1", "f2");
    UTIL.getAdmin().disableTable(tableName);

    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillBeforeStoreUpdate(procExec, true);
    ProcedureTestingUtility.setToggleKillBeforeStoreUpdate(procExec, true);

    // Start the Delete procedure && kill the executor
    long procId =
      procExec.submitProcedure(new DeleteTableProcedure(procExec.getEnvironment(), tableName));
    testRecoveryAndDoubleExecution(UTIL, procId, step);

    MasterProcedureTestingUtility.validateTableDeletion(UTIL.getHBaseCluster().getMaster(),
      tableName);
  }
}
