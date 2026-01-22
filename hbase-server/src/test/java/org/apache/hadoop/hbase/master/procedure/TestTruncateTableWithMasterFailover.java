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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TruncateTableState;

@Category({ MasterTests.class, MediumTests.class })
public class TestTruncateTableWithMasterFailover extends MasterFailoverWithProceduresTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTruncateTableWithMasterFailover.class);

  // ==========================================================================
  // Test Truncate Table
  // ==========================================================================
  @Test
  public void testTruncateWithFailover() throws Exception {
    // TODO: Should we try every step? (master failover takes long time)
    // It is already covered by TestTruncateTableProcedure
    // but without the master restart, only the executor/store is restarted.
    // Without Master restart we may not find bug in the procedure code
    // like missing "wait" for resources to be available (e.g. RS)
    testTruncateWithFailoverAtStep(true, TruncateTableState.TRUNCATE_TABLE_ADD_TO_META.ordinal());
  }

  private void testTruncateWithFailoverAtStep(final boolean preserveSplits, final int step)
    throws Exception {
    final TableName tableName = TableName.valueOf("testTruncateWithFailoverAtStep" + step);

    // create the table
    final String[] families = new String[] { "f1", "f2" };
    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c") };
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(getMasterProcedureExecutor(),
      tableName, splitKeys, families);
    // load and verify that there are rows in the table
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 100, splitKeys,
      families);
    assertEquals(100, UTIL.countRows(tableName));
    // disable the table
    UTIL.getAdmin().disableTable(tableName);

    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Truncate procedure && kill the executor
    long procId = procExec.submitProcedure(
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, preserveSplits));
    testRecoveryAndDoubleExecution(UTIL, procId, step);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    UTIL.waitUntilAllRegionsAssigned(tableName);

    // validate the table regions and layout
    regions = UTIL.getAdmin().getRegions(tableName).toArray(new RegionInfo[0]);
    if (preserveSplits) {
      assertEquals(1 + splitKeys.length, regions.length);
    } else {
      assertEquals(1, regions.length);
    }
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, families);

    // verify that there are no rows in the table
    assertEquals(0, UTIL.countRows(tableName));

    // verify that the table is read/writable
    MasterProcedureTestingUtility.loadData(UTIL.getConnection(), tableName, 50, splitKeys,
      families);
    assertEquals(50, UTIL.countRows(tableName));
  }
}
