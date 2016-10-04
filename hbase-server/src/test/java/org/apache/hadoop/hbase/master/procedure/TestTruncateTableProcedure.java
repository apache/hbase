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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TruncateTableState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestTruncateTableProcedure extends TestTableDDLProcedureBase {
  private static final Log LOG = LogFactory.getLog(TestTruncateTableProcedure.class);

  @Test(timeout=60000)
  public void testTruncateNotExistentTable() throws Exception {
    final TableName tableName = TableName.valueOf("testTruncateNotExistentTable");

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
        new TruncateTableProcedure(procExec.getEnvironment(), tableName, true));

    // Second delete should fail with TableNotFound
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Truncate failed with exception: " + result.getExceptionFullMessage());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotFoundException);
  }

  @Test(timeout=60000)
  public void testTruncateNotDisabledTable() throws Exception {
    final TableName tableName = TableName.valueOf("testTruncateNotDisabledTable");

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");

    long procId = ProcedureTestingUtility.submitAndWait(procExec,
        new TruncateTableProcedure(procExec.getEnvironment(), tableName, false));

    // Second delete should fail with TableNotDisabled
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Truncate failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotDisabledException);
  }

  @Test(timeout=60000)
  public void testSimpleTruncatePreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf("testSimpleTruncatePreserveSplits");
    testSimpleTruncate(tableName, true);
  }

  @Test(timeout=60000)
  public void testSimpleTruncateNoPreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf("testSimpleTruncateNoPreserveSplits");
    testSimpleTruncate(tableName, false);
  }

  private void testSimpleTruncate(final TableName tableName, final boolean preserveSplits)
      throws Exception {
    final String[] families = new String[] { "f1", "f2" };
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };

    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      getMasterProcedureExecutor(), tableName, splitKeys, families);
    // load and verify that there are rows in the table
    MasterProcedureTestingUtility.loadData(
      UTIL.getConnection(), tableName, 100, splitKeys, families);
    assertEquals(100, UTIL.countRows(tableName));
    // disable the table
    UTIL.getHBaseAdmin().disableTable(tableName);

    // truncate the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, preserveSplits));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    UTIL.waitUntilAllRegionsAssigned(tableName);

    // validate the table regions and layout
    regions = UTIL.getHBaseAdmin().getTableRegions(tableName).toArray(new HRegionInfo[0]);
    if (preserveSplits) {
      assertEquals(1 + splitKeys.length, regions.length);
    } else {
      assertEquals(1, regions.length);
    }
    MasterProcedureTestingUtility.validateTableCreation(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, families);

    // verify that there are no rows in the table
    assertEquals(0, UTIL.countRows(tableName));

    // verify that the table is read/writable
    MasterProcedureTestingUtility.loadData(
      UTIL.getConnection(), tableName, 50, splitKeys, families);
    assertEquals(50, UTIL.countRows(tableName));
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecutionPreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionPreserveSplits");
    testRecoveryAndDoubleExecution(tableName, true);
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecutionNoPreserveSplits() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionNoPreserveSplits");
    testRecoveryAndDoubleExecution(tableName, false);
  }

  private void testRecoveryAndDoubleExecution(final TableName tableName,
      final boolean preserveSplits) throws Exception {
    final String[] families = new String[] { "f1", "f2" };

    // create the table
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      getMasterProcedureExecutor(), tableName, splitKeys, families);
    // load and verify that there are rows in the table
    MasterProcedureTestingUtility.loadData(
      UTIL.getConnection(), tableName, 100, splitKeys, families);
    assertEquals(100, UTIL.countRows(tableName));
    // disable the table
    UTIL.getHBaseAdmin().disableTable(tableName);

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Truncate procedure && kill the executor
    long procId = procExec.submitProcedure(
      new TruncateTableProcedure(procExec.getEnvironment(), tableName, preserveSplits),
      nonceGroup,
      nonce);

    // Restart the executor and execute the step twice
    // NOTE: the 7 (number of TruncateTableState steps) is hardcoded,
    //       so you have to look at this test at least once when you add a new step.
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, 7);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    UTIL.waitUntilAllRegionsAssigned(tableName);

    // validate the table regions and layout
    regions = UTIL.getHBaseAdmin().getTableRegions(tableName).toArray(new HRegionInfo[0]);
    if (preserveSplits) {
      assertEquals(1 + splitKeys.length, regions.length);
    } else {
      assertEquals(1, regions.length);
    }
    MasterProcedureTestingUtility.validateTableCreation(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, families);

    // verify that there are no rows in the table
    assertEquals(0, UTIL.countRows(tableName));

    // verify that the table is read/writable
    MasterProcedureTestingUtility.loadData(
      UTIL.getConnection(), tableName, 50, splitKeys, families);
    assertEquals(50, UTIL.countRows(tableName));
  }
}
