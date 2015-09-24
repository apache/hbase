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

import java.util.Random;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class TestProcedureAdmin {
  private static final Log LOG = LogFactory.getLog(TestProcedureAdmin.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private long nonceGroup = HConstants.NO_NONCE;
  private long nonce = HConstants.NO_NONCE;

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    try {
      UTIL.shutdownMiniCluster();
    } catch (Exception e) {
      LOG.warn("failure shutting down cluster", e);
    }
  }

  @Before
  public void setup() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    assertTrue("expected executor to be running", procExec.isRunning());

    nonceGroup =
        MasterProcedureTestingUtility.generateNonceGroup(UTIL.getHBaseCluster().getMaster());
    nonce = MasterProcedureTestingUtility.generateNonce(UTIL.getHBaseCluster().getMaster());
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
    for (HTableDescriptor htd: UTIL.getHBaseAdmin().listTables()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test(timeout=60000)
  public void testAbortProcedureSuccess() throws Exception {
    final TableName tableName = TableName.valueOf("testAbortProcedureSuccess");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    // Submit an abortable procedure
    long procId = procExec.submitProcedure(
        new DisableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);

    boolean abortResult = procExec.abort(procId, true);
    assertTrue(abortResult);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    ProcedureTestingUtility.restart(procExec);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    // Validate the disable table procedure was aborted successfully
    MasterProcedureTestingUtility.validateTableIsEnabled(
      UTIL.getHBaseCluster().getMaster(),
      tableName);
  }

  @Test(timeout=60000)
  public void testAbortProcedureFailure() throws Exception {
    final TableName tableName = TableName.valueOf("testAbortProcedureFailure");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo[] regions =
        MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");
    UTIL.getHBaseAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    // Submit an un-abortable procedure
    long procId = procExec.submitProcedure(
        new DeleteTableProcedure(procExec.getEnvironment(), tableName), nonceGroup, nonce);

    boolean abortResult = procExec.abort(procId, true);
    assertFalse(abortResult);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    ProcedureTestingUtility.restart(procExec);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    // Validate the delete table procedure was not aborted
    MasterProcedureTestingUtility.validateTableDeletion(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, "f");
  }

  @Test(timeout=60000)
  public void testAbortProcedureInterruptedNotAllowed() throws Exception {
    final TableName tableName = TableName.valueOf("testAbortProcedureInterruptedNotAllowed");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    HRegionInfo[] regions =
        MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    // Submit a procedure
    long procId = procExec.submitProcedure(
        new DisableTableProcedure(procExec.getEnvironment(), tableName, true), nonceGroup, nonce);
    // Wait for one step to complete
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // Set the mayInterruptIfRunning flag to false
    boolean abortResult = procExec.abort(procId, false);
    assertFalse(abortResult);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    ProcedureTestingUtility.restart(procExec);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    // Validate the delete table procedure was not aborted
    MasterProcedureTestingUtility.validateTableIsDisabled(
      UTIL.getHBaseCluster().getMaster(), tableName);
  }

  @Test(timeout=60000)
  public void testAbortNonExistProcedure() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    Random randomGenerator = new Random();
    long procId;
    // Generate a non-existing procedure
    do {
      procId = randomGenerator.nextLong();
    } while (procExec.getResult(procId) != null);

    boolean abortResult = procExec.abort(procId, true);
    assertFalse(abortResult);
  }

  @Test(timeout=60000)
  public void testListProcedure() throws Exception {
    final TableName tableName = TableName.valueOf("testListProcedure");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    long procId = procExec.submitProcedure(
      new DisableTableProcedure(procExec.getEnvironment(), tableName, false), nonceGroup, nonce);

    List<ProcedureInfo> listProcedures = procExec.listProcedures();
    assertTrue(listProcedures.size() >= 1);
    boolean found = false;
    for (ProcedureInfo procInfo: listProcedures) {
      if (procInfo.getProcId() == procId) {
        assertTrue(procInfo.getProcState() == ProcedureState.RUNNABLE);
        found = true;
      } else {
        assertTrue(procInfo.getProcState() == ProcedureState.FINISHED);
      }
    }
    assertTrue(found);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    ProcedureTestingUtility.restart(procExec);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    listProcedures = procExec.listProcedures();
    for (ProcedureInfo procInfo: listProcedures) {
      assertTrue(procInfo.getProcState() == ProcedureState.FINISHED);
    }
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
