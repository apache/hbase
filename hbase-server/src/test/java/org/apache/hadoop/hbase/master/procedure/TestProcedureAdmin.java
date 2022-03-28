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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestProcedureAdmin {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureAdmin.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestProcedureAdmin.class);
  @Rule public TestName name = new TestName();

  protected static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

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
  }

  @After
  public void tearDown() throws Exception {
    assertTrue("expected executor to be running", getMasterProcedureExecutor().isRunning());
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
    for (TableDescriptor htd: UTIL.getAdmin().listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testAbortProcedureSuccess() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    // Submit an abortable procedure
    long procId = procExec.submitProcedure(
        new DisableTableProcedure(procExec.getEnvironment(), tableName, false));
    // Wait for one step to complete
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    boolean abortResult = procExec.abort(procId, true);
    assertTrue(abortResult);

    MasterProcedureTestingUtility.testRestartWithAbort(procExec, procId);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    // Validate the disable table procedure was aborted successfully
    MasterProcedureTestingUtility.validateTableIsEnabled(
      UTIL.getHBaseCluster().getMaster(),
      tableName);
  }

  @Test
  public void testAbortProcedureFailure() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo[] regions =
        MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");
    UTIL.getAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    // Submit an un-abortable procedure
    long procId = procExec.submitProcedure(
        new DeleteTableProcedure(procExec.getEnvironment(), tableName));
    // Wait for a couple of steps to complete (first step "prepare" is abortable)
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    for (int i = 0; i < 2; ++i) {
      ProcedureTestingUtility.assertProcNotYetCompleted(procExec, procId);
      ProcedureTestingUtility.restart(procExec);
      ProcedureTestingUtility.waitProcedure(procExec, procId);
    }

    boolean abortResult = procExec.abort(procId, true);
    assertFalse(abortResult);

    MasterProcedureTestingUtility.testRestartWithAbort(procExec, procId);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    // Validate the delete table procedure was not aborted
    MasterProcedureTestingUtility.validateTableDeletion(
      UTIL.getHBaseCluster().getMaster(), tableName);
  }

  @Test
  public void testAbortProcedureInterruptedNotAllowed() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo[] regions =
        MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    // Submit a procedure
    long procId = procExec.submitProcedure(
        new DisableTableProcedure(procExec.getEnvironment(), tableName, true));
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

  @Test
  public void testAbortNonExistProcedure() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId;
    // Generate a non-existing procedure
    do {
      procId = ThreadLocalRandom.current().nextLong();
    } while (procExec.getResult(procId) != null);

    boolean abortResult = procExec.abort(procId, true);
    assertFalse(abortResult);
  }

  @Test
  public void testGetProcedure() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    long procId = procExec.submitProcedure(
      new DisableTableProcedure(procExec.getEnvironment(), tableName, false));
    // Wait for one step to complete
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    List<Procedure<MasterProcedureEnv>> procedures = procExec.getProcedures();
    assertTrue(procedures.size() >= 1);
    boolean found = false;
    for (Procedure<?> proc: procedures) {
      if (proc.getProcId() == procId) {
        assertTrue(proc.isRunnable());
        found = true;
      } else {
        assertTrue(proc.isSuccess());
      }
    }
    assertTrue(found);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    ProcedureTestingUtility.restart(procExec);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    procedures = procExec.getProcedures();
    for (Procedure proc: procedures) {
      assertTrue(proc.isSuccess());
    }
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
