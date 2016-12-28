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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DeleteNamespaceState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestDeleteNamespaceProcedure {
  private static final Log LOG = LogFactory.getLog(TestDeleteNamespaceProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

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
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
    for (HTableDescriptor htd: UTIL.getHBaseAdmin().listTables()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test(timeout = 60000)
  public void testDeleteNamespace() throws Exception {
    final String namespaceName = "testDeleteNamespace";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    createNamespaceForTesting(namespaceName);

    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    validateNamespaceNotExist(namespaceName);
  }

  @Test(timeout=60000)
  public void testDeleteNonExistNamespace() throws Exception {
    final String namespaceName = "testDeleteNonExistNamespace";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    validateNamespaceNotExist(namespaceName);

    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    // Expect fail with NamespaceNotFoundException
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Delete namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof NamespaceNotFoundException);
  }

  @Test(timeout=60000)
  public void testDeleteSystemNamespace() throws Exception {
    final String namespaceName = NamespaceDescriptor.SYSTEM_NAMESPACE.getName();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Delete namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test(timeout=60000)
  public void testDeleteNonEmptyNamespace() throws Exception {
    final String namespaceName = "testDeleteNonExistNamespace";
    final TableName tableName = TableName.valueOf("testDeleteNonExistNamespace:t1");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    // create namespace
    createNamespaceForTesting(namespaceName);
    // create the table under the new namespace
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1");

    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Delete namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test(timeout = 60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final String namespaceName = "testRecoveryAndDoubleExecution";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    createNamespaceForTesting(namespaceName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the DeleteNamespace procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));
    // Restart the executor and execute the step twice
    int numberOfSteps = DeleteNamespaceState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      DeleteNamespaceState.values());

    // Validate the deletion of namespace
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    validateNamespaceNotExist(namespaceName);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final String namespaceName = "testRollbackAndDoubleExecution";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    createNamespaceForTesting(namespaceName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the DeleteNamespace procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));

    int numberOfSteps = DeleteNamespaceState.values().length - 2; // failing in the middle of proc
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      DeleteNamespaceState.values());

    // Validate the namespace still exists
    NamespaceDescriptor createdNsDescriptor=
        UTIL.getHBaseAdmin().getNamespaceDescriptor(namespaceName);
    assertNotNull(createdNsDescriptor);
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  private void createNamespaceForTesting(final String namespaceName) throws Exception {
    final NamespaceDescriptor nsd = NamespaceDescriptor.create(namespaceName).build();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }

  public static void validateNamespaceNotExist(final String nsName) throws IOException {
    try {
      NamespaceDescriptor nsDescriptor = UTIL.getHBaseAdmin().getNamespaceDescriptor(nsName);
      assertNull(nsDescriptor);
    } catch (NamespaceNotFoundException nsnfe) {
      // Expected
    }
  }
}
