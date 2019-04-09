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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.constraint.ConstraintException;
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
public class TestDeleteNamespaceProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDeleteNamespaceProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestDeleteNamespaceProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

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
    for (TableDescriptor htd: UTIL.getAdmin().listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
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

  @Test
  public void testDeleteNonExistNamespace() throws Exception {
    final String namespaceName = "testDeleteNonExistNamespace";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    validateNamespaceNotExist(namespaceName);

    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    // Expect fail with NamespaceNotFoundException
    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Delete namespace failed with exception: " + result.getException());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof NamespaceNotFoundException);
  }

  @Test
  public void testDeleteSystemNamespace() throws Exception {
    final String namespaceName = NamespaceDescriptor.SYSTEM_NAMESPACE.getName();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Delete namespace failed with exception: " + result.getException());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test
  public void testDeleteNonEmptyNamespace() throws Exception {
    final String namespaceName = "testDeleteNonExistNamespace";
    final TableName tableName = TableName.valueOf("testDeleteNonExistNamespace:" + name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    // create namespace
    createNamespaceForTesting(namespaceName);
    // create the table under the new namespace
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1");

    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Delete namespace failed with exception: " + result.getException());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test
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
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    // Validate the deletion of namespace
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    validateNamespaceNotExist(namespaceName);
  }

  @Test
  public void testRollbackAndDoubleExecution() throws Exception {
    final String namespaceName = "testRollbackAndDoubleExecution";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    createNamespaceForTesting(namespaceName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the DeleteNamespace procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DeleteNamespaceProcedure(procExec.getEnvironment(), namespaceName));

    int lastStep = 2; // failing before DELETE_NAMESPACE_DELETE_FROM_NS_TABLE
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep);

    // Validate the namespace still exists
    NamespaceDescriptor createdNsDescriptor=
        UTIL.getAdmin().getNamespaceDescriptor(namespaceName);
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
      NamespaceDescriptor nsDescriptor = UTIL.getAdmin().getNamespaceDescriptor(nsName);
      assertNull(nsDescriptor);
    } catch (NamespaceNotFoundException nsnfe) {
      // Expected
    }
  }
}
