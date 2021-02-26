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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestModifyNamespaceProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestModifyNamespaceProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestModifyNamespaceProcedure.class);

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
    for (TableDescriptor htd: UTIL.getAdmin().listTableDescriptors()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }


  @Test
  public void testModifyNamespace() throws Exception {
    final NamespaceDescriptor nsd = NamespaceDescriptor.create("testModifyNamespace").build();
    final String nsKey1 = "hbase.namespace.quota.maxregions";
    final String nsValue1before = "1111";
    final String nsValue1after = "9999";
    final String nsKey2 = "hbase.namespace.quota.maxtables";
    final String nsValue2 = "10";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    nsd.setConfiguration(nsKey1, nsValue1before);
    createNamespaceForTesting(nsd);

    // Before modify
    NamespaceDescriptor currentNsDescriptor =
        UTIL.getAdmin().getNamespaceDescriptor(nsd.getName());
    assertEquals(nsValue1before, currentNsDescriptor.getConfigurationValue(nsKey1));
    assertNull(currentNsDescriptor.getConfigurationValue(nsKey2));

    // Update
    nsd.setConfiguration(nsKey1, nsValue1after);
    nsd.setConfiguration(nsKey2, nsValue2);

    long procId1 = procExec.submitProcedure(
      new ModifyNamespaceProcedure(procExec.getEnvironment(), nsd));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);

    // Verify the namespace is updated.
    currentNsDescriptor =
        UTIL.getAdmin().getNamespaceDescriptor(nsd.getName());
    assertEquals(nsValue1after, nsd.getConfigurationValue(nsKey1));
    assertEquals(nsValue2, currentNsDescriptor.getConfigurationValue(nsKey2));
  }

  @Test
  public void testModifyNonExistNamespace() throws Exception {
    final String namespaceName = "testModifyNonExistNamespace";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    try {
      NamespaceDescriptor nsDescriptor = UTIL.getAdmin().getNamespaceDescriptor(namespaceName);
      assertNull(nsDescriptor);
    } catch (NamespaceNotFoundException nsnfe) {
      // Expected
      LOG.debug("The namespace " + namespaceName + " does not exist.  This is expected.");
    }

    final NamespaceDescriptor nsd = NamespaceDescriptor.create(namespaceName).build();

    long procId = procExec.submitProcedure(
      new ModifyNamespaceProcedure(procExec.getEnvironment(), nsd));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);

    // Expect fail with NamespaceNotFoundException
    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("modify namespace failed with exception: " + result.getException());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof NamespaceNotFoundException);
  }

  @Test
  public void testModifyNamespaceWithInvalidRegionCount() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testModifyNamespaceWithInvalidRegionCount").build();
    final String nsKey = "hbase.namespace.quota.maxregions";
    final String nsValue = "-1";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    createNamespaceForTesting(nsd);

    // Modify
    nsd.setConfiguration(nsKey, nsValue);

    long procId = procExec.submitProcedure(
      new ModifyNamespaceProcedure(procExec.getEnvironment(), nsd));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Modify namespace failed with exception: " + result.getException());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test
  public void testModifyNamespaceWithInvalidTableCount() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testModifyNamespaceWithInvalidTableCount").build();
    final String nsKey = "hbase.namespace.quota.maxtables";
    final String nsValue = "-1";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    createNamespaceForTesting(nsd);

    // Modify
    nsd.setConfiguration(nsKey, nsValue);

    long procId = procExec.submitProcedure(
      new ModifyNamespaceProcedure(procExec.getEnvironment(), nsd));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Modify namespace failed with exception: " + result.getException());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testRecoveryAndDoubleExecution").build();
    final String nsKey = "foo";
    final String nsValue = "bar";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    createNamespaceForTesting(nsd);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Modify
    nsd.setConfiguration(nsKey, nsValue);

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyNamespaceProcedure(procExec.getEnvironment(), nsd));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    // Validate
    NamespaceDescriptor currentNsDescriptor =
        UTIL.getAdmin().getNamespaceDescriptor(nsd.getName());
    assertEquals(nsValue, currentNsDescriptor.getConfigurationValue(nsKey));
  }

  @Test
  public void testRollbackAndDoubleExecution() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testRollbackAndDoubleExecution").build();
    final String nsKey = "foo";
    final String nsValue = "bar";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    createNamespaceForTesting(nsd);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Modify
    nsd.setConfiguration(nsKey, nsValue);

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyNamespaceProcedure(procExec.getEnvironment(), nsd));

    int lastStep = 2; // failing before MODIFY_NAMESPACE_UPDATE_NS_TABLE
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep);

    // Validate
    NamespaceDescriptor currentNsDescriptor =
        UTIL.getAdmin().getNamespaceDescriptor(nsd.getName());
    assertNull(currentNsDescriptor.getConfigurationValue(nsKey));
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  private void createNamespaceForTesting(NamespaceDescriptor nsDescriptor) throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsDescriptor));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
  }
}
