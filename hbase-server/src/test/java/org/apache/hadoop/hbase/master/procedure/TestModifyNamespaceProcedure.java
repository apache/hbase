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


import static org.junit.Assert.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyNamespaceState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestModifyNamespaceProcedure {
  private static final Log LOG = LogFactory.getLog(TestModifyNamespaceProcedure.class);

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
        UTIL.getHBaseAdmin().getNamespaceDescriptor(nsd.getName());
    assertEquals(currentNsDescriptor.getConfigurationValue(nsKey1), nsValue1before);
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
        UTIL.getHBaseAdmin().getNamespaceDescriptor(nsd.getName());
    assertEquals(nsd.getConfigurationValue(nsKey1), nsValue1after);
    assertEquals(currentNsDescriptor.getConfigurationValue(nsKey2), nsValue2);
  }

  @Test(timeout=60000)
  public void testModifyNonExistNamespace() throws Exception {
    final String namespaceName = "testModifyNonExistNamespace";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    try {
      NamespaceDescriptor nsDescriptor = UTIL.getHBaseAdmin().getNamespaceDescriptor(namespaceName);
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
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("modify namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof NamespaceNotFoundException);
  }

  @Test(timeout=60000)
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
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Modify namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test(timeout=60000)
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
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Modify namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test(timeout = 60000)
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
    int numberOfSteps = ModifyNamespaceState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps);

    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    // Validate
    NamespaceDescriptor currentNsDescriptor =
        UTIL.getHBaseAdmin().getNamespaceDescriptor(nsd.getName());
    assertEquals(currentNsDescriptor.getConfigurationValue(nsKey), nsValue);
  }

  @Test(timeout = 60000)
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

    int numberOfSteps = 0; // failing at pre operation
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, numberOfSteps);

    // Validate
    NamespaceDescriptor currentNsDescriptor =
        UTIL.getHBaseAdmin().getNamespaceDescriptor(nsd.getName());
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
