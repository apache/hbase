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
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.NamespaceNotFoundException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CreateNamespaceState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCreateNamespaceProcedure {
  private static final Log LOG = LogFactory.getLog(TestCreateNamespaceProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static long nonceGroup = HConstants.NO_NONCE;
  private static long nonce = HConstants.NO_NONCE;

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
    nonceGroup =
        MasterProcedureTestingUtility.generateNonceGroup(UTIL.getHBaseCluster().getMaster());
    nonce = MasterProcedureTestingUtility.generateNonce(UTIL.getHBaseCluster().getMaster());
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
  }

  @Test(timeout = 60000)
  public void testCreateNamespace() throws Exception {
    final NamespaceDescriptor nsd = NamespaceDescriptor.create("testCreateNamespace").build();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    validateNamespaceCreated(nsd);
  }

  @Test(timeout=60000)
  public void testCreateSameNamespaceTwice() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testCreateSameNamespaceTwice").build();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId1 = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);

    // Create the namespace that exists
    long procId2 = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup + 1,
      nonce + 1);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);

    // Second create should fail with NamespaceExistException
    ProcedureInfo result = procExec.getResult(procId2);
    assertTrue(result.isFailed());
    LOG.debug("Create namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof NamespaceExistException);
  }

  @Test(timeout=60000)
  public void testCreateSystemNamespace() throws Exception {
    final NamespaceDescriptor nsd =
        UTIL.getHBaseAdmin().getNamespaceDescriptor(NamespaceDescriptor.SYSTEM_NAMESPACE.getName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Create namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof NamespaceExistException);
  }

  @Test(timeout=60000)
  public void testCreateNamespaceWithInvalidRegionCount() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testCreateNamespaceWithInvalidRegionCount").build();
    final String nsKey = "hbase.namespace.quota.maxregions";
    final String nsValue = "-1";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    nsd.setConfiguration(nsKey, nsValue);

    long procId = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Create namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test(timeout=60000)
  public void testCreateNamespaceWithInvalidTableCount() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testCreateNamespaceWithInvalidTableCount").build();
    final String nsKey = "hbase.namespace.quota.maxtables";
    final String nsValue = "-1";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    nsd.setConfiguration(nsKey, nsValue);

    long procId = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Create namespace failed with exception: " + result.getExceptionFullMessage());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof ConstraintException);
  }

  @Test(timeout=60000)
  public void testCreateSameNamespaceTwiceWithSameNonce() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testCreateSameNamespaceTwiceWithSameNonce").build();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId1 = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);
    long procId2 = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);

    validateNamespaceCreated(nsd);

    // Wait the completion and expect not fail - because it is the same proc
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    assertTrue(procId1 == procId2);
  }

  @Test(timeout = 60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testRecoveryAndDoubleExecution").build();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the CreateNamespace procedure && kill the executor
    long procId = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = CreateNamespaceState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      CreateNamespaceState.values());

    // Validate the creation of namespace
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    validateNamespaceCreated(nsd);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final NamespaceDescriptor nsd =
        NamespaceDescriptor.create("testRollbackAndDoubleExecution").build();
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the CreateNamespace procedure && kill the executor
    long procId = procExec.submitProcedure(
      new CreateNamespaceProcedure(procExec.getEnvironment(), nsd),
      nonceGroup,
      nonce);

    int numberOfSteps = CreateNamespaceState.values().length - 2; // failing in the middle of proc
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      CreateNamespaceState.values());

    // Validate the non-existence of namespace
    try {
      NamespaceDescriptor nsDescriptor = UTIL.getHBaseAdmin().getNamespaceDescriptor(nsd.getName());
      assertNull(nsDescriptor);
    } catch (NamespaceNotFoundException nsnfe) {
      // Expected
      LOG.info("The namespace " + nsd.getName() + " is not created.");
    }
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  private void validateNamespaceCreated(NamespaceDescriptor nsd) throws IOException {
    NamespaceDescriptor createdNsDescriptor =
        UTIL.getHBaseAdmin().getNamespaceDescriptor(nsd.getName());
    assertNotNull(createdNsDescriptor);
  }
}
