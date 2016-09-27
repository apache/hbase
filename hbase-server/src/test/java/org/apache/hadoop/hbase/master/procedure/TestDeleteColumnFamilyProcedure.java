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

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DeleteColumnFamilyState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, MediumTests.class})
public class TestDeleteColumnFamilyProcedure extends TestTableDDLProcedureBase {
  private static final Log LOG = LogFactory.getLog(TestDeleteColumnFamilyProcedure.class);

  @Test(timeout = 60000)
  public void testDeleteColumnFamily() throws Exception {
    final TableName tableName = TableName.valueOf("testDeleteColumnFamily");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final String cf1 = "cf1";
    final String cf2 = "cf2";

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2, "f3");

    // Test 1: delete the column family that exists online
    long procId1 = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf1.getBytes()),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);

    MasterProcedureTestingUtility.validateColumnFamilyDeletion(UTIL.getHBaseCluster().getMaster(),
      tableName, cf1);

    // Test 2: delete the column family that exists offline
    UTIL.getHBaseAdmin().disableTable(tableName);
    long procId2 = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf2.getBytes()),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
  }

  @Test(timeout=60000)
  public void testDeleteColumnFamilyTwice() throws Exception {
    final TableName tableName = TableName.valueOf("testDeleteColumnFamilyTwice");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final String cf2 = "cf2";

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", cf2);

    // delete the column family that exists
    long procId1 = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf2.getBytes()),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    // First delete should succeed
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);

    MasterProcedureTestingUtility.validateColumnFamilyDeletion(UTIL.getHBaseCluster().getMaster(),
      tableName, cf2);

    // delete the column family that does not exist
    long procId2 = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf2.getBytes()),
      nonceGroup + 1,
      nonce + 1);

    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);

    // Second delete should fail with InvalidFamilyOperationException
    ProcedureInfo result = procExec.getResult(procId2);
    assertTrue(result.isFailed());
    LOG.debug("Delete online failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof InvalidFamilyOperationException);

    // Try again, this time with table disabled.
    UTIL.getHBaseAdmin().disableTable(tableName);
    long procId3 = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf2.getBytes()),
      nonceGroup + 2,
      nonce + 2);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId3);
    // Expect fail with InvalidFamilyOperationException
    result = procExec.getResult(procId2);
    assertTrue(result.isFailed());
    LOG.debug("Delete offline failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof InvalidFamilyOperationException);
  }

  @Test(timeout=60000)
  public void testDeleteColumnFamilyTwiceWithSameNonce() throws Exception {
    final TableName tableName = TableName.valueOf("testDeleteColumnFamilyTwiceWithSameNonce");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final String cf2 = "cf2";

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", cf2);

    // delete the column family that exists
    long procId1 = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf2.getBytes()),
      nonceGroup,
      nonce);
    long procId2 = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf2.getBytes()),
      nonceGroup,
      nonce);

    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    MasterProcedureTestingUtility.validateColumnFamilyDeletion(UTIL.getHBaseCluster().getMaster(),
      tableName, cf2);

    // Wait the completion and expect not fail - because it is the same proc
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    assertTrue(procId1 == procId2);
  }

  @Test(timeout=60000)
  public void testDeleteNonExistingColumnFamily() throws Exception {
    final TableName tableName = TableName.valueOf("testDeleteNonExistingColumnFamily");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    final String cf3 = "cf3";

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");

    // delete the column family that does not exist
    long procId1 = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf3.getBytes()),
      nonceGroup,
      nonce);
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);

    ProcedureInfo result = procExec.getResult(procId1);
    assertTrue(result.isFailed());
    LOG.debug("Delete failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof InvalidFamilyOperationException);
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecutionOffline() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionOffline");
    final String cf4 = "cf4";

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2", "f3", cf4);
    UTIL.getHBaseAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Delete procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf4.getBytes()),
      nonceGroup,
      nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = DeleteColumnFamilyState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps);

    MasterProcedureTestingUtility.validateColumnFamilyDeletion(UTIL.getHBaseCluster().getMaster(),
      tableName, cf4);
  }

  @Test(timeout = 60000)
  public void testRecoveryAndDoubleExecutionOnline() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecutionOnline");
    final String cf5 = "cf5";

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2", "f3", cf5);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Delete procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf5.getBytes()),
      nonceGroup,
      nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = DeleteColumnFamilyState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps);

    MasterProcedureTestingUtility.validateColumnFamilyDeletion(UTIL.getHBaseCluster().getMaster(),
      tableName, cf5);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final String cf5 = "cf5";

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "f1", "f2", "f3", cf5);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Delete procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DeleteColumnFamilyProcedure(procExec.getEnvironment(), tableName, cf5.getBytes()),
      nonceGroup,
      nonce);

    int numberOfSteps = 1; // failing at pre operation
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, numberOfSteps);

    MasterProcedureTestingUtility.validateTableCreation(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, "f1", "f2", "f3", cf5);
  }
}
