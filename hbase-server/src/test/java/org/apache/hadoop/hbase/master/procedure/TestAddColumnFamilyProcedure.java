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
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category({MasterTests.class, MediumTests.class})
public class TestAddColumnFamilyProcedure extends TestTableDDLProcedureBase {
  private static final Log LOG = LogFactory.getLog(TestAddColumnFamilyProcedure.class);
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().withTimeout(this.getClass()).
      withLookingForStuckThread(true).build();

  @Rule public TestName name = new TestName();

  @Test(timeout = 60000)
  public void testAddColumnFamily() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    final HColumnDescriptor columnDescriptor1 = new HColumnDescriptor(cf1);
    final HColumnDescriptor columnDescriptor2 = new HColumnDescriptor(cf2);
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f3");

    // Test 1: Add a column family online
    long procId1 = procExec.submitProcedure(
      new AddColumnFamilyProcedure(procExec.getEnvironment(), tableName, columnDescriptor1));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);

    MasterProcedureTestingUtility.validateColumnFamilyAddition(getMaster(), tableName, cf1);

    // Test 2: Add a column family offline
    UTIL.getAdmin().disableTable(tableName);
    long procId2 = procExec.submitProcedure(
      new AddColumnFamilyProcedure(procExec.getEnvironment(), tableName, columnDescriptor2));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    MasterProcedureTestingUtility.validateColumnFamilyAddition(getMaster(), tableName, cf2);
  }

  @Test(timeout=60000)
  public void testAddSameColumnFamilyTwice() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf2 = "cf2";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf2);

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1");

    // add the column family
    long procId1 = procExec.submitProcedure(
      new AddColumnFamilyProcedure(procExec.getEnvironment(), tableName, columnDescriptor));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    MasterProcedureTestingUtility.validateColumnFamilyAddition(getMaster(), tableName, cf2);

    // add the column family that exists
    long procId2 = procExec.submitProcedure(
      new AddColumnFamilyProcedure(procExec.getEnvironment(), tableName, columnDescriptor));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId2);

    // Second add should fail with InvalidFamilyOperationException
    ProcedureInfo result = procExec.getResult(procId2);
    assertTrue(result.isFailed());
    LOG.debug("Add failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof InvalidFamilyOperationException);

    // Do the same add the existing column family - this time offline
    UTIL.getAdmin().disableTable(tableName);
    long procId3 = procExec.submitProcedure(
      new AddColumnFamilyProcedure(procExec.getEnvironment(), tableName, columnDescriptor));
    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId3);

    // Second add should fail with InvalidFamilyOperationException
    result = procExec.getResult(procId3);
    assertTrue(result.isFailed());
    LOG.debug("Add failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof InvalidFamilyOperationException);
  }

  @Test(timeout = 60000)
  public void testRecoveryAndDoubleExecutionOffline() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf4 = "cf4";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf4);
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    // create the table
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2", "f3");
    UTIL.getAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the AddColumnFamily procedure && kill the executor
    long procId = procExec.submitProcedure(
      new AddColumnFamilyProcedure(procExec.getEnvironment(), tableName, columnDescriptor));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    MasterProcedureTestingUtility.validateColumnFamilyAddition(getMaster(), tableName, cf4);
  }

  @Test(timeout = 60000)
  public void testRecoveryAndDoubleExecutionOnline() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf5 = "cf5";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf5);
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    // create the table
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2", "f3");

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the AddColumnFamily procedure && kill the executor
    long procId = procExec.submitProcedure(
      new AddColumnFamilyProcedure(procExec.getEnvironment(), tableName, columnDescriptor));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    MasterProcedureTestingUtility.validateColumnFamilyAddition(getMaster(), tableName, cf5);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf6 = "cf6";
    final HColumnDescriptor columnDescriptor = new HColumnDescriptor(cf6);
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f1", "f2");
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the AddColumnFamily procedure && kill the executor
    long procId = procExec.submitProcedure(
      new AddColumnFamilyProcedure(procExec.getEnvironment(), tableName, columnDescriptor));

    int numberOfSteps = 0; // failing at "pre operations"
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, numberOfSteps);

    MasterProcedureTestingUtility.validateColumnFamilyDeletion(getMaster(), tableName, cf6);
  }
}
