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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.PerClientRandomNonceGenerator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility.StepHook;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.NonceKey;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MasterTests.class, MediumTests.class})
public class TestModifyTableProcedure extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestModifyTableProcedure.class);

  @Rule public TestName name = new TestName();

  @Test
  public void testModifyTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf");
    UTIL.getAdmin().disableTable(tableName);

    // Modify the table descriptor
    HTableDescriptor htd = new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));

    // Test 1: Modify 1 property
    long newMaxFileSize = htd.getMaxFileSize() * 2;
    htd.setMaxFileSize(newMaxFileSize);
    htd.setRegionReplication(3);

    long procId1 = ProcedureTestingUtility.submitAndWait(
        procExec, new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId1));

    HTableDescriptor currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(newMaxFileSize, currentHtd.getMaxFileSize());

    // Test 2: Modify multiple properties
    boolean newReadOnlyOption = htd.isReadOnly() ? false : true;
    long newMemStoreFlushSize = htd.getMemStoreFlushSize() * 2;
    htd.setReadOnly(newReadOnlyOption);
    htd.setMemStoreFlushSize(newMemStoreFlushSize);

    long procId2 = ProcedureTestingUtility.submitAndWait(
        procExec, new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(newReadOnlyOption, currentHtd.isReadOnly());
    assertEquals(newMemStoreFlushSize, currentHtd.getMemStoreFlushSize());
  }

  @Test
  public void testModifyTableAddCF() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf1");
    HTableDescriptor currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(1, currentHtd.getFamiliesKeys().size());

    // Test 1: Modify the table descriptor online
    String cf2 = "cf2";
    HTableDescriptor htd = new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    htd.addFamily(new HColumnDescriptor(cf2));

    long procId = ProcedureTestingUtility.submitAndWait(
        procExec, new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(2, currentHtd.getFamiliesKeys().size());
    assertTrue(currentHtd.hasFamily(Bytes.toBytes(cf2)));

    // Test 2: Modify the table descriptor offline
    UTIL.getAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    String cf3 = "cf3";
    HTableDescriptor htd2 =
        new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    htd2.addFamily(new HColumnDescriptor(cf3));

    long procId2 =
        ProcedureTestingUtility.submitAndWait(procExec,
          new ModifyTableProcedure(procExec.getEnvironment(), htd2));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertTrue(currentHtd.hasFamily(Bytes.toBytes(cf3)));
    assertEquals(3, currentHtd.getFamiliesKeys().size());
  }

  @Test
  public void testModifyTableDeleteCF() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2, cf3);
    HTableDescriptor currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(3, currentHtd.getFamiliesKeys().size());

    // Test 1: Modify the table descriptor
    HTableDescriptor htd = new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    htd.removeFamily(Bytes.toBytes(cf2));

    long procId = ProcedureTestingUtility.submitAndWait(
        procExec, new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(2, currentHtd.getFamiliesKeys().size());
    assertFalse(currentHtd.hasFamily(Bytes.toBytes(cf2)));

    // Test 2: Modify the table descriptor offline
    UTIL.getAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);

    HTableDescriptor htd2 =
        new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    htd2.removeFamily(Bytes.toBytes(cf3));
    // Disable Sanity check
    htd2.setConfiguration("hbase.table.sanity.checks", Boolean.FALSE.toString());

    long procId2 =
        ProcedureTestingUtility.submitAndWait(procExec,
          new ModifyTableProcedure(procExec.getEnvironment(), htd2));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(1, currentHtd.getFamiliesKeys().size());
    assertFalse(currentHtd.hasFamily(Bytes.toBytes(cf3)));

    //Removing the last family will fail
    HTableDescriptor htd3 =
        new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    htd3.removeFamily(Bytes.toBytes(cf1));
    long procId3 =
        ProcedureTestingUtility.submitAndWait(procExec,
            new ModifyTableProcedure(procExec.getEnvironment(), htd3));
    final Procedure<?> result = procExec.getResult(procId3);
    assertEquals(true, result.isFailed());
    Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
    assertTrue("expected DoNotRetryIOException, got " + cause,
        cause instanceof DoNotRetryIOException);
    assertEquals(1, currentHtd.getFamiliesKeys().size());
    assertTrue(currentHtd.hasFamily(Bytes.toBytes(cf1)));
  }

  @Test
  public void testRecoveryAndDoubleExecutionOffline() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "cf1", cf3);
    UTIL.getAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Modify multiple properties of the table.
    TableDescriptor oldDescriptor = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor newDescriptor = TableDescriptorBuilder.newBuilder(oldDescriptor)
        .setCompactionEnabled(!oldDescriptor.isCompactionEnabled())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf2))
        .removeColumnFamily(Bytes.toBytes(cf3))
        .setRegionReplication(3)
        .build();

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), newDescriptor));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    // Validate descriptor
    TableDescriptor currentDescriptor = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(newDescriptor.isCompactionEnabled(), currentDescriptor.isCompactionEnabled());
    assertEquals(2, newDescriptor.getColumnFamilyNames().size());

    // cf2 should be added cf3 should be removed
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, false, "cf1", cf2);
  }

  @Test
  public void testRecoveryAndDoubleExecutionOnline() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "cf1", cf3);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Modify multiple properties of the table.
    HTableDescriptor htd = new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    boolean newCompactionEnableOption = htd.isCompactionEnabled() ? false : true;
    htd.setCompactionEnabled(newCompactionEnableOption);
    htd.addFamily(new HColumnDescriptor(cf2));
    htd.removeFamily(Bytes.toBytes(cf3));

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    // Validate descriptor
    HTableDescriptor currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(newCompactionEnableOption, currentHtd.isCompactionEnabled());
    assertEquals(2, currentHtd.getFamiliesKeys().size());
    assertTrue(currentHtd.hasFamily(Bytes.toBytes(cf2)));
    assertFalse(currentHtd.hasFamily(Bytes.toBytes(cf3)));

    // cf2 should be added cf3 should be removed
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "cf1", cf2);
  }

  @Test
  public void testColumnFamilyAdditionTwiceWithNonce() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    RegionInfo[] regions =
        MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf1", cf3);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
    // Modify multiple properties of the table.
    final HTableDescriptor htd =
        new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    boolean newCompactionEnableOption = htd.isCompactionEnabled() ? false : true;
    htd.setCompactionEnabled(newCompactionEnableOption);
    htd.addFamily(new HColumnDescriptor(cf2));

    PerClientRandomNonceGenerator nonceGenerator = PerClientRandomNonceGenerator.get();
    long nonceGroup = nonceGenerator.getNonceGroup();
    long newNonce = nonceGenerator.newNonce();
    NonceKey nonceKey = new NonceKey(nonceGroup, newNonce);
    procExec.registerNonce(nonceKey);

    // Start the Modify procedure && kill the executor
    final long procId = procExec
        .submitProcedure(new ModifyTableProcedure(procExec.getEnvironment(), htd), nonceKey);

    // Restart the executor after MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR and try to add column family
    // as nonce are there , we should not fail
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, new StepHook() {
      @Override
      public boolean execute(int step) throws IOException {
        if (step == 3) {
          return procId == UTIL.getHBaseCluster().getMaster().addColumn(tableName,
            ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf2)).build(), nonceGroup,
            newNonce);
        }
        return true;
      }
    });

    //Try with different nonce, now it should fail the checks
    try {
      UTIL.getHBaseCluster().getMaster().addColumn(tableName,
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf2)).build(), nonceGroup,
        nonceGenerator.newNonce());
      Assert.fail();
    } catch (InvalidFamilyOperationException e) {
    }

    // Validate descriptor
    HTableDescriptor currentHtd = UTIL.getAdmin().getTableDescriptor(tableName);
    assertEquals(newCompactionEnableOption, currentHtd.isCompactionEnabled());
    assertEquals(3, currentHtd.getFamiliesKeys().size());
    assertTrue(currentHtd.hasFamily(Bytes.toBytes(cf2)));
    assertTrue(currentHtd.hasFamily(Bytes.toBytes(cf3)));

    // cf2 should be added
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "cf1", cf2, cf3);
  }

  @Test
  public void testRollbackAndDoubleExecutionOnline() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String familyName = "cf2";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "cf1");

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    HTableDescriptor htd = new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    boolean newCompactionEnableOption = htd.isCompactionEnabled() ? false : true;
    htd.setCompactionEnabled(newCompactionEnableOption);
    htd.addFamily(new HColumnDescriptor(familyName));

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd));

    int numberOfSteps = 0; // failing at pre operation
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, numberOfSteps);

    // cf2 should not be present
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "cf1");
  }

  @Test
  public void testRollbackAndDoubleExecutionOffline() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String familyName = "cf2";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "cf1");
    UTIL.getAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    HTableDescriptor htd = new HTableDescriptor(UTIL.getAdmin().getTableDescriptor(tableName));
    boolean newCompactionEnableOption = htd.isCompactionEnabled() ? false : true;
    htd.setCompactionEnabled(newCompactionEnableOption);
    htd.addFamily(new HColumnDescriptor(familyName));
    htd.setRegionReplication(3);

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), htd));

    // Restart the executor and rollback the step twice
    int numberOfSteps = 0; // failing at pre operation
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, numberOfSteps);

    // cf2 should not be present
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "cf1");
  }
}
