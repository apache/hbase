/*
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
import org.apache.hadoop.hbase.ConcurrentTableModificationException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CoprocessorDescriptorBuilder;
import org.apache.hadoop.hbase.client.PerClientRandomNonceGenerator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.constraint.ConstraintProcessor;
import org.apache.hadoop.hbase.coprocessor.SimpleRegionObserver;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureTestingUtility.StepHook;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.NonceKey;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MasterTests.class, LargeTests.class })
public class TestModifyTableProcedure extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestModifyTableProcedure.class);

  @Rule
  public TestName name = new TestName();

  private static final String column_Family1 = "cf1";
  private static final String column_Family2 = "cf2";
  private static final String column_Family3 = "cf3";

  @Test
  public void testModifyTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf");
    UTIL.getAdmin().disableTable(tableName);

    // Modify the table descriptor
    TableDescriptor htd = UTIL.getAdmin().getDescriptor(tableName);

    // Test 1: Modify 1 property
    long newMaxFileSize = htd.getMaxFileSize() * 2;
    htd = TableDescriptorBuilder.newBuilder(htd).setMaxFileSize(newMaxFileSize)
      .setRegionReplication(3).build();

    long procId1 = ProcedureTestingUtility.submitAndWait(procExec,
      new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId1));

    TableDescriptor currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(newMaxFileSize, currentHtd.getMaxFileSize());

    // Test 2: Modify multiple properties
    boolean newReadOnlyOption = htd.isReadOnly() ? false : true;
    long newMemStoreFlushSize = htd.getMemStoreFlushSize() * 2;
    htd = TableDescriptorBuilder.newBuilder(htd).setReadOnly(newReadOnlyOption)
      .setMemStoreFlushSize(newMemStoreFlushSize).build();

    long procId2 = ProcedureTestingUtility.submitAndWait(procExec,
      new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(newReadOnlyOption, currentHtd.isReadOnly());
    assertEquals(newMemStoreFlushSize, currentHtd.getMemStoreFlushSize());
  }

  @Test
  public void testModifyTableAddCF() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf1");
    TableDescriptor currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(1, currentHtd.getColumnFamilyNames().size());

    // Test 1: Modify the table descriptor online
    String cf2 = "cf2";
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(UTIL.getAdmin().getDescriptor(tableName));
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf2)).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new ModifyTableProcedure(procExec.getEnvironment(), tableDescriptorBuilder.build()));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(2, currentHtd.getColumnFamilyNames().size());
    assertTrue(currentHtd.hasColumnFamily(Bytes.toBytes(cf2)));

    // Test 2: Modify the table descriptor offline
    UTIL.getAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    String cf3 = "cf3";
    tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(UTIL.getAdmin().getDescriptor(tableName));
    columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf3)).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);

    long procId2 = ProcedureTestingUtility.submitAndWait(procExec,
      new ModifyTableProcedure(procExec.getEnvironment(), tableDescriptorBuilder.build()));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertTrue(currentHtd.hasColumnFamily(Bytes.toBytes(cf3)));
    assertEquals(3, currentHtd.getColumnFamilyNames().size());
  }

  @Test
  public void testModifyTableDeleteCF() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2, cf3);
    TableDescriptor currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(3, currentHtd.getColumnFamilyNames().size());

    // Test 1: Modify the table descriptor
    TableDescriptor htd = UTIL.getAdmin().getDescriptor(tableName);
    htd = TableDescriptorBuilder.newBuilder(htd).removeColumnFamily(Bytes.toBytes(cf2)).build();

    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new ModifyTableProcedure(procExec.getEnvironment(), htd));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(2, currentHtd.getColumnFamilyNames().size());
    assertFalse(currentHtd.hasColumnFamily(Bytes.toBytes(cf2)));

    // Test 2: Modify the table descriptor offline
    UTIL.getAdmin().disableTable(tableName);
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);

    TableDescriptor htd2 = UTIL.getAdmin().getDescriptor(tableName);
    // Disable Sanity check
    htd2 = TableDescriptorBuilder.newBuilder(htd2).removeColumnFamily(Bytes.toBytes(cf3))
      .setValue(TableDescriptorChecker.TABLE_SANITY_CHECKS, Boolean.FALSE.toString()).build();

    long procId2 = ProcedureTestingUtility.submitAndWait(procExec,
      new ModifyTableProcedure(procExec.getEnvironment(), htd2));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(1, currentHtd.getColumnFamilyNames().size());
    assertFalse(currentHtd.hasColumnFamily(Bytes.toBytes(cf3)));

    // Removing the last family will fail
    TableDescriptor htd3 = UTIL.getAdmin().getDescriptor(tableName);
    htd3 = TableDescriptorBuilder.newBuilder(htd3).removeColumnFamily(Bytes.toBytes(cf1)).build();
    long procId3 = ProcedureTestingUtility.submitAndWait(procExec,
      new ModifyTableProcedure(procExec.getEnvironment(), htd3));
    final Procedure<?> result = procExec.getResult(procId3);
    assertEquals(true, result.isFailed());
    Throwable cause = ProcedureTestingUtility.getExceptionCause(result);
    assertTrue("expected DoNotRetryIOException, got " + cause,
      cause instanceof DoNotRetryIOException);
    assertEquals(1, currentHtd.getColumnFamilyNames().size());
    assertTrue(currentHtd.hasColumnFamily(Bytes.toBytes(cf1)));
  }

  @Test
  public void testRecoveryAndDoubleExecutionOffline() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String cf2 = "cf2";
    final String cf3 = "cf3";
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // create the table
    RegionInfo[] regions =
      MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf1", cf3);
    UTIL.getAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Modify multiple properties of the table.
    TableDescriptor oldDescriptor = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor newDescriptor = TableDescriptorBuilder.newBuilder(oldDescriptor)
      .setCompactionEnabled(!oldDescriptor.isCompactionEnabled())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf2)).removeColumnFamily(Bytes.toBytes(cf3))
      .setRegionReplication(3).build();

    // Start the Modify procedure && kill the executor
    long procId =
      procExec.submitProcedure(new ModifyTableProcedure(procExec.getEnvironment(), newDescriptor));

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
    RegionInfo[] regions =
      MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf1", cf3);

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Modify multiple properties of the table.
    TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(UTIL.getAdmin().getDescriptor(tableName));
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf2)).build();
    boolean newCompactionEnableOption = !tableDescriptorBuilder.build().isCompactionEnabled();
    tableDescriptorBuilder.setCompactionEnabled(newCompactionEnableOption);
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    tableDescriptorBuilder.removeColumnFamily(Bytes.toBytes(cf3));

    // Start the Modify procedure && kill the executor
    long procId = procExec.submitProcedure(
      new ModifyTableProcedure(procExec.getEnvironment(), tableDescriptorBuilder.build()));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    // Validate descriptor
    TableDescriptor currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(newCompactionEnableOption, currentHtd.isCompactionEnabled());
    assertEquals(2, currentHtd.getColumnFamilyNames().size());
    assertTrue(currentHtd.hasColumnFamily(Bytes.toBytes(cf2)));
    assertFalse(currentHtd.hasColumnFamily(Bytes.toBytes(cf3)));

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
    TableDescriptor td = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(td).setCompactionEnabled(!td.isCompactionEnabled())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf2)).build();

    PerClientRandomNonceGenerator nonceGenerator = PerClientRandomNonceGenerator.get();
    long nonceGroup = nonceGenerator.getNonceGroup();
    long newNonce = nonceGenerator.newNonce();
    NonceKey nonceKey = new NonceKey(nonceGroup, newNonce);
    procExec.registerNonce(nonceKey);

    // Start the Modify procedure && kill the executor
    final long procId = procExec
      .submitProcedure(new ModifyTableProcedure(procExec.getEnvironment(), newTd), nonceKey);

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

    // Try with different nonce, now it should fail the checks
    try {
      UTIL.getHBaseCluster().getMaster().addColumn(tableName,
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf2)).build(), nonceGroup,
        nonceGenerator.newNonce());
      Assert.fail();
    } catch (InvalidFamilyOperationException e) {
    }

    // Validate descriptor
    TableDescriptor currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(!td.isCompactionEnabled(), currentHtd.isCompactionEnabled());
    assertEquals(3, currentHtd.getColumnFamilyCount());
    assertTrue(currentHtd.hasColumnFamily(Bytes.toBytes(cf2)));
    assertTrue(currentHtd.hasColumnFamily(Bytes.toBytes(cf3)));

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
    RegionInfo[] regions =
      MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf1");

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    TableDescriptor td = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(td).setCompactionEnabled(!td.isCompactionEnabled())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName)).build();

    // Start the Modify procedure && kill the executor
    long procId =
      procExec.submitProcedure(new ModifyTableProcedure(procExec.getEnvironment(), newTd));

    int lastStep = 8; // failing before MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep);

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
    RegionInfo[] regions =
      MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf1");
    UTIL.getAdmin().disableTable(tableName);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    TableDescriptor td = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(td).setCompactionEnabled(!td.isCompactionEnabled())
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(familyName)).setRegionReplication(3)
        .build();

    // Start the Modify procedure && kill the executor
    long procId =
      procExec.submitProcedure(new ModifyTableProcedure(procExec.getEnvironment(), newTd));

    // Restart the executor and rollback the step twice
    int lastStep = 8; // failing before MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep);

    // cf2 should not be present
    MasterProcedureTestingUtility.validateTableCreation(UTIL.getHBaseCluster().getMaster(),
      tableName, regions, "cf1");
  }

  @Test
  public void testConcurrentAddColumnFamily() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, column_Family1);

    class ConcurrentAddColumnFamily extends Thread {
      TableName tableName = null;
      ColumnFamilyDescriptor columnFamilyDescriptor;
      boolean exception;

      public ConcurrentAddColumnFamily(TableName tableName,
        ColumnFamilyDescriptor columnFamilyDescriptor) {
        this.tableName = tableName;
        this.columnFamilyDescriptor = columnFamilyDescriptor;
        this.exception = false;
      }

      public void run() {
        try {
          UTIL.getAdmin().addColumnFamily(tableName, columnFamilyDescriptor);
        } catch (Exception e) {
          if (e.getClass().equals(ConcurrentTableModificationException.class)) {
            this.exception = true;
          }
        }
      }
    }
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column_Family2)).build();
    ConcurrentAddColumnFamily t1 = new ConcurrentAddColumnFamily(tableName, columnFamilyDescriptor);
    columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column_Family3)).build();
    ConcurrentAddColumnFamily t2 = new ConcurrentAddColumnFamily(tableName, columnFamilyDescriptor);

    t1.start();
    t2.start();

    t1.join();
    t2.join();
    int noOfColumnFamilies = UTIL.getAdmin().getDescriptor(tableName).getColumnFamilies().length;
    assertTrue("Expected ConcurrentTableModificationException.",
      ((t1.exception || t2.exception) && noOfColumnFamilies == 2) || noOfColumnFamilies == 3);
  }

  @Test
  public void testConcurrentDeleteColumnFamily() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
    ColumnFamilyDescriptor columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column_Family1)).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column_Family2)).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    columnFamilyDescriptor =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column_Family3)).build();
    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
    UTIL.getAdmin().createTable(tableDescriptorBuilder.build());

    class ConcurrentCreateDeleteTable extends Thread {
      TableName tableName = null;
      String columnFamily = null;
      boolean exception;

      public ConcurrentCreateDeleteTable(TableName tableName, String columnFamily) {
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.exception = false;
      }

      public void run() {
        try {
          UTIL.getAdmin().deleteColumnFamily(tableName, columnFamily.getBytes());
        } catch (Exception e) {
          if (e.getClass().equals(ConcurrentTableModificationException.class)) {
            this.exception = true;
          }
        }
      }
    }
    ConcurrentCreateDeleteTable t1 = new ConcurrentCreateDeleteTable(tableName, column_Family2);
    ConcurrentCreateDeleteTable t2 = new ConcurrentCreateDeleteTable(tableName, column_Family3);

    t1.start();
    t2.start();

    t1.join();
    t2.join();
    int noOfColumnFamilies = UTIL.getAdmin().getDescriptor(tableName).getColumnFamilies().length;
    assertTrue("Expected ConcurrentTableModificationException.",
      ((t1.exception || t2.exception) && noOfColumnFamilies == 2) || noOfColumnFamilies == 1);
  }

  @Test
  public void testConcurrentModifyColumnFamily() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, column_Family1);

    class ConcurrentModifyColumnFamily extends Thread {
      TableName tableName = null;
      ColumnFamilyDescriptor hcd = null;
      boolean exception;

      public ConcurrentModifyColumnFamily(TableName tableName, ColumnFamilyDescriptor hcd) {
        this.tableName = tableName;
        this.hcd = hcd;
        this.exception = false;
      }

      public void run() {
        try {
          UTIL.getAdmin().modifyColumnFamily(tableName, hcd);
        } catch (Exception e) {
          if (e.getClass().equals(ConcurrentTableModificationException.class)) {
            this.exception = true;
          }
        }
      }
    }
    ColumnFamilyDescriptor modColumnFamily1 =
      ColumnFamilyDescriptorBuilder.newBuilder(column_Family1.getBytes()).setMaxVersions(5).build();
    ColumnFamilyDescriptor modColumnFamily2 =
      ColumnFamilyDescriptorBuilder.newBuilder(column_Family1.getBytes()).setMaxVersions(6).build();

    ConcurrentModifyColumnFamily t1 = new ConcurrentModifyColumnFamily(tableName, modColumnFamily1);
    ConcurrentModifyColumnFamily t2 = new ConcurrentModifyColumnFamily(tableName, modColumnFamily2);

    t1.start();
    t2.start();

    t1.join();
    t2.join();

    int maxVersions = UTIL.getAdmin().getDescriptor(tableName)
      .getColumnFamily(column_Family1.getBytes()).getMaxVersions();
    assertTrue("Expected ConcurrentTableModificationException.", (t1.exception && maxVersions == 5)
      || (t2.exception && maxVersions == 6) || !(t1.exception && t2.exception));
  }

  @Test
  public void testConcurrentModifyTable() throws IOException, InterruptedException {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    UTIL.createTable(tableName, column_Family1);

    class ConcurrentModifyTable extends Thread {
      TableName tableName = null;
      TableDescriptor htd = null;
      boolean exception;

      public ConcurrentModifyTable(TableName tableName, TableDescriptor htd) {
        this.tableName = tableName;
        this.htd = htd;
        this.exception = false;
      }

      public void run() {
        try {
          UTIL.getAdmin().modifyTable(htd);
        } catch (Exception e) {
          if (e.getClass().equals(ConcurrentTableModificationException.class)) {
            this.exception = true;
          }
        }
      }
    }
    TableDescriptor htd = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor modifiedDescriptor =
      TableDescriptorBuilder.newBuilder(htd).setCompactionEnabled(false).build();

    ConcurrentModifyTable t1 = new ConcurrentModifyTable(tableName, modifiedDescriptor);
    ConcurrentModifyTable t2 = new ConcurrentModifyTable(tableName, modifiedDescriptor);

    t1.start();
    t2.start();

    t1.join();
    t2.join();
    assertFalse("Expected ConcurrentTableModificationException.", (t1.exception || t2.exception));
  }

  @Test
  public void testModifyWillNotReopenRegions() throws IOException {
    final boolean reopenRegions = false;
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf");

    // Test 1: Modify table without reopening any regions
    TableDescriptor htd = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor modifiedDescriptor = TableDescriptorBuilder.newBuilder(htd)
      .setValue("test" + ".hbase.conf", "test.hbase.conf.value").build();
    long procId1 = ProcedureTestingUtility.submitAndWait(procExec, new ModifyTableProcedure(
      procExec.getEnvironment(), modifiedDescriptor, null, htd, false, reopenRegions));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId1));
    TableDescriptor currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals("test.hbase.conf.value", currentHtd.getValue("test.hbase.conf"));
    // Regions should not aware of any changes.
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      Assert.assertNull(r.getTableDescriptor().getValue("test.hbase.conf"));
    }
    // Force regions to reopen
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      getMaster().getAssignmentManager().move(r.getRegionInfo());
    }
    // After the regions reopen, ensure that the configuration is updated.
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      assertEquals("test.hbase.conf.value", r.getTableDescriptor().getValue("test.hbase.conf"));
    }

    // Test 2: Modifying region replication is not allowed
    htd = UTIL.getAdmin().getDescriptor(tableName);
    long oldRegionReplication = htd.getRegionReplication();
    modifiedDescriptor = TableDescriptorBuilder.newBuilder(htd).setRegionReplication(3).build();
    try {
      ProcedureTestingUtility.submitAndWait(procExec, new ModifyTableProcedure(
        procExec.getEnvironment(), modifiedDescriptor, null, htd, false, reopenRegions));
      Assert.fail(
        "An exception should have been thrown while modifying region replication properties.");
    } catch (HBaseIOException e) {
      assertTrue(e.getMessage().contains("Can not modify"));
    }
    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    // Nothing changed
    assertEquals(oldRegionReplication, currentHtd.getRegionReplication());

    // Test 3: Adding CFs is not allowed
    htd = UTIL.getAdmin().getDescriptor(tableName);
    modifiedDescriptor = TableDescriptorBuilder.newBuilder(htd)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("NewCF".getBytes()).build())
      .build();
    try {
      ProcedureTestingUtility.submitAndWait(procExec, new ModifyTableProcedure(
        procExec.getEnvironment(), modifiedDescriptor, null, htd, false, reopenRegions));
      Assert.fail("Should have thrown an exception while modifying CF!");
    } catch (HBaseIOException e) {
      assertTrue(e.getMessage().contains("Cannot add or remove column families"));
    }
    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    Assert.assertNull(currentHtd.getColumnFamily("NewCF".getBytes()));

    // Test 4: Modifying CF property is allowed
    htd = UTIL.getAdmin().getDescriptor(tableName);
    modifiedDescriptor =
      TableDescriptorBuilder
        .newBuilder(htd).modifyColumnFamily(ColumnFamilyDescriptorBuilder
          .newBuilder("cf".getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build())
        .build();
    ProcedureTestingUtility.submitAndWait(procExec, new ModifyTableProcedure(
      procExec.getEnvironment(), modifiedDescriptor, null, htd, false, reopenRegions));
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      Assert.assertEquals(Compression.Algorithm.NONE,
        r.getTableDescriptor().getColumnFamily("cf".getBytes()).getCompressionType());
    }
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      getMaster().getAssignmentManager().move(r.getRegionInfo());
    }
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      Assert.assertEquals(Compression.Algorithm.SNAPPY,
        r.getTableDescriptor().getColumnFamily("cf".getBytes()).getCompressionType());
    }

    // Test 5: Modifying coprocessor is not allowed
    htd = UTIL.getAdmin().getDescriptor(tableName);
    modifiedDescriptor =
      TableDescriptorBuilder.newBuilder(htd).setCoprocessor(CoprocessorDescriptorBuilder
        .newBuilder("any.coprocessor.name").setJarPath("fake/path").build()).build();
    try {
      ProcedureTestingUtility.submitAndWait(procExec, new ModifyTableProcedure(
        procExec.getEnvironment(), modifiedDescriptor, null, htd, false, reopenRegions));
      Assert.fail("Should have thrown an exception while modifying coprocessor!");
    } catch (HBaseIOException e) {
      assertTrue(e.getMessage().contains("Can not modify Coprocessor"));
    }
    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(0, currentHtd.getCoprocessorDescriptors().size());

    // Test 6: Modifying is not allowed
    htd = UTIL.getAdmin().getDescriptor(tableName);
    modifiedDescriptor = TableDescriptorBuilder.newBuilder(htd).setRegionReplication(3).build();
    try {
      ProcedureTestingUtility.submitAndWait(procExec, new ModifyTableProcedure(
        procExec.getEnvironment(), modifiedDescriptor, null, htd, false, reopenRegions));
      Assert.fail("Should have thrown an exception while modifying coprocessor!");
    } catch (HBaseIOException e) {
      System.out.println(e.getMessage());
      assertTrue(e.getMessage().contains("Can not modify REGION_REPLICATION"));
    }
  }

  @Test
  public void testModifyTableWithCoprocessorAndColumnFamilyPropertyChange() throws IOException {
    // HBASE-29706 - This test validates the fix for the bug where modifying only column family
    // properties
    // (like COMPRESSION) with REOPEN_REGIONS=false would incorrectly throw an error when
    // coprocessors are present. The bug was caused by comparing collection hash codes
    // instead of actual descriptor content, which failed when HashMap iteration order varied.

    final boolean reopenRegions = false;
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "cf");

    // Step 1: Add coprocessors to the table
    TableDescriptor htd = UTIL.getAdmin().getDescriptor(tableName);
    final String cp2 = ConstraintProcessor.class.getName();
    TableDescriptor descriptorWithCoprocessor = TableDescriptorBuilder.newBuilder(htd)
      .setCoprocessor(CoprocessorDescriptorBuilder.newBuilder(SimpleRegionObserver.class.getName())
        .setPriority(100).build())
      .setCoprocessor(CoprocessorDescriptorBuilder.newBuilder(cp2).setPriority(200).build())
      .build();
    long procId = ProcedureTestingUtility.submitAndWait(procExec, new ModifyTableProcedure(
      procExec.getEnvironment(), descriptorWithCoprocessor, null, htd, false, true));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    // Verify coprocessors were added
    TableDescriptor currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals(2, currentHtd.getCoprocessorDescriptors().size());
    assertTrue("First coprocessor should be present",
      currentHtd.hasCoprocessor(SimpleRegionObserver.class.getName()));
    assertTrue("Second coprocessor should be present", currentHtd.hasCoprocessor(cp2));

    // Step 2: Modify only the column family property (COMPRESSION) with REOPEN_REGIONS=false
    // This should SUCCEED because we're not actually modifying the coprocessor,
    // just the column family compression setting.
    htd = UTIL.getAdmin().getDescriptor(tableName);
    TableDescriptor modifiedDescriptor =
      TableDescriptorBuilder
        .newBuilder(htd).modifyColumnFamily(ColumnFamilyDescriptorBuilder
          .newBuilder("cf".getBytes()).setCompressionType(Compression.Algorithm.SNAPPY).build())
        .build();

    // This should NOT throw an error - the fix ensures order-independent coprocessor comparison
    long procId2 = ProcedureTestingUtility.submitAndWait(procExec, new ModifyTableProcedure(
      procExec.getEnvironment(), modifiedDescriptor, null, htd, false, reopenRegions));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));

    // Verify the modification succeeded
    currentHtd = UTIL.getAdmin().getDescriptor(tableName);
    assertEquals("Coprocessors should still be present", 2,
      currentHtd.getCoprocessorDescriptors().size());
    assertTrue("First coprocessor should still be present",
      currentHtd.hasCoprocessor(SimpleRegionObserver.class.getName()));
    assertTrue("Second coprocessor should still be present", currentHtd.hasCoprocessor(cp2));
    assertEquals("Compression should be updated in table descriptor", Compression.Algorithm.SNAPPY,
      currentHtd.getColumnFamily("cf".getBytes()).getCompressionType());

    // Verify regions haven't picked up the change yet (since reopenRegions=false)
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      assertEquals("Regions should still have old compression", Compression.Algorithm.NONE,
        r.getTableDescriptor().getColumnFamily("cf".getBytes()).getCompressionType());
    }

    // Force regions to reopen
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      getMaster().getAssignmentManager().move(r.getRegionInfo());
    }

    // After reopen, regions should have the new compression setting
    for (HRegion r : UTIL.getHBaseCluster().getRegions(tableName)) {
      assertEquals("Regions should now have new compression after reopen",
        Compression.Algorithm.SNAPPY,
        r.getTableDescriptor().getColumnFamily("cf".getBytes()).getCompressionType());
    }
  }
}
