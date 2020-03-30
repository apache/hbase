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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({MasterTests.class, LargeTests.class})
public class TestRestoreSnapshotProcedure extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRestoreSnapshotProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRestoreSnapshotProcedure.class);

  protected final TableName snapshotTableName = TableName.valueOf("testRestoreSnapshot");
  protected final byte[] CF1 = Bytes.toBytes("cf1");
  protected final byte[] CF2 = Bytes.toBytes("cf2");
  protected final byte[] CF3 = Bytes.toBytes("cf3");
  protected final byte[] CF4 = Bytes.toBytes("cf4");
  protected final int rowCountCF1 = 10;
  protected final int rowCountCF2 = 40;
  protected final int rowCountCF3 = 40;
  protected final int rowCountCF4 = 40;
  protected final int rowCountCF1addition = 10;

  private SnapshotProtos.SnapshotDescription snapshot = null;
  private TableDescriptor snapshotHTD = null;

  @Rule
  public TestName name = new TestName();

  @Before
  @Override
  public void setup() throws Exception {
    super.setup();
    setupSnapshotAndUpdateTable();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  private int getNumReplicas() {
    return 1;
  }

  private void setupSnapshotAndUpdateTable() throws Exception {
    long tid = System.currentTimeMillis();
    final String snapshotName = "snapshot-" + tid;
    Admin admin = UTIL.getAdmin();
    // create Table
    SnapshotTestingUtils.createTable(UTIL, snapshotTableName, getNumReplicas(), CF1, CF2);
    // Load data
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF1, CF1);
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF2, CF2);
    SnapshotTestingUtils.verifyRowCount(UTIL, snapshotTableName, rowCountCF1 + rowCountCF2);

    snapshotHTD = admin.getDescriptor(snapshotTableName);

    admin.disableTable(snapshotTableName);
    // take a snapshot
    admin.snapshot(snapshotName, snapshotTableName);

    List<SnapshotDescription> snapshotList = admin.listSnapshots();
    snapshot = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotList.get(0));

    // modify the table
    ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor columnFamilyDescriptor3 =
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(CF3);
    ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor columnFamilyDescriptor4 =
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(CF4);
    admin.addColumnFamily(snapshotTableName, columnFamilyDescriptor3);
    admin.addColumnFamily(snapshotTableName, columnFamilyDescriptor4);
    admin.deleteColumnFamily(snapshotTableName, CF2);
    // enable table and insert data
    admin.enableTable(snapshotTableName);
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF3, CF3);
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF4, CF4);
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF1addition, CF1);
    HTableDescriptor currentHTD = new HTableDescriptor(admin.getDescriptor(snapshotTableName));
    assertTrue(currentHTD.hasFamily(CF1));
    assertFalse(currentHTD.hasFamily(CF2));
    assertTrue(currentHTD.hasFamily(CF3));
    assertTrue(currentHTD.hasFamily(CF4));
    assertNotEquals(currentHTD.getFamiliesKeys().size(), snapshotHTD.getColumnFamilies().length);
    SnapshotTestingUtils.verifyRowCount(
      UTIL, snapshotTableName, rowCountCF1 + rowCountCF3 + rowCountCF4 + rowCountCF1addition);
    admin.disableTable(snapshotTableName);
  }

  private static TableDescriptor createHTableDescriptor(
      final TableName tableName, final byte[] ... family) {
    TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(tableName);

    for (int i = 0; i < family.length; ++i) {
      tableDescriptor.setColumnFamily(
        new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(family[i]));
    }
    return tableDescriptor;
  }

  @Test
  public void testRestoreSnapshot() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId = ProcedureTestingUtility.submitAndWait(
      procExec,
      new RestoreSnapshotProcedure(procExec.getEnvironment(), snapshotHTD, snapshot));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    validateSnapshotRestore();
  }

  @Test
  public void testRestoreSnapshotToDifferentTable() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName restoredTableName = TableName.valueOf(name.getMethodName());
    final TableDescriptor tableDescriptor = createHTableDescriptor(restoredTableName, CF1, CF2);

    long procId = ProcedureTestingUtility.submitAndWait(
      procExec, new RestoreSnapshotProcedure(procExec.getEnvironment(), tableDescriptor,
        snapshot));
    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Restore snapshot failed with exception: " + result.getException());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotFoundException);
  }

  @Test
  public void testRestoreSnapshotToEnabledTable() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    try {
      UTIL.getAdmin().enableTable(snapshotTableName);

      long procId = ProcedureTestingUtility.submitAndWait(
        procExec,
        new RestoreSnapshotProcedure(procExec.getEnvironment(), snapshotHTD, snapshot));
      Procedure<?> result = procExec.getResult(procId);
      assertTrue(result.isFailed());
      LOG.debug("Restore snapshot failed with exception: " + result.getException());
      assertTrue(
        ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotDisabledException);
    } finally {
      UTIL.getAdmin().disableTable(snapshotTableName);
    }
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Restore snapshot procedure && kill the executor
    long procId = procExec.submitProcedure(
      new RestoreSnapshotProcedure(procExec.getEnvironment(), snapshotHTD, snapshot));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    resetProcExecutorTestingKillFlag();
    validateSnapshotRestore();
  }

  private void validateSnapshotRestore() throws IOException {
    try {
      UTIL.getAdmin().enableTable(snapshotTableName);

      HTableDescriptor currentHTD =
        new HTableDescriptor(UTIL.getAdmin().getDescriptor(snapshotTableName));
      assertTrue(currentHTD.hasFamily(CF1));
      assertTrue(currentHTD.hasFamily(CF2));
      assertFalse(currentHTD.hasFamily(CF3));
      assertFalse(currentHTD.hasFamily(CF4));
      assertEquals(currentHTD.getFamiliesKeys().size(), snapshotHTD.getColumnFamilies().length);
      SnapshotTestingUtils.verifyRowCount(UTIL, snapshotTableName, rowCountCF1 + rowCountCF2);
    } finally {
      UTIL.getAdmin().disableTable(snapshotTableName);
    }
  }
}
