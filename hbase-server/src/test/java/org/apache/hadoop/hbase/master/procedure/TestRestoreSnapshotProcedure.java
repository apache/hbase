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

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RestoreSnapshotState;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestRestoreSnapshotProcedure extends TestTableDDLProcedureBase {
  private static final Log LOG = LogFactory.getLog(TestRestoreSnapshotProcedure.class);

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

  private HBaseProtos.SnapshotDescription snapshot = null;
  private HTableDescriptor snapshotHTD = null;

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
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getHBaseAdmin());
    SnapshotTestingUtils.deleteArchiveDirectory(UTIL);
  }

  private int getNumReplicas() {
    return 1;
  }

  private void setupSnapshotAndUpdateTable() throws Exception {
    long tid = System.currentTimeMillis();
    final byte[] snapshotName = Bytes.toBytes("snapshot-" + tid);
    Admin admin = UTIL.getHBaseAdmin();
    // create Table
    SnapshotTestingUtils.createTable(UTIL, snapshotTableName, getNumReplicas(), CF1, CF2);
    // Load data
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF1, CF1);
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF2, CF2);
    SnapshotTestingUtils.verifyRowCount(UTIL, snapshotTableName, rowCountCF1 + rowCountCF2);

    snapshotHTD = admin.getTableDescriptor(snapshotTableName);

    admin.disableTable(snapshotTableName);
    // take a snapshot
    admin.snapshot(snapshotName, snapshotTableName);

    List<SnapshotDescription> snapshotList = admin.listSnapshots();
    snapshot = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotList.get(0));

    // modify the table
    HColumnDescriptor columnFamilyDescriptor3 = new HColumnDescriptor(CF3);
    HColumnDescriptor columnFamilyDescriptor4 = new HColumnDescriptor(CF4);
    admin.addColumnFamily(snapshotTableName, columnFamilyDescriptor3);
    admin.addColumnFamily(snapshotTableName, columnFamilyDescriptor4);
    admin.deleteColumnFamily(snapshotTableName, CF2);
    // enable table and insert data
    admin.enableTable(snapshotTableName);
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF3, CF3);
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF4, CF4);
    SnapshotTestingUtils.loadData(UTIL, snapshotTableName, rowCountCF1addition, CF1);
    HTableDescriptor currentHTD = admin.getTableDescriptor(snapshotTableName);
    assertTrue(currentHTD.hasFamily(CF1));
    assertFalse(currentHTD.hasFamily(CF2));
    assertTrue(currentHTD.hasFamily(CF3));
    assertTrue(currentHTD.hasFamily(CF4));
    assertNotEquals(currentHTD.getFamiliesKeys().size(), snapshotHTD.getFamiliesKeys().size());
    SnapshotTestingUtils.verifyRowCount(
      UTIL, snapshotTableName, rowCountCF1 + rowCountCF3 + rowCountCF4 + rowCountCF1addition);
    admin.disableTable(snapshotTableName);
  }

  private static HTableDescriptor createHTableDescriptor(
      final TableName tableName, final byte[] ... family) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (int i = 0; i < family.length; ++i) {
      htd.addFamily(new HColumnDescriptor(family[i]));
    }
    return htd;
  }

  @Test(timeout=600000)
  public void testRestoreSnapshot() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId = ProcedureTestingUtility.submitAndWait(
      procExec,
      new RestoreSnapshotProcedure(procExec.getEnvironment(), snapshotHTD, snapshot));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));

    validateSnapshotRestore();
  }

  @Test(timeout = 60000)
  public void testRestoreSnapshotTwiceWithSameNonce() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    long procId1 = procExec.submitProcedure(
      new RestoreSnapshotProcedure(procExec.getEnvironment(), snapshotHTD, snapshot),
      nonceGroup,
      nonce);
    long procId2 = procExec.submitProcedure(
      new RestoreSnapshotProcedure(procExec.getEnvironment(), snapshotHTD, snapshot),
      nonceGroup,
      nonce);

    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    // The second proc should succeed too - because it is the same proc.
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    assertTrue(procId1 == procId2);

    validateSnapshotRestore();
  }

  @Test(timeout=60000)
  public void testRestoreSnapshotToDifferentTable() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName restoredTableName = TableName.valueOf("testRestoreSnapshotToDifferentTable");
    final HTableDescriptor newHTD = createHTableDescriptor(restoredTableName, CF1, CF2);

    long procId = ProcedureTestingUtility.submitAndWait(
      procExec, new RestoreSnapshotProcedure(procExec.getEnvironment(), newHTD, snapshot));
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Restore snapshot failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotFoundException);
  }

  @Test(timeout=60000)
  public void testRestoreSnapshotToEnabledTable() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    try {
      UTIL.getHBaseAdmin().enableTable(snapshotTableName);

      long procId = ProcedureTestingUtility.submitAndWait(
        procExec,
        new RestoreSnapshotProcedure(procExec.getEnvironment(), snapshotHTD, snapshot));
      ProcedureInfo result = procExec.getResult(procId);
      assertTrue(result.isFailed());
      LOG.debug("Restore snapshot failed with exception: " + result.getExceptionFullMessage());
      assertTrue(
        ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotDisabledException);
    } finally {
      UTIL.getHBaseAdmin().disableTable(snapshotTableName);
    }
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Restore snapshot procedure && kill the executor
    long procId = procExec.submitProcedure(
      new RestoreSnapshotProcedure(procExec.getEnvironment(), snapshotHTD, snapshot),
      nonceGroup,
      nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = RestoreSnapshotState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps);

    resetProcExecutorTestingKillFlag();
    validateSnapshotRestore();
  }

  private void validateSnapshotRestore() throws IOException {
    try {
      UTIL.getHBaseAdmin().enableTable(snapshotTableName);

      HTableDescriptor currentHTD = UTIL.getHBaseAdmin().getTableDescriptor(snapshotTableName);
      assertTrue(currentHTD.hasFamily(CF1));
      assertTrue(currentHTD.hasFamily(CF2));
      assertFalse(currentHTD.hasFamily(CF3));
      assertFalse(currentHTD.hasFamily(CF4));
      assertEquals(currentHTD.getFamiliesKeys().size(), snapshotHTD.getFamiliesKeys().size());
      SnapshotTestingUtils.verifyRowCount(UTIL, snapshotTableName, rowCountCF1 + rowCountCF2);
    } finally {
      UTIL.getHBaseAdmin().disableTable(snapshotTableName);
    }
  }
}
