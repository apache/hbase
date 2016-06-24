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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ProcedureInfo;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CloneSnapshotState;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestCloneSnapshotProcedure {
  private static final Log LOG = LogFactory.getLog(TestCloneSnapshotProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected final byte[] CF = Bytes.toBytes("cf1");

  private static long nonceGroup = HConstants.NO_NONCE;
  private static long nonce = HConstants.NO_NONCE;

  private static HBaseProtos.SnapshotDescription snapshot = null;

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
    resetProcExecutorTestingKillFlag();
    nonceGroup =
        MasterProcedureTestingUtility.generateNonceGroup(UTIL.getHBaseCluster().getMaster());
    nonce = MasterProcedureTestingUtility.generateNonce(UTIL.getHBaseCluster().getMaster());
  }

  @After
  public void tearDown() throws Exception {
    resetProcExecutorTestingKillFlag();
  }

  private void resetProcExecutorTestingKillFlag() {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    assertTrue("expected executor to be running", procExec.isRunning());
  }

  private HBaseProtos.SnapshotDescription getSnapshot() throws Exception {
    if (snapshot == null) {
      final TableName snapshotTableName = TableName.valueOf("testCloneSnapshot");
      long tid = System.currentTimeMillis();
      final byte[] snapshotName = Bytes.toBytes("snapshot-" + tid);

      Admin admin = UTIL.getHBaseAdmin();
      // create Table
      SnapshotTestingUtils.createTable(UTIL, snapshotTableName, getNumReplicas(), CF);
      // Load data
      SnapshotTestingUtils.loadData(UTIL, snapshotTableName, 500, CF);
      admin.disableTable(snapshotTableName);
      // take a snapshot
      admin.snapshot(snapshotName, snapshotTableName);
      admin.enableTable(snapshotTableName);

      List<SnapshotDescription> snapshotList = admin.listSnapshots();
      snapshot = ProtobufUtil.createHBaseProtosSnapshotDesc(snapshotList.get(0));
    }
    return snapshot;
  }

  private int getNumReplicas() {
    return 1;
  }

  public static HTableDescriptor createHTableDescriptor(
      final TableName tableName, final byte[] ... family) {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (int i = 0; i < family.length; ++i) {
      htd.addFamily(new HColumnDescriptor(family[i]));
    }
    return htd;
  }

  @Test(timeout=60000)
  public void testCloneSnapshot() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf("testCloneSnapshot2");
    final HTableDescriptor htd = createHTableDescriptor(clonedTableName, CF);

    // take the snapshot
    HBaseProtos.SnapshotDescription snapshotDesc = getSnapshot();

    long procId = ProcedureTestingUtility.submitAndWait(
      procExec, new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));
    MasterProcedureTestingUtility.validateTableIsEnabled(
      UTIL.getHBaseCluster().getMaster(),
      clonedTableName);
  }

  @Test(timeout = 60000)
  public void testCloneSnapshotTwiceWithSameNonce() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf("testCloneSnapshotTwiceWithSameNonce");
    final HTableDescriptor htd = createHTableDescriptor(clonedTableName, CF);

    // take the snapshot
    HBaseProtos.SnapshotDescription snapshotDesc = getSnapshot();

    long procId1 = procExec.submitProcedure(
      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc), nonceGroup, nonce);
    long procId2 = procExec.submitProcedure(
      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc), nonceGroup, nonce);

    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    // The second proc should succeed too - because it is the same proc.
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);
    assertTrue(procId1 == procId2);
  }

  @Test(timeout=60000)
  public void testCloneSnapshotToSameTable() throws Exception {
    // take the snapshot
    HBaseProtos.SnapshotDescription snapshotDesc = getSnapshot();

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf(snapshotDesc.getTable());
    final HTableDescriptor htd = createHTableDescriptor(clonedTableName, CF);

    long procId = ProcedureTestingUtility.submitAndWait(
      procExec, new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc));
    ProcedureInfo result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Clone snapshot failed with exception: " + result.getExceptionFullMessage());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof TableExistsException);
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf("testRecoveryAndDoubleExecution");
    final HTableDescriptor htd = createHTableDescriptor(clonedTableName, CF);

    // take the snapshot
    HBaseProtos.SnapshotDescription snapshotDesc = getSnapshot();

    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Clone snapshot procedure && kill the executor
    long procId = procExec.submitProcedure(
      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc), nonceGroup, nonce);

    // Restart the executor and execute the step twice
    int numberOfSteps = CloneSnapshotState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      CloneSnapshotState.values());

    MasterProcedureTestingUtility.validateTableIsEnabled(
      UTIL.getHBaseCluster().getMaster(),
      clonedTableName);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final HTableDescriptor htd = createHTableDescriptor(clonedTableName, CF);

    // take the snapshot
    HBaseProtos.SnapshotDescription snapshotDesc = getSnapshot();

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Clone snapshot procedure && kill the executor
    long procId = procExec.submitProcedure(
      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc), nonceGroup, nonce);

    int numberOfSteps = CloneSnapshotState.values().length - 2; // failing in the middle of proc
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(
      procExec,
      procId,
      numberOfSteps,
      CloneSnapshotState.values());

    MasterProcedureTestingUtility.validateTableDeletion(
      UTIL.getHBaseCluster().getMaster(), clonedTableName);

  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
