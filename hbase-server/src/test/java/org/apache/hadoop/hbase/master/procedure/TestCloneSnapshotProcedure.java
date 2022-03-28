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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

@Category({MasterTests.class, MediumTests.class})
public class TestCloneSnapshotProcedure extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCloneSnapshotProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCloneSnapshotProcedure.class);

  protected final byte[] CF = Bytes.toBytes("cf1");

  private static SnapshotProtos.SnapshotDescription snapshot = null;

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    SnapshotTestingUtils.deleteAllSnapshots(UTIL.getAdmin());
    snapshot = null;
  }

  private SnapshotProtos.SnapshotDescription getSnapshot() throws Exception {
    if (snapshot == null) {
      final TableName snapshotTableName = TableName.valueOf("testCloneSnapshot");
      long tid = EnvironmentEdgeManager.currentTime();
      final String snapshotName = "snapshot-" + tid;

      Admin admin = UTIL.getAdmin();
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

  private static TableDescriptor createTableDescriptor(TableName tableName, byte[]... family) {
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(tableName).setValue(StoreFileTrackerFactory.TRACKER_IMPL,
        UTIL.getConfiguration().get(StoreFileTrackerFactory.TRACKER_IMPL,
          StoreFileTrackerFactory.Trackers.DEFAULT.name()));
    Stream.of(family).map(ColumnFamilyDescriptorBuilder::of)
      .forEachOrdered(builder::setColumnFamily);
    return builder.build();
  }

  @Test
  public void testCloneSnapshot() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf("testCloneSnapshot2");
    final TableDescriptor htd = createTableDescriptor(clonedTableName, CF);

    // take the snapshot
    SnapshotProtos.SnapshotDescription snapshotDesc = getSnapshot();

    long procId = ProcedureTestingUtility.submitAndWait(
      procExec, new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc));
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId));
    MasterProcedureTestingUtility.validateTableIsEnabled(
      UTIL.getHBaseCluster().getMaster(),
      clonedTableName);
  }

  @Test
  public void testCloneSnapshotToSameTable() throws Exception {
    // take the snapshot
    SnapshotProtos.SnapshotDescription snapshotDesc = getSnapshot();

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf(snapshotDesc.getTable());
    final TableDescriptor htd = createTableDescriptor(clonedTableName, CF);

    long procId = ProcedureTestingUtility.submitAndWait(
      procExec, new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc));
    Procedure<?> result = procExec.getResult(procId);
    assertTrue(result.isFailed());
    LOG.debug("Clone snapshot failed with exception: " + result.getException());
    assertTrue(
      ProcedureTestingUtility.getExceptionCause(result) instanceof TableExistsException);
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf("testRecoveryAndDoubleExecution");
    final TableDescriptor htd = createTableDescriptor(clonedTableName, CF);

    // take the snapshot
    SnapshotProtos.SnapshotDescription snapshotDesc = getSnapshot();

    // Here if you enable this then we will enter an infinite loop, as we will fail either after
    // TRSP.openRegion or after OpenRegionProcedure.execute, so we can never finish the TRSP...
    ProcedureTestingUtility.setKillIfHasParent(procExec, false);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Clone snapshot procedure && kill the executor
    long procId = procExec.submitProcedure(
      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    MasterProcedureTestingUtility.validateTableIsEnabled(
      UTIL.getHBaseCluster().getMaster(),
      clonedTableName);
  }

  @Test
  public void testRecoverWithRestoreAclFlag() throws Exception {
    // This test is to solve the problems mentioned in HBASE-26462,
    // this needs to simulate the case of CloneSnapshotProcedure failure and recovery,
    // and verify whether 'restoreAcl' flag can obtain the correct value.

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf("testRecoverWithRestoreAclFlag");
    final TableDescriptor htd = createTableDescriptor(clonedTableName, CF);

    SnapshotProtos.SnapshotDescription snapshotDesc = getSnapshot();
    ProcedureTestingUtility.setKillIfHasParent(procExec, false);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Clone snapshot procedure (with restoreAcl 'true') && kill the executor
    long procId = procExec.submitProcedure(
      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc, true));

    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    CloneSnapshotProcedure result = (CloneSnapshotProcedure)procExec.getResult(procId);
    // check whether the 'restoreAcl' flag is true after deserialization from Pb.
    assertEquals(true, result.getRestoreAcl());
  }

  @Test
  public void testRollbackAndDoubleExecution() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final TableName clonedTableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final TableDescriptor htd = createTableDescriptor(clonedTableName, CF);

    // take the snapshot
    SnapshotProtos.SnapshotDescription snapshotDesc = getSnapshot();

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Clone snapshot procedure && kill the executor
    long procId = procExec.submitProcedure(
      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc));

    int lastStep = 2; // failing before CLONE_SNAPSHOT_WRITE_FS_LAYOUT
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep);

    MasterProcedureTestingUtility.validateTableDeletion(
      UTIL.getHBaseCluster().getMaster(), clonedTableName);
  }
}
