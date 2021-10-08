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

import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.TRACKER_IMPL;
import static org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory.Trackers.FILE;
import static org.junit.Assert.assertTrue;
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
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Stream;

@Category({MasterTests.class, MediumTests.class})
public class TestCloneSnapshotProcedureFileBasedSFT extends TestCloneSnapshotProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCloneSnapshotProcedureFileBasedSFT.class);

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.getConfiguration().set(TRACKER_IMPL, FILE.name());
    UTIL.getConfiguration().setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    UTIL.startMiniCluster(1);
  }

  @Test
  public void testCloneSnapshot() throws Exception {
    super.testCloneSnapshot();
  }

  @Test
  public void testCloneSnapshotToSameTableFileSFT() throws Exception {
    super.testCloneSnapshotToSameTable();
  }

//  @Test
//  public void testRecoveryAndDoubleExecution() throws Exception {
//    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
//    final TableName clonedTableName = TableName.valueOf("testRecoveryAndDoubleExecution");
//    final TableDescriptor htd = createTableDescriptor(clonedTableName, CF);
//
//    // take the snapshot
//    SnapshotProtos.SnapshotDescription snapshotDesc = getSnapshot();
//
//    // Here if you enable this then we will enter an infinite loop, as we will fail either after
//    // TRSP.openRegion or after OpenRegionProcedure.execute, so we can never finish the TRSP...
//    ProcedureTestingUtility.setKillIfHasParent(procExec, false);
//    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
//
//    // Start the Clone snapshot procedure && kill the executor
//    long procId = procExec.submitProcedure(
//      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc));
//
//    // Restart the executor and execute the step twice
//    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);
//
//    MasterProcedureTestingUtility.validateTableIsEnabled(
//      UTIL.getHBaseCluster().getMaster(),
//      clonedTableName);
//  }
//
//  @Test
//  public void testRollbackAndDoubleExecution() throws Exception {
//    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
//    final TableName clonedTableName = TableName.valueOf("testRollbackAndDoubleExecution");
//    final TableDescriptor htd = createTableDescriptor(clonedTableName, CF);
//
//    // take the snapshot
//    SnapshotProtos.SnapshotDescription snapshotDesc = getSnapshot();
//
//    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
//    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);
//
//    // Start the Clone snapshot procedure && kill the executor
//    long procId = procExec.submitProcedure(
//      new CloneSnapshotProcedure(procExec.getEnvironment(), htd, snapshotDesc));
//
//    int lastStep = 2; // failing before CLONE_SNAPSHOT_WRITE_FS_LAYOUT
//    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, lastStep);
//
//    MasterProcedureTestingUtility.validateTableDeletion(
//      UTIL.getHBaseCluster().getMaster(), clonedTableName);
//  }
}
