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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveTestingUtil;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({MasterTests.class, MediumTests.class})
public class TestDeleteTableProcedure extends TestTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDeleteTableProcedure.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestDeleteTableProcedure.class);
  @Rule public TestName name = new TestName();

  @Test(expected=TableNotFoundException.class)
  public void testDeleteNotExistentTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
        new DeleteTableProcedure(procExec.getEnvironment(), tableName, latch));
    latch.await();
  }

  @Test(expected=TableNotDisabledException.class)
  public void testDeleteNotDisabledTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, "f");

    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
        new DeleteTableProcedure(procExec.getEnvironment(), tableName, latch));
    latch.await();
  }

  @Test
  public void testDeleteDeletedTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      procExec, tableName, null, "f");
    UTIL.getAdmin().disableTable(tableName);

    // delete the table (that exists)
    long procId1 = procExec.submitProcedure(
        new DeleteTableProcedure(procExec.getEnvironment(), tableName));
    // delete the table (that will no longer exist)
    long procId2 = procExec.submitProcedure(
        new DeleteTableProcedure(procExec.getEnvironment(), tableName));

    // Wait the completion
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.waitProcedure(procExec, procId2);

    // First delete should succeed
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);

    // Second delete should fail with TableNotFound
    Procedure<?> result = procExec.getResult(procId2);
    assertTrue(result.isFailed());
    LOG.debug("Delete failed with exception: " + result.getException());
    assertTrue(ProcedureTestingUtility.getExceptionCause(result) instanceof TableNotFoundException);
  }

  @Test
  public void testSimpleDelete() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[][] splitKeys = null;
    testSimpleDelete(tableName, splitKeys);
  }

  @Test
  public void testSimpleDeleteWithSplits() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    testSimpleDelete(tableName, splitKeys);
  }

  @Test
  public void testDeleteFromMeta() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      getMasterProcedureExecutor(), tableName, null, "f1", "f2");
    List<RegionInfo> regionsList = new ArrayList<>();
    UTIL.getAdmin().disableTable(tableName);
    MasterProcedureEnv procedureEnv = getMasterProcedureExecutor().getEnvironment();
    assertNotNull("Table should be on TableDescriptors cache.",
      procedureEnv.getMasterServices().getTableDescriptors().get(tableName));
    DeleteTableProcedure.deleteFromMeta(procedureEnv, tableName, regionsList);
    assertNull("Table shouldn't be on TableDescriptors anymore.",
      procedureEnv.getMasterServices().getTableDescriptors().get(tableName));
  }

  private void testSimpleDelete(final TableName tableName, byte[][] splitKeys) throws Exception {
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      getMasterProcedureExecutor(), tableName, splitKeys, "f1", "f2");
    UTIL.getAdmin().disableTable(tableName);

    // delete the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new DeleteTableProcedure(procExec.getEnvironment(), tableName));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);
  }

  @Test
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());

    // create the table
    byte[][] splitKeys = null;
    RegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      getMasterProcedureExecutor(), tableName, splitKeys, "f1", "f2");
    UTIL.getAdmin().disableTable(tableName);

    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Delete procedure && kill the executor
    long procId = procExec.submitProcedure(
      new DeleteTableProcedure(procExec.getEnvironment(), tableName));

    // Restart the executor and execute the step twice
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId);

    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);
  }

  @Test
  public void testDeleteWhenTempDirIsNotEmpty() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String FAM = "fam";
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("b"), Bytes.toBytes("c"), Bytes.toBytes("d")
    };

    // create the table
    MasterProcedureTestingUtility.createTable(
      getMasterProcedureExecutor(), tableName, splitKeys, FAM);

    // get the current store files for the regions
    List<HRegion> regions = UTIL.getHBaseCluster().getRegions(tableName);
    // make sure we have 4 regions serving this table
    assertEquals(4, regions.size());

    // load the table
    try (Table table = UTIL.getConnection().getTable(tableName)) {
      UTIL.loadTable(table, Bytes.toBytes(FAM));
    }

    // disable the table so that we can manipulate the files
    UTIL.getAdmin().disableTable(tableName);

    final MasterFileSystem masterFileSystem =
      UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    final Path tableDir = CommonFSUtils.getTableDir(masterFileSystem.getRootDir(), tableName);
    final Path tempDir = masterFileSystem.getTempDir();
    final Path tempTableDir = CommonFSUtils.getTableDir(tempDir, tableName);
    final FileSystem fs = masterFileSystem.getFileSystem();

    // copy the table to the temporary directory to make sure the temp directory is not empty
    if (!FileUtil.copy(fs, tableDir, fs, tempTableDir, false, UTIL.getConfiguration())) {
      fail();
    }

    // delete the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    long procId = ProcedureTestingUtility.submitAndWait(procExec,
      new DeleteTableProcedure(procExec.getEnvironment(), tableName));
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);
    MasterProcedureTestingUtility.validateTableDeletion(getMaster(), tableName);

    // check if the temporary directory is deleted
    assertFalse(fs.exists(tempTableDir));

    // check for the existence of the archive directory
    for (HRegion region : regions) {
      Path archiveDir = HFileArchiveTestingUtil.getRegionArchiveDir(UTIL.getConfiguration(),
        region);
      assertTrue(fs.exists(archiveDir));
    }
  }
}
