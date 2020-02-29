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

import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_MAX_SPLITTER;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.SplitWALManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestSplitWALProcedure {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitWALProcedure.class);

  private static HBaseTestingUtility TEST_UTIL;
  private HMaster master;
  private TableName TABLE_NAME;
  private SplitWALManager splitWALManager;
  private byte[] FAMILY;

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.getConfiguration().setBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, false);
    TEST_UTIL.getConfiguration().setInt(HBASE_SPLIT_WAL_MAX_SPLITTER, 1);
    TEST_UTIL.startMiniCluster(3);
    master = TEST_UTIL.getHBaseCluster().getMaster();
    splitWALManager = master.getSplitWALManager();
    TABLE_NAME = TableName.valueOf(Bytes.toBytes("TestSplitWALProcedure"));
    FAMILY = Bytes.toBytes("test");
  }

  @After
  public void teardown() throws Exception {
    if (this.master != null) {
      ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(
        master.getMasterProcedureExecutor(), false);
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testHandleDeadWorker() throws Exception {
    Table table = TEST_UTIL.createTable(TABLE_NAME, FAMILY, TEST_UTIL.KEYS_FOR_HBA_CREATE_TABLE);
    for (int i = 0; i < 10; i++) {
      TEST_UTIL.loadTable(table, FAMILY);
    }
    HRegionServer testServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    ProcedureExecutor<MasterProcedureEnv> masterPE = master.getMasterProcedureExecutor();
    List<FileStatus> wals = splitWALManager.getWALsToSplit(testServer.getServerName(), false);
    Assert.assertEquals(1, wals.size());
    TEST_UTIL.getHBaseCluster().killRegionServer(testServer.getServerName());
    TEST_UTIL.waitFor(30000, () -> master.getProcedures().stream()
        .anyMatch(procedure -> procedure instanceof SplitWALProcedure));
    Procedure splitWALProcedure = master.getProcedures().stream()
        .filter(procedure -> procedure instanceof SplitWALProcedure).findAny().get();
    Assert.assertNotNull(splitWALProcedure);
    TEST_UTIL.waitFor(5000, () -> ((SplitWALProcedure) splitWALProcedure).getWorker() != null);
    TEST_UTIL.getHBaseCluster()
        .killRegionServer(((SplitWALProcedure) splitWALProcedure).getWorker());
    ProcedureTestingUtility.waitProcedure(masterPE, splitWALProcedure.getProcId());
    Assert.assertTrue(splitWALProcedure.isSuccess());
    ProcedureTestingUtility.waitAllProcedures(masterPE);
  }

  @Test
  public void testMasterRestart() throws Exception {
    Table table = TEST_UTIL.createTable(TABLE_NAME, FAMILY, TEST_UTIL.KEYS_FOR_HBA_CREATE_TABLE);
    for (int i = 0; i < 10; i++) {
      TEST_UTIL.loadTable(table, FAMILY);
    }
    HRegionServer testServer = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    List<FileStatus> wals = splitWALManager.getWALsToSplit(testServer.getServerName(), false);
    Assert.assertEquals(1, wals.size());
    SplitWALProcedure splitWALProcedure =
        new SplitWALProcedure(wals.get(0).getPath().toString(), testServer.getServerName());
    long pid = ProcedureTestingUtility.submitProcedure(master.getMasterProcedureExecutor(),
        splitWALProcedure, HConstants.NO_NONCE, HConstants.NO_NONCE);
    TEST_UTIL.waitFor(5000, () -> splitWALProcedure.getWorker() != null);
    // Kill master
    TEST_UTIL.getHBaseCluster().killMaster(master.getServerName());
    TEST_UTIL.getHBaseCluster().waitForMasterToStop(master.getServerName(), 20000);
    // restart master
    TEST_UTIL.getHBaseCluster().startMaster();
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    this.master = TEST_UTIL.getHBaseCluster().getMaster();
    ProcedureTestingUtility.waitProcedure(master.getMasterProcedureExecutor(), pid);
    Optional<Procedure<?>> procedure =
        master.getProcedures().stream().filter(p -> p.getProcId() == pid).findAny();
    // make sure procedure is successful and wal is deleted
    Assert.assertTrue(procedure.isPresent());
    Assert.assertTrue(procedure.get().isSuccess());
    Assert.assertFalse(TEST_UTIL.getTestFileSystem().exists(wals.get(0).getPath()));
  }

}
