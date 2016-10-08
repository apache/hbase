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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.DispatchMergingRegionsState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestDispatchMergingRegionsProcedure {
  private static final Log LOG = LogFactory.getLog(TestDispatchMergingRegionsProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static long nonceGroup = HConstants.NO_NONCE;
  private static long nonce = HConstants.NO_NONCE;

  private final static byte[] FAMILY = Bytes.toBytes("FAMILY");
  final static Configuration conf = UTIL.getConfiguration();
  private static Admin admin;

  private static void setupConf(Configuration conf) {
    // Reduce the maximum attempts to speed up the test
    conf.setInt("hbase.assignment.maximum.attempts", 3);
    conf.setInt("hbase.master.maximum.ping.server.attempts", 3);
    conf.setInt("hbase.master.ping.server.retry.sleep.interval", 1);

    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(conf);
    UTIL.startMiniCluster(1);
    admin = UTIL.getHBaseAdmin();
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
    // Turn off balancer so it doesn't cut in and mess up our placements.
    UTIL.getHBaseAdmin().setBalancerRunning(false, true);
    // Turn off the meta scanner so it don't remove parent on us.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
    resetProcExecutorTestingKillFlag();
  }

  @After
  public void tearDown() throws Exception {
    resetProcExecutorTestingKillFlag();
    for (HTableDescriptor htd: UTIL.getHBaseAdmin().listTables()) {
      LOG.info("Tear down, remove table=" + htd.getTableName());
      UTIL.deleteTable(htd.getTableName());
    }
  }

  private void resetProcExecutorTestingKillFlag() {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, false);
    assertTrue("expected executor to be running", procExec.isRunning());
  }

  /**
   * This tests two region merges
   */
  @Test(timeout=60000)
  public void testMergeTwoRegions() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeTwoRegions");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<HRegionInfo> tableRegions = createTable(tableName, 3);

    HRegionInfo[] regionsToMerge = new HRegionInfo[2];
    regionsToMerge[0] = tableRegions.get(0);
    regionsToMerge[1] = tableRegions.get(1);

    long procId = procExec.submitProcedure(new DispatchMergingRegionsProcedure(
      procExec.getEnvironment(), tableName, regionsToMerge, true));
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    assertRegionCount(tableName, 2);
  }

  /**
   * This tests two concurrent region merges
   */
  @Test(timeout=90000)
  public void testMergeRegionsConcurrently() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeTwoRegions");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<HRegionInfo> tableRegions = createTable(tableName, 4);

    HRegionInfo[] regionsToMerge1 = new HRegionInfo[2];
    HRegionInfo[] regionsToMerge2 = new HRegionInfo[2];
    regionsToMerge1[0] = tableRegions.get(0);
    regionsToMerge1[1] = tableRegions.get(1);
    regionsToMerge2[0] = tableRegions.get(2);
    regionsToMerge2[1] = tableRegions.get(3);

    long procId1 = procExec.submitProcedure(new DispatchMergingRegionsProcedure(
      procExec.getEnvironment(), tableName, regionsToMerge1, true));
    long procId2 = procExec.submitProcedure(new DispatchMergingRegionsProcedure(
      procExec.getEnvironment(), tableName, regionsToMerge2, true));
    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);

    assertRegionCount(tableName, 2);
  }

  @Test(timeout=60000)
  public void testMergeRegionsTwiceWithSameNonce() throws Exception {
    final TableName tableName = TableName.valueOf("testMergeRegionsTwiceWithSameNonce");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<HRegionInfo> tableRegions = createTable(tableName, 3);

    HRegionInfo[] regionsToMerge = new HRegionInfo[2];
    regionsToMerge[0] = tableRegions.get(0);
    regionsToMerge[1] = tableRegions.get(1);

    long procId1 = procExec.submitProcedure(new DispatchMergingRegionsProcedure(
      procExec.getEnvironment(), tableName, regionsToMerge, true), nonceGroup, nonce);
    long procId2 = procExec.submitProcedure(new DispatchMergingRegionsProcedure(
      procExec.getEnvironment(), tableName, regionsToMerge, true), nonceGroup, nonce);
    assertEquals(procId1, procId2);

    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId1);
    // The second proc should succeed too - because it is the same proc.
    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId2);

    assertRegionCount(tableName, 2);
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<HRegionInfo> tableRegions = createTable(tableName, 3);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    HRegionInfo[] regionsToMerge = new HRegionInfo[2];
    regionsToMerge[0] = tableRegions.get(0);
    regionsToMerge[1] = tableRegions.get(1);

    long procId = procExec.submitProcedure(
      new DispatchMergingRegionsProcedure(
        procExec.getEnvironment(), tableName, regionsToMerge, true));

    // Restart the executor and execute the step twice
    int numberOfSteps = DispatchMergingRegionsState.values().length;
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(procExec, procId, numberOfSteps);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    assertRegionCount(tableName, 2);
  }

  @Test(timeout = 60000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    List<HRegionInfo> tableRegions = createTable(tableName, 3);

    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    HRegionInfo[] regionsToMerge = new HRegionInfo[2];
    regionsToMerge[0] = tableRegions.get(0);
    regionsToMerge[1] = tableRegions.get(1);

    long procId = procExec.submitProcedure(
      new DispatchMergingRegionsProcedure(
        procExec.getEnvironment(), tableName, regionsToMerge, true));

    int numberOfSteps = DispatchMergingRegionsState.values().length - 3;
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(procExec, procId, numberOfSteps);
  }

  private List<HRegionInfo> createTable(final TableName tableName, final int nregions)
      throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    byte[][] splitRows = new byte[nregions - 1][];
    for (int i = 0; i < splitRows.length; ++i) {
      splitRows[i] = Bytes.toBytes(String.format("%d", i));
    }
    admin.createTable(desc, splitRows);
    return assertRegionCount(tableName, nregions);
  }

  public List<HRegionInfo> assertRegionCount(final TableName tableName, final int nregions)
      throws Exception {
    UTIL.waitUntilNoRegionsInTransition();
    List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);
    assertEquals(nregions, tableRegions.size());
    return tableRegions;
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }
}
