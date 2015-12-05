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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.CreateTableState;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestCreateTableProcedure {
  private static final Log LOG = LogFactory.getLog(TestCreateTableProcedure.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static long nonceGroup = HConstants.NO_NONCE;
  private static long nonce = HConstants.NO_NONCE;

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

  @Test(timeout=60000)
  public void testSimpleCreate() throws Exception {
    final TableName tableName = TableName.valueOf("testSimpleCreate");
    final byte[][] splitKeys = null;
    testSimpleCreate(tableName, splitKeys);
  }

  @Test(timeout=60000)
  public void testSimpleCreateWithSplits() throws Exception {
    final TableName tableName = TableName.valueOf("testSimpleCreateWithSplits");
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    testSimpleCreate(tableName, splitKeys);
  }

  private void testSimpleCreate(final TableName tableName, byte[][] splitKeys) throws Exception {
    HRegionInfo[] regions = MasterProcedureTestingUtility.createTable(
      getMasterProcedureExecutor(), tableName, splitKeys, "f1", "f2");
    MasterProcedureTestingUtility.validateTableCreation(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, "f1", "f2");
  }

  @Test(timeout=60000, expected=TableExistsException.class)
  public void testCreateExisting() throws Exception {
    final TableName tableName = TableName.valueOf("testCreateExisting");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final HTableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f");
    final HRegionInfo[] regions = ModifyRegionUtils.createHRegionInfos(htd, null);

    // create the table
    long procId1 = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions), nonceGroup, nonce);

    // create another with the same name
    ProcedurePrepareLatch latch2 = new ProcedurePrepareLatch.CompatibilityLatch();
    long procId2 = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions, latch2),
      nonceGroup + 1,
      nonce + 1);

    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId1));

    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    latch2.await();
  }

  @Test(timeout=60000)
  public void testCreateTwiceWithSameNonce() throws Exception {
    final TableName tableName = TableName.valueOf("testCreateTwiceWithSameNonce");
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    final HTableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f");
    final HRegionInfo[] regions = ModifyRegionUtils.createHRegionInfos(htd, null);

    // create the table
    long procId1 = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions), nonceGroup, nonce);

    // create another with the same name
    long procId2 = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions), nonceGroup, nonce);

    ProcedureTestingUtility.waitProcedure(procExec, procId1);
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId1));

    ProcedureTestingUtility.waitProcedure(procExec, procId2);
    ProcedureTestingUtility.assertProcNotFailed(procExec.getResult(procId2));
    assertTrue(procId1 == procId2);
  }

  @Test(timeout=60000)
  public void testRecoveryAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRecoveryAndDoubleExecution");

    // create the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Create procedure && kill the executor
    byte[][] splitKeys = null;
    HTableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f1", "f2");
    HRegionInfo[] regions = ModifyRegionUtils.createHRegionInfos(htd, splitKeys);
    long procId = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions), nonceGroup, nonce);

    // Restart the executor and execute the step twice
    // NOTE: the 6 (number of CreateTableState steps) is hardcoded,
    //       so you have to look at this test at least once when you add a new step.
    MasterProcedureTestingUtility.testRecoveryAndDoubleExecution(
      procExec, procId, 6, CreateTableState.values());

    MasterProcedureTestingUtility.validateTableCreation(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, "f1", "f2");
  }

  @Test(timeout=90000)
  public void testRollbackAndDoubleExecution() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackAndDoubleExecution");

    // create the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Create procedure && kill the executor
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    HTableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f1", "f2");
    htd.setRegionReplication(3);
    HRegionInfo[] regions = ModifyRegionUtils.createHRegionInfos(htd, splitKeys);
    long procId = procExec.submitProcedure(
      new CreateTableProcedure(procExec.getEnvironment(), htd, regions), nonceGroup, nonce);

    // NOTE: the 4 (number of CreateTableState steps) is hardcoded,
    //       so you have to look at this test at least once when you add a new step.
    MasterProcedureTestingUtility.testRollbackAndDoubleExecution(
        procExec, procId, 4, CreateTableState.values());

    MasterProcedureTestingUtility.validateTableDeletion(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, "f1", "f2");

    // are we able to create the table after a rollback?
    resetProcExecutorTestingKillFlag();
    testSimpleCreate(tableName, splitKeys);
  }

  @Test(timeout=90000)
  public void testRollbackRetriableFailure() throws Exception {
    final TableName tableName = TableName.valueOf("testRollbackRetriableFailure");

    // create the table
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(procExec, true);

    // Start the Create procedure && kill the executor
    final byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c")
    };
    HTableDescriptor htd = MasterProcedureTestingUtility.createHTD(tableName, "f1", "f2");
    HRegionInfo[] regions = ModifyRegionUtils.createHRegionInfos(htd, splitKeys);
    long procId = procExec.submitProcedure(
      new FaultyCreateTableProcedure(procExec.getEnvironment(), htd, regions), nonceGroup, nonce);

    // NOTE: the 4 (number of CreateTableState steps) is hardcoded,
    //       so you have to look at this test at least once when you add a new step.
    MasterProcedureTestingUtility.testRollbackRetriableFailure(
        procExec, procId, 4, CreateTableState.values());

    MasterProcedureTestingUtility.validateTableDeletion(
      UTIL.getHBaseCluster().getMaster(), tableName, regions, "f1", "f2");

    // are we able to create the table after a rollback?
    resetProcExecutorTestingKillFlag();
    testSimpleCreate(tableName, splitKeys);
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  public static class FaultyCreateTableProcedure extends CreateTableProcedure {
    private int retries = 0;

    public FaultyCreateTableProcedure() {
      // Required by the Procedure framework to create the procedure on replay
    }

    public FaultyCreateTableProcedure(final MasterProcedureEnv env,
        final HTableDescriptor hTableDescriptor, final HRegionInfo[] newRegions)
        throws IOException {
      super(env, hTableDescriptor, newRegions);
    }

    @Override
    protected void rollbackState(final MasterProcedureEnv env, final CreateTableState state)
        throws IOException {
      if (retries++ < 3) {
        LOG.info("inject rollback failure state=" + state);
        throw new IOException("injected failure number " + retries);
      } else {
        super.rollbackState(env, state);
        retries = 0;
      }
    }
  }
}
