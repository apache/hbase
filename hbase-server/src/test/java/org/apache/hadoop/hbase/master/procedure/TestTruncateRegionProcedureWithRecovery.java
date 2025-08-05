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

import static org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil.insertData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.TruncateRegionState;

@Category({ MasterTests.class, LargeTests.class })
public class TestTruncateRegionProcedureWithRecovery extends TestTableDDLProcedureBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTruncateRegionProcedureWithRecovery.class);
  private static final Logger LOG =
    LoggerFactory.getLogger(TestTruncateRegionProcedureWithRecovery.class);

  @Rule
  public TestName name = new TestName();

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 0);
    conf.setBoolean(HConstants.SNAPSHOT_BEFORE_DESTRUCTIVE_ACTION_ENABLED_KEY, true);
    conf.setInt("hbase.client.sync.wait.timeout.msec", 60000);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(3);
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
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);

    // Turn off balancer, so it doesn't cut in and mess up our placements.
    UTIL.getAdmin().balancerSwitch(false, true);
    // Turn off the meta scanner, so it doesn't remove, parent on us.
    UTIL.getHBaseCluster().getMaster().setCatalogJanitorEnabled(false);
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
    for (TableDescriptor htd : UTIL.getAdmin().listTableDescriptors()) {
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testRecoverySnapshotRollback() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String[] families = new String[] { "f1", "f2" };
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // Create table with split keys
    final byte[][] splitKeys = new byte[][] { Bytes.toBytes("30"), Bytes.toBytes("60") };
    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, families);

    // Insert data
    insertData(UTIL, tableName, 2, 20, families);
    insertData(UTIL, tableName, 2, 31, families);
    insertData(UTIL, tableName, 2, 61, families);

    // Get a region to truncate
    MasterProcedureEnv environment = procExec.getEnvironment();
    RegionInfo regionToTruncate = environment.getAssignmentManager().getAssignedRegions().stream()
      .filter(r -> tableName.getNameAsString().equals(r.getTable().getNameAsString()))
      .min((o1, o2) -> Bytes.compareTo(o1.getStartKey(), o2.getStartKey())).get();

    // Create a procedure that might fail. Use a simple approach that creates a custom procedure
    // that fails after snapshot.
    // Submit the failing procedure
    long procId =
      procExec.submitProcedure(new FailingTruncateRegionProcedure(environment, regionToTruncate));

    // Wait for procedure to complete (should fail)
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Procedure<MasterProcedureEnv> result = procExec.getResult(procId);
    assertTrue("Procedure should have failed", result.isFailed());

    // Verify no recovery snapshots remain after rollback
    boolean snapshotFound = false;
    for (SnapshotDescription snapshot : UTIL.getAdmin().listSnapshots()) {
      if (snapshot.getName().startsWith("auto_" + tableName.getNameAsString())) {
        snapshotFound = true;
        break;
      }
    }
    assertTrue("Recovery snapshot should have been cleaned up during rollback", !snapshotFound);
  }

  @Test
  public void testRecoverySnapshotAndRestore() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final TableName restoredTableName = TableName.valueOf(name.getMethodName() + "_restored");
    final String[] families = new String[] { "f1", "f2" };
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();

    // Create table with split keys
    final byte[][] splitKeys = new byte[][] { Bytes.toBytes("30"), Bytes.toBytes("60") };
    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, families);

    // Insert data
    insertData(UTIL, tableName, 2, 20, families);
    insertData(UTIL, tableName, 2, 31, families);
    insertData(UTIL, tableName, 2, 61, families);
    int initialRowCount = UTIL.countRows(tableName);

    // Get a region to truncate
    MasterProcedureEnv environment = procExec.getEnvironment();
    RegionInfo regionToTruncate = environment.getAssignmentManager().getAssignedRegions().stream()
      .filter(r -> tableName.getNameAsString().equals(r.getTable().getNameAsString()))
      .min((o1, o2) -> Bytes.compareTo(o1.getStartKey(), o2.getStartKey())).get();

    // Truncate the region (this should create a recovery snapshot)
    long procId =
      procExec.submitProcedure(new TruncateRegionProcedure(environment, regionToTruncate));
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    ProcedureTestingUtility.assertProcNotFailed(procExec, procId);

    // Verify region is truncated (should have fewer rows)
    int rowsAfterTruncate = UTIL.countRows(tableName);
    assertTrue("Should have fewer rows after truncate", rowsAfterTruncate < initialRowCount);

    // Find the recovery snapshot
    String recoverySnapshotName = null;
    for (SnapshotDescription snapshot : UTIL.getAdmin().listSnapshots()) {
      if (snapshot.getName().startsWith("auto_" + tableName.getNameAsString())) {
        recoverySnapshotName = snapshot.getName();
        break;
      }
    }
    assertTrue("Recovery snapshot should exist", recoverySnapshotName != null);

    // Restore from snapshot by cloning to a new table
    UTIL.getAdmin().cloneSnapshot(recoverySnapshotName, restoredTableName);
    UTIL.waitUntilAllRegionsAssigned(restoredTableName);

    // Verify restored table has original data
    assertEquals("Restored table should have original data", initialRowCount,
      UTIL.countRows(restoredTableName));

    // Clean up the cloned table
    UTIL.getAdmin().disableTable(restoredTableName);
    UTIL.getAdmin().deleteTable(restoredTableName);
  }

  public static class FailingTruncateRegionProcedure extends TruncateRegionProcedure {
    private boolean failOnce = false;

    public FailingTruncateRegionProcedure() {
      super();
    }

    public FailingTruncateRegionProcedure(MasterProcedureEnv env, RegionInfo region)
      throws HBaseIOException {
      super(env, region);
    }

    @Override
    protected Flow executeFromState(MasterProcedureEnv env, TruncateRegionState state)
      throws InterruptedException {
      if (!failOnce && state == TruncateRegionState.TRUNCATE_REGION_MAKE_OFFLINE) {
        failOnce = true;
        throw new RuntimeException("Simulated failure");
      }
      return super.executeFromState(env, state);
    }
  }
}
