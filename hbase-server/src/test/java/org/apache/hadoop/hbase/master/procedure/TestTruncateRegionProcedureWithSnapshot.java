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
import static org.junit.Assert.assertNotEquals;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, LargeTests.class })
public class TestTruncateRegionProcedureWithSnapshot extends TestSnapshottingTableDDLProcedureBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTruncateRegionProcedureWithSnapshot.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestTruncateRegionProcedureWithSnapshot.class);

  @Rule
  public TestName name = new TestName();

  @Test
  public void testTruncateRegion() throws Exception {
    final String cf1 = "cf1";
    final int rows = 100;
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1);
    // Load data
    insertData(tableName, rows / 2, 1, cf1);
    insertData(tableName, rows / 2, 100, cf1);
    assertEquals("Table load failed", rows, countRows(tableName, cf1));
    // Split the table
    LOG.info("Splitting {}", tableName);
    Admin admin = UTIL.getAdmin();
    admin.split(tableName, Bytes.toBytes("" + 100));
    ProcedureTestingUtility.waitNoProcedureRunning(procExec);
    List<RegionInfo> regions = admin.getRegions(tableName);
    RegionInfo region = regions.get(0);
    LOG.info("Submitting TruncateRegionProcedure for {} {}", tableName, region);
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    TruncateRegionProcedure proc =
      new TruncateRegionProcedure(procExec.getEnvironment(), region, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    procExec.submitProcedure(proc);
    latch.await();
    // We should have too few rows now
    assertNotEquals("Did not truncate the region?", rows, countRows(tableName, cf1));
    // There should be a recovery snapshot
    assertSnapshotExists(snapshotName);
    // The recovery snapshot should have the correct TTL
    assertSnapshotHasTtl(snapshotName, ttlForTest);
    // And we should be able to recover
    LOG.info("Disabling {}", tableName);
    admin.disableTable(tableName);
    LOG.info("Restoring {} from {}", tableName, snapshotName);
    admin.restoreSnapshot(snapshotName, false);
    LOG.info("Enabling {}", tableName);
    admin.enableTable(tableName);
    // The data should have been restored
    assertEquals("Restore from snapshot failed", rows, countRows(tableName, cf1));
  }

}
