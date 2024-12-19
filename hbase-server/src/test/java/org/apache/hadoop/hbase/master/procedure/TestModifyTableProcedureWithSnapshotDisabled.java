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

import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ MasterTests.class, MediumTests.class })
public class TestModifyTableProcedureWithSnapshotDisabled
  extends TestSnapshottingTableDDLProcedureBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestModifyTableProcedureWithSnapshotDisabled.class);

  @Rule
  public TestName name = new TestName();

  private static final Logger LOG =
    LoggerFactory.getLogger(TestModifyTableProcedureWithSnapshotDisabled.class);

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.getConfiguration().setBoolean(
      AbstractSnapshottingStateMachineTableProcedure.SNAPSHOT_BEFORE_DELETE_ENABLED_KEY, false);
    TestTableDDLProcedureBase.setupCluster();
  }

  @Test
  public void testModifyTableRemoveCFWithSnapshotDisabled() throws Exception {
    assertFalse("SNAPSHOT_BEFORE_DELETE_ENABLED is on", getMaster().getConfiguration().getBoolean(
      AbstractSnapshottingStateMachineTableProcedure.SNAPSHOT_BEFORE_DELETE_ENABLED_KEY, false));
    final String cf1 = "cf1";
    final String cf2 = "cf2";
    // Create the test table
    TableName tableName = TableName.valueOf(name.getMethodName());
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    LOG.info("Creating {}", tableName);
    MasterProcedureTestingUtility.createTable(procExec, tableName, null, cf1, cf2);
    // Prepare the procedure and set a snapshot name for testing that we will check for later
    List<ColumnFamilyDescriptor> families = new ArrayList<>();
    families.add(ColumnFamilyDescriptorBuilder.of(cf1)); // Only cf1, we will drop cf2
    TableDescriptor newTd =
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamilies(families).build();
    ProcedurePrepareLatch latch = new ProcedurePrepareLatch.CompatibilityLatch();
    ModifyTableProcedure proc = new ModifyTableProcedure(procExec.getEnvironment(), newTd, latch);
    String snapshotName = makeSnapshotName(name);
    proc.setSnapshotName(snapshotName);
    // Execute the procedure
    LOG.info("Submitting ModifyTableProcedure for {}", tableName);
    ProcedureTestingUtility.submitAndWait(procExec, proc);
    latch.await();
    // Validate that we really did remove the CF
    TableDescriptor currentTd = UTIL.getAdmin().getDescriptor(tableName);
    assertFalse("cf2 should not exist", currentTd.hasColumnFamily(Bytes.toBytes(cf2)));
    // Snapshotting is disabled so a recovery snapshot should not exist
    assertSnapshotAbsent(snapshotName);
  }

}
