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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@Category({ MasterTests.class, LargeTests.class })
public class TestTruncateRegionProcedure extends TestTableDDLProcedureBase {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTruncateRegionProcedure.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestTruncateRegionProcedure.class);

  @Rule
  public TestName name = new TestName();

  private static void setupConf(Configuration conf) {
    conf.setInt(MasterProcedureConstants.MASTER_PROCEDURE_THREADS, 1);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 0);
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
  public void testTruncateRegionProcedure() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    // Arrange - Load table and prepare arguments values.
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String[] families = new String[] { "f1", "f2" };
    final byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("30"), Bytes.toBytes("60"), Bytes.toBytes("90") };

    MasterProcedureTestingUtility.createTable(procExec, tableName, splitKeys, families);

    insertData(UTIL, tableName, 2, 20, families);
    insertData(UTIL, tableName, 2, 31, families);
    insertData(UTIL, tableName, 2, 61, families);
    insertData(UTIL, tableName, 2, 91, families);

    assertEquals(8, UTIL.countRows(tableName));

    int rowsBeforeDropRegion = 8;

    MasterProcedureEnv environment = procExec.getEnvironment();
    RegionInfo regionToBeTruncated = environment.getAssignmentManager().getAssignedRegions()
      .stream().filter(r -> tableName.getNameAsString().equals(r.getTable().getNameAsString()))
      .min((o1, o2) -> Bytes.compareTo(o1.getStartKey(), o2.getStartKey())).get();

    // Act - Execute Truncate region procedure
    long procId =
      procExec.submitProcedure(new TruncateRegionProcedure(environment, regionToBeTruncated));
    ProcedureTestingUtility.waitProcedure(procExec, procId);
    assertEquals(8 - 2, UTIL.countRows(tableName));

    int rowsAfterDropRegion = UTIL.countRows(tableName);
    assertTrue("Row counts after truncate region should be less than row count before it",
      rowsAfterDropRegion < rowsBeforeDropRegion);
    assertEquals(rowsBeforeDropRegion, rowsAfterDropRegion + 2);

    insertData(UTIL, tableName, 2, 20, families);
    assertEquals(8, UTIL.countRows(tableName));
  }

  @Test
  public void testTruncateRegionProcedureErrorWhenSpecifiedReplicaRegionID() throws Exception {
    final ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    // Arrange - Load table and prepare arguments values.
    final TableName tableName = TableName.valueOf(name.getMethodName());
    final String[] families = new String[] { "f1", "f2" };
    createTable(tableName, families, 2);
    insertData(UTIL, tableName, 2, 20, families);
    insertData(UTIL, tableName, 2, 30, families);
    insertData(UTIL, tableName, 2, 60, families);

    assertEquals(6, UTIL.countRows(tableName));

    MasterProcedureEnv environment = procExec.getEnvironment();
    RegionInfo regionToBeTruncated = environment.getAssignmentManager().getAssignedRegions()
      .stream().filter(r -> tableName.getNameAsString().equals(r.getTable().getNameAsString()))
      .min((o1, o2) -> Bytes.compareTo(o1.getStartKey(), o2.getStartKey())).get();

    RegionInfo replicatedRegionId =
      RegionReplicaUtil.getRegionInfoForReplica(regionToBeTruncated, 1);

    // Act - Execute Truncate region procedure
    long procId =
      procExec.submitProcedure(new TruncateRegionProcedure(environment, replicatedRegionId));

    ProcedureTestingUtility.waitProcedure(procExec, procId);
    Procedure<MasterProcedureEnv> result = procExec.getResult(procId);
    // Asserts

    assertEquals(ProcedureProtos.ProcedureState.ROLLEDBACK, result.getState());
    assertTrue(result.getException().getMessage()
      .endsWith("Can't truncate replicas directly. Replicas are auto-truncated "
        + "when their primary is truncated."));
  }

  private TableDescriptor tableDescriptor(final TableName tableName, String[] families,
    final int replicaCount) {
    return TableDescriptorBuilder.newBuilder(tableName).setRegionReplication(replicaCount)
      .setColumnFamilies(columnFamilyDescriptor(families)).build();
  }

  private List<ColumnFamilyDescriptor> columnFamilyDescriptor(String[] families) {
    return Arrays.stream(families).map(ColumnFamilyDescriptorBuilder::of)
      .collect(Collectors.toList());
  }

  @SuppressWarnings("SameParameterValue")
  private void createTable(final TableName tableName, String[] families, final int replicaCount)
    throws IOException {
    UTIL.getAdmin().createTable(tableDescriptor(tableName, families, replicaCount));
  }
}
