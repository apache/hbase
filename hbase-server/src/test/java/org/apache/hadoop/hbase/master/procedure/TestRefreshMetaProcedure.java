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

import static org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.assertProcNotFailed;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestRefreshMetaProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRefreshMetaProcedure.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private ProcedureExecutor<MasterProcedureEnv> procExecutor;
  List<RegionInfo> activeRegions;
  TableName tableName = TableName.valueOf("testRefreshMeta");

  @Before
  public void setup() throws Exception {
    TEST_UTIL.getConfiguration().set("USE_META_REPLICAS", "false");
    TEST_UTIL.startMiniCluster();
    procExecutor = TEST_UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
    byte[][] splitKeys =
      new byte[][] { Bytes.toBytes("split1"), Bytes.toBytes("split2"), Bytes.toBytes("split3") };
    TEST_UTIL.createTable(tableName, Bytes.toBytes("cf"), splitKeys);
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.getAdmin().flush(tableName);
    activeRegions = TEST_UTIL.getAdmin().getRegions(tableName);
    assertFalse(activeRegions.isEmpty());
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRefreshMetaProcedureExecutesSuccessfully() {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());
    long procId = procExecutor.submitProcedure(procedure);
    ProcedureTestingUtility.waitProcedure(procExecutor, procId);
    assertProcNotFailed(procExecutor.getResult(procId));
  }

  @Test
  public void testGetCurrentRegions() throws Exception {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());
    List<RegionInfo> regions = procedure.getCurrentRegions(TEST_UTIL.getConnection());
    assertFalse("Should have found regions in meta", regions.isEmpty());
    assertTrue("Should include test table region",
      regions.stream().anyMatch(r -> r.getTable().getNameAsString().equals("testRefreshMeta")));
  }

  @Test
  public void testScanBackingStorage() throws Exception {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());

    List<RegionInfo> fsRegions = procedure.scanBackingStorage(TEST_UTIL.getConnection());

    assertTrue("All regions from meta should be found in the storage",
      activeRegions.stream().allMatch(reg -> fsRegions.stream()
        .anyMatch(r -> r.getRegionNameAsString().equals(reg.getRegionNameAsString()))));
  }

  @Test
  public void testHasBoundaryChanged() throws Exception {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());
    RegionInfo region1 = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(Bytes.toBytes("start1")).setEndKey(Bytes.toBytes("end1")).build();

    RegionInfo region2 = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(Bytes.toBytes("start2")).setEndKey(Bytes.toBytes("end1")).build();

    RegionInfo region3 = RegionInfoBuilder.newBuilder(tableName)
      .setStartKey(Bytes.toBytes("start1")).setEndKey(Bytes.toBytes("end2")).build();

    assertTrue("Different start keys should have been detected",
      procedure.hasBoundaryChanged(region1, region2));

    assertTrue("Different end keys should have been detected",
      procedure.hasBoundaryChanged(region1, region3));

    assertFalse("Identical boundaries should not have been identified",
      procedure.hasBoundaryChanged(region1, region1));
  }
}
