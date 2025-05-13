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

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.assertProcNotFailed;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
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
    // Create a test table to have some regions
    byte[][] splitKeys = new byte[][] {
      Bytes.toBytes("split1"),
      Bytes.toBytes("split2"),
      Bytes.toBytes("split3")
    };
    TEST_UTIL.createTable(tableName, Bytes.toBytes("cf"), splitKeys);
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.getAdmin().flush(tableName);
    Thread.sleep(1000);
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
  public void testNeedsUpdateWithDifferentRegions() {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());
    List<RegionInfo> currentRegions = createTestRegions(2);
    List<RegionInfo> latestRegions = createTestRegions(3, "differenttable");

    assertTrue("Different regions should need update",
      procedure.needsUpdate(currentRegions, latestRegions));
  }

  @Test
  public void testNeedsUpdateWithNullLists() {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());

    assertFalse("Null current regions should not need update",
      procedure.needsUpdate(null, createTestRegions(1)));
    assertFalse("Null latest regions should not need update",
      procedure.needsUpdate(createTestRegions(1), null));
    assertFalse("Both null regions should not need update",
      procedure.needsUpdate(null, null));
  }

  @Test
  public void testGetCurrentRegions() throws Exception {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());
    List<RegionInfo> regions = procedure.getCurrentRegions(TEST_UTIL.getConnection());
    assertFalse("Should have found regions in meta",
      regions.isEmpty());
    assertTrue("Should include test table region",
      regions.stream()
      .anyMatch(r -> r.getTable().getNameAsString().equals("testRefreshMeta")));
  }

  @Test
  public void testDetectBoundaryChangesInRegions() throws Exception {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());
    List<RegionInfo> currentRegions = createTestRegions(2);
    List<RegionInfo> latestRegions = new ArrayList<>(currentRegions);

    latestRegions.set(0, RegionInfoBuilder.newBuilder(currentRegions.get(0).getTable())
      .setStartKey(Bytes.toBytes("newStart"))
      .setEndKey(Bytes.toBytes("newEnd"))
      .build());
    assertTrue("Boundary changes should need update",
      procedure.needsUpdate(currentRegions, latestRegions));
  }

  @Test
  public void testScanBackingStorage() throws Exception {
    RefreshMetaProcedure procedure = new RefreshMetaProcedure(procExecutor.getEnvironment());
    List<RegionInfo> fsRegions = procedure.scanBackingStorage(TEST_UTIL.getConnection());

    assertTrue("All regions from meta should be found in the storage",
      activeRegions.stream()
        .allMatch(reg -> fsRegions.stream()
          .anyMatch(r -> r.getRegionNameAsString().equals(reg.getRegionNameAsString()))));
  }

  private List<RegionInfo> createTestRegions(int count) {
    return createTestRegions(count, "test");
  }

  private List<RegionInfo> createTestRegions(int count, String tablePrefix) {
    List<RegionInfo> regions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      byte[] start = Bytes.toBytes("start" + i);
      byte[] end = Bytes.toBytes("start" + (i + 1));
      regions.add(RegionInfoBuilder
        .newBuilder(TableName.valueOf(tablePrefix + i))
        .setStartKey(start)
        .setEndKey(end)
        .build());
    }
    return regions;
  }
}
