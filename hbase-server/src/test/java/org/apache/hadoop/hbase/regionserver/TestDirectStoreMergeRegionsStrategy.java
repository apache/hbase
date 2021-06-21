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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.regionserver.HRegionFileSystem.REGION_WRITE_STRATEGY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.DirectStoreMergeRegionsStrategy;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Test for DirectStoreMergeRegionStrategy
 */
@Category({ RegionServerTests.class, LargeTests.class})
public class TestDirectStoreMergeRegionsStrategy {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDirectStoreMergeRegionsStrategy.class);

  @Rule
  public TestName name = new TestName();

  private static final String CF_NAME = "cf";

  private final byte[] cf = Bytes.toBytes(CF_NAME);

  private HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private TableName table;

  @Before
  public void setup() throws Exception {
    UTIL.startMiniCluster();
    table = TableName.valueOf(name.getMethodName());
    UTIL.createTable(table, cf);

  }

  @After
  public void shutdown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateMergedRegion() throws IOException {
    testMergeStrategyMethod( (p, r) -> {
      HRegion regionToMerge = p.getFirst();
      Path tableDir = regionToMerge.getRegionFileSystem().getTableDir();
      Path mergeRegionDir = new Path(tableDir, r.getEncodedName());
      mergeRegionDir = new Path(mergeRegionDir, CF_NAME);
      try {
        assertTrue(regionToMerge.getRegionFileSystem().getFileSystem().exists(mergeRegionDir));
        assertEquals(2,
          regionToMerge.getRegionFileSystem().getFileSystem().listStatus(mergeRegionDir).length);
      } catch(Exception e){
        fail(e.getMessage());
      }
    });
  }

  @Test
  public void testCleanupMergedRegion() throws IOException {
    final DirectStoreMergeRegionsStrategy mergeStrategy = new DirectStoreMergeRegionsStrategy();
    testMergeStrategyMethod( (p, r) -> {
      try {
      mergeStrategy.cleanupMergedRegion(
        UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment(),
          new RegionInfo[]{p.getFirst().getRegionInfo(), p.getSecond().getRegionInfo()}, r);
        Path tableDir = p.getFirst().getRegionFileSystem().getTableDir();
        FileSystem fs = p.getFirst().getRegionFileSystem().getFileSystem();
        assertFalse(fs.exists(new Path(tableDir, r.getEncodedName())));
      } catch (IOException e) {
        fail(e.getMessage());
      }
    });
  }


  private void testMergeStrategyMethod(BiConsumer<Pair<HRegion, HRegion>, RegionInfo> validator)
      throws IOException{
    List<HRegion> regions = splitTableAndPutData();
    HRegion first = regions.get(0);
    HRegion second = regions.get(1);
    DirectStoreMergeRegionsStrategy mergeStrategy = new DirectStoreMergeRegionsStrategy();
    RegionInfo mergeResult = createMergeResult(first.getRegionInfo(), second.getRegionInfo());
    mergeStrategy.createMergedRegion(
      UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor().getEnvironment(),
      new RegionInfo[]{first.getRegionInfo(), second.getRegionInfo()}, mergeResult);
    validator.accept(new Pair<>(first, second), mergeResult);

  }

  private List<HRegion> splitTableAndPutData() throws IOException{
    //splitting the table first
    UTIL.getAdmin().split(table, Bytes.toBytes("002"));
    //Add data and flush to create files in the two different regions
    putThreeRowsAndFlush(table);
    StoreFileTrackingUtils.init(UTIL.getHBaseCluster().getMaster());
    UTIL.getHBaseCluster().getMaster().getConfiguration().set(REGION_WRITE_STRATEGY,
      DirectStoreFSWriteStrategy.class.getName());
    return UTIL.getHBaseCluster().getRegions(table);
  }

  private RegionInfo createMergeResult(RegionInfo first, RegionInfo second) {
    return RegionInfoBuilder.newBuilder(table)
      .setStartKey(first.getStartKey())
      .setEndKey(second.getEndKey())
      .setSplit(false)
      .setRegionId(first.getRegionId() + EnvironmentEdgeManager.currentTime())
      .build();
  }

  private void putThreeRowsAndFlush(TableName table) throws IOException {
    Table tbl = UTIL.getConnection().getTable(table);
    Put put = new Put(Bytes.toBytes("001"));
    byte[] qualifier = Bytes.toBytes("1");
    put.addColumn(cf, qualifier, Bytes.toBytes(1));
    tbl.put(put);
    put = new Put(Bytes.toBytes("002"));
    put.addColumn(cf, qualifier, Bytes.toBytes(2));
    tbl.put(put);
    put = new Put(Bytes.toBytes("003"));
    put.addColumn(cf, qualifier, Bytes.toBytes(2));
    tbl.put(put);
    UTIL.flush(table);
  }

}
