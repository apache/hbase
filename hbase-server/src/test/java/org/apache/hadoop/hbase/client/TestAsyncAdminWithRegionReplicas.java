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
package org.apache.hadoop.hbase.client;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ LargeTests.class, ClientTests.class })
public class TestAsyncAdminWithRegionReplicas extends TestAsyncAdminBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncAdminWithRegionReplicas.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestAsyncAdminBase.setUpBeforeClass();
    HBaseTestingUtility.setReplicas(TEST_UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
    try (ConnectionRegistry registry =
      ConnectionRegistryFactory.getRegistry(TEST_UTIL.getConfiguration())) {
      RegionReplicaTestHelper.waitUntilAllMetaReplicasAreReady(TEST_UTIL, registry);
    }
  }

  private void testMoveNonDefaultReplica(TableName tableName)
      throws InterruptedException, ExecutionException {
    AsyncTableRegionLocator locator = ASYNC_CONN.getRegionLocator(tableName);
    List<HRegionLocation> locs = locator.getAllRegionLocations().get();
    // try region name
    admin.move(locs.get(1).getRegion().getRegionName()).get();
    assertNotEquals(locs.get(1).getServerName(),
      locator.getRegionLocation(HConstants.EMPTY_START_ROW, 1, true).get());
    // try encoded region name
    admin.move(locs.get(2).getRegion().getEncodedNameAsBytes()).get();
    assertNotEquals(locs.get(2).getServerName(),
      locator.getRegionLocation(HConstants.EMPTY_START_ROW, 2, true).get());
  }

  @Test
  public void testMoveNonDefaultReplica()
      throws InterruptedException, ExecutionException, IOException {
    createTableWithDefaultConf(tableName, 3);
    testMoveNonDefaultReplica(tableName);
    testMoveNonDefaultReplica(TableName.META_TABLE_NAME);
  }

  @Test
  public void testSplitNonDefaultReplica()
      throws InterruptedException, ExecutionException, IOException {
    createTableWithDefaultConf(tableName, 3);
    List<HRegionLocation> locs =
      ASYNC_CONN.getRegionLocator(tableName).getAllRegionLocations().get();
    try {
      admin.splitRegion(locs.get(1).getRegion().getRegionName()).get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
    }
    try {
      admin.splitRegion(locs.get(2).getRegion().getEncodedNameAsBytes()).get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
    }
  }

  @Test
  public void testMergeNonDefaultReplicas()
      throws InterruptedException, ExecutionException, IOException {
    byte[][] splitRows = new byte[][] { Bytes.toBytes(0) };
    createTableWithDefaultConf(tableName, 3, splitRows);
    List<HRegionLocation> locs =
      ASYNC_CONN.getRegionLocator(tableName).getAllRegionLocations().get();
    assertEquals(6, locs.size());
    Map<Integer, List<RegionInfo>> replicaId2RegionInfo = locs.stream()
      .map(HRegionLocation::getRegion).collect(Collectors.groupingBy(RegionInfo::getReplicaId));
    List<RegionInfo> replicaOnes = replicaId2RegionInfo.get(1);
    try {
      admin
        .mergeRegions(replicaOnes.get(0).getRegionName(), replicaOnes.get(1).getRegionName(), false)
        .get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
    }
    List<RegionInfo> replicaTwos = replicaId2RegionInfo.get(2);
    try {
      admin
        .mergeRegions(replicaTwos.get(0).getRegionName(), replicaTwos.get(1).getRegionName(), false)
        .get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
    }
  }

  @Test
  public void testCloneTableSchema() throws IOException, InterruptedException, ExecutionException {
    createTableWithDefaultConf(tableName, 3);
    admin.cloneTableSchema(tableName, TableName.valueOf(tableName.getNameAsString() + "_new"), true)
      .get();
  }

  @Test
  public void testGetTableRegions() throws InterruptedException, ExecutionException, IOException {
    List<RegionInfo> metaRegions = admin.getRegions(TableName.META_TABLE_NAME).get();
    assertEquals(3, metaRegions.size());
    for (int i = 0; i < 3; i++) {
      RegionInfo metaRegion = metaRegions.get(i);
      assertEquals(TableName.META_TABLE_NAME, metaRegion.getTable());
      assertEquals(i, metaRegion.getReplicaId());
    }
    createTableWithDefaultConf(tableName, 3);
    List<RegionInfo> regions = admin.getRegions(tableName).get();
    assertEquals(3, metaRegions.size());
    for (int i = 0; i < 3; i++) {
      RegionInfo region = regions.get(i);
      assertEquals(tableName, region.getTable());
      assertEquals(i, region.getReplicaId());
    }
  }
}
