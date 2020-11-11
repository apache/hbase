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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicasWithModifyTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionReplicasWithModifyTable.class);

  private static final int NB_SERVERS = 3;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @Parameter
  public boolean disableBeforeModifying;

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { true }, new Object[] { false });
  }

  @BeforeClass
  public static void before() throws Exception {
    HTU.startMiniCluster(NB_SERVERS);
  }

  private void enableReplicationByModification(boolean withReplica, int initialReplicaCount,
    int enableReplicaCount, int splitCount) throws IOException, InterruptedException {
    TableName tableName = name.getTableName();
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    if (withReplica) {
      builder.setRegionReplication(initialReplicaCount);
    }
    TableDescriptor htd = builder.build();
    if (splitCount > 0) {
      byte[][] splits = getSplits(splitCount);
      HTU.createTable(htd, new byte[][] { f }, splits, new Configuration(HTU.getConfiguration()));
    } else {
      HTU.createTable(htd, new byte[][] { f }, (byte[][]) null,
        new Configuration(HTU.getConfiguration()));
    }
    if (disableBeforeModifying) {
      HTU.getAdmin().disableTable(tableName);
    }
    HBaseTestingUtility.setReplicas(HTU.getAdmin(), tableName, enableReplicaCount);
    if (disableBeforeModifying) {
      HTU.getAdmin().enableTable(tableName);
    }
    int expectedRegionCount;
    if (splitCount > 0) {
      expectedRegionCount = enableReplicaCount * splitCount;
    } else {
      expectedRegionCount = enableReplicaCount;
    }
    assertTotalRegions(expectedRegionCount);
  }

  private static byte[][] getSplits(int numRegions) {
    RegionSplitter.UniformSplit split = new RegionSplitter.UniformSplit();
    split.setFirstRow(Bytes.toBytes(0L));
    split.setLastRow(Bytes.toBytes(Long.MAX_VALUE));
    return split.split(numRegions);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws IOException {
    TableName tableName = name.getTableName();
    HTU.getAdmin().disableTable(tableName);
    HTU.getAdmin().deleteTable(tableName);
  }

  private void assertTotalRegions(int expected) {
    int actual = HTU.getHBaseCluster().getRegions(name.getTableName()).size();
    assertEquals(expected, actual);
  }

  @Test
  public void testRegionReplicasUsingEnableTable() throws Exception {
    enableReplicationByModification(false, 0, 3, 0);
  }

  @Test
  public void testRegionReplicasUsingEnableTableForMultipleRegions() throws Exception {
    enableReplicationByModification(false, 0, 3, 10);
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsIncreased() throws Exception {
    enableReplicationByModification(true, 2, 3, 0);
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsDecreased() throws Exception {
    enableReplicationByModification(true, 3, 2, 0);
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsDecreasedWithMultipleRegions()
    throws Exception {
    enableReplicationByModification(true, 3, 2, 20);
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsIncreasedWithmultipleRegions()
    throws Exception {
    enableReplicationByModification(true, 2, 3, 15);
  }
}
