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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicasWithModifyTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicasWithModifyTable.class);

  private static final int NB_SERVERS = 3;
  private static Table table;
  private static final byte[] row = "TestRegionReplicasWithModifyTable".getBytes();

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void before() throws Exception {
    HTU.startMiniCluster(NB_SERVERS);
  }

  private static void enableReplicationByModification(final TableName tableName,
      boolean withReplica, int initialReplicaCount, int enableReplicaCount, int splitCount)
      throws IOException, InterruptedException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    if (withReplica) {
      htd.setRegionReplication(initialReplicaCount);
    }
    if (splitCount > 0) {
      byte[][] splits = getSplits(splitCount);
      table = HTU.createTable(htd, new byte[][] { f }, splits,
        new Configuration(HTU.getConfiguration()));

    } else {
      table = HTU.createTable(htd, new byte[][] { f }, (byte[][]) null,
        new Configuration(HTU.getConfiguration()));
    }
    HBaseTestingUtility.setReplicas(HTU.getAdmin(), table.getName(), enableReplicaCount);
  }

  private static byte[][] getSplits(int numRegions) {
    RegionSplitter.UniformSplit split = new RegionSplitter.UniformSplit();
    split.setFirstRow(Bytes.toBytes(0L));
    split.setLastRow(Bytes.toBytes(Long.MAX_VALUE));
    return split.split(numRegions);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HRegionServer.TEST_SKIP_REPORTING_TRANSITION = false;
    table.close();
    HTU.shutdownMiniCluster();
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private HRegionServer getSecondaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(1);
  }

  private HRegionServer getTertiaryRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(2);
  }

  @Test
  public void testRegionReplicasUsingEnableTable() throws Exception {
    TableName tableName = null;
    try {
      tableName = TableName.valueOf(name.getMethodName());
      enableReplicationByModification(tableName, false, 0, 3, 0);
      List<HRegion> onlineRegions = getRS().getRegions(tableName);
      List<HRegion> onlineRegions2 = getSecondaryRS().getRegions(tableName);
      List<HRegion> onlineRegions3 = getTertiaryRS().getRegions(tableName);
      int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
      assertEquals("the number of regions should be more than 1", 3, totalRegions);
    } finally {
      disableAndDeleteTable(tableName);
    }
  }

  private void disableAndDeleteTable(TableName tableName) throws IOException {
    HTU.getAdmin().disableTable(tableName);
    HTU.getAdmin().deleteTable(tableName);
  }

  @Test
  public void testRegionReplicasUsingEnableTableForMultipleRegions() throws Exception {
    TableName tableName = null;
    try {
      tableName = TableName.valueOf(name.getMethodName());
      enableReplicationByModification(tableName, false, 0, 3, 10);
      List<HRegion> onlineRegions = getRS().getRegions(tableName);
      List<HRegion> onlineRegions2 = getSecondaryRS().getRegions(tableName);
      List<HRegion> onlineRegions3 = getTertiaryRS().getRegions(tableName);
      int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
      assertEquals("the number of regions should be equal to 30", 30, totalRegions);
    } finally {
      disableAndDeleteTable(tableName);
    }
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsIncreased() throws Exception {
    TableName tableName = null;
    try {
      tableName = TableName.valueOf(name.getMethodName());
      enableReplicationByModification(tableName, true, 2, 3, 0);
      List<HRegion> onlineRegions = getRS().getRegions(tableName);
      List<HRegion> onlineRegions2 = getSecondaryRS().getRegions(tableName);
      List<HRegion> onlineRegions3 = getTertiaryRS().getRegions(tableName);
      int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
      assertEquals("the number of regions should be 3", 3, totalRegions);
    } finally {
      disableAndDeleteTable(tableName);
    }
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsDecreased() throws Exception {
    TableName tableName = null;
    try {
      tableName = TableName.valueOf(name.getMethodName());
      enableReplicationByModification(tableName, true, 3, 2, 0);
      List<HRegion> onlineRegions = getRS().getRegions(tableName);
      List<HRegion> onlineRegions2 = getSecondaryRS().getRegions(tableName);
      List<HRegion> onlineRegions3 = getTertiaryRS().getRegions(tableName);
      int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
      assertEquals("the number of regions should be reduced to 2", 2, totalRegions);
    } finally {
      disableAndDeleteTable(tableName);
    }
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsDecreasedWithMultipleRegions()
      throws Exception {
    TableName tableName = null;
    try {
      tableName = TableName.valueOf(name.getMethodName());
      enableReplicationByModification(tableName, true, 3, 2, 20);
      List<HRegion> onlineRegions = getRS().getRegions(tableName);
      List<HRegion> onlineRegions2 = getSecondaryRS().getRegions(tableName);
      List<HRegion> onlineRegions3 = getTertiaryRS().getRegions(tableName);
      int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
      assertEquals("the number of regions should be reduced to 40", 40, totalRegions);
    } finally {
      disableAndDeleteTable(tableName);
    }
  }

  @Test
  public void testRegionReplicasByEnableTableWhenReplicaCountIsIncreasedWithmultipleRegions()
      throws Exception {
    TableName tableName = null;
    try {
      tableName = TableName.valueOf(name.getMethodName());
      enableReplicationByModification(tableName, true, 2, 3, 15);
      List<HRegion> onlineRegions = getRS().getRegions(tableName);
      List<HRegion> onlineRegions2 = getSecondaryRS().getRegions(tableName);
      List<HRegion> onlineRegions3 = getTertiaryRS().getRegions(tableName);
      int totalRegions = onlineRegions.size() + onlineRegions2.size() + onlineRegions3.size();
      assertEquals("the number of regions should be equal to 45", 3 * 15, totalRegions);
    } finally {
      disableAndDeleteTable(tableName);
    }
  }
}
