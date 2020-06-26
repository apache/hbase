/**
 *
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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertNotEquals;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaTestHelper;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestRegionReplicaSplit {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicaSplit.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestRegionReplicaSplit.class);

  private static final int NB_SERVERS = 4;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void beforeClass() throws Exception {
    HTU.getConfiguration().setInt("hbase.master.wait.on.regionservers.mintostart", 3);
    HTU.startMiniCluster(NB_SERVERS);
  }

  @Rule
  public TestName name = new TestName();

  private static Table createTableAndLoadData(final TableName tableName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setRegionReplication(3);
    // create a table with 3 replication
    Table table = HTU.createTable(builder.build(), new byte[][] { f }, getSplits(2),
      new Configuration(HTU.getConfiguration()));
    HTU.loadTable(HTU.getConnection().getTable(tableName), f);
    return table;
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
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testRegionReplicaSplitRegionAssignment() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    Table table = null;
    try {
      table = createTableAndLoadData(tn);
      HTU.loadNumericRows(table, f, 0, 3);
      // split the table
      List<RegionInfo> regions = new ArrayList<RegionInfo>();
      for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
        for (Region r : rs.getRegionServer().getRegions(table.getName())) {
          regions.add(r.getRegionInfo());
        }
      }
      // There are 6 regions before split, 9 regions after split.
      HTU.getAdmin().split(table.getName(), Bytes.toBytes(1));
      int count = 0;
      while (true) {
        for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
          for (Region r : rs.getRegionServer().getRegions(table.getName())) {
            count++;
          }
        }
        if (count >= 9) {
          break;
        }
        count = 0;
      }
      RegionReplicaTestHelper.assertReplicaDistributed(HTU, table);
    } finally {
      if (table != null) {
        HTU.deleteTable(tn);
      }
    }
  }

  @Test
  public void testAssignFakeReplicaRegion() throws Exception {
    TableName tn = TableName.valueOf(this.name.getMethodName());
    Table table = null;
    try {
      table = createTableAndLoadData(tn);
      final RegionInfo fakeHri =
        RegionInfoBuilder.newBuilder(table.getName()).setStartKey(Bytes.toBytes("a"))
          .setEndKey(Bytes.toBytes("b")).setReplicaId(1)
          .setRegionId(System.currentTimeMillis()).build();

      // To test AssignProcedure can defend this case.
      HTU.getMiniHBaseCluster().getMaster().getAssignmentManager().assign(fakeHri);
      // Wait until all assigns are done.
      HBaseTestingUtility.await(50, () -> {
        return HTU.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor().getActiveProcIds()
          .isEmpty();
      });

      // Make sure the region is not online.
      for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
        for (Region r : rs.getRegionServer().getRegions(table.getName())) {
          assertNotEquals(r.getRegionInfo(), fakeHri);
        }
      }
    } finally {
      if (table != null) {
        HTU.deleteTable(tn);
      }
    }
  }
}
