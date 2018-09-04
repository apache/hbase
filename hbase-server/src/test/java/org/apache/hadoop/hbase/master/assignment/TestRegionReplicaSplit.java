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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.LargeTests;
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

@Category({ RegionServerTests.class, LargeTests.class })
public class TestRegionReplicaSplit {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionReplicaSplit.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestRegionReplicaSplit.class);

  private static final int NB_SERVERS = 4;
  private static Table table;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void beforeClass() throws Exception {
    HTU.getConfiguration().setInt("hbase.master.wait.on.regionservers.mintostart", 3);
    HTU.startMiniCluster(NB_SERVERS);
    final TableName tableName = TableName.valueOf(TestRegionReplicaSplit.class.getSimpleName());

    // Create table then get the single region for our new table.
    createTable(tableName);
  }

  @Rule
  public TestName name = new TestName();

  private static void createTable(final TableName tableName) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setRegionReplication(3);
    // create a table with 3 replication
    table = HTU.createTable(builder.build(), new byte[][] { f }, getSplits(2),
      new Configuration(HTU.getConfiguration()));
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

  public void testRegionReplicaSplitRegionAssignment() throws Exception {
    HTU.loadNumericRows(table, f, 0, 3);
    // split the table
    List<RegionInfo> regions = new ArrayList<RegionInfo>();
    for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      for (Region r : rs.getRegionServer().getRegions(table.getName())) {
        System.out.println("the region before split is is " + r.getRegionInfo()
            + rs.getRegionServer().getServerName());
        regions.add(r.getRegionInfo());
      }
    }
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
    List<ServerName> newRegionLocations = new ArrayList<ServerName>();
    for (RegionServerThread rs : HTU.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionInfo prevInfo = null;
      for (Region r : rs.getRegionServer().getRegions(table.getName())) {
        if (!regions.contains(r.getRegionInfo())
            && !RegionReplicaUtil.isDefaultReplica(r.getRegionInfo())) {
          LOG.info("The region is " + r.getRegionInfo() + " the location is "
              + rs.getRegionServer().getServerName());
          if (!RegionReplicaUtil.isDefaultReplica(r.getRegionInfo())
              && newRegionLocations.contains(rs.getRegionServer().getServerName())
              && prevInfo != null
              && Bytes.equals(prevInfo.getStartKey(), r.getRegionInfo().getStartKey())
              && Bytes.equals(prevInfo.getEndKey(), r.getRegionInfo().getEndKey())) {
            fail("Splitted regions should not be assigned to same region server");
          } else {
            prevInfo = r.getRegionInfo();
            if (!RegionReplicaUtil.isDefaultReplica(r.getRegionInfo())
                && !newRegionLocations.contains(rs.getRegionServer().getServerName())) {
              newRegionLocations.add(rs.getRegionServer().getServerName());
            }
          }
        }
      }
    }
    // since we assign the daughter regions in round robin fashion, both the daugther region
    // replicas will be assigned to two unique servers.
    assertEquals("The new regions should be assigned to 3 unique servers ", 3,
      newRegionLocations.size());
  }
}
