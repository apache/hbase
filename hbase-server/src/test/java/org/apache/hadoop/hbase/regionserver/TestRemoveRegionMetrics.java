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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompatibilityFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({RegionServerTests.class, LargeTests.class})
public class TestRemoveRegionMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRemoveRegionMetrics.class);

  private static MiniHBaseCluster cluster;
  private static Configuration conf;
  private static HBaseTestingUtility TEST_UTIL;
  private static MetricsAssertHelper metricsHelper;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void startCluster() throws Exception {
    metricsHelper = CompatibilityFactory.getInstance(MetricsAssertHelper.class);
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);

    TEST_UTIL.startMiniCluster(2);
    cluster = TEST_UTIL.getHBaseCluster();

    cluster.waitForActiveAndReadyMaster();

    while (cluster.getLiveRegionServerThreads().size() < 2) {
      Threads.sleep(100);
    }
  }


  @Test
  public void testMoveRegion() throws IOException, InterruptedException {
    String tableNameString = name.getMethodName();
    TableName tableName = TableName.valueOf(tableNameString);
    Table t = TEST_UTIL.createTable(tableName, Bytes.toBytes("D"));
    TEST_UTIL.waitUntilAllRegionsAssigned(t.getName());
    Admin admin = TEST_UTIL.getAdmin();
    HRegionInfo regionInfo;
    byte[] row =  Bytes.toBytes("r1");


    for (int i = 0; i < 30; i++) {
      boolean moved = false;
      try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(tableName)) {
        regionInfo = locator.getRegionLocation(row, true).getRegionInfo();
      }

      int currentServerIdx = cluster.getServerWith(regionInfo.getRegionName());
      int destServerIdx = (currentServerIdx +1)% cluster.getLiveRegionServerThreads().size();
      HRegionServer currentServer = cluster.getRegionServer(currentServerIdx);
      HRegionServer destServer = cluster.getRegionServer(destServerIdx);


      // Do a put. The counters should be non-zero now
      Put p = new Put(row);
      p.addColumn(Bytes.toBytes("D"), Bytes.toBytes("Zero"), Bytes.toBytes("VALUE"));
      t.put(p);


      MetricsRegionAggregateSource currentAgg = currentServer.getRegion(regionInfo.getRegionName())
          .getMetrics()
          .getSource()
          .getAggregateSource();

      String prefix = "namespace_"+ NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR+
          "_table_"+tableNameString +
          "_region_" + regionInfo.getEncodedName()+
          "_metric";

      metricsHelper.assertCounter(prefix + "_putCount", 1, currentAgg);


      try {
        TEST_UTIL.moveRegionAndWait(regionInfo, destServer.getServerName());
        moved = true;
      } catch (IOException ioe) {
        moved = false;
      }

      if (moved) {
        MetricsRegionAggregateSource destAgg = destServer.getRegion(regionInfo.getRegionName())
            .getMetrics()
            .getSource()
            .getAggregateSource();
        metricsHelper.assertCounter(prefix + "_putCount", 0, destAgg);
      }
    }

    TEST_UTIL.deleteTable(tableName);

  }
}
