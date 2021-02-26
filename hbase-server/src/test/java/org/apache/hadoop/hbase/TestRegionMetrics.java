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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

@Category({ MiscTests.class, MediumTests.class })
public class TestRegionMetrics {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionMetrics.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionMetrics.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Admin admin;

  private static final TableName TABLE_1 = TableName.valueOf("table_1");
  private static final TableName TABLE_2 = TableName.valueOf("table_2");
  private static final TableName TABLE_3 = TableName.valueOf("table_3");
  private static final TableName[] tables = new TableName[] { TABLE_1, TABLE_2, TABLE_3 };
  private static final int MSG_INTERVAL = 500; // ms

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Make servers report eagerly. This test is about looking at the cluster status reported.
    // Make it so we don't have to wait around too long to see change.
    UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", MSG_INTERVAL);
    UTIL.startMiniCluster(4);
    admin = UTIL.getAdmin();
    admin.balancerSwitch(false, true);
    byte[] FAMILY = Bytes.toBytes("f");
    for (TableName tableName : tables) {
      Table table = UTIL.createMultiRegionTable(tableName, FAMILY, 16);
      UTIL.waitTableAvailable(tableName);
      UTIL.loadTable(table, FAMILY);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    for (TableName table : tables) {
      UTIL.deleteTableIfAny(table);
    }
    UTIL.shutdownMiniCluster();
  }


  @Test
  public void testRegionMetrics() throws Exception {

    // Check if regions match with the RegionMetrics from the server
    for (ServerName serverName : admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
        .getLiveServerMetrics().keySet()) {
      List<RegionInfo> regions = admin.getRegions(serverName);
      Collection<RegionMetrics> regionMetricsList =
          admin.getRegionMetrics(serverName);
      checkRegionsAndRegionMetrics(regions, regionMetricsList);
    }

    // Check if regionMetrics matches the table's regions and nothing is missed
    for (TableName table : new TableName[] { TABLE_1, TABLE_2, TABLE_3 }) {
      List<RegionInfo> tableRegions = admin.getRegions(table);

      List<RegionMetrics> regionMetrics = new ArrayList<>();
      for (ServerName serverName : admin.getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS))
          .getLiveServerMetrics().keySet()) {
        regionMetrics.addAll(admin.getRegionMetrics(serverName, table));
      }
      checkRegionsAndRegionMetrics(tableRegions, regionMetrics);
    }

    // Just wait here. If this fixes the test, come back and do a better job.
    // Would have to redo the below so can wait on cluster status changing.
    // Admin#getClusterMetrics retrieves data from HMaster. Admin#getRegionMetrics, by contrast,
    // get the data from RS. Hence, it will fail if we do the assert check before RS has done
    // the report.
    TimeUnit.MILLISECONDS.sleep(3 * MSG_INTERVAL);

    // Check RegionMetrics matches the RegionMetrics from ClusterMetrics
    for (Map.Entry<ServerName, ServerMetrics> entry : admin
      .getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().entrySet()) {
      ServerName serverName = entry.getKey();
      ServerMetrics serverMetrics = entry.getValue();
      List<RegionMetrics> regionMetrics = admin.getRegionMetrics(serverName);
      LOG.debug("serverName=" + serverName + ", getRegionLoads=" +
        serverMetrics.getRegionMetrics().keySet().stream().map(r -> Bytes.toString(r)).
          collect(Collectors.toList()));
      LOG.debug("serverName=" + serverName + ", regionLoads=" +
        regionMetrics.stream().map(r -> Bytes.toString(r.getRegionName())).
          collect(Collectors.toList()));
      assertEquals(serverMetrics.getRegionMetrics().size(), regionMetrics.size());
      checkMetricsValue(regionMetrics, serverMetrics);
    }
  }

  private void checkMetricsValue(List<RegionMetrics> regionMetrics, ServerMetrics serverMetrics)
    throws InvocationTargetException, IllegalAccessException {
    for (RegionMetrics fromRM : regionMetrics) {
      RegionMetrics fromSM = serverMetrics.getRegionMetrics().get(fromRM.getRegionName());
      Class clazz = RegionMetrics.class;
      for (Method method : clazz.getMethods()) {
        // check numeric values only
        if (method.getReturnType().equals(Size.class)
          || method.getReturnType().equals(int.class)
          || method.getReturnType().equals(long.class)
          || method.getReturnType().equals(float.class)) {
          Object valueRm = method.invoke(fromRM);
          Object valueSM = method.invoke(fromSM);
          assertEquals("Return values of method " + method.getName() + " are different",
              valueRm.toString(), valueSM.toString());
        }
      }
    }
  }

  private void checkRegionsAndRegionMetrics(Collection<RegionInfo> regions,
      Collection<RegionMetrics> regionMetrics) {

    assertEquals("No of regions and regionMetrics doesn't match", regions.size(),
        regionMetrics.size());

    Map<byte[], RegionMetrics> regionMetricsMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (RegionMetrics r : regionMetrics) {
      regionMetricsMap.put(r.getRegionName(), r);
    }
    for (RegionInfo info : regions) {
      assertTrue("Region not in RegionMetricsMap region:"
          + info.getRegionNameAsString() + " regionMap: "
          + regionMetricsMap, regionMetricsMap.containsKey(info.getRegionName()));
    }
  }
}
