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
package org.apache.hadoop.hbase.master.balancer;

import static org.apache.hadoop.hbase.master.balancer.BalancerConditionalsTestUtil.validateAssertionsWithRetries;

import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ LargeTests.class, MasterTests.class })
public class TestReplicaDistributionBalancerConditional {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicaDistributionBalancerConditional.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestReplicaDistributionBalancerConditional.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int REPLICAS = 3;
  private static final int NUM_SERVERS = REPLICAS;
  private static final int REGIONS_PER_SERVER = 5;

  @Before
  public void setUp() throws Exception {
    DistributeReplicasTestConditional
      .enableConditionalReplicaDistributionForTest(TEST_UTIL.getConfiguration());
    TEST_UTIL.getConfiguration()
      .setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setLong(HConstants.HBASE_BALANCER_PERIOD, 1000L);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.balancer.stochastic.runMaxSteps", true);

    // turn off replica cost functions
    TEST_UTIL.getConfiguration()
      .setLong("hbase.master.balancer.stochastic.regionReplicaRackCostKey", 0);
    TEST_UTIL.getConfiguration()
      .setLong("hbase.master.balancer.stochastic.regionReplicaHostCostKey", 0);

    TEST_UTIL.startMiniCluster(NUM_SERVERS);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testReplicaDistribution() throws Exception {
    Connection connection = TEST_UTIL.getConnection();
    Admin admin = connection.getAdmin();

    // Create a "replicated_table" with region replicas
    TableName replicatedTableName = TableName.valueOf("replicated_table");
    TableDescriptor replicatedTableDescriptor =
      TableDescriptorBuilder.newBuilder(replicatedTableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("0")).build())
        .setRegionReplication(REPLICAS).build();
    admin.createTable(replicatedTableDescriptor,
      BalancerConditionalsTestUtil.generateSplits(REGIONS_PER_SERVER * NUM_SERVERS));

    // Pause the balancer
    admin.balancerSwitch(false, true);

    // Collect all region replicas and place them on one RegionServer
    List<RegionInfo> allRegions = admin.getRegions(replicatedTableName);
    String targetServer =
      TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName().getServerName();

    for (RegionInfo region : allRegions) {
      admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(targetServer));
    }

    BalancerConditionalsTestUtil.printRegionLocations(TEST_UTIL.getConnection());
    validateAssertionsWithRetries(TEST_UTIL, false, () -> BalancerConditionalsTestUtil
      .validateReplicaDistribution(connection, replicatedTableName, false));

    // Unpause the balancer and trigger balancing
    admin.balancerSwitch(true, true);
    admin.balance();

    validateAssertionsWithRetries(TEST_UTIL, true, () -> BalancerConditionalsTestUtil
      .validateReplicaDistribution(connection, replicatedTableName, true));
    BalancerConditionalsTestUtil.printRegionLocations(TEST_UTIL.getConnection());
  }
}
