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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test prefetchCacheCostFunction balancer function
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestPrefetchCacheCostBalancer extends StochasticBalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestPrefetchCacheCostBalancer.class);

  private static HBaseTestingUtil TEST_UTIL;

  private static final int REGION_SERVERS = 3;

  private static final int REGION_NUM = REGION_SERVERS * 50;

  private Admin admin;

  private SingleProcessHBaseCluster cluster;

  private Configuration conf;

  private enum FunctionCostKeys {
    REGIONCOUNTCOST("hbase.master.balancer.stochastic.regionCountCost"),
    PRIMARYREGIONCOUNTCOST("hbase.master.balancer.stochastic.primaryRegionCountCost"),
    MOVECOST("hbase.master.balancer.stochastic.moveCost"),
    LOCALITYCOST("hbase.master.balancer.stochastic.localityCost"),
    RACKLOCALITYCOST("hbase.master.balancer.stochastic.rackLocalityCost"),
    TABLESKEWCOST("hbase.master.balancer.stochastic.tableSkewCost"),
    REGIONREPLICAHOSTCOSTKEY("hbase.master.balancer.stochastic.regionReplicaHostCostKey"),
    REGIONREPLICARACKCOSTKEY("hbase.master.balancer.stochastic.regionReplicaRackCostKey"),
    READREQUESTCOST("hbase.master.balancer.stochastic.readRequestCost"),
    CPREQUESTCOST("hbase.master.balancer.stochastic.cpRequestCost"),
    WRITEREQUESTCOST("hbase.master.balancer.stochastic.writeRequestCost"),
    MEMSTORESIZECOST("hbase.master.balancer.stochastic.memstoreSizeCost"),
    STOREFILESIZECOST("hbase.master.balancer.stochastic.storefileSizeCost");

    private final String costKey;

    FunctionCostKeys(String key) {
      this.costKey = key;
    }

    public String getValue() {
      return costKey;
    }
  }

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    conf = TEST_UTIL.getConfiguration();
    Path testDir = TEST_UTIL.getDataTestDir();

    // Enable prefetch persistence which will enable prefetch cache cost function
    Path p = new Path(testDir, "bc.txt");
    FileSystem fs = FileSystem.get(this.conf);
    fs.create(p).close();
    // Must use file based bucket cache here.
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "file:" + p);
    String prefetchPersistencePath = testDir + "/prefetch.persistence";
    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    conf.set(HConstants.PREFETCH_PERSISTENCE_PATH_KEY, prefetchPersistencePath);
    // Must use the ByteBuffAllocator here
    conf.setBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.1f);
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    // 32MB for BucketCache.
    conf.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 32);
  }

  @After
  public void cleanup() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupTestDir();
  }

  // Verify whether the given cost function is enabled/disabled. A function is enabled if the
  // cost key is set to a positive number and disabled if it is set to 0
  private void verifyCostFunctionState(Configuration myConf, String costFunctionName,
    boolean isEnabled) {
    float costKeyValue = myConf.getFloat(costFunctionName, Float.MAX_VALUE);
    assertEquals(isEnabled, costKeyValue > 0.0f);
  }

  @Test
  public void testOnlyPrefetchCacheCostFunctionDisabled() throws Exception {
    // Test strategy
    // 1. Turn off the prefetch cache cost function by setting the parameter
    // hbase.master.balancer.stochastic.prefetchCacheCost to 0
    // 2. Create the cluster
    // 3. Find a region server to shutdown and restart
    // 4. Assert that the region server identified in 3. has more than 1 regions assigned
    // 5. Shutdown the region server
    // 6. Get the number of regions assigned to the other region server and assert that it matched
    // the total number of regions in the cluster
    // 7. Start the region server identified in 3.
    // 8. Trigger the balancer
    // 9. Assert that some regions are assigned to the region server identified in 3.

    // Disable the prefetch cache cost function
    conf.setFloat("hbase.master.balancer.stochastic.prefetchCacheCost", 0.0f);
    loadBalancer.loadConf(conf);

    TEST_UTIL.startMiniCluster(REGION_SERVERS);
    TEST_UTIL.getDFSCluster().waitClusterUp();

    cluster = TEST_UTIL.getHBaseCluster();
    admin = TEST_UTIL.getAdmin();
    admin.balancerSwitch(false, true);
    TableName tableName = TableName.valueOf("testTablePrefetchCacheCostFunctionDisabled");
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    admin.createTable(tableDescriptor, Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), REGION_NUM);
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.loadTable(admin.getConnection().getTable(tableName), HConstants.CATALOG_FAMILY);
    admin.flush(tableName);
    TEST_UTIL.compact(true);

    // Validate that all the other cost functions are enabled
    Arrays.stream(FunctionCostKeys.values())
      .forEach(functionCostKey -> verifyCostFunctionState(admin.getConfiguration(),
        functionCostKey.getValue(), true));

    // Validate that the prefetch cache cost function is disabled
    verifyCostFunctionState(admin.getConfiguration(),
      "hbase.master.balancer.stochastic.prefetchCacheCost", false);

    // Run the balancer and wait until all the region movement is finished
    admin.balancerSwitch(true, true);
    assertTrue("Balancer did not run", admin.balance());
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(120000);

    Map<ServerName, ServerMetrics> ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, ssmap.size());

    // Get the name of the region server to shutdown and restart
    ServerName serverName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    ServerMetrics sm = ssmap.get(serverName);
    // Verify that some regions are assigned to this region server
    assertTrue(0.0f != sm.getRegionMetrics().size());

    // Shutdown the region server and wait for the regions hosted by this server to be reassigned
    // to other region servers
    cluster.stopRegionServer(serverName);
    cluster.waitForRegionServerToStop(serverName, 1000);
    // Compact the table so that all the regions are reassigned to the running region servers
    TEST_UTIL.compact(true);
    TEST_UTIL.waitUntilNoRegionsInTransition(12000);

    ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS - 1, ssmap.size());
    sm = ssmap.get(serverName);
    // Validate that no server metrics is found for the non-active server
    assertNull(sm);

    // Restart the region server and run balancer and validate that some regions are reassigned to
    // this region server
    cluster.startRegionServer(serverName.getHostname(), serverName.getPort());
    // Get the name of the region server
    cluster.waitForRegionServerToStart(serverName.getHostname(), serverName.getPort(), 1000);
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(12000);
    ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, ssmap.size());

    ServerName newServerName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    // Verify that the same region server has been started
    assertTrue(ServerName.isSameAddress(serverName, newServerName));
    sm = ssmap.get(newServerName);

    assertNotNull(sm);
    assertTrue(sm.getRegionMetrics().size() > 0);
  }

  @Test
  public void testOnlyPrefetchCacheCostFunctionEnabled() throws Exception {
    // Test strategy
    // 1. Turn off all other cost functions. NOTE: Please add to the list of cost functions that
    // need
    // to be turned off when a new function is added
    // 2. Create a cluster only with prefetchCacheCostFunction enabled
    // 3. Find a regionserver to shutdown and restart
    // 4. Assert that the region server identified in 3. has more than 1 regions assigned
    // 5. Shutdown the region server identified in 3.
    // 6. Get the number of regions assigned to the other region servers and assert that it matches
    // the total number of regions in the cluster
    // 7. Start the region server identified in 3.
    // 8. Trigger the balancer
    // 9. Assert that no regions are assigned to the region server identified in 3.

    Arrays.stream(FunctionCostKeys.values())
      .forEach(functionCostKey -> conf.setFloat(functionCostKey.getValue(), 0.0f));
    loadBalancer.loadConf(conf);

    TEST_UTIL.startMiniCluster(REGION_SERVERS);
    TEST_UTIL.getDFSCluster().waitClusterUp();
    cluster = TEST_UTIL.getHBaseCluster();
    admin = TEST_UTIL.getAdmin();
    admin.balancerSwitch(false, true);
    TableName tableName = TableName.valueOf("testTableOnlyPrefetchCacheCostFunctionEnabled");
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    admin.createTable(tableDescriptor, Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), REGION_NUM);
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.loadTable(admin.getConnection().getTable(tableName), HConstants.CATALOG_FAMILY);
    admin.flush(tableName);
    TEST_UTIL.compact(true);

    // Validate that all the other cost functions are disabled
    Arrays.stream(FunctionCostKeys.values())
      .forEach(functionCostKey -> verifyCostFunctionState(admin.getConfiguration(),
        functionCostKey.getValue(), false));

    verifyCostFunctionState(admin.getConfiguration(),
      "hbase.master.balancer.stochastic.prefetchCacheCost", true);

    // Run balancer and wait until all the region have been moved
    // started.
    admin.balancerSwitch(true, true);
    assertTrue("Balancer did not run", admin.balance());
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(120000);

    Map<ServerName, ServerMetrics> ssmap = cluster.getClusterMetrics().getLiveServerMetrics();

    assertEquals(REGION_SERVERS, ssmap.size());

    // Shutdown the last server. This is because the server id for an inactive server is reassigned
    // to the next running server. As soon as this server is restarted, it is assigned the next
    // available
    // server id. In our case, we want to track the same server and hence, it's safe to restart the
    // last server in the list
    ServerName serverName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    ServerMetrics sm = ssmap.get(serverName);
    assertTrue(0 != sm.getRegionMetrics().size());

    cluster.stopRegionServer(serverName);
    cluster.waitForRegionServerToStop(serverName, 1000);
    TEST_UTIL.compact(true);
    TEST_UTIL.waitUntilNoRegionsInTransition(12000);
    ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS - 1, ssmap.size());
    sm = ssmap.get(serverName);
    assertNull(sm);

    // Restart the region server
    cluster.startRegionServer(serverName.getHostname(), serverName.getPort());
    cluster.waitForRegionServerToStart(serverName.getHostname(), serverName.getPort(), 1000);
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(120000);
    ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, ssmap.size());

    ServerName newServerName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    // Verify that the same region server has been started
    assertTrue(ServerName.isSameAddress(serverName, newServerName));
    sm = ssmap.get(newServerName);

    assertNotNull(sm);
    assertEquals(0, sm.getRegionMetrics().size());
  }

  @Test
  public void testStressTestWithOnlyPrefetchCacheCostFunctionEnabled() throws Exception {
    // Test the prefetch cache cost returned by the cost function when random servers are
    // restarted and only the PrefetchCacheCostFunction is enabled. Ensure that the prefetch cost
    // returned by the cost function is always between 0 and 1.

    // Disable all other cost functions
    Arrays.stream(FunctionCostKeys.values())
      .forEach(functionCostKey -> conf.setFloat(functionCostKey.getValue(), 0.0f));
    loadBalancer.loadConf(conf);

    TEST_UTIL.startMiniCluster(REGION_SERVERS);
    TEST_UTIL.getDFSCluster().waitClusterUp();
    cluster = TEST_UTIL.getHBaseCluster();
    admin = TEST_UTIL.getAdmin();
    admin.balancerSwitch(false, true);
    TableName tableName = TableName.valueOf("testTableOnlyPrefetchCacheCostFunctionEnabled");
    TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    admin.createTable(tableDescriptor, Bytes.toBytes("aaa"), Bytes.toBytes("zzz"), REGION_NUM);
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.loadTable(admin.getConnection().getTable(tableName), HConstants.CATALOG_FAMILY);
    admin.flush(tableName);
    TEST_UTIL.compact(true);

    // Verify that all the other cost functions except the PrefetchCacheCostFunction are disabled
    Arrays.stream(FunctionCostKeys.values())
      .forEach(functionCostKey -> verifyCostFunctionState(admin.getConfiguration(),
        functionCostKey.getValue(), false));

    verifyCostFunctionState(admin.getConfiguration(),
      "hbase.master.balancer.stochastic.prefetchCacheCost", true);

    admin.balancerSwitch(true, true);
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(120000);

    Random rand = new Random();
    for (int i = 0; i < 5; i++) {
      int randomServerID = rand.nextInt(REGION_SERVERS);
      ServerName sn = cluster.getClusterMetrics().getServersName().get(randomServerID);
      cluster.stopRegionServer(sn);
      cluster.waitForRegionServerToStop(sn, 1000);
      TEST_UTIL.compact(true);
      TEST_UTIL.waitUntilNoRegionsInTransition(12000);
      cluster.startRegionServer(sn.getHostname(), sn.getPort());
      cluster.waitForRegionServerToStart(sn.getHostname(), sn.getPort(), 1000);
      admin.balance();
      // Verify that the same server was restarted
      verifyServerActive(sn);
      assertEquals(REGION_SERVERS, cluster.getClusterMetrics().getLiveServerMetrics().size());
      validatePrefetchCacheCost(loadBalancer.getCostFunctions());
    }
  }

  private void verifyServerActive(ServerName serverName) throws Exception {
    // The server id of the region server may change post restart. The only way to ensure that the
    // same server has been restarted is by searching for the server address (host:port) in the
    // active server list
    boolean found = false;
    for (ServerName sname : cluster.getClusterMetrics().getServersName()) {
      if (ServerName.isSameAddress(sname, serverName)) {
        found = true;
        break;
      }
    }
    assertTrue(found);
  }

  private void validatePrefetchCacheCost(List<CostFunction> cf) {
    for (CostFunction c : cf) {
      if (c.getMultiplier() > 0.0f) {
        assertTrue(c.cost() >= 0.0 && c.cost() <= 1.0);
      }
    }
  }
}
