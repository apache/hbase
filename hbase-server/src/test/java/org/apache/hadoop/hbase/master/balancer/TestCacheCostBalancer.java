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

import static org.apache.hadoop.hbase.HConstants.BUCKET_CACHE_PERSISTENT_PATH_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test prefetchCacheCostFunction balancer function
 */
@Ignore
@Category({ MasterTests.class, MediumTests.class })
public class TestCacheCostBalancer extends BalancerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCacheCostBalancer.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestCacheCostBalancer.class);

  private static HBaseTestingUtility TEST_UTIL;

  private static final int REGION_SERVERS = 5;

  private static final int REGION_NUM = REGION_SERVERS * 5;

  private Admin admin;

  private MiniHBaseCluster cluster;

  protected static Configuration conf;

  private enum FunctionCostKeys {
    REGIONCOUNTCOST("hbase.master.balancer.stochastic.regionCountCost"),
    PREFETCHCOST("hbase.master.balancer.stochastic.prefetchCacheCost");

    private final String costKey;

    FunctionCostKeys(String key) {
      this.costKey = key;
    }

    public String getValue() {
      return costKey;
    }
  }

  @BeforeClass
  public static void beforeAllTests() throws Exception {
  }

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS,
      CacheAwareLoadBalancer.class, LoadBalancer.class);
    verifyCostFunctionState(conf, FunctionCostKeys.PREFETCHCOST.costKey, true);
    verifyCostFunctionState(conf, FunctionCostKeys.REGIONCOUNTCOST.costKey, true);
    loadBalancer = new CacheAwareLoadBalancer();
    loadBalancer.setConf(conf);
    Path testDir = TEST_UTIL.getDataTestDir();

    // Enable prefetch persistence which will enable prefetch cache cost function
    Path p = new Path(testDir, "bc.txt");
    FileSystem fs = FileSystem.get(this.conf);
    fs.create(p).close();
    // Must use file based bucket cache here.
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "file:" + p);
    String prefetchPersistencePath = testDir + "/prefetch.persistence";
    conf.setBoolean(CacheConfig.PREFETCH_BLOCKS_ON_OPEN_KEY, true);
    conf.set(BUCKET_CACHE_PERSISTENT_PATH_KEY, prefetchPersistencePath);
    // Must use the ByteBuffAllocator here
    conf.setBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.1f);
    // 32MB for BucketCache.
    conf.setFloat(HConstants.BUCKET_CACHE_SIZE_KEY, 32);
    conf.set(CacheConfig.EVICT_BLOCKS_ON_CLOSE_KEY, "false");
  }

  @After
  public void cleanup() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    TEST_UTIL.cleanupTestDir();
  }

  private ServerName getServerNameAfterRestart(ServerName serverName) throws IOException {
    for (ServerName sname : cluster.getClusterMetrics().getServersName()) {
      if (ServerName.isSameAddress(sname, serverName)) {
        return sname;
      }
    }
    return null;
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

  // Verify whether the given cost function is enabled/disabled. A function is enabled if the
  // cost key is set to a positive number and disabled if it is set to 0
  private void verifyCostFunctionState(Configuration myConf, String costFunctionName,
    boolean isEnabled) {
    float costKeyValue = myConf.getFloat(costFunctionName, Float.MAX_VALUE);
    assertEquals(isEnabled, costKeyValue > 0.0f);
  }

  private void validateAllOldRegionsAreAssignedBack(List<String> oldList, List<String> newList) {
    // Validate that all the regions in the old list are in the new list
    assertTrue(newList.containsAll(oldList));
  }

  private List<String> getRegionsOnServer(Map<ServerName, ServerMetrics> metricsMap,
    TableName tableName, ServerName serverName) throws IOException {
    List<String> regionList = new ArrayList<>();
    List<HRegion> tableRegions = cluster.getRegions(tableName);
    tableRegions.forEach(hr -> {
      regionList.add(hr.getRegionInfo().getRegionNameAsString());
    });
    return regionList;
  }

  private List<String> getListOfPrefetchedRegionsOnServer(Map<ServerName, ServerMetrics> metricsMap,
    TableName tableName, ServerName serverName) throws IOException {
    List<String> regionList = new ArrayList<>();

    List<HRegion> tableRegions = cluster.getRegions(tableName);
    metricsMap.get(serverName).getRegionMetrics().forEach((rn, rm) -> {
      tableRegions.forEach(r -> {
        if (r.getRegionInfo().getRegionNameAsString().equals(rm.getNameAsString()) &&
          rm.getCurrentRegionCachedRatio() == 1.0f) {
          regionList.add(rm.getNameAsString());
        }
      });
    });
    return regionList;
  }

  @Test
  public void testBasicBalance() throws Exception {
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

    // Validate that only skewness cost function is enabled
    verifyCostFunctionState(conf, FunctionCostKeys.REGIONCOUNTCOST.costKey, true);
    verifyCostFunctionState(conf, FunctionCostKeys.PREFETCHCOST.costKey, true);

    // Run the balancer and wait until all the region movement is finished
    admin.balancerSwitch(true, true);
    assertTrue("Balancer did not run", admin.balance());
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(120000);

    // Get the list of regions for the table and create a map of region --> server
    Map<ServerName, ServerMetrics> ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, ssmap.size());
    TEST_UTIL.waitForMajorityRegionsForTableToBePrefetched(tableName, 120000);

    // Get the name of the region server to shutdown and restart
    ServerName serverName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    ServerMetrics sm = ssmap.get(serverName);
    // Verify that some regions are assigned to this region server
    assertTrue(0.0f != sm.getRegionMetrics().size());

    // Get the list of regions on this server
    List<String> originalRegionList = getListOfPrefetchedRegionsOnServer(ssmap, tableName, serverName);

    // Shutdown the region server and wait for the regions hosted by this server to be reassigned
    // to other region servers
    cluster.stopRegionServer(serverName);
    cluster.waitForRegionServerToStop(serverName, 1000);
    // Compact the table so that all the regions are reassigned to the running region servers
    TEST_UTIL.compact(true);
    TEST_UTIL.waitUntilNoRegionsInTransition(12000);

    Map<ServerName, ServerMetrics> newssmap = admin.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS - 1, newssmap.size());
    sm = newssmap.get(serverName);
    // Validate that no server metrics is found for the non-active server
    assertNull(sm);

    // Restart the region server and run balancer and validate that some regions are reassigned to
    // this region server
    cluster.startRegionServer(serverName.getHostname(), serverName.getPort());
    // Get the name of the region server
    cluster.waitForRegionServerToStart(serverName.getHostname(), serverName.getPort(), 1000);
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(12000);
    Map<ServerName, ServerMetrics> postrestartssmap = admin.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, postrestartssmap.size());

    ServerName newServerName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    assertTrue(ServerName.isSameAddress(serverName, newServerName));
    sm = postrestartssmap.get(newServerName);

    assertNotNull(sm);
    assertTrue(sm.getRegionMetrics().size() > 0);

    List<String> newRegionList = getRegionsOnServer(postrestartssmap, tableName, newServerName);
    assertTrue(!newRegionList.isEmpty());
    validateAllOldRegionsAreAssignedBack(originalRegionList, newRegionList);
  }

  @Test
  public void testOnlySkewnessCostFunctionEnabled() throws Exception {
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
    conf.setFloat(FunctionCostKeys.PREFETCHCOST.costKey, 0.0f);
    loadBalancer.setConf(conf);

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

    // Validate that only skewness cost function is enabled
    verifyCostFunctionState(conf, FunctionCostKeys.REGIONCOUNTCOST.costKey, true);
    verifyCostFunctionState(conf, FunctionCostKeys.PREFETCHCOST.costKey, false);

    // Run the balancer and wait until all the region movement is finished
    admin.balancerSwitch(true, true);
    assertTrue("Balancer did not run", admin.balance());
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(120000);

    // Get the list of regions for the table and create a map of region --> server
    Map<ServerName, ServerMetrics> ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, ssmap.size());
    TEST_UTIL.waitForMajorityRegionsForTableToBePrefetched(tableName, 120000);

    // Get the name of the region server to shutdown and restart
    ServerName serverName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    ServerMetrics sm = ssmap.get(serverName);
    // Verify that some regions are assigned to this region server
    assertTrue(0.0f != sm.getRegionMetrics().size());

    // Get the list of regions on this server
    List<String> originalRegionList = getListOfPrefetchedRegionsOnServer(ssmap, tableName, serverName);

    // Shutdown the region server and wait for the regions hosted by this server to be reassigned
    // to other region servers
    cluster.stopRegionServer(serverName);
    cluster.waitForRegionServerToStop(serverName, 1000);
    // Compact the table so that all the regions are reassigned to the running region servers
    TEST_UTIL.compact(true);
    TEST_UTIL.waitUntilNoRegionsInTransition(12000);

    Map<ServerName, ServerMetrics> newssmap = admin.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS - 1, newssmap.size());
    sm = newssmap.get(serverName);
    // Validate that no server metrics is found for the non-active server
    assertNull(sm);

    // Restart the region server and run balancer and validate that some regions are reassigned to
    // this region server
    cluster.startRegionServer(serverName.getHostname(), serverName.getPort());
    // Get the name of the region server
    cluster.waitForRegionServerToStart(serverName.getHostname(), serverName.getPort(), 1000);
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(12000);
    Map<ServerName, ServerMetrics> postrestartssmap = admin.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, postrestartssmap.size());

    ServerName newServerName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    assertTrue(ServerName.isSameAddress(serverName, newServerName));
    sm = postrestartssmap.get(newServerName);

    assertNotNull(sm);
    assertTrue(sm.getRegionMetrics().size() > 0);

    List<String> newRegionList = getRegionsOnServer(postrestartssmap, tableName, newServerName);
    assertTrue(!newRegionList.isEmpty());
    validateAllOldRegionsAreAssignedBack(originalRegionList, newRegionList);
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

    // Disable the prefetch cache cost function
    conf.setFloat(FunctionCostKeys.REGIONCOUNTCOST.costKey, 0.0f);
    // Make the balancer very aggressive
    conf.setFloat("hbase.master.balancer.stochastic.minCostNeedBalance", 0.01f);
    loadBalancer.setConf(conf);

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

    // Validate that only prefetch cache cost function is enabled
    verifyCostFunctionState(conf, FunctionCostKeys.REGIONCOUNTCOST.costKey, false);
    verifyCostFunctionState(conf, FunctionCostKeys.PREFETCHCOST.costKey, true);

    // Run the balancer and wait until all the region movement is finished
    admin.balancerSwitch(true, true);
    assertTrue("Balancer did not run", admin.balance());
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(120000);

    // Get the list of regions for the table and create a map of region --> server
    Map<ServerName, ServerMetrics> ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, ssmap.size());
    TEST_UTIL.waitForMajorityRegionsForTableToBePrefetched(tableName, 120000);

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

    Map<ServerName, ServerMetrics> newssmap = admin.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS - 1, newssmap.size());
    sm = newssmap.get(serverName);
    // Validate that no server metrics is found for the non-active server
    assertNull(sm);

    // Restart the region server and run balancer and validate that some regions are reassigned to
    // this region server
    cluster.startRegionServer(serverName.getHostname(), serverName.getPort());
    // Get the name of the region server
    cluster.waitForRegionServerToStart(serverName.getHostname(), serverName.getPort(), 1000);
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(12000);
    Map<ServerName, ServerMetrics> postrestartssmap = admin.getClusterMetrics().getLiveServerMetrics();
    assertEquals(REGION_SERVERS, postrestartssmap.size());

    ServerName newServerName = cluster.getClusterMetrics().getServersName().get(REGION_SERVERS - 1);
    assertTrue(ServerName.isSameAddress(serverName, newServerName));
    sm = postrestartssmap.get(newServerName);

    assertNotNull(sm);

    // Since the skewness cost function is disabled and the regions get prefetched almost immediately on the new server
    // the prefetch cost is less than the min balance cost needed
    assertTrue(sm.getRegionMetrics().size() == 0);
  }

  @Test
  public void testStressTest() throws Exception {
    // Test the prefetch cache cost returned by the cost function when random servers are
    // restarted and only the PrefetchCacheCostFunction is enabled. Ensure that the prefetch cost
    // returned by the cost function is always between 0 and 1.

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

    admin.balancerSwitch(true, true);
    admin.balance();
    TEST_UTIL.waitUntilNoRegionsInTransition(120000);

    Random rand = new Random();
    for (int i = 0; i < 5; i++) {
      int randomServerID = rand.nextInt(REGION_SERVERS);
      ServerName sn = cluster.getClusterMetrics().getServersName().get(randomServerID);
      Map<ServerName, ServerMetrics> ssmap = cluster.getClusterMetrics().getLiveServerMetrics();
      TEST_UTIL.waitForAtleastOneRegionToBePrefetchedOnServer(tableName, sn, 10000);
      List<String> oldRegionList = getListOfPrefetchedRegionsOnServer(ssmap, tableName, sn);
      cluster.stopRegionServer(sn);
      cluster.waitForRegionServerToStop(sn, 1000);
      TEST_UTIL.compact(true);
      TEST_UTIL.waitUntilNoRegionsInTransition(12000);
      cluster.startRegionServer(sn.getHostname(), sn.getPort());
      cluster.waitForRegionServerToStart(sn.getHostname(), sn.getPort(), 1000);
      admin.balance();
      // Verify that the same server was restarted
      verifyServerActive(sn);

      // Get the new ServerName for the restarted server
      Map<ServerName, ServerMetrics> newssmap = cluster.getClusterMetrics().getLiveServerMetrics();
      ServerName newsn = getServerNameAfterRestart(sn);
      List<String> newRegionList = getRegionsOnServer(newssmap, tableName, newsn);
      assertEquals(REGION_SERVERS, cluster.getClusterMetrics().getLiveServerMetrics().size());
      validateAllOldRegionsAreAssignedBack(oldRegionList, newRegionList);
    }
  }
}
