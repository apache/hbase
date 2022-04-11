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

import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;
import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestCatalogReplicaLoadBalanceSimpleSelector {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCatalogReplicaLoadBalanceSimpleSelector.class);

  private static final Logger LOG = LoggerFactory.getLogger(
    TestCatalogReplicaLoadBalanceSimpleSelector.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int NB_SERVERS = 4;
  private static int numOfMetaReplica = NB_SERVERS - 1;

  private static AsyncConnectionImpl CONN;

  private static ConnectionRegistry registry;
  private static Admin admin;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    TEST_UTIL.startMiniCluster(NB_SERVERS);
    admin = TEST_UTIL.getAdmin();
    admin.balancerSwitch(false, true);

    // Enable hbase:meta replication.
    HBaseTestingUtility.setReplicas(admin, TableName.META_TABLE_NAME, numOfMetaReplica);
    TEST_UTIL.waitFor(30000, () -> TEST_UTIL.getMiniHBaseCluster().getRegions(
      TableName.META_TABLE_NAME).size() >= numOfMetaReplica);

    registry = ConnectionRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
    CONN = new AsyncConnectionImpl(conf, registry,
      registry.getClusterId().get(), User.getCurrent());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaChangeFromReplicaNoReplica() throws IOException, InterruptedException {
    String replicaSelectorClass = CONN.getConfiguration().
      get(RegionLocator.LOCATOR_META_REPLICAS_MODE_LOADBALANCE_SELECTOR,
        CatalogReplicaLoadBalanceSimpleSelector.class.getName());

    CatalogReplicaLoadBalanceSelector metaSelector = CatalogReplicaLoadBalanceSelectorFactory
      .createSelector(replicaSelectorClass, META_TABLE_NAME, CONN.getChoreService(), () -> {
        int numOfReplicas = CatalogReplicaLoadBalanceSelector.UNINITIALIZED_NUM_OF_REPLICAS;
        try {
          RegionLocations metaLocations = CONN.registry.getMetaRegionLocations().get
            (CONN.connConf.getReadRpcTimeoutNs(), TimeUnit.NANOSECONDS);
          numOfReplicas = metaLocations.size();
        } catch (Exception e) {
          LOG.error("Failed to get table {}'s region replication, ", META_TABLE_NAME, e);
        }
        return numOfReplicas;
      });

    // Loop for 100 times, it should cover all replica ids.
    int[] replicaIdCount = new int[numOfMetaReplica];
    IntStream.range(1, 100).forEach(i -> replicaIdCount[metaSelector.select(
      TableName.valueOf("test"), EMPTY_START_ROW, RegionLocateType.CURRENT)] ++);

    // Make sure each replica id is returned by select() call, including primary replica id.
    IntStream.range(0, numOfMetaReplica).forEach(i -> assertNotEquals(replicaIdCount[i], 0));

    // Change to No meta replica
    HBaseTestingUtility.setReplicas(admin, TableName.META_TABLE_NAME, 1);
    TEST_UTIL.waitFor(30000, () -> TEST_UTIL.getMiniHBaseCluster().getRegions(
      TableName.META_TABLE_NAME).size() == 1);

    CatalogReplicaLoadBalanceSelector metaSelectorWithNoReplica =
      CatalogReplicaLoadBalanceSelectorFactory.createSelector(
        replicaSelectorClass, META_TABLE_NAME, CONN.getChoreService(), () -> {
        int numOfReplicas = CatalogReplicaLoadBalanceSelector.UNINITIALIZED_NUM_OF_REPLICAS;
        try {
          RegionLocations metaLocations = CONN.registry.getMetaRegionLocations().get(
            CONN.connConf.getReadRpcTimeoutNs(), TimeUnit.NANOSECONDS);
          numOfReplicas = metaLocations.size();
        } catch (Exception e) {
          LOG.error("Failed to get table {}'s region replication, ", META_TABLE_NAME, e);
        }
        return numOfReplicas;
      });
    assertEquals(
      metaSelectorWithNoReplica.select(TableName.valueOf("test"), EMPTY_START_ROW,
        RegionLocateType.CURRENT), RegionReplicaUtil.DEFAULT_REPLICA_ID);
  }
}
