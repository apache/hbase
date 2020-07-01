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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.junit.AfterClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for testing the scenarios where replicas are enabled for the meta table.
 */
public class MetaWithReplicasTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(MetaWithReplicasTestBase.class);

  protected static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected static final int REGIONSERVERS_COUNT = 3;

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  protected static void startCluster() throws Exception {
    TEST_UTIL.getConfiguration().setInt("zookeeper.session.timeout", 30000);
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    TEST_UTIL.getConfiguration()
      .setInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 1000);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numAlwaysStandByMasters(1)
      .numMasters(1).numRegionServers(REGIONSERVERS_COUNT).build();
    TEST_UTIL.startMiniCluster(option);
    AssignmentManager am = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    Set<ServerName> sns = new HashSet<ServerName>();
    RegionInfo metaRegionInfo;
    ServerName hbaseMetaServerName;
    try (RegionLocator locator =
      TEST_UTIL.getConnection().getRegionLocator(TableName.META_TABLE_NAME)) {
      HRegionLocation loc = locator.getRegionLocation(HConstants.EMPTY_START_ROW);
      metaRegionInfo = loc.getRegion();
      hbaseMetaServerName = loc.getServerName();
    }
    LOG.info("HBASE:META DEPLOY: on " + hbaseMetaServerName);
    sns.add(hbaseMetaServerName);
    for (int replicaId = 1; replicaId < 3; replicaId++) {
      RegionInfo h = RegionReplicaUtil.getRegionInfoForReplica(metaRegionInfo, replicaId);
      AssignmentTestingUtil.waitForAssignment(am, h);
      ServerName sn = am.getRegionStates().getRegionServerOfRegion(h);
      assertNotNull(sn);
      LOG.info("HBASE:META DEPLOY: " + h.getRegionNameAsString() + " on " + sn);
      sns.add(sn);
    }
    // Fun. All meta region replicas have ended up on the one server. This will cause this test
    // to fail ... sometimes.
    if (sns.size() == 1) {
      int count = TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size();
      assertTrue("count=" + count, count == REGIONSERVERS_COUNT);
      LOG.warn("All hbase:meta replicas are on the one server; moving hbase:meta: " + sns);
      int metaServerIndex = TEST_UTIL.getHBaseCluster().getServerWithMeta();
      int newServerIndex = metaServerIndex;
      while (newServerIndex == metaServerIndex) {
        newServerIndex = (newServerIndex + 1) % REGIONSERVERS_COUNT;
      }
      assertNotEquals(metaServerIndex, newServerIndex);
      ServerName destinationServerName =
        TEST_UTIL.getHBaseCluster().getRegionServer(newServerIndex).getServerName();
      ServerName metaServerName =
        TEST_UTIL.getHBaseCluster().getRegionServer(metaServerIndex).getServerName();
      assertNotEquals(destinationServerName, metaServerName);
      TEST_UTIL.getAdmin().move(metaRegionInfo.getEncodedNameAsBytes(), destinationServerName);
    }
    // Disable the balancer
    LoadBalancerTracker l =
      new LoadBalancerTracker(TEST_UTIL.getZooKeeperWatcher(), new Abortable() {
        AtomicBoolean aborted = new AtomicBoolean(false);

        @Override
        public boolean isAborted() {
          return aborted.get();
        }

        @Override
        public void abort(String why, Throwable e) {
          aborted.set(true);
        }
      });
    l.setBalancerOn(false);
    LOG.debug("All meta replicas assigned");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
