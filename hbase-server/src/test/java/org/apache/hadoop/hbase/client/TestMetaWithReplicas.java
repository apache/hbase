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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the scenarios where replicas are enabled for the meta table
 */
@Category(LargeTests.class)
public class TestMetaWithReplicas {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaWithReplicas.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMetaWithReplicas.class);
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int REGIONSERVERS_COUNT = 3;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setup() throws Exception {
    TEST_UTIL.getConfiguration().setInt("zookeeper.session.timeout", 30000);
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    TEST_UTIL.getConfiguration().setInt(
        StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 1000);
    TEST_UTIL.startMiniCluster(REGIONSERVERS_COUNT);
    AssignmentManager am = TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager();
    Set<ServerName> sns = new HashSet<ServerName>();
    ServerName hbaseMetaServerName =
      MetaTableLocator.getMetaRegionLocation(TEST_UTIL.getZooKeeperWatcher());
    LOG.info("HBASE:META DEPLOY: on " + hbaseMetaServerName);
    sns.add(hbaseMetaServerName);
    for (int replicaId = 1; replicaId < 3; replicaId++) {
      RegionInfo h = RegionReplicaUtil
        .getRegionInfoForReplica(RegionInfoBuilder.FIRST_META_REGIONINFO, replicaId);
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
      TEST_UTIL.getAdmin().move(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
        destinationServerName);
    }
    // Disable the balancer
    LoadBalancerTracker l = new LoadBalancerTracker(TEST_UTIL.getZooKeeperWatcher(),
        new Abortable() {
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

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaHTDReplicaCount() throws Exception {
    assertEquals(3,
      TEST_UTIL.getAdmin().getDescriptor(TableName.META_TABLE_NAME).getRegionReplication());
  }

  @Test
  public void testZookeeperNodesForReplicas() throws Exception {
    // Checks all the znodes exist when meta's replicas are enabled
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    Configuration conf = TEST_UTIL.getConfiguration();
    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZNodePaths.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    // check that the data in the znode is parseable (this would also mean the znode exists)
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ProtobufUtil.toServerName(data);
    for (int i = 1; i < 3; i++) {
      String secZnode = ZNodePaths.joinZNode(baseZNode,
          conf.get("zookeeper.znode.metaserver", "meta-region-server") + "-" + i);
      String str = zkw.getZNodePaths().getZNodeForReplica(i);
      assertTrue(str.equals(secZnode));
      // check that the data in the znode is parseable (this would also mean the znode exists)
      data = ZKUtil.getData(zkw, secZnode);
      ProtobufUtil.toServerName(data);
    }
  }

  @Test
  public void testShutdownHandling() throws Exception {
    // This test creates a table, flushes the meta (with 3 replicas), kills the
    // server holding the primary meta replica. Then it does a put/get into/from
    // the test table. The put/get operations would use the replicas to locate the
    // location of the test table's region
    shutdownMetaAndDoValidations(TEST_UTIL);
  }

  public static void shutdownMetaAndDoValidations(HBaseTestingUtility util) throws Exception {
    // This test creates a table, flushes the meta (with 3 replicas), kills the
    // server holding the primary meta replica. Then it does a put/get into/from
    // the test table. The put/get operations would use the replicas to locate the
    // location of the test table's region
    ZKWatcher zkw = util.getZooKeeperWatcher();
    Configuration conf = util.getConfiguration();
    conf.setBoolean(HConstants.USE_META_REPLICAS, true);

    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZNodePaths.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName primary = ProtobufUtil.toServerName(data);
    LOG.info("Primary=" + primary.toString());

    TableName TABLE = TableName.valueOf("testShutdownHandling");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    if (util.getAdmin().tableExists(TABLE)) {
      util.getAdmin().disableTable(TABLE);
      util.getAdmin().deleteTable(TABLE);
    }
    byte[] row = Bytes.toBytes("test");
    ServerName master = null;
    try (
      ConnectionImplementation c = ConnectionFactory.createConnectionImpl(util.getConfiguration(),
        null, UserProvider.instantiate(util.getConfiguration()).getCurrent())) {
      try (Table htable = util.createTable(TABLE, FAMILIES)) {
        util.getAdmin().flush(TableName.META_TABLE_NAME);
        Thread.sleep(
          conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 30000) * 6);
        List<RegionInfo> regions = MetaTableAccessor.getTableRegions(c, TABLE);
        HRegionLocation hrl = MetaTableAccessor.getRegionLocation(c, regions.get(0));
        // Ensure that the primary server for test table is not the same one as the primary
        // of the meta region since we will be killing the srv holding the meta's primary...
        // We want to be able to write to the test table even when the meta is not present ..
        // If the servers are the same, then move the test table's region out of the server
        // to another random server
        if (hrl.getServerName().equals(primary)) {
          util.getAdmin().move(hrl.getRegion().getEncodedNameAsBytes());
          // wait for the move to complete
          do {
            Thread.sleep(10);
            hrl = MetaTableAccessor.getRegionLocation(c, regions.get(0));
          } while (primary.equals(hrl.getServerName()));
          util.getAdmin().flush(TableName.META_TABLE_NAME);
          Thread.sleep(conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
              30000) * 3);
        }
        // Ensure all metas are not on same hbase:meta replica=0 server!

        master = util.getHBaseClusterInterface().getClusterMetrics().getMasterName();
        // kill the master so that regionserver recovery is not triggered at all
        // for the meta server
        LOG.info("Stopping master=" + master.toString());
        util.getHBaseClusterInterface().stopMaster(master);
        util.getHBaseClusterInterface().waitForMasterToStop(master, 60000);
        LOG.info("Master " + master + " stopped!");
        if (!master.equals(primary)) {
          util.getHBaseClusterInterface().killRegionServer(primary);
          util.getHBaseClusterInterface().waitForRegionServerToStop(primary, 60000);
        }
        c.clearRegionLocationCache();
      }
      LOG.info("Running GETs");
      try (Table htable = c.getTable(TABLE)) {
        Put put = new Put(row);
        put.addColumn(Bytes.toBytes("foo"), row, row);
        BufferedMutator m = c.getBufferedMutator(TABLE);
        m.mutate(put);
        m.flush();
        // Try to do a get of the row that was just put
        Result r = htable.get(new Get(row));
        assertTrue(Arrays.equals(r.getRow(), row));
        // now start back the killed servers and disable use of replicas. That would mean
        // calls go to the primary
        LOG.info("Starting Master");
        util.getHBaseClusterInterface().startMaster(master.getHostname(), 0);
        util.getHBaseClusterInterface().startRegionServer(primary.getHostname(), 0);
        util.getHBaseClusterInterface().waitForActiveAndReadyMaster();
        LOG.info("Master active!");
        c.clearRegionLocationCache();
      }
    }
    conf.setBoolean(HConstants.USE_META_REPLICAS, false);
    LOG.info("Running GETs no replicas");
    try (Connection c = ConnectionFactory.createConnection(conf)) {
      try (Table htable = c.getTable(TABLE)) {
        Result r = htable.get(new Get(row));
        assertTrue(Arrays.equals(r.getRow(), row));
      }
    }
  }

  @Test
  public void testAccessingUnknownTables() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(HConstants.USE_META_REPLICAS, true);
    Table table = TEST_UTIL.getConnection().getTable(TableName.valueOf(name.getMethodName()));
    Get get = new Get(Bytes.toBytes("foo"));
    try {
      table.get(get);
    } catch (TableNotFoundException t) {
      return;
    }
    fail("Expected TableNotFoundException");
  }

  @Test
  public void testMetaAddressChange() throws Exception {
    // checks that even when the meta's location changes, the various
    // caches update themselves. Uses the master operations to test
    // this
    Configuration conf = TEST_UTIL.getConfiguration();
    ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZNodePaths.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    // check that the data in the znode is parseable (this would also mean the znode exists)
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName currentServer = ProtobufUtil.toServerName(data);
    Collection<ServerName> liveServers = TEST_UTIL.getAdmin()
        .getClusterMetrics(EnumSet.of(Option.LIVE_SERVERS)).getLiveServerMetrics().keySet();
    ServerName moveToServer = null;
    for (ServerName s : liveServers) {
      if (!currentServer.equals(s)) {
        moveToServer = s;
      }
    }
    assertNotNull(moveToServer);
    final TableName tableName = TableName.valueOf(name.getMethodName());
    TEST_UTIL.createTable(tableName, "f");
    assertTrue(TEST_UTIL.getAdmin().tableExists(tableName));
    TEST_UTIL.getAdmin().move(RegionInfoBuilder.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
      moveToServer);
    int i = 0;
    assertNotEquals(currentServer, moveToServer);
    LOG.info("CurrentServer=" + currentServer + ", moveToServer=" + moveToServer);
    final int max = 10000;
    do {
      Thread.sleep(10);
      data = ZKUtil.getData(zkw, primaryMetaZnode);
      currentServer = ProtobufUtil.toServerName(data);
      i++;
    } while (!moveToServer.equals(currentServer) && i < max); //wait for 10 seconds overall
    assertNotEquals(max, i);
    TEST_UTIL.getAdmin().disableTable(tableName);
    assertTrue(TEST_UTIL.getAdmin().isTableDisabled(tableName));
  }

  @Test
  public void testShutdownOfReplicaHolder() throws Exception {
    // checks that the when the server holding meta replica is shut down, the meta replica
    // can be recovered
    try (
      Connection conn = ConnectionFactory.createConnectionImpl(TEST_UTIL.getConfiguration(), null,
        UserProvider.instantiate(TEST_UTIL.getConfiguration()).getCurrent());
      RegionLocator locator = conn.getRegionLocator(TableName.META_TABLE_NAME)) {
      HRegionLocation hrl = locator.getRegionLocations(HConstants.EMPTY_START_ROW, true).get(1);
      ServerName oldServer = hrl.getServerName();
      TEST_UTIL.getHBaseClusterInterface().killRegionServer(oldServer);
      int i = 0;
      do {
        LOG.debug("Waiting for the replica " + hrl.getRegion() + " to come up");
        Thread.sleep(10000); // wait for the detection/recovery
        hrl = locator.getRegionLocations(HConstants.EMPTY_START_ROW, true).get(1);
        i++;
      } while ((hrl == null || hrl.getServerName().equals(oldServer)) && i < 3);
      assertNotEquals(3, i);
    }
  }
}
