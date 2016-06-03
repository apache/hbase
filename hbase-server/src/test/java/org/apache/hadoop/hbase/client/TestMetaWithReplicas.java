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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.assertErrors;
import static org.apache.hadoop.hbase.util.hbck.HbckTestingUtil.doFsck;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.regionserver.StorefileRefresherChore;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseFsck;
import org.apache.hadoop.hbase.util.HBaseFsckRepair;
import org.apache.hadoop.hbase.util.HBaseFsck.ErrorReporter.ERROR_CODE;
import org.apache.hadoop.hbase.util.hbck.HbckTestingUtil;
import org.apache.hadoop.hbase.zookeeper.LoadBalancerTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestRule;

/**
 * Tests the scenarios where replicas are enabled for the meta table
 */
@Category(LargeTests.class)
public class TestMetaWithReplicas {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).
      withLookingForStuckThread(true).
      build();
  private static final Log LOG = LogFactory.getLog(TestMetaWithReplicas.class);
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Before
  public void setup() throws Exception {
    TEST_UTIL.getConfiguration().setInt("zookeeper.session.timeout", 30000);
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM, 3);
    TEST_UTIL.getConfiguration().setInt(
        StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, 1000);
    TEST_UTIL.startMiniCluster(3);
    // disable the balancer
    LoadBalancerTracker l = new LoadBalancerTracker(TEST_UTIL.getZooKeeperWatcher(),
        new Abortable() {
      boolean aborted = false;
      @Override
      public boolean isAborted() {
        return aborted;
      }
      @Override
      public void abort(String why, Throwable e) {
        aborted = true;
      }
    });
    l.setBalancerOn(false);
    for (int replicaId = 1; replicaId < 3; replicaId ++) {
      HRegionInfo h = RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO,
        replicaId);
      TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().waitForAssignment(h);
    }
    LOG.debug("All meta replicas assigned");
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMetaHTDReplicaCount() throws Exception {
    assertTrue(TEST_UTIL.getHBaseAdmin().getTableDescriptor(TableName.META_TABLE_NAME)
        .getRegionReplication() == 3);
  }

  @Test
  public void testZookeeperNodesForReplicas() throws Exception {
    // Checks all the znodes exist when meta's replicas are enabled
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    Configuration conf = TEST_UTIL.getConfiguration();
    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    // check that the data in the znode is parseable (this would also mean the znode exists)
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName.parseFrom(data);
    for (int i = 1; i < 3; i++) {
      String secZnode = ZKUtil.joinZNode(baseZNode,
          conf.get("zookeeper.znode.metaserver", "meta-region-server") + "-" + i);
      String str = zkw.getZNodeForReplica(i);
      assertTrue(str.equals(secZnode));
      // check that the data in the znode is parseable (this would also mean the znode exists)
      data = ZKUtil.getData(zkw, secZnode);
      ServerName.parseFrom(data);
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
    ZooKeeperWatcher zkw = util.getZooKeeperWatcher();
    Configuration conf = util.getConfiguration();
    conf.setBoolean(HConstants.USE_META_REPLICAS, true);

    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName primary = ServerName.parseFrom(data);

    byte[] TABLE = Bytes.toBytes("testShutdownHandling");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    if (util.getHBaseAdmin().tableExists(TABLE)) {
      util.getHBaseAdmin().disableTable(TABLE);
      util.getHBaseAdmin().deleteTable(TABLE);
    }
    ServerName master = null;
    try (Connection c = ConnectionFactory.createConnection(util.getConfiguration());) {
      try (Table htable = util.createTable(TABLE, FAMILIES, conf);) {
        util.getHBaseAdmin().flush(TableName.META_TABLE_NAME);
        Thread.sleep(conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
           30000) * 6);
        List<HRegionInfo> regions = MetaTableAccessor.getTableRegions(zkw, c,
          TableName.valueOf(TABLE));
        HRegionLocation hrl = MetaTableAccessor.getRegionLocation(c, regions.get(0));
        // Ensure that the primary server for test table is not the same one as the primary
        // of the meta region since we will be killing the srv holding the meta's primary...
        // We want to be able to write to the test table even when the meta is not present ..
        // If the servers are the same, then move the test table's region out of the server
        // to another random server
        if (hrl.getServerName().equals(primary)) {
          util.getHBaseAdmin().move(hrl.getRegionInfo().getEncodedNameAsBytes(), null);
          // wait for the move to complete
          do {
            Thread.sleep(10);
            hrl = MetaTableAccessor.getRegionLocation(c, regions.get(0));
          } while (primary.equals(hrl.getServerName()));
          util.getHBaseAdmin().flush(TableName.META_TABLE_NAME);
          Thread.sleep(conf.getInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD,
             30000) * 3);
        }
        master = util.getHBaseClusterInterface().getClusterStatus().getMaster();
        // kill the master so that regionserver recovery is not triggered at all
        // for the meta server
        util.getHBaseClusterInterface().stopMaster(master);
        util.getHBaseClusterInterface().waitForMasterToStop(master, 60000);
        if (!master.equals(primary)) {
          util.getHBaseClusterInterface().killRegionServer(primary);
          util.getHBaseClusterInterface().waitForRegionServerToStop(primary, 60000);
        }
        ((ClusterConnection)c).clearRegionCache();
      }
      Get get = null;
      Result r = null;
      byte[] row = "test".getBytes();
      try (Table htable = c.getTable(TableName.valueOf(TABLE));) {
        Put put = new Put(row);
        put.add("foo".getBytes(), row, row);
        BufferedMutator m = c.getBufferedMutator(TableName.valueOf(TABLE));
        m.mutate(put);
        m.flush();
        // Try to do a get of the row that was just put
        get = new Get(row);
        r = htable.get(get);
        assertTrue(Arrays.equals(r.getRow(), row));
        // now start back the killed servers and disable use of replicas. That would mean
        // calls go to the primary
        util.getHBaseClusterInterface().startMaster(master.getHostname(), 0);
        util.getHBaseClusterInterface().startRegionServer(primary.getHostname(), 0);
        util.getHBaseClusterInterface().waitForActiveAndReadyMaster();
        ((ClusterConnection)c).clearRegionCache();
      }
      conf.setBoolean(HConstants.USE_META_REPLICAS, false);
      try (Table htable = c.getTable(TableName.valueOf(TABLE));) {
        r = htable.get(get);
        assertTrue(Arrays.equals(r.getRow(), row));
      }
    }
  }

  @Test
  public void testMetaLookupThreadPoolCreated() throws Exception {
    byte[] TABLE = Bytes.toBytes("testMetaLookupThreadPoolCreated");
    byte[][] FAMILIES = new byte[][] { Bytes.toBytes("foo") };
    if (TEST_UTIL.getHBaseAdmin().tableExists(TABLE)) {
      TEST_UTIL.getHBaseAdmin().disableTable(TABLE);
      TEST_UTIL.getHBaseAdmin().deleteTable(TABLE);
    }
    try (Table htable =
        TEST_UTIL.createTable(TABLE, FAMILIES, TEST_UTIL.getConfiguration());) {
      byte[] row = "test".getBytes();
      HConnectionImplementation c = ((HConnectionImplementation)((HTable)htable).connection);
      // check that metalookup pool would get created
      c.relocateRegion(TABLE, row);
      ExecutorService ex = c.getCurrentMetaLookupPool();
      assert(ex != null);
    }
  }

  @Test
  public void testChangingReplicaCount() throws Exception {
    // tests changing the replica count across master restarts
    // reduce the replica count from 3 to 2
    stopMasterAndValidateReplicaCount(3, 2);
    // increase the replica count from 2 to 3
    stopMasterAndValidateReplicaCount(2, 3);
  }

  private void stopMasterAndValidateReplicaCount(int originalReplicaCount, int newReplicaCount)
      throws Exception {
    ServerName sn = TEST_UTIL.getHBaseClusterInterface().getClusterStatus().getMaster();
    TEST_UTIL.getHBaseClusterInterface().stopMaster(sn);
    TEST_UTIL.getHBaseClusterInterface().waitForMasterToStop(sn, 60000);
    List<String> metaZnodes = TEST_UTIL.getZooKeeperWatcher().getMetaReplicaNodes();
    assert(metaZnodes.size() == originalReplicaCount); //we should have what was configured before
    TEST_UTIL.getHBaseClusterInterface().getConf().setInt(HConstants.META_REPLICAS_NUM,
        newReplicaCount);
    TEST_UTIL.getHBaseClusterInterface().startMaster(sn.getHostname(), 0);
    TEST_UTIL.getHBaseClusterInterface().waitForActiveAndReadyMaster();
    int count = 0;
    do {
      metaZnodes = TEST_UTIL.getZooKeeperWatcher().getMetaReplicaNodes();
      Thread.sleep(10);
      count++;
      // wait for the count to be different from the originalReplicaCount. When the
      // replica count is reduced, that will happen when the master unassigns excess
      // replica, and deletes the excess znodes
    } while (metaZnodes.size() == originalReplicaCount && count < 1000);
    assert(metaZnodes.size() == newReplicaCount);
    // also check if hbck returns without errors
    TEST_UTIL.getConfiguration().setInt(HConstants.META_REPLICAS_NUM,
        newReplicaCount);
    HBaseFsck hbck = HbckTestingUtil.doFsck(TEST_UTIL.getConfiguration(), false);
    HbckTestingUtil.assertNoErrors(hbck);
  }

  @Test
  public void testHBaseFsckWithMetaReplicas() throws Exception {
    HBaseFsck hbck = HbckTestingUtil.doFsck(TEST_UTIL.getConfiguration(), false);
    HbckTestingUtil.assertNoErrors(hbck);
  }

  @Test
  public void testHBaseFsckWithFewerMetaReplicas() throws Exception {
    ClusterConnection c = (ClusterConnection)ConnectionFactory.createConnection(
        TEST_UTIL.getConfiguration());
    RegionLocations rl = c.locateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW,
        false, false);
    HBaseFsckRepair.closeRegionSilentlyAndWait(c,
        rl.getRegionLocation(1).getServerName(), rl.getRegionLocation(1).getRegionInfo());
    // check that problem exists
    HBaseFsck hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{ERROR_CODE.UNKNOWN,ERROR_CODE.NO_META_REGION});
    // fix the problem
    hbck = doFsck(TEST_UTIL.getConfiguration(), true);
    // run hbck again to make sure we don't see any errors
    hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{});
  }

  @Test
  public void testHBaseFsckWithFewerMetaReplicaZnodes() throws Exception {
    ClusterConnection c = (ClusterConnection)ConnectionFactory.createConnection(
        TEST_UTIL.getConfiguration());
    RegionLocations rl = c.locateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW,
        false, false);
    HBaseFsckRepair.closeRegionSilentlyAndWait(c,
        rl.getRegionLocation(2).getServerName(), rl.getRegionLocation(2).getRegionInfo());
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    ZKUtil.deleteNode(zkw, zkw.getZNodeForReplica(2));
    // check that problem exists
    HBaseFsck hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{ERROR_CODE.UNKNOWN,ERROR_CODE.NO_META_REGION});
    // fix the problem
    hbck = doFsck(TEST_UTIL.getConfiguration(), true);
    // run hbck again to make sure we don't see any errors
    hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{});
  }

  @Test
  public void testAccessingUnknownTables() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(HConstants.USE_META_REPLICAS, true);
    Table table = TEST_UTIL.getConnection().getTable(TableName.valueOf("RandomTable"));
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
    ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
    String baseZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    String primaryMetaZnode = ZKUtil.joinZNode(baseZNode,
        conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    // check that the data in the znode is parseable (this would also mean the znode exists)
    byte[] data = ZKUtil.getData(zkw, primaryMetaZnode);
    ServerName currentServer = ServerName.parseFrom(data);
    Collection<ServerName> liveServers = TEST_UTIL.getHBaseAdmin().getClusterStatus().getServers();
    ServerName moveToServer = null;
    for (ServerName s : liveServers) {
      if (!currentServer.equals(s)) {
        moveToServer = s;
      }
    }
    assert(moveToServer != null);
    String tableName = "randomTable5678";
    TEST_UTIL.createTable(TableName.valueOf(tableName), "f");
    assertTrue(TEST_UTIL.getHBaseAdmin().tableExists(tableName));
    TEST_UTIL.getHBaseAdmin().move(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes(),
        Bytes.toBytes(moveToServer.getServerName()));
    int i = 0;
    do {
      Thread.sleep(10);
      data = ZKUtil.getData(zkw, primaryMetaZnode);
      currentServer = ServerName.parseFrom(data);
      i++;
    } while (!moveToServer.equals(currentServer) && i < 1000); //wait for 10 seconds overall
    assert(i != 1000);
    TEST_UTIL.getHBaseAdmin().disableTable("randomTable5678");
    assertTrue(TEST_UTIL.getHBaseAdmin().isTableDisabled("randomTable5678"));
  }

  @Test
  public void testShutdownOfReplicaHolder() throws Exception {
    // checks that the when the server holding meta replica is shut down, the meta replica
    // can be recovered
    RegionLocations rl = ConnectionManager.getConnectionInternal(TEST_UTIL.getConfiguration()).
        locateRegion(TableName.META_TABLE_NAME, Bytes.toBytes(""), false, true);
    HRegionLocation hrl = rl.getRegionLocation(1);
    ServerName oldServer = hrl.getServerName();
    TEST_UTIL.getHBaseClusterInterface().killRegionServer(oldServer);
    int i = 0;
    do {
      LOG.debug("Waiting for the replica " + hrl.getRegionInfo() + " to come up");
      Thread.sleep(30000); //wait for the detection/recovery
      rl = ConnectionManager.getConnectionInternal(TEST_UTIL.getConfiguration()).
          locateRegion(TableName.META_TABLE_NAME, Bytes.toBytes(""), false, true);
      hrl = rl.getRegionLocation(1);
      i++;
    } while ((hrl == null || hrl.getServerName().equals(oldServer)) && i < 3);
    assertTrue(i != 3);
  }

  @Test
  public void testHBaseFsckWithExcessMetaReplicas() throws Exception {
    // Create a meta replica (this will be the 4th one) and assign it
    HRegionInfo h = RegionReplicaUtil.getRegionInfoForReplica(
        HRegionInfo.FIRST_META_REGIONINFO, 3);
    // create in-memory state otherwise master won't assign
    TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager()
             .getRegionStates().createRegionState(h);
    TEST_UTIL.assignRegion(h);
    HBaseFsckRepair.waitUntilAssigned(TEST_UTIL.getHBaseAdmin(), h);
    // check that problem exists
    HBaseFsck hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{ERROR_CODE.UNKNOWN, ERROR_CODE.SHOULD_NOT_BE_DEPLOYED});
    // fix the problem
    hbck = doFsck(TEST_UTIL.getConfiguration(), true);
    // run hbck again to make sure we don't see any errors
    hbck = doFsck(TEST_UTIL.getConfiguration(), false);
    assertErrors(hbck, new ERROR_CODE[]{});
  }
}
