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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.RegionTransitionData;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;


/**
 * Test {@link AssignmentManager}
 */
@Category(SmallTests.class)
public class TestAssignmentManager {
  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final ServerName RANDOM_SERVERNAME =
    new ServerName("example.org", 1234, 5678);
  private Server server;
  private ServerManager serverManager;
  private CatalogTracker ct;
  private ExecutorService executor;
  private ZooKeeperWatcher watcher;

  @BeforeClass
  public static void beforeClass() throws Exception {
    HTU.startMiniZKCluster();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    HTU.shutdownMiniZKCluster();
  }

  @Before
  public void before() throws ZooKeeperConnectionException, IOException {
    // Mock a Server.  Have it return a legit Configuration and ZooKeeperWatcher.
    // If abort is called, be sure to fail the test (don't just swallow it
    // silently as is mockito default).
    this.server = Mockito.mock(Server.class);
    Mockito.when(server.getConfiguration()).thenReturn(HTU.getConfiguration());
    this.watcher =
      new ZooKeeperWatcher(HTU.getConfiguration(), "mocked server", this.server, true);
    Mockito.when(server.getZooKeeper()).thenReturn(this.watcher);
    Mockito.doThrow(new RuntimeException("Aborted")).
      when(server).abort(Mockito.anyString(), (Throwable)Mockito.anyObject());
    // Mock a ServerManager.  Say the server RANDOME_SERVERNAME is online.
    // Also, when someone sends sendRegionClose, say true it succeeded.
    this.serverManager = Mockito.mock(ServerManager.class);
    Mockito.when(this.serverManager.isServerOnline(RANDOM_SERVERNAME)).thenReturn(true);
    this.ct = Mockito.mock(CatalogTracker.class);
    this.executor = Mockito.mock(ExecutorService.class);
  }

  @After
  public void after() {
    if (this.watcher != null) this.watcher.close();
  }

  @Test
  public void testUnassignWithSplitAtSameTime() throws KeeperException, IOException {
    // Region to use in test.
    final HRegionInfo hri = HRegionInfo.FIRST_META_REGIONINFO;
    // First amend the servermanager mock so that when we do send close of the
    // first meta region on RANDOM_SERVERNAME, it will return true rather than
    // default null.
    Mockito.when(this.serverManager.sendRegionClose(RANDOM_SERVERNAME, hri)).thenReturn(true);
    // Create an AM.
    AssignmentManager am =
      new AssignmentManager(this.server, this.serverManager, this.ct, this.executor);
    try {
      // First make sure my mock up basically works.  Unassign a region.
      unassign(am, RANDOM_SERVERNAME, hri);
      // This delete will fail if the previous unassign did wrong thing.
      ZKAssign.deleteClosingNode(this.watcher, hri);
      // Now put a SPLITTING region in the way.  I don't have to assert it
      // go put in place.  This method puts it in place then asserts it still
      // owns it by moving state from SPLITTING to SPLITTING.
      int version = createNodeSplitting(this.watcher, hri, RANDOM_SERVERNAME);
      // Now, retry the unassign with the SPLTTING in place.  It should just
      // complete without fail; a sort of 'silent' recognition that the
      // region to unassign has been split and no longer exists: TOOD: what if
      // the split fails and the parent region comes back to life?
      unassign(am, RANDOM_SERVERNAME, hri);
      // This transition should fail if the znode has been messed with.
      ZKAssign.transitionNode(this.watcher, hri, RANDOM_SERVERNAME,
        EventType.RS_ZK_REGION_SPLITTING, EventType.RS_ZK_REGION_SPLITTING, version);
      assertTrue(am.isRegionInTransition(hri) == null);
    } finally {
      am.shutdown();
    }
  }

  /**
   * Creates a new ephemeral node in the SPLITTING state for the specified region.
   * Create it ephemeral in case regionserver dies mid-split.
   *
   * <p>Does not transition nodes from other states.  If a node already exists
   * for this region, a {@link NodeExistsException} will be thrown.
   *
   * @param zkw zk reference
   * @param region region to be created as offline
   * @param serverName server event originates from
   * @return Version of znode created.
   * @throws KeeperException 
   * @throws IOException 
   */
  // Copied from SplitTransaction rather than open the method over there in
  // the regionserver package.
  private static int createNodeSplitting(final ZooKeeperWatcher zkw,
      final HRegionInfo region, final ServerName serverName)
  throws KeeperException, IOException {
    RegionTransitionData data =
      new RegionTransitionData(EventType.RS_ZK_REGION_SPLITTING,
        region.getRegionName(), serverName);

    String node = ZKAssign.getNodeName(zkw, region.getEncodedName());
    if (!ZKUtil.createEphemeralNodeAndWatch(zkw, node, data.getBytes())) {
      throw new IOException("Failed create of ephemeral " + node);
    }
    // Transition node from SPLITTING to SPLITTING and pick up version so we
    // can be sure this znode is ours; version is needed deleting.
    return transitionNodeSplitting(zkw, region, serverName, -1);
  }

  // Copied from SplitTransaction rather than open the method over there in
  // the regionserver package.
  private static int transitionNodeSplitting(final ZooKeeperWatcher zkw,
      final HRegionInfo parent,
      final ServerName serverName, final int version)
  throws KeeperException, IOException {
    return ZKAssign.transitionNode(zkw, parent, serverName,
      EventType.RS_ZK_REGION_SPLITTING, EventType.RS_ZK_REGION_SPLITTING, version);
  }

  private void unassign(final AssignmentManager am, final ServerName sn,
      final HRegionInfo hri) {
    // Before I can unassign a region, I need to set it online.
    am.regionOnline(hri, sn);
    // Unassign region.
    am.unassign(hri);
  }
}