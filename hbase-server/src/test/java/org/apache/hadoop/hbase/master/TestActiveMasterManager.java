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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the {@link ActiveMasterManager}.
 */
@Category({MasterTests.class, MediumTests.class})
public class TestActiveMasterManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestActiveMasterManager.class);

  private final static Logger LOG = LoggerFactory.getLogger(TestActiveMasterManager.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test public void testRestartMaster() throws IOException, KeeperException {
    try (ZKWatcher zk = new ZKWatcher(TEST_UTIL.getConfiguration(),
      "testActiveMasterManagerFromZK", null, true)) {
      try {
        ZKUtil.deleteNode(zk, zk.getZNodePaths().masterAddressZNode);
        ZKUtil.deleteNode(zk, zk.getZNodePaths().clusterStateZNode);
      } catch (KeeperException.NoNodeException nne) {
      }

      // Create the master node with a dummy address
      ServerName master = ServerName.valueOf("localhost", 1, System.currentTimeMillis());
      // Should not have a master yet
      DummyMaster dummyMaster = new DummyMaster(zk, master);
      ClusterStatusTracker clusterStatusTracker =
          dummyMaster.getClusterStatusTracker();
      ActiveMasterManager activeMasterManager =
          dummyMaster.getActiveMasterManager();
      assertFalse(activeMasterManager.clusterHasActiveMaster.get());
      assertFalse(activeMasterManager.getActiveMasterServerName().isPresent());

      // First test becoming the active master uninterrupted
      MonitoredTask status = Mockito.mock(MonitoredTask.class);
      clusterStatusTracker.setClusterUp();

      activeMasterManager.blockUntilBecomingActiveMaster(100, status);
      assertTrue(activeMasterManager.clusterHasActiveMaster.get());
      assertMaster(zk, master);
      assertMaster(zk, activeMasterManager.getActiveMasterServerName().get());

      // Now pretend master restart
      DummyMaster secondDummyMaster = new DummyMaster(zk, master);
      ActiveMasterManager secondActiveMasterManager =
          secondDummyMaster.getActiveMasterManager();
      assertFalse(secondActiveMasterManager.clusterHasActiveMaster.get());
      activeMasterManager.blockUntilBecomingActiveMaster(100, status);
      assertTrue(activeMasterManager.clusterHasActiveMaster.get());
      assertMaster(zk, master);
      assertMaster(zk, activeMasterManager.getActiveMasterServerName().get());
      assertMaster(zk, secondActiveMasterManager.getActiveMasterServerName().get());
    }
  }

  /**
   * Unit tests that uses ZooKeeper but does not use the master-side methods
   * but rather acts directly on ZK.
   * @throws Exception
   */
  @Test
  public void testActiveMasterManagerFromZK() throws Exception {
    try (ZKWatcher zk = new ZKWatcher(TEST_UTIL.getConfiguration(),
      "testActiveMasterManagerFromZK", null, true)) {
      try {
        ZKUtil.deleteNode(zk, zk.getZNodePaths().masterAddressZNode);
        ZKUtil.deleteNode(zk, zk.getZNodePaths().clusterStateZNode);
      } catch (KeeperException.NoNodeException nne) {
      }

      // Create the master node with a dummy address
      ServerName firstMasterAddress =
          ServerName.valueOf("localhost", 1, System.currentTimeMillis());
      ServerName secondMasterAddress =
          ServerName.valueOf("localhost", 2, System.currentTimeMillis());

      // Should not have a master yet
      DummyMaster ms1 = new DummyMaster(zk, firstMasterAddress);
      ActiveMasterManager activeMasterManager =
          ms1.getActiveMasterManager();
      assertFalse(activeMasterManager.clusterHasActiveMaster.get());
      assertFalse(activeMasterManager.getActiveMasterServerName().isPresent());

      // First test becoming the active master uninterrupted
      ClusterStatusTracker clusterStatusTracker =
          ms1.getClusterStatusTracker();
      clusterStatusTracker.setClusterUp();
      activeMasterManager.blockUntilBecomingActiveMaster(100,
          Mockito.mock(MonitoredTask.class));
      assertTrue(activeMasterManager.clusterHasActiveMaster.get());
      assertMaster(zk, firstMasterAddress);
      assertMaster(zk, activeMasterManager.getActiveMasterServerName().get());

      // New manager will now try to become the active master in another thread
      WaitToBeMasterThread t = new WaitToBeMasterThread(zk, secondMasterAddress);
      t.start();
      // Wait for this guy to figure out there is another active master
      // Wait for 1 second at most
      int sleeps = 0;
      while (!t.manager.clusterHasActiveMaster.get() && sleeps < 100) {
        Thread.sleep(10);
        sleeps++;
      }

      // Both should see that there is an active master
      assertTrue(activeMasterManager.clusterHasActiveMaster.get());
      assertTrue(t.manager.clusterHasActiveMaster.get());
      // But secondary one should not be the active master
      assertFalse(t.isActiveMaster);
      // Verify the active master ServerName is populated in standby master.
      assertEquals(firstMasterAddress, t.manager.getActiveMasterServerName().get());

      // Close the first server and delete it's master node
      ms1.stop("stopping first server");

      // Use a listener to capture when the node is actually deleted
      NodeDeletionListener listener = new NodeDeletionListener(zk,
          zk.getZNodePaths().masterAddressZNode);
      zk.registerListener(listener);

      LOG.info("Deleting master node");
      ZKUtil.deleteNode(zk, zk.getZNodePaths().masterAddressZNode);

      // Wait for the node to be deleted
      LOG.info("Waiting for active master manager to be notified");
      listener.waitForDeletion();
      LOG.info("Master node deleted");

      // Now we expect the secondary manager to have and be the active master
      // Wait for 1 second at most
      sleeps = 0;
      while (!t.isActiveMaster && sleeps < 100) {
        Thread.sleep(10);
        sleeps++;
      }
      LOG.debug("Slept " + sleeps + " times");

      assertTrue(t.manager.clusterHasActiveMaster.get());
      assertTrue(t.isActiveMaster);
      assertEquals(secondMasterAddress, t.manager.getActiveMasterServerName().get());

      LOG.info("Deleting master node");

      ZKUtil.deleteNode(zk, zk.getZNodePaths().masterAddressZNode);
    }
  }

  @Test
  public void testBackupMasterUpdates() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (ZKWatcher zk = new ZKWatcher(conf, "testBackupMasterUpdates", null, true)) {
      ServerName sn1 = ServerName.valueOf("localhost", 1, -1);
      DummyMaster master1 = new DummyMaster(zk, sn1);
      ActiveMasterManager activeMasterManager = master1.getActiveMasterManager();
      activeMasterManager.blockUntilBecomingActiveMaster(100,
          Mockito.mock(MonitoredTask.class));
      assertEquals(sn1, activeMasterManager.getActiveMasterServerName().get());
      assertEquals(0, activeMasterManager.getBackupMasters().size());
      // Add backup masters
      List<String> backupZNodes = new ArrayList<>();
      for (int i = 1; i <= 10; i++) {
        ServerName backupSn = ServerName.valueOf("localhost", 1000 + i, -1);
        String backupZn = ZNodePaths.joinZNode(
            zk.getZNodePaths().backupMasterAddressesZNode, backupSn.toString());
        backupZNodes.add(backupZn);
        MasterAddressTracker.setMasterAddress(zk, backupZn, backupSn, 1234);
        TEST_UTIL.waitFor(10000,
          () -> activeMasterManager.getBackupMasters().size() == backupZNodes.size());
      }
      // Remove backup masters
      int numBackups = backupZNodes.size();
      for (String backupZNode: backupZNodes) {
        ZKUtil.deleteNode(zk, backupZNode);
        final int currentBackups = --numBackups;
        TEST_UTIL.waitFor(10000,
          () -> activeMasterManager.getBackupMasters().size() == currentBackups);
      }
    }
  }

  /**
   * Assert there is an active master and that it has the specified address.
   * @param zk single Zookeeper watcher
   * @param expectedAddress the expected address of the master
   * @throws KeeperException unexpected Zookeeper exception
   * @throws IOException if an IO problem is encountered
   */
  private void assertMaster(ZKWatcher zk, ServerName expectedAddress) throws
      KeeperException, IOException {
    ServerName readAddress = MasterAddressTracker.getMasterAddress(zk);
    assertNotNull(readAddress);
    assertEquals(expectedAddress, readAddress);
  }

  public static class WaitToBeMasterThread extends Thread {

    ActiveMasterManager manager;
    DummyMaster dummyMaster;
    boolean isActiveMaster;

    public WaitToBeMasterThread(ZKWatcher zk, ServerName address) throws InterruptedIOException {
      this.dummyMaster = new DummyMaster(zk,address);
      this.manager = this.dummyMaster.getActiveMasterManager();
      isActiveMaster = false;
    }

    @Override
    public void run() {
      manager.blockUntilBecomingActiveMaster(100,
          Mockito.mock(MonitoredTask.class));
      LOG.info("Second master has become the active master!");
      isActiveMaster = true;
    }
  }

  public static class NodeDeletionListener extends ZKListener {
    private static final Logger LOG = LoggerFactory.getLogger(NodeDeletionListener.class);

    private Semaphore lock;
    private String node;

    public NodeDeletionListener(ZKWatcher watcher, String node) {
      super(watcher);
      lock = new Semaphore(0);
      this.node = node;
    }

    @Override
    public void nodeDeleted(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeDeleted(" + path + ")");
        lock.release();
      }
    }

    public void waitForDeletion() throws InterruptedException {
      lock.acquire();
    }
  }

  /**
   * Dummy Master Implementation.
   */
  public static class DummyMaster implements Server {
    private volatile boolean stopped;
    private ClusterStatusTracker clusterStatusTracker;
    private ActiveMasterManager activeMasterManager;

    public DummyMaster(ZKWatcher zk, ServerName master) throws InterruptedIOException {
      this.clusterStatusTracker =
        new ClusterStatusTracker(zk, this);
      clusterStatusTracker.start();

      this.activeMasterManager =
        new ActiveMasterManager(zk, master, this);
      zk.registerListener(activeMasterManager);
    }

    @Override
    public void abort(final String msg, final Throwable t) {}

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public Configuration getConfiguration() {
      return null;
    }

    @Override
    public ZKWatcher getZooKeeper() {
      return null;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return null;
    }

    @Override
    public boolean isStopped() {
      return this.stopped;
    }

    @Override
    public void stop(String why) {
      this.stopped = true;
    }

    @Override
    public ClusterConnection getConnection() {
      return null;
    }

    public ClusterStatusTracker getClusterStatusTracker() {
      return clusterStatusTracker;
    }

    public ActiveMasterManager getActiveMasterManager() {
      return activeMasterManager;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    @Override
    public ClusterConnection getClusterConnection() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public FileSystem getFileSystem() {
      return null;
    }

    @Override
    public boolean isStopping() {
      return false;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return null;
    }
  }
}
