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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.hadoop.hbase.HBaseZKTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(ZKTests.TAG)
@Tag(MediumTests.TAG)
public class TestMasterAddressTracker {

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterAddressTracker.class);

  private final static HBaseZKTestingUtil TEST_UTIL = new HBaseZKTestingUtil();

  // Cleaned up after each unit test.
  private ZKWatcher zk;

  private String methodName;

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    methodName = testInfo.getTestMethod().get().getName();
  }

  @AfterEach
  public void cleanUp() {
    if (zk != null) {
      zk.close();
    }
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testDeleteIfEquals() throws Exception {
    final ServerName sn =
      ServerName.valueOf("localhost", 1234, EnvironmentEdgeManager.currentTime());
    final MasterAddressTracker addressTracker = setupMasterTracker(sn, 1772);
    try {
      assertFalse(
        MasterAddressTracker.deleteIfEquals(addressTracker.getWatcher(), "some other string."),
        "shouldn't have deleted wrong master server.");
    } finally {
      assertTrue(MasterAddressTracker.deleteIfEquals(addressTracker.getWatcher(), sn.toString()),
        "Couldn't clean up master");
    }
  }

  /**
   * create an address tracker instance
   * @param sn       if not-null set the active master
   * @param infoPort if there is an active master, set its info port.
   */
  private MasterAddressTracker setupMasterTracker(final ServerName sn, final int infoPort)
    throws Exception {
    zk = new ZKWatcher(TEST_UTIL.getConfiguration(), methodName, null);
    ZKUtil.createAndFailSilent(zk, zk.getZNodePaths().baseZNode);
    ZKUtil.createAndFailSilent(zk, zk.getZNodePaths().backupMasterAddressesZNode);

    // Should not have a master yet
    MasterAddressTracker addressTracker = new MasterAddressTracker(zk, null);
    addressTracker.start();
    assertFalse(addressTracker.hasMaster());
    zk.registerListener(addressTracker);

    // Use a listener to capture when the node is actually created
    NodeCreationListener listener =
      new NodeCreationListener(zk, zk.getZNodePaths().masterAddressZNode);
    zk.registerListener(listener);

    if (sn != null) {
      LOG.info("Creating master node");
      MasterAddressTracker.setMasterAddress(zk, zk.getZNodePaths().masterAddressZNode, sn,
        infoPort);

      // Wait for the node to be created
      LOG.info("Waiting for master address manager to be notified");
      listener.waitForCreation();
      LOG.info("Master node created");
    }
    return addressTracker;
  }

  /**
   * Unit tests that uses ZooKeeper but does not use the master-side methods but rather acts
   * directly on ZK.
   */
  @Test
  public void testMasterAddressTrackerFromZK() throws Exception {
    // Create the master node with a dummy address
    final int infoPort = 1235;
    final ServerName sn =
      ServerName.valueOf("localhost", 1234, EnvironmentEdgeManager.currentTime());
    final MasterAddressTracker addressTracker = setupMasterTracker(sn, infoPort);
    try {
      assertTrue(addressTracker.hasMaster());
      ServerName pulledAddress = addressTracker.getMasterAddress();
      assertEquals(pulledAddress, sn);
      assertEquals(infoPort, addressTracker.getMasterInfoPort());
    } finally {
      assertTrue(MasterAddressTracker.deleteIfEquals(addressTracker.getWatcher(), sn.toString()),
        "Couldn't clean up master");
    }
  }

  @Test
  public void testParsingNull() throws Exception {
    assertNull(MasterAddressTracker.parse(null), "parse on null data should return null.");
  }

  @Test
  public void testNoBackups() throws Exception {
    final ServerName sn =
      ServerName.valueOf("localhost", 1234, EnvironmentEdgeManager.currentTime());
    final MasterAddressTracker addressTracker = setupMasterTracker(sn, 1772);
    try {
      assertEquals(0, addressTracker.getBackupMasterInfoPort(
        ServerName.valueOf("doesnotexist.example.com", 1234, EnvironmentEdgeManager.currentTime())),
        "Should receive 0 for backup not found.");
    } finally {
      assertTrue(MasterAddressTracker.deleteIfEquals(addressTracker.getWatcher(), sn.toString()),
        "Couldn't clean up master");
    }
  }

  @Test
  public void testNoMaster() throws Exception {
    final MasterAddressTracker addressTracker = setupMasterTracker(null, 1772);
    assertFalse(addressTracker.hasMaster());
    assertNull(addressTracker.getMasterAddress(), "should get null master when none active.");
    assertEquals(0, addressTracker.getMasterInfoPort(), "Should receive 0 for backup not found.");
  }

  @Test
  public void testBackupMasters() throws Exception {
    final ServerName sn =
      ServerName.valueOf("localhost", 5678, EnvironmentEdgeManager.currentTime());
    final MasterAddressTracker addressTracker = setupMasterTracker(sn, 1111);
    assertTrue(addressTracker.hasMaster());
    ServerName activeMaster = addressTracker.getMasterAddress();
    assertEquals(activeMaster, sn);
    // No current backup masters
    List<ServerName> backupMasters = addressTracker.getBackupMasters();
    assertEquals(0, backupMasters.size());
    ServerName backupMaster1 = ServerName.valueOf("localhost", 2222, -1);
    ServerName backupMaster2 = ServerName.valueOf("localhost", 3333, -1);
    String backupZNode1 =
      ZNodePaths.joinZNode(zk.getZNodePaths().backupMasterAddressesZNode, backupMaster1.toString());
    String backupZNode2 =
      ZNodePaths.joinZNode(zk.getZNodePaths().backupMasterAddressesZNode, backupMaster2.toString());
    // Add backup masters
    MasterAddressTracker.setMasterAddress(zk, backupZNode1, backupMaster1, 2222);
    MasterAddressTracker.setMasterAddress(zk, backupZNode2, backupMaster2, 3333);
    TEST_UTIL.waitFor(30000, () -> addressTracker.getBackupMasters().size() == 2);
    backupMasters = addressTracker.getBackupMasters();
    assertEquals(2, backupMasters.size());
    assertTrue(backupMasters.contains(backupMaster1));
    assertTrue(backupMasters.contains(backupMaster2));
  }

  public static class NodeCreationListener extends ZKListener {
    private static final Logger LOG = LoggerFactory.getLogger(NodeCreationListener.class);

    private Semaphore lock;
    private String node;

    public NodeCreationListener(ZKWatcher watcher, String node) {
      super(watcher);
      lock = new Semaphore(0);
      this.node = node;
    }

    @Override
    public void nodeCreated(String path) {
      if (path.equals(node)) {
        LOG.debug("nodeCreated(" + path + ")");
        lock.release();
      }
    }

    public void waitForCreation() throws InterruptedException {
      lock.acquire();
    }
  }
}
