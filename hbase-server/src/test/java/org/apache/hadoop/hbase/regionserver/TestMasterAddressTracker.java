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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestMasterAddressTracker {
  private static final Log LOG = LogFactory.getLog(TestMasterAddressTracker.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testDeleteIfEquals() throws Exception {
    final ServerName sn = ServerName.valueOf("localhost", 1234, System.currentTimeMillis());
    final MasterAddressTracker addressTracker = setupMasterTracker(sn, 1772);
    try {
      assertFalse("shouldn't have deleted wrong master server.",
          MasterAddressTracker.deleteIfEquals(addressTracker.getWatcher(), "some other string."));
    } finally {
      assertTrue("Couldn't clean up master",
          MasterAddressTracker.deleteIfEquals(addressTracker.getWatcher(), sn.toString()));
    }
  }

  /**
   * create an address tracker instance
   * @param sn if not-null set the active master
   * @param infoPort if there is an active master, set its info port.
   */
  private MasterAddressTracker setupMasterTracker(final ServerName sn, final int infoPort)
      throws Exception {
    ZooKeeperWatcher zk = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        name.getMethodName(), null);
    ZKUtil.createAndFailSilent(zk, zk.znodePaths.baseZNode);

    // Should not have a master yet
    MasterAddressTracker addressTracker = new MasterAddressTracker(zk, null);
    addressTracker.start();
    assertFalse(addressTracker.hasMaster());
    zk.registerListener(addressTracker);

    // Use a listener to capture when the node is actually created
    NodeCreationListener listener = new NodeCreationListener(zk, zk.znodePaths.masterAddressZNode);
    zk.registerListener(listener);

    if (sn != null) {
      LOG.info("Creating master node");
      MasterAddressTracker.setMasterAddress(zk, zk.znodePaths.masterAddressZNode, sn, infoPort);

      // Wait for the node to be created
      LOG.info("Waiting for master address manager to be notified");
      listener.waitForCreation();
      LOG.info("Master node created");
    }
    return addressTracker;
  }

  /**
   * Unit tests that uses ZooKeeper but does not use the master-side methods
   * but rather acts directly on ZK.
   * @throws Exception
   */
  @Test
  public void testMasterAddressTrackerFromZK() throws Exception {
    // Create the master node with a dummy address
    final int infoPort = 1235;
    final ServerName sn = ServerName.valueOf("localhost", 1234, System.currentTimeMillis());
    final MasterAddressTracker addressTracker = setupMasterTracker(sn, infoPort);
    try {
      assertTrue(addressTracker.hasMaster());
      ServerName pulledAddress = addressTracker.getMasterAddress();
      assertTrue(pulledAddress.equals(sn));
      assertEquals(infoPort, addressTracker.getMasterInfoPort());
    } finally {
      assertTrue("Couldn't clean up master",
          MasterAddressTracker.deleteIfEquals(addressTracker.getWatcher(), sn.toString()));
    }
  }


  @Test
  public void testParsingNull() throws Exception {
    assertNull("parse on null data should return null.", MasterAddressTracker.parse(null));
  }

  @Test
  public void testNoBackups() throws Exception {
    final ServerName sn = ServerName.valueOf("localhost", 1234, System.currentTimeMillis());
    final MasterAddressTracker addressTracker = setupMasterTracker(sn, 1772);
    try {
      assertEquals("Should receive 0 for backup not found.", 0,
          addressTracker.getBackupMasterInfoPort(
              ServerName.valueOf("doesnotexist.example.com", 1234, System.currentTimeMillis())));
    } finally {
      assertTrue("Couldn't clean up master",
          MasterAddressTracker.deleteIfEquals(addressTracker.getWatcher(), sn.toString()));
    }
  }

  @Test
  public void testNoMaster() throws Exception {
    final MasterAddressTracker addressTracker = setupMasterTracker(null, 1772);
    assertFalse(addressTracker.hasMaster());
    assertNull("should get null master when none active.", addressTracker.getMasterAddress());
    assertEquals("Should receive 0 for backup not found.", 0, addressTracker.getMasterInfoPort());
  }

  public static class NodeCreationListener extends ZooKeeperListener {
    private static final Log LOG = LogFactory.getLog(NodeCreationListener.class);

    private Semaphore lock;
    private String node;

    public NodeCreationListener(ZooKeeperWatcher watcher, String node) {
      super(watcher);
      lock = new Semaphore(0);
      this.node = node;
    }

    @Override
    public void nodeCreated(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeCreated(" + path + ")");
        lock.release();
      }
    }

    public void waitForCreation() throws InterruptedException {
      lock.acquire();
    }
  }

}

