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
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMasterAddressTracker {
  private static final Log LOG = LogFactory.getLog(TestMasterAddressTracker.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }
  /**
   * Unit tests that uses ZooKeeper but does not use the master-side methods
   * but rather acts directly on ZK.
   * @throws Exception
   */
  @Test
  public void testMasterAddressTrackerFromZK() throws Exception {

    ZooKeeperWatcher zk = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "testMasterAddressTrackerFromZK", null);
    ZKUtil.createAndFailSilent(zk, zk.baseZNode);

    // Should not have a master yet
    MasterAddressTracker addressTracker = new MasterAddressTracker(zk, null);
    addressTracker.start();
    assertFalse(addressTracker.hasMaster());
    zk.registerListener(addressTracker);

    // Use a listener to capture when the node is actually created
    NodeCreationListener listener = new NodeCreationListener(zk, zk.getMasterAddressZNode());
    zk.registerListener(listener);

    // Create the master node with a dummy address
    String host = "localhost";
    int port = 1234;
    int infoPort = 1235;
    ServerName sn = ServerName.valueOf(host, port, System.currentTimeMillis());
    LOG.info("Creating master node");
    MasterAddressTracker.setMasterAddress(zk, zk.getMasterAddressZNode(), sn, infoPort);

    // Wait for the node to be created
    LOG.info("Waiting for master address manager to be notified");
    listener.waitForCreation();
    LOG.info("Master node created");
    assertTrue(addressTracker.hasMaster());
    ServerName pulledAddress = addressTracker.getMasterAddress();
    assertTrue(pulledAddress.equals(sn));
    assertEquals(infoPort, addressTracker.getMasterInfoPort());
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

