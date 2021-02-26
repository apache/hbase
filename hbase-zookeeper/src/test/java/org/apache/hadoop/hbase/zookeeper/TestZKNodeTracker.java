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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ZKTests.class, MediumTests.class })
public class TestZKNodeTracker {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKNodeTracker.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestZKNodeTracker.class);
  private final static HBaseZKTestingUtility TEST_UTIL = new HBaseZKTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  /**
   * Test that we can interrupt a node that is blocked on a wait.
   */
  @Test
  public void testInterruptible() throws IOException, InterruptedException {
    Abortable abortable = new StubAbortable();
    ZKWatcher zk = new ZKWatcher(TEST_UTIL.getConfiguration(), "testInterruptible", abortable);
    final TestTracker tracker = new TestTracker(zk, "/xyz", abortable);
    tracker.start();
    Thread t = new Thread(() -> {
      try {
        tracker.blockUntilAvailable();
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted", e);
      }
    });
    t.start();
    while (!t.isAlive()) {
      Threads.sleep(1);
    }
    tracker.stop();
    t.join();
    // If it wasn't interruptible, we'd never get to here.
  }

  @Test
  public void testNodeTracker() throws Exception {
    Abortable abortable = new StubAbortable();
    ZKWatcher zk = new ZKWatcher(TEST_UTIL.getConfiguration(),
        "testNodeTracker", abortable);
    ZKUtil.createAndFailSilent(zk, zk.getZNodePaths().baseZNode);

    final String node = ZNodePaths.joinZNode(zk.getZNodePaths().baseZNode,
      Long.toString(ThreadLocalRandom.current().nextLong()));

    final byte [] dataOne = Bytes.toBytes("dataOne");
    final byte [] dataTwo = Bytes.toBytes("dataTwo");

    // Start a ZKNT with no node currently available
    TestTracker localTracker = new TestTracker(zk, node, abortable);
    localTracker.start();
    zk.registerListener(localTracker);

    // Make sure we don't have a node
    assertNull(localTracker.getData(false));

    // Spin up a thread with another ZKNT and have it block
    WaitToGetDataThread thread = new WaitToGetDataThread(zk, node);
    thread.start();

    // Verify the thread doesn't have a node
    assertFalse(thread.hasData);

    // Now, start a new ZKNT with the node already available
    TestTracker secondTracker = new TestTracker(zk, node, null);
    secondTracker.start();
    zk.registerListener(secondTracker);

    // Put up an additional zk listener so we know when zk event is done
    TestingZKListener zkListener = new TestingZKListener(zk, node);
    zk.registerListener(zkListener);
    assertEquals(0, zkListener.createdLock.availablePermits());

    // Create a completely separate zk connection for test triggers and avoid
    // any weird watcher interactions from the test
    final ZooKeeper zkconn = ZooKeeperHelper.
        getConnectedZooKeeper(ZKConfig.getZKQuorumServersString(TEST_UTIL.getConfiguration()),
            60000);

    // Add the node with data one
    zkconn.create(node, dataOne, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // Wait for the zk event to be processed
    zkListener.waitForCreation();
    thread.join();

    // Both trackers should have the node available with data one
    assertNotNull(localTracker.getData(false));
    assertNotNull(localTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(localTracker.getData(false), dataOne));
    assertTrue(thread.hasData);
    assertTrue(Bytes.equals(thread.tracker.getData(false), dataOne));
    LOG.info("Successfully got data one");

    // Make sure it's available and with the expected data
    assertNotNull(secondTracker.getData(false));
    assertNotNull(secondTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(secondTracker.getData(false), dataOne));
    LOG.info("Successfully got data one with the second tracker");

    // Drop the node
    zkconn.delete(node, -1);
    zkListener.waitForDeletion();

    // Create a new thread but with the existing thread's tracker to wait
    TestTracker threadTracker = thread.tracker;
    thread = new WaitToGetDataThread(threadTracker);
    thread.start();

    // Verify other guys don't have data
    assertFalse(thread.hasData);
    assertNull(secondTracker.getData(false));
    assertNull(localTracker.getData(false));
    LOG.info("Successfully made unavailable");

    // Create with second data
    zkconn.create(node, dataTwo, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    // Wait for the zk event to be processed
    zkListener.waitForCreation();
    thread.join();

    // All trackers should have the node available with data two
    assertNotNull(localTracker.getData(false));
    assertNotNull(localTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(localTracker.getData(false), dataTwo));
    assertNotNull(secondTracker.getData(false));
    assertNotNull(secondTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(secondTracker.getData(false), dataTwo));
    assertTrue(thread.hasData);
    assertTrue(Bytes.equals(thread.tracker.getData(false), dataTwo));
    LOG.info("Successfully got data two on all trackers and threads");

    // Change the data back to data one
    zkconn.setData(node, dataOne, -1);

    // Wait for zk event to be processed
    zkListener.waitForDataChange();

    // All trackers should have the node available with data one
    assertNotNull(localTracker.getData(false));
    assertNotNull(localTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(localTracker.getData(false), dataOne));
    assertNotNull(secondTracker.getData(false));
    assertNotNull(secondTracker.blockUntilAvailable());
    assertTrue(Bytes.equals(secondTracker.getData(false), dataOne));
    assertTrue(thread.hasData);
    assertTrue(Bytes.equals(thread.tracker.getData(false), dataOne));
    LOG.info("Successfully got data one following a data change on all trackers and threads");
  }

  public static class WaitToGetDataThread extends Thread {
    TestTracker tracker;
    boolean hasData;

    WaitToGetDataThread(ZKWatcher zk, String node) {
      tracker = new TestTracker(zk, node, null);
      tracker.start();
      zk.registerListener(tracker);
      hasData = false;
    }

    WaitToGetDataThread(TestTracker tracker) {
      this.tracker = tracker;
      hasData = false;
    }

    @Override
    public void run() {
      LOG.info("Waiting for data to be available in WaitToGetDataThread");
      try {
        tracker.blockUntilAvailable();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      LOG.info("Data now available in tracker from WaitToGetDataThread");
      hasData = true;
    }
  }

  public static class TestTracker extends ZKNodeTracker {
    TestTracker(ZKWatcher watcher, String node, Abortable abortable) {
      super(watcher, node, abortable);
    }
  }

  public static class TestingZKListener extends ZKListener {
    private static final Logger LOG = LoggerFactory.getLogger(TestingZKListener.class);

    private Semaphore deletedLock;
    private Semaphore createdLock;
    private Semaphore changedLock;
    private String node;

    TestingZKListener(ZKWatcher watcher, String node) {
      super(watcher);
      deletedLock = new Semaphore(0);
      createdLock = new Semaphore(0);
      changedLock = new Semaphore(0);
      this.node = node;
    }

    @Override
    public void nodeDeleted(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeDeleted(" + path + ")");
        deletedLock.release();
      }
    }

    @Override
    public void nodeCreated(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeCreated(" + path + ")");
        createdLock.release();
      }
    }

    @Override
    public void nodeDataChanged(String path) {
      if(path.equals(node)) {
        LOG.debug("nodeDataChanged(" + path + ")");
        changedLock.release();
      }
    }

    void waitForDeletion() throws InterruptedException {
      deletedLock.acquire();
    }

    void waitForCreation() throws InterruptedException {
      createdLock.acquire();
    }

    void waitForDataChange() throws InterruptedException {
      changedLock.acquire();
    }
  }

  public static class StubAbortable implements Abortable {
    @Override
    public void abort(final String msg, final Throwable t) {}

    @Override
    public boolean isAborted() {
      return false;
    }
  }

  @Test
  public void testCleanZNode() throws Exception {
    ZKWatcher zkw = new ZKWatcher(TEST_UTIL.getConfiguration(),
        "testNodeTracker", new TestZKNodeTracker.StubAbortable());

    final ServerName sn = ServerName.valueOf("127.0.0.1:52", 45L);

    ZKUtil.createAndFailSilent(zkw,
        TEST_UTIL.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT,
            HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT));

    final String nodeName =  zkw.getZNodePaths().masterAddressZNode;

    // Check that we manage the case when there is no data
    ZKUtil.createAndFailSilent(zkw, nodeName);
    MasterAddressTracker.deleteIfEquals(zkw, sn.toString());
    assertNotNull(ZKUtil.getData(zkw, nodeName));

    // Check that we don't delete if we're not supposed to
    ZKUtil.setData(zkw, nodeName, MasterAddressTracker.toByteArray(sn, 0));
    MasterAddressTracker.deleteIfEquals(zkw, ServerName.valueOf("127.0.0.2:52", 45L).toString());
    assertNotNull(ZKUtil.getData(zkw, nodeName));

    // Check that we delete when we're supposed to
    ZKUtil.setData(zkw, nodeName,MasterAddressTracker.toByteArray(sn, 0));
    MasterAddressTracker.deleteIfEquals(zkw, sn.toString());
    assertNull(ZKUtil.getData(zkw, nodeName));

    // Check that we support the case when the znode does not exist
    MasterAddressTracker.deleteIfEquals(zkw, sn.toString()); // must not throw an exception
  }
}
