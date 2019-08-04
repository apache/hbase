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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ZKTests.class, MediumTests.class })
public class TestZKLeaderManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZKLeaderManager.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestZKLeaderManager.class);

  private static final String LEADER_ZNODE =
      "/test/" + TestZKLeaderManager.class.getSimpleName();

  private static class MockAbortable implements Abortable {
    private boolean aborted;

    @Override
    public void abort(String why, Throwable e) {
      aborted = true;
      LOG.error(HBaseMarkers.FATAL, "Aborting during test: "+why, e);
      fail("Aborted during test: " + why);
    }

    @Override
    public boolean isAborted() {
      return aborted;
    }
  }

  private static class MockLeader extends Thread implements Stoppable {
    private volatile boolean stopped;
    private ZKWatcher watcher;
    private ZKLeaderManager zkLeader;
    private AtomicBoolean master = new AtomicBoolean(false);
    private int index;

    MockLeader(ZKWatcher watcher, int index) {
      setDaemon(true);
      setName("TestZKLeaderManager-leader-" + index);
      this.index = index;
      this.watcher = watcher;
      this.zkLeader = new ZKLeaderManager(watcher, LEADER_ZNODE,
          Bytes.toBytes(index), this);
    }

    public boolean isMaster() {
      return master.get();
    }

    public int getIndex() {
      return index;
    }

    public ZKWatcher getWatcher() {
      return watcher;
    }

    @Override
    public void run() {
      while (!stopped) {
        zkLeader.start();
        zkLeader.waitToBecomeLeader();
        master.set(true);

        while (master.get() && !stopped) {
          try {
            Thread.sleep(10);
          } catch (InterruptedException ignored) {}
        }
      }
    }

    void abdicate() {
      zkLeader.stepDownAsLeader();
      master.set(false);
    }

    @Override
    public void stop(String why) {
      stopped = true;
      abdicate();
      Threads.sleep(100);
      watcher.close();
    }

    @Override
    public boolean isStopped() {
      return stopped;
    }
  }

  private static HBaseZKTestingUtility TEST_UTIL;
  private static MockLeader[] CANDIDATES;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    TEST_UTIL = new HBaseZKTestingUtility();
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();

    // use an abortable to fail the test in the case of any KeeperExceptions
    MockAbortable abortable = new MockAbortable();
    int count = 5;
    CANDIDATES = new MockLeader[count];
    for (int i = 0; i < count; i++) {
      ZKWatcher watcher = newZK(conf, "server"+i, abortable);
      CANDIDATES[i] = new MockLeader(watcher, i);
      CANDIDATES[i].start();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testLeaderSelection() throws Exception {
    MockLeader currentLeader = getCurrentLeader();
    // one leader should have been found
    assertNotNull("Leader should exist", currentLeader);
    LOG.debug("Current leader index is "+currentLeader.getIndex());

    byte[] znodeData = ZKUtil.getData(currentLeader.getWatcher(), LEADER_ZNODE);
    assertNotNull("Leader znode should contain leader index", znodeData);
    assertTrue("Leader znode should not be empty", znodeData.length > 0);
    int storedIndex = Bytes.toInt(znodeData);
    LOG.debug("Stored leader index in ZK is "+storedIndex);
    assertEquals("Leader znode should match leader index",
        currentLeader.getIndex(), storedIndex);

    // force a leader transition
    currentLeader.abdicate();

    // check for new leader
    currentLeader = getCurrentLeader();
    // one leader should have been found
    assertNotNull("New leader should exist after abdication", currentLeader);
    LOG.debug("New leader index is "+currentLeader.getIndex());

    znodeData = ZKUtil.getData(currentLeader.getWatcher(), LEADER_ZNODE);
    assertNotNull("Leader znode should contain leader index", znodeData);
    assertTrue("Leader znode should not be empty", znodeData.length > 0);
    storedIndex = Bytes.toInt(znodeData);
    LOG.debug("Stored leader index in ZK is "+storedIndex);
    assertEquals("Leader znode should match leader index",
        currentLeader.getIndex(), storedIndex);

    // force another transition by stopping the current
    currentLeader.stop("Stopping for test");

    // check for new leader
    currentLeader = getCurrentLeader();
    // one leader should have been found
    assertNotNull("New leader should exist after stop", currentLeader);
    LOG.debug("New leader index is "+currentLeader.getIndex());

    znodeData = ZKUtil.getData(currentLeader.getWatcher(), LEADER_ZNODE);
    assertNotNull("Leader znode should contain leader index", znodeData);
    assertTrue("Leader znode should not be empty", znodeData.length > 0);
    storedIndex = Bytes.toInt(znodeData);
    LOG.debug("Stored leader index in ZK is "+storedIndex);
    assertEquals("Leader znode should match leader index",
        currentLeader.getIndex(), storedIndex);

    // with a second stop we can guarantee that a previous leader has resumed leading
    currentLeader.stop("Stopping for test");

    // check for new
    currentLeader = getCurrentLeader();
    assertNotNull("New leader should exist", currentLeader);
  }

  private MockLeader getCurrentLeader() {
    MockLeader currentLeader = null;

    // Wait up to 10 secs for initial leader
    for (int i = 0; i < 1000; i++) {
      for (int j = 0; j < CANDIDATES.length; j++) {
        if (CANDIDATES[j].isMaster()) {
          // should only be one leader
          if (currentLeader != null) {
            fail("Both candidate "+currentLeader.getIndex()+" and "+j+" claim to be leader!");
          }
          currentLeader = CANDIDATES[j];
        }
      }
      if (currentLeader != null) {
        break;
      }
      Threads.sleep(100);
    }
    return currentLeader;
  }

  private static ZKWatcher newZK(Configuration conf, String name, Abortable abort)
      throws Exception {
    Configuration copy = HBaseConfiguration.create(conf);
    return new ZKWatcher(copy, name, abort);
  }
}
