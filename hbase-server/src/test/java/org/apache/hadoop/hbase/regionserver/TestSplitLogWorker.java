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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSplitLogWorker {
  private static final Log LOG = LogFactory.getLog(TestSplitLogWorker.class);
  private final ServerName MANAGER = new ServerName("manager,1,1");
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }
  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private ZooKeeperWatcher zkw;
  private SplitLogWorker slw;

  private void waitForCounter(AtomicLong ctr, long oldval, long newval,
      long timems) {
    assertTrue("ctr=" + ctr.get() + ", oldval=" + oldval + ", newval=" + newval,
      waitForCounterBoolean(ctr, oldval, newval, timems));
  }

  private boolean waitForCounterBoolean(AtomicLong ctr, long oldval, long newval,
      long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.get() == oldval) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, ctr.get());
        return true;
      }
    }
    return false;
  }

  @Before
  public void setup() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "split-log-worker-tests", null);
    ZKUtil.deleteChildrenRecursively(zkw, zkw.baseZNode);
    ZKUtil.createAndFailSilent(zkw, zkw.baseZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.baseZNode) != -1);
    LOG.debug(zkw.baseZNode + " created");
    ZKUtil.createAndFailSilent(zkw, zkw.splitLogZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.splitLogZNode) != -1);
    LOG.debug(zkw.splitLogZNode + " created");
    SplitLogCounters.resetCounters();

  }

  @After
  public void teardown() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  SplitLogWorker.TaskExecutor neverEndingTask =
    new SplitLogWorker.TaskExecutor() {

      @Override
      public Status exec(String name, CancelableProgressable p) {
        while (true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            return Status.PREEMPTED;
          }
          if (!p.progress()) {
            return Status.PREEMPTED;
          }
        }
      }

  };

  @Test
  public void testAcquireTaskAtStartup() throws Exception {
    LOG.info("testAcquireTaskAtStartup");
    SplitLogCounters.resetCounters();
    final String TATAS = "tatas";
    final ServerName RS = new ServerName("rs,1,1");
    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, TATAS),
      new SplitLogTask.Unassigned(new ServerName("mgr,1,1")).toByteArray(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    SplitLogWorker slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), RS, neverEndingTask);
    slw.start();
    try {
      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, 1000);
      byte [] bytes = ZKUtil.getData(zkw, ZKSplitLog.getEncodedNodeName(zkw, TATAS));
      SplitLogTask slt = SplitLogTask.parseFrom(bytes);
      assertTrue(slt.isOwned(RS));
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  private void stopSplitLogWorker(final SplitLogWorker slw)
  throws InterruptedException {
    if (slw != null) {
      slw.stop();
      slw.worker.join(3000);
      if (slw.worker.isAlive()) {
        assertTrue(("Could not stop the worker thread slw=" + slw) == null);
      }
    }
  }

  @Test
  public void testRaceForTask() throws Exception {
    LOG.info("testRaceForTask");
    SplitLogCounters.resetCounters();
    final String TRFT = "trft";
    final ServerName SVR1 = new ServerName("svr1,1,1");
    final ServerName SVR2 = new ServerName("svr2,1,1");
    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, TRFT),
      new SplitLogTask.Unassigned(MANAGER).toByteArray(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    SplitLogWorker slw1 = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), SVR1, neverEndingTask);
    SplitLogWorker slw2 = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), SVR2, neverEndingTask);
    slw1.start();
    slw2.start();
    try {
      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, 1000);
      // Assert that either the tot_wkr_failed_to_grab_task_owned count was set of if
      // not it, that we fell through to the next counter in line and it was set.
      assertTrue(waitForCounterBoolean(SplitLogCounters.tot_wkr_failed_to_grab_task_owned, 0, 1, 1000) ||
          SplitLogCounters.tot_wkr_failed_to_grab_task_lost_race.get() == 1);
      byte [] bytes = ZKUtil.getData(zkw, ZKSplitLog.getEncodedNodeName(zkw, TRFT));
      SplitLogTask slt = SplitLogTask.parseFrom(bytes);
      assertTrue(slt.isOwned(SVR1) || slt.isOwned(SVR2));
    } finally {
      stopSplitLogWorker(slw1);
      stopSplitLogWorker(slw2);
    }
  }

  @Test
  public void testPreemptTask() throws Exception {
    LOG.info("testPreemptTask");
    SplitLogCounters.resetCounters();
    final ServerName SRV = new ServerName("tpt_svr,1,1");
    final String PATH = ZKSplitLog.getEncodedNodeName(zkw, "tpt_task");
    SplitLogWorker slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), SRV, neverEndingTask);
    slw.start();
    try {
      Thread.yield(); // let the worker start
      Thread.sleep(100);

      // this time create a task node after starting the splitLogWorker
      zkw.getRecoverableZooKeeper().create(PATH,
        new SplitLogTask.Unassigned(MANAGER).toByteArray(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, 1000);
      assertEquals(1, slw.taskReadySeq);
      byte [] bytes = ZKUtil.getData(zkw, PATH);
      SplitLogTask slt = SplitLogTask.parseFrom(bytes);
      assertTrue(slt.isOwned(SRV));
      slt = new SplitLogTask.Unassigned(MANAGER);
      ZKUtil.setData(zkw, PATH, slt.toByteArray());
      waitForCounter(SplitLogCounters.tot_wkr_preempt_task, 0, 1, 1000);
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  @Test
  public void testMultipleTasks() throws Exception {
    LOG.info("testMultipleTasks");
    SplitLogCounters.resetCounters();
    final ServerName SRV = new ServerName("tmt_svr,1,1");
    final String PATH1 = ZKSplitLog.getEncodedNodeName(zkw, "tmt_task");
    SplitLogWorker slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), SRV, neverEndingTask);
    slw.start();
    try {
      Thread.yield(); // let the worker start
      Thread.sleep(100);
      SplitLogTask unassignedManager = new SplitLogTask.Unassigned(MANAGER);
      zkw.getRecoverableZooKeeper().create(PATH1, unassignedManager.toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, 1000);
      // now the worker is busy doing the above task

      // create another task
      final String PATH2 = ZKSplitLog.getEncodedNodeName(zkw, "tmt_task_2");
      zkw.getRecoverableZooKeeper().create(PATH2, unassignedManager.toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      // preempt the first task, have it owned by another worker
      final ServerName anotherWorker = new ServerName("another-worker,1,1");
      SplitLogTask slt = new SplitLogTask.Owned(anotherWorker);
      ZKUtil.setData(zkw, PATH1, slt.toByteArray());
      waitForCounter(SplitLogCounters.tot_wkr_preempt_task, 0, 1, 1000);

      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 1, 2, 1000);
      assertEquals(2, slw.taskReadySeq);
      byte [] bytes = ZKUtil.getData(zkw, PATH2);
      slt = SplitLogTask.parseFrom(bytes);
      assertTrue(slt.isOwned(SRV));
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  @Test
  public void testRescan() throws Exception {
    LOG.info("testRescan");
    SplitLogCounters.resetCounters();
    final ServerName SRV = new ServerName("svr,1,1");
    slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), SRV, neverEndingTask);
    slw.start();
    Thread.yield(); // let the worker start
    Thread.sleep(100);

    String task = ZKSplitLog.getEncodedNodeName(zkw, "task");
    SplitLogTask slt = new SplitLogTask.Unassigned(MANAGER);
    zkw.getRecoverableZooKeeper().create(task,slt.toByteArray(), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);

    waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, 1000);
    // now the worker is busy doing the above task

    // preempt the task, have it owned by another worker
    ZKUtil.setData(zkw, task, slt.toByteArray());
    waitForCounter(SplitLogCounters.tot_wkr_preempt_task, 0, 1, 1000);

    // create a RESCAN node
    String rescan = ZKSplitLog.getEncodedNodeName(zkw, "RESCAN");
    rescan = zkw.getRecoverableZooKeeper().create(rescan, slt.toByteArray(), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT_SEQUENTIAL);

    waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 1, 2, 1000);
    // RESCAN node might not have been processed if the worker became busy
    // with the above task. preempt the task again so that now the RESCAN
    // node is processed
    ZKUtil.setData(zkw, task, slt.toByteArray());
    waitForCounter(SplitLogCounters.tot_wkr_preempt_task, 1, 2, 1000);
    waitForCounter(SplitLogCounters.tot_wkr_task_acquired_rescan, 0, 1, 1000);

    List<String> nodes = ZKUtil.listChildrenNoWatch(zkw, zkw.splitLogZNode);
    LOG.debug(nodes);
    int num = 0;
    for (String node : nodes) {
      num++;
      if (node.startsWith("RESCAN")) {
        String name = ZKSplitLog.getEncodedNodeName(zkw, node);
        String fn = ZKSplitLog.getFileName(name);
        byte [] data = ZKUtil.getData(zkw, ZKUtil.joinZNode(zkw.splitLogZNode, fn));
        slt = SplitLogTask.parseFrom(data);
        assertTrue(slt.toString(), slt.isDone(SRV));
      }
    }
    assertEquals(2, num);
  }

}
