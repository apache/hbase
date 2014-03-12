/**
 * Copyright 2011 The Apache Software Foundation
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

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog.TaskState;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;



public class TestSplitLogWorker {
  private static final Log LOG = LogFactory.getLog(TestSplitLogWorker.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private ZooKeeperWrapper zkw;
  private SplitLogWorker slw;
  
  private interface Expr {
    public long eval();
  }
  
  private void waitForCounter(final AtomicLong ctr, long oldval, long newval,
      long timems) {
    Expr e = new Expr() {
      public long eval() {
        return ctr.get();
      }
    };
    waitForCounter(e, oldval, newval, timems);
    return;
  }
 
  private void waitForCounter(Expr e, long oldval, long newval,
      long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (e.eval() == oldval) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException eintr) {
        }
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, e.eval());
        return;
      }
    }
    assertTrue(false);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniZKCluster();
  }

  @Before
  public void setup() throws Exception {
    slw = null;
    zkw = ZooKeeperWrapper.createInstance(TEST_UTIL.getConfiguration(),
        "split-log-worker-tests");
    zkw.deleteChildrenRecursively(zkw.parentZNode);
    zkw.createZNodeIfNotExists(zkw.parentZNode);
    assertTrue(zkw.checkExists(zkw.parentZNode) != -1);
    LOG.debug(zkw.parentZNode + " created");
    zkw.createZNodeIfNotExists(zkw.splitLogZNode);
    assertTrue(zkw.checkExists(zkw.splitLogZNode) != -1);
    LOG.debug(zkw.splitLogZNode + " created");
    resetCounters();
  }

  @After
  public void teardown() throws Exception {
    zkw.close();
    if (slw != null) {
      slw.stop();
      slw.worker.join(3000);
      if (slw.worker.isAlive()) {
        assertTrue("could not stop the worker thread" == null);
      }
    }
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

    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tatas"),
        TaskState.TASK_UNASSIGNED.get("mgr"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), "rs",
        neverEndingTask);
    slw.start();
    waitForCounter(tot_wkr_task_acquired, 0, 1, 100);
    assertTrue(TaskState.TASK_OWNED.equals(
        zkw.getData("", ZKSplitLog.getEncodedNodeName(zkw, "tatas")), "rs"));
  }

  @Test
  public void testRaceForTask() throws Exception {
    LOG.info("testRaceForTask");

    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "trft"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    SplitLogWorker slw1 = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "svr1", neverEndingTask);
    SplitLogWorker slw2 = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "svr2", neverEndingTask);
    slw1.start();
    slw2.start();
    waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
    Expr e = new Expr() {
      public long eval() {
        return tot_wkr_failed_to_grab_task_lost_race.get() +
            tot_wkr_failed_to_grab_task_owned.get();
      }
    };
    waitForCounter(e, 0, 1, 1000);
    assertTrue(TaskState.TASK_OWNED.equals(
        zkw.getData("", ZKSplitLog.getEncodedNodeName(zkw, "trft")), "svr1")
        || TaskState.TASK_OWNED
            .equals(
                zkw.getData("", ZKSplitLog.getEncodedNodeName(zkw, "trft")),
                "svr2"));
    slw1.stop();
    slw2.stop();
    slw1.worker.join();
    slw2.worker.join();
  }

  @Test
  public void testPreemptTask() throws Exception {
    LOG.info("testPreemptTask");

    slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "tpt_svr", neverEndingTask);
    slw.start();
    Thread.yield(); // let the worker start
    Thread.sleep(100);

    // this time create a task node after starting the splitLogWorker
    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tpt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
    assertEquals(1, slw.taskReadySeq);
    assertTrue(TaskState.TASK_OWNED.equals(
        zkw.getData("", ZKSplitLog.getEncodedNodeName(zkw, "tpt_task")),
        "tpt_svr"));

    zkw.setData(ZKSplitLog.getEncodedNodeName(zkw, "tpt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"));
    waitForCounter(tot_wkr_preempt_task, 0, 1, 1000);
  }

  @Test
  public void testMultipleTasks() throws Exception {
    LOG.info("testMultipleTasks");
    slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "tmt_svr", neverEndingTask);
    slw.start();
    Thread.yield(); // let the worker start
    Thread.sleep(100);

    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tmt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
    // now the worker is busy doing the above task

    // create another task
    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tmt_task_2"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    // preempt the first task, have it owned by another worker
    zkw.setData(ZKSplitLog.getEncodedNodeName(zkw, "tmt_task"),
        TaskState.TASK_OWNED.get("another-worker"));
    waitForCounter(tot_wkr_preempt_task, 0, 1, 1000);

    waitForCounter(tot_wkr_task_acquired, 1, 2, 1000);
    assertEquals(2, slw.taskReadySeq);
    assertTrue(TaskState.TASK_OWNED.equals(
        zkw.getData("", ZKSplitLog.getEncodedNodeName(zkw, "tmt_task_2")),
        "tmt_svr"));
  }

  @Test
  public void testRescan() throws Exception {
    LOG.info("testRescan");
    slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(),
        "svr", neverEndingTask);
    slw.start();
    Thread.yield(); // let the worker start
    Thread.sleep(100);

    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "task"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    waitForCounter(tot_wkr_task_acquired, 0, 1, 1000);
    // now the worker is busy doing the above task

    // preempt the task, have it owned by another worker
    zkw.setData(ZKSplitLog.getEncodedNodeName(zkw, "task"),
        TaskState.TASK_UNASSIGNED.get("manager"));
    waitForCounter(tot_wkr_preempt_task, 0, 1, 1000);

    // create a RESCAN node
    zkw.getZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "RESCAN"),
        TaskState.TASK_DONE.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);

    waitForCounter(tot_wkr_task_acquired, 1, 2, 1000);
    // RESCAN node might not have been processed if the worker became busy
    // with the above task. preempt the task again so that now the RESCAN
    // node is processed
    zkw.setData(ZKSplitLog.getEncodedNodeName(zkw, "task"),
        TaskState.TASK_UNASSIGNED.get("manager"));
    waitForCounter(tot_wkr_preempt_task, 1, 2, 1000);

    List<String> nodes = zkw.listChildrenNoWatch(zkw.splitLogZNode);
    LOG.debug(nodes);
    assertEquals(2, nodes.size());
  }
}
