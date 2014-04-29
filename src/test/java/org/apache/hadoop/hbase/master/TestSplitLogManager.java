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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.resetCounters;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_get_data_nonode;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_heartbeat;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_log_split_batch_success;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_node_create_queued;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_node_create_result;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_orphan_task_acquired;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_rescan;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_rescan_deleted;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_resubmit;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_resubmit_dead_server_task;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_resubmit_failed;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_resubmit_threshold_reached;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_resubmit_unassigned;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_mgr_task_deleted;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.SplitLogManager.Task;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskBatch;
import org.apache.hadoop.hbase.util.TagRunner;
import org.apache.hadoop.hbase.util.TestTag;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog.TaskState;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(TagRunner.class)
public class TestSplitLogManager {
  private static final Log LOG = LogFactory.getLog(TestSplitLogManager.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  private ZooKeeperWrapper zkw;
  private SplitLogManager slm;
  private Configuration conf;

  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();

  private volatile boolean stopped = false;

  /**
   * Additional amount of time we wait for events to happen. Added where unit
   * test failures have been observed.
   */
  private static final int EXTRA_TOLERANCE_MS = 2000;

  private Stoppable stopper = new Stoppable() {
    @Override
    public boolean isStopped() {
      return stopped;
    }

    @Override
    public void stop(String why) {
      stopped = true;
    }

    @Override
    public String getStopReason() {
      return "test";
    }
  };

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
    conf = TEST_UTIL.getConfiguration();
    // Use a different ZK wrapper instance for each tests.
    zkw = ZooKeeperWrapper.createInstance(conf, "split-log-manager-tests-" +
        UUID.randomUUID().toString());
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
  public void teardown() throws IOException, KeeperException {
    stopped = true;
    zkw.close();
    slm.stop();
  }

  private interface Expr {
    public long eval();
  }

  private void waitForCounter(final AtomicLong ctr, long oldval, long newval,
      long timems) {
    Expr e = new Expr() {
      @Override
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
    LOG.debug("Wait for counter failed");
    assertTrue(false);
  }

  private String submitTaskAndWait(TaskBatch batch, String name)
  throws KeeperException, InterruptedException {
    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, name);
    NodeCreationListener listener = new NodeCreationListener(zkw, tasknode);
    zkw.registerListener(listener);
    zkw.watchAndCheckExists(tasknode);

    slm.installTask(name, batch);
    assertEquals(1, batch.installed);
    assertTrue(slm.findOrCreateOrphanTask(tasknode).batch == batch);
    assertEquals(1L, tot_mgr_node_create_queued.get());

    LOG.debug("waiting for task node creation");
    listener.waitForCreation();
    LOG.debug("task created");
    return tasknode;
  }

  /**
   * Test whether the splitlog correctly creates a task in zookeeper
   * @throws Exception
   */
  @Test
  public void testTaskCreation() throws Exception {
    LOG.info("TestTaskCreation - test the creation of a task in zk");

    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");

    byte[] data = zkw.getData("", tasknode);
    LOG.info("Task node created " + new String(data));
    assertTrue(TaskState.TASK_UNASSIGNED.equals(data, "dummy-master"));
  }

  // Marked as unstable and recorded in #3376834
  @TestTag({ "unstable" })
  @Test
  public void testOrphanTaskAcquisition() throws Exception {
    LOG.info("TestOrphanTaskAcquisition");

    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, "orphan/test/slash");
    zkw.getZooKeeper().create(tasknode,
        TaskState.TASK_OWNED.get("dummy-worker"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    int to = 1000;
    conf.setInt("hbase.splitlog.manager.timeout", to);
    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);
    to = to + 2 * 100;


    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, 1000);
    Task task = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task.isOrphan());
    waitForCounter(tot_mgr_heartbeat, 0, 1, 100);
    assertFalse(task.isUnassigned());
    long curt = System.currentTimeMillis();
    assertTrue((task.last_update <= curt) &&
        (task.last_update > (curt - 1000)));
    LOG.info("waiting for manager to resubmit the orphan task");
    waitForCounter(tot_mgr_resubmit, 0, 1, to + 3000);
    assertTrue(task.isUnassigned());
    waitForCounter(tot_mgr_rescan, 0, 1, to + 1000);
  }

  @Test
  public void testUnassignedOrphan() throws Exception {
    LOG.info("TestUnassignedOrphan - an unassigned task is resubmitted at" +
        " startup");
    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, "orphan/test/slash");
    //create an unassigned orphan task
    zkw.getZooKeeper().create(tasknode,
        TaskState.TASK_UNASSIGNED.get("dummy-worker"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    int version = zkw.checkExists(tasknode);

    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, 1000);
    Task task = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task.isOrphan());
    assertTrue(task.isUnassigned());
    // wait for RESCAN node to be created
    waitForCounter(tot_mgr_rescan, 0, 1, 500);
    Task task2 = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task == task2);
    LOG.debug("task = " + task);
    assertEquals(1L, tot_mgr_resubmit.get());
    assertEquals(1, task.incarnation);
    assertEquals(0, task.unforcedResubmits);
    assertTrue(task.isOrphan());
    assertTrue(task.isUnassigned());
    assertTrue(zkw.checkExists(tasknode) > version);
  }

  @Test
  public void testMultipleResubmits() throws Exception {
    LOG.info("TestMultipleResbmits - no indefinite resubmissions");

    int to = 1000;
    conf.setInt("hbase.splitlog.manager.timeout", to);
    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);
    to = to + 2 * 100;

    conf.setInt("hbase.splitlog.max.resubmit", 2);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = zkw.checkExists(tasknode);

    zkw.setData(tasknode, TaskState.TASK_OWNED.get("worker1"));
    waitForCounter(tot_mgr_heartbeat, 0, 1, 1000);
    waitForCounter(tot_mgr_resubmit, 0, 1, to + EXTRA_TOLERANCE_MS);
    int version1 = zkw.checkExists(tasknode);
    assertTrue(version1 > version);
    zkw.setData(tasknode, TaskState.TASK_OWNED.get("worker2"));
    waitForCounter(tot_mgr_heartbeat, 1, 2, 1000);
    waitForCounter(tot_mgr_resubmit, 1, 2, to + EXTRA_TOLERANCE_MS);
    int version2 = zkw.checkExists(tasknode);
    assertTrue(version2 > version1);
    zkw.setData(tasknode, TaskState.TASK_OWNED.get("worker3"));
    waitForCounter(tot_mgr_heartbeat, 1, 2, 1000);
    waitForCounter(tot_mgr_resubmit_threshold_reached, 0, 1, to + EXTRA_TOLERANCE_MS);
    Thread.sleep(to + EXTRA_TOLERANCE_MS);
    assertEquals(2L, tot_mgr_resubmit.get());
  }

  @Test
  public void testRescanCleanup() throws Exception {
    LOG.info("TestRescanCleanup - ensure RESCAN nodes are cleaned up");

    conf.setInt("hbase.splitlog.manager.timeout", 1000);
    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = zkw.checkExists(tasknode);

    zkw.setData(tasknode, TaskState.TASK_OWNED.get("worker1"));
    waitForCounter(new Expr() {
      @Override
      public long eval() {
        return (tot_mgr_resubmit.get() + tot_mgr_resubmit_failed.get());
      }
    }, 0, 1, 5*60000); // wait long enough
    if (tot_mgr_resubmit_failed.get() == 0) {
      int version1 = zkw.checkExists(tasknode);
      assertTrue(version1 > version);
      byte[] taskstate = zkw.getData("", tasknode);
      assertTrue(Arrays.equals(TaskState.TASK_UNASSIGNED.get("dummy-master"),
          taskstate));

      waitForCounter(tot_mgr_rescan_deleted, 0, 1, 1000);
    } else {
      LOG.warn("Could not run test. Lost ZK connection?");
    }
    return;
  }

  @Test
  public void testTaskDone() throws Exception {
    LOG.info("TestTaskDone - cleanup task node once in DONE state");

    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();
    String tasknode = submitTaskAndWait(batch, "foo/1");
    zkw.setData(tasknode, TaskState.TASK_DONE.get("worker"));
    synchronized (batch) {
      while (batch.installed != batch.done) {
        batch.wait();
      }
    }
    waitForCounter(tot_mgr_task_deleted, 0, 1, 1000);
    assertTrue(zkw.checkExists(tasknode) == -1);
  }

  @Test
  public void testTaskErr() throws Exception {
    LOG.info("TestTaskErr - cleanup task node once in ERR state");

    conf.setInt("hbase.splitlog.max.resubmit", 0);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    zkw.setData(tasknode, TaskState.TASK_ERR.get("worker"));
    synchronized (batch) {
      while (batch.installed != batch.error) {
        batch.wait();
      }
    }
    waitForCounter(tot_mgr_task_deleted, 0, 1, 1000);
    assertTrue(zkw.checkExists(tasknode) == -1);
    conf.setInt("hbase.splitlog.max.resubmit", ZKSplitLog.DEFAULT_MAX_RESUBMIT);
  }

  @Test
  public void testTaskResigned() throws Exception {
    LOG.info("TestTaskResigned - resubmit task node once in RESIGNED state");

    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();
    String tasknode = submitTaskAndWait(batch, "foo/1");
    zkw.setData(tasknode, TaskState.TASK_RESIGNED.get("worker"));
    int version = zkw.checkExists(tasknode);

    waitForCounter(tot_mgr_resubmit, 0, 1, 1000);
    int version1 = zkw.checkExists(tasknode);
    assertTrue(version1 > version);

    byte[] taskstate = zkw.getData("", tasknode);
    assertTrue(Arrays.equals(taskstate,
        TaskState.TASK_UNASSIGNED.get("dummy-master")));
  }

  @Test
  public void testUnassignedTimeout() throws Exception {
    LOG.info("TestUnassignedTimeout - iff all tasks are unassigned then" +
        " resubmit");

    // create an orphan task in OWNED state
    String tasknode1 = ZKSplitLog.getEncodedNodeName(zkw, "orphan/1");
    zkw.getZooKeeper().create(tasknode1,
        TaskState.TASK_OWNED.get("dummy-worker"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    int to = 1000;
    conf.setInt("hbase.splitlog.manager.timeout", to);
    conf.setInt("hbase.splitlog.manager.unassigned.timeout", 2 * to);
    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);


    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, 1000);


    // submit another task which will stay in unassigned mode
    TaskBatch batch = new TaskBatch();
    submitTaskAndWait(batch, "foo/1");

    // keep updating the orphan owned node every to/2 seconds
    for (int i = 0; i < (3 * to)/100; i++) {
      Thread.sleep(100);
      zkw.setData(tasknode1,
          TaskState.TASK_OWNED.get("dummy-worker"));
    }


    // since we have stopped heartbeating the owned node therefore it should
    // get resubmitted
    LOG.info("waiting for manager to resubmit the orphan task");
    waitForCounter(tot_mgr_resubmit, 0, 1, to + 500);

    // now all the nodes are unassigned. manager should post another rescan
    waitForCounter(tot_mgr_resubmit_unassigned, 0, 1, 2 * to + 500);
  }

  @Test
  public void testDeadWorker() throws Exception {
    LOG.info("testDeadWorker");

    conf.setLong("hbase.splitlog.max.resubmit", 0);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = zkw.checkExists(tasknode);

    zkw.setData(tasknode, TaskState.TASK_OWNED.get("worker1"));
    waitForCounter(tot_mgr_heartbeat, 0, 1, 1000);
    slm.handleDeadServer("worker1");
    waitForCounter(tot_mgr_resubmit, 0, 1, 1000);
    waitForCounter(tot_mgr_resubmit_dead_server_task, 0, 1, 1000);

    int version1 = zkw.checkExists(tasknode);
    assertTrue(version1 > version);
    byte[] taskstate = zkw.getData("", tasknode);
    assertTrue(Arrays.equals(TaskState.TASK_UNASSIGNED.get("dummy-master"),
        taskstate));
    return;
  }

  @Test
  public void testEmptyLogDir() throws Exception {
    LOG.info("testEmptyLogDir");
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    MiniDFSCluster cluster = TEST_UTIL.startMiniDFSCluster(1);
    FileSystem fs = cluster.getFileSystem();
    Path emptyLogDirPath = new Path(fs.getWorkingDirectory(),
        UUID.randomUUID().toString());
    fs.mkdirs(emptyLogDirPath);
    slm.splitLogDistributed(emptyLogDirPath);
    assertFalse(fs.exists(emptyLogDirPath));
    cluster.shutdown();
  }

  @Test
  public void testVanishingTaskZNode() throws Exception {
    LOG.info("testVanishingTaskZNode");
    conf.setInt("hbase.splitlog.manager.unassigned.timeout", 0);
    slm = new SplitLogManager(zkw, conf, stopper, "dummy-master", null);
    slm.finishInitialization();
    MiniDFSCluster dfscluster = TEST_UTIL.startMiniDFSCluster(1);
    FileSystem fs = dfscluster.getFileSystem();
    final Path logDir = new Path(fs.getWorkingDirectory(),
        UUID.randomUUID().toString());
    fs.mkdirs(logDir);
    Path logFile = new Path(logDir, UUID.randomUUID().toString());
    fs.createNewFile(logFile);
    new Thread() {
      @Override
      public void run() {
        try {
          // this call will block because there are no SplitLogWorkers
          slm.splitLogDistributed(logDir);
        } catch (Exception e) {
          LOG.warn("splitLogDistributed failed", e);
          fail();
        }
      }
    }.start();
    waitForCounter(tot_mgr_node_create_result, 0, 1, 10000);
    String znode = ZKSplitLog.getEncodedNodeName(zkw, logFile.toString());
    // remove the task znode
    zkw.deleteZNode(znode);
    waitForCounter(tot_mgr_get_data_nonode, 0, 1, 30000);
    waitForCounter(tot_mgr_log_split_batch_success, 0, 1, 1000);
    assertTrue(fs.exists(logFile));
    fs.delete(logDir, true);
  }


  public static class NodeCreationListener implements Watcher {
    private static final Log LOG = LogFactory
        .getLog(NodeCreationListener.class);

    private Semaphore lock;
    private String node;
    protected ZooKeeperWrapper watcher;

    public NodeCreationListener(ZooKeeperWrapper watcher, String node) {
      this.watcher = watcher;
      this.lock = new Semaphore(0);
      this.node = node;
    }

    private void nodeCreated(String path) {
      if (path.equals(node)) {
        LOG.debug("nodeCreated(" + path + ")");
        lock.release();
      }
    }

    public void waitForCreation() throws InterruptedException {
      lock.acquire();
    }

    @Override
    public void process(WatchedEvent event) {
      switch (event.getType()) {
      case NodeCreated:
        nodeCreated(event.getPath());
        break;
      case NodeDeleted:
        // no-op
        break;
      case NodeDataChanged:
        // no-op
        break;
      case NodeChildrenChanged:
        // no-op
        break;
      }
    }
  }
}
