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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.SplitLogCounters.resetCounters;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_heartbeat;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_node_create_queued;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_orphan_task_acquired;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_rescan;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_rescan_deleted;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_resubmit;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_resubmit_dead_server_task;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_resubmit_failed;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_resubmit_force;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_resubmit_threshold_reached;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_resubmit_unassigned;
import static org.apache.hadoop.hbase.SplitLogCounters.tot_mgr_task_deleted;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.CoordinatedStateManagerFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination;
import org.apache.hadoop.hbase.master.SplitLogManager.Task;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskBatch;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.TestMasterAddressTracker.NodeCreationListener;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({MasterTests.class, MediumTests.class})
public class TestSplitLogManager {
  private static final Log LOG = LogFactory.getLog(TestSplitLogManager.class);
  private final ServerName DUMMY_MASTER = ServerName.valueOf("dummy-master,1,1");
  private final ServerManager sm = Mockito.mock(ServerManager.class);
  private final MasterServices master = Mockito.mock(MasterServices.class);

  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  private ZooKeeperWatcher zkw;
  private DummyServer ds;
  private static boolean stopped = false;
  private SplitLogManager slm;
  private Configuration conf;
  private int to;
  private RecoveryMode mode;

  private static HBaseTestingUtility TEST_UTIL;

  class DummyServer implements Server {
    private ZooKeeperWatcher zkw;
    private Configuration conf;
    private CoordinatedStateManager cm;

    public DummyServer(ZooKeeperWatcher zkw, Configuration conf) {
      this.zkw = zkw;
      this.conf = conf;
      cm = CoordinatedStateManagerFactory.getCoordinatedStateManager(conf);
      cm.initialize(this);
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public ServerName getServerName() {
      return null;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return cm;
    }

    @Override
    public HConnection getShortCircuitConnection() {
      return null;
    }

    @Override
    public MetaTableLocator getMetaTableLocator() {
      return null;
    }

  }

  static Stoppable stopper = new Stoppable() {
    @Override
    public void stop(String why) {
      stopped = true;
    }

    @Override
    public boolean isStopped() {
      return stopped;
    }
  };

  @Before
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniZKCluster();
    conf = TEST_UTIL.getConfiguration();
    // Use a different ZK wrapper instance for each tests.
    zkw =
        new ZooKeeperWatcher(conf, "split-log-manager-tests" + UUID.randomUUID().toString(), null);
    ds = new DummyServer(zkw, conf);

    ZKUtil.deleteChildrenRecursively(zkw, zkw.baseZNode);
    ZKUtil.createAndFailSilent(zkw, zkw.baseZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.baseZNode) != -1);
    LOG.debug(zkw.baseZNode + " created");
    ZKUtil.createAndFailSilent(zkw, zkw.splitLogZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.splitLogZNode) != -1);
    LOG.debug(zkw.splitLogZNode + " created");

    stopped = false;
    resetCounters();

    // By default, we let the test manage the error as before, so the server
    // does not appear as dead from the master point of view, only from the split log pov.
    Mockito.when(sm.isServerOnline(Mockito.any(ServerName.class))).thenReturn(true);
    Mockito.when(master.getServerManager()).thenReturn(sm);

    to = 6000;
    conf.setInt(HConstants.HBASE_SPLITLOG_MANAGER_TIMEOUT, to);
    conf.setInt("hbase.splitlog.manager.unassigned.timeout", 2 * to);

    conf.setInt("hbase.splitlog.manager.timeoutmonitor.period", 100);
    to = to + 16 * 100;

    this.mode =
        (conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false) ? RecoveryMode.LOG_REPLAY
            : RecoveryMode.LOG_SPLITTING);
  }

  @After
  public void teardown() throws IOException, KeeperException {
    stopper.stop("");
    if (slm != null) slm.stop();
    TEST_UTIL.shutdownMiniZKCluster();
  }

  private interface Expr {
    long eval();
  }

  private void waitForCounter(final AtomicLong ctr, long oldval, long newval, long timems)
      throws Exception {
    Expr e = new Expr() {
      @Override
      public long eval() {
        return ctr.get();
      }
    };
    waitForCounter(e, oldval, newval, timems);
    return;
  }

  private void waitForCounter(final Expr e, final long oldval, long newval, long timems)
      throws Exception {

    TEST_UTIL.waitFor(timems, 10, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return (e.eval() != oldval);
      }
    });

    assertEquals(newval, e.eval());
  }

  private String submitTaskAndWait(TaskBatch batch, String name) throws KeeperException,
      InterruptedException {
    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, name);
    NodeCreationListener listener = new NodeCreationListener(zkw, tasknode);
    zkw.registerListener(listener);
    ZKUtil.watchAndCheckExists(zkw, tasknode);

    slm.enqueueSplitTask(name, batch);
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
  @Test (timeout=180000)
  public void testTaskCreation() throws Exception {

    LOG.info("TestTaskCreation - test the creation of a task in zk");
    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");

    byte[] data = ZKUtil.getData(zkw, tasknode);
    SplitLogTask slt = SplitLogTask.parseFrom(data);
    LOG.info("Task node created " + slt.toString());
    assertTrue(slt.isUnassigned(DUMMY_MASTER));
  }

  @Test (timeout=180000)
  public void testOrphanTaskAcquisition() throws Exception {
    LOG.info("TestOrphanTaskAcquisition");

    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, "orphan/test/slash");
    SplitLogTask slt = new SplitLogTask.Owned(DUMMY_MASTER, this.mode);
    zkw.getRecoverableZooKeeper().create(tasknode, slt.toByteArray(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, to/2);
    Task task = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task.isOrphan());
    waitForCounter(tot_mgr_heartbeat, 0, 1, to/2);
    assertFalse(task.isUnassigned());
    long curt = System.currentTimeMillis();
    assertTrue((task.last_update <= curt) &&
        (task.last_update > (curt - 1000)));
    LOG.info("waiting for manager to resubmit the orphan task");
    waitForCounter(tot_mgr_resubmit, 0, 1, to + to/2);
    assertTrue(task.isUnassigned());
    waitForCounter(tot_mgr_rescan, 0, 1, to + to/2);
  }

  @Test (timeout=180000)
  public void testUnassignedOrphan() throws Exception {
    LOG.info("TestUnassignedOrphan - an unassigned task is resubmitted at" +
        " startup");
    String tasknode = ZKSplitLog.getEncodedNodeName(zkw, "orphan/test/slash");
    //create an unassigned orphan task
    SplitLogTask slt = new SplitLogTask.Unassigned(DUMMY_MASTER, this.mode);
    zkw.getRecoverableZooKeeper().create(tasknode, slt.toByteArray(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    int version = ZKUtil.checkExists(zkw, tasknode);

    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, to/2);
    Task task = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task.isOrphan());
    assertTrue(task.isUnassigned());
    // wait for RESCAN node to be created
    waitForCounter(tot_mgr_rescan, 0, 1, to/2);
    Task task2 = slm.findOrCreateOrphanTask(tasknode);
    assertTrue(task == task2);
    LOG.debug("task = " + task);
    assertEquals(1L, tot_mgr_resubmit.get());
    assertEquals(1, task.incarnation);
    assertEquals(0, task.unforcedResubmits.get());
    assertTrue(task.isOrphan());
    assertTrue(task.isUnassigned());
    assertTrue(ZKUtil.checkExists(zkw, tasknode) > version);
  }

  @Test (timeout=180000)
  public void testMultipleResubmits() throws Exception {
    LOG.info("TestMultipleResbmits - no indefinite resubmissions");
    conf.setInt("hbase.splitlog.max.resubmit", 2);
    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = ZKUtil.checkExists(zkw, tasknode);
    final ServerName worker1 = ServerName.valueOf("worker1,1,1");
    final ServerName worker2 = ServerName.valueOf("worker2,1,1");
    final ServerName worker3 = ServerName.valueOf("worker3,1,1");
    SplitLogTask slt = new SplitLogTask.Owned(worker1, this.mode);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());
    waitForCounter(tot_mgr_heartbeat, 0, 1, to/2);
    waitForCounter(tot_mgr_resubmit, 0, 1, to + to/2);
    int version1 = ZKUtil.checkExists(zkw, tasknode);
    assertTrue(version1 > version);
    slt = new SplitLogTask.Owned(worker2, this.mode);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());
    waitForCounter(tot_mgr_heartbeat, 1, 2, to/2);
    waitForCounter(tot_mgr_resubmit, 1, 2, to + to/2);
    int version2 = ZKUtil.checkExists(zkw, tasknode);
    assertTrue(version2 > version1);
    slt = new SplitLogTask.Owned(worker3, this.mode);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());
    waitForCounter(tot_mgr_heartbeat, 2, 3, to/2);
    waitForCounter(tot_mgr_resubmit_threshold_reached, 0, 1, to + to/2);
    Thread.sleep(to + to/2);
    assertEquals(2L, tot_mgr_resubmit.get() - tot_mgr_resubmit_force.get());
  }

  @Test (timeout=180000)
  public void testRescanCleanup() throws Exception {
    LOG.info("TestRescanCleanup - ensure RESCAN nodes are cleaned up");

    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = ZKUtil.checkExists(zkw, tasknode);
    final ServerName worker1 = ServerName.valueOf("worker1,1,1");
    SplitLogTask slt = new SplitLogTask.Owned(worker1, this.mode);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());
    waitForCounter(tot_mgr_heartbeat, 0, 1, to/2);
    waitForCounter(new Expr() {
      @Override
      public long eval() {
        return (tot_mgr_resubmit.get() + tot_mgr_resubmit_failed.get());
      }
    }, 0, 1, 5*60000); // wait long enough
    Assert.assertEquals("Could not run test. Lost ZK connection?", 0, tot_mgr_resubmit_failed.get());
    int version1 = ZKUtil.checkExists(zkw, tasknode);
    assertTrue(version1 > version);
    byte[] taskstate = ZKUtil.getData(zkw, tasknode);
    slt = SplitLogTask.parseFrom(taskstate);
    assertTrue(slt.isUnassigned(DUMMY_MASTER));

    waitForCounter(tot_mgr_rescan_deleted, 0, 1, to/2);
  }

  @Test (timeout=180000)
  public void testTaskDone() throws Exception {
    LOG.info("TestTaskDone - cleanup task node once in DONE state");

    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    TaskBatch batch = new TaskBatch();
    String tasknode = submitTaskAndWait(batch, "foo/1");
    final ServerName worker1 = ServerName.valueOf("worker1,1,1");
    SplitLogTask slt = new SplitLogTask.Done(worker1, this.mode);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());
    synchronized (batch) {
      while (batch.installed != batch.done) {
        batch.wait();
      }
    }
    waitForCounter(tot_mgr_task_deleted, 0, 1, to/2);
    assertTrue(ZKUtil.checkExists(zkw, tasknode) == -1);
  }

  @Test (timeout=180000)
  public void testTaskErr() throws Exception {
    LOG.info("TestTaskErr - cleanup task node once in ERR state");

    conf.setInt("hbase.splitlog.max.resubmit", 0);
    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    final ServerName worker1 = ServerName.valueOf("worker1,1,1");
    SplitLogTask slt = new SplitLogTask.Err(worker1, this.mode);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());

    synchronized (batch) {
      while (batch.installed != batch.error) {
        batch.wait();
      }
    }
    waitForCounter(tot_mgr_task_deleted, 0, 1, to/2);
    assertTrue(ZKUtil.checkExists(zkw, tasknode) == -1);
    conf.setInt("hbase.splitlog.max.resubmit", ZKSplitLogManagerCoordination.DEFAULT_MAX_RESUBMIT);
  }

  @Test (timeout=180000)
  public void testTaskResigned() throws Exception {
    LOG.info("TestTaskResigned - resubmit task node once in RESIGNED state");
    assertEquals(tot_mgr_resubmit.get(), 0);
    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    assertEquals(tot_mgr_resubmit.get(), 0);
    TaskBatch batch = new TaskBatch();
    String tasknode = submitTaskAndWait(batch, "foo/1");
    assertEquals(tot_mgr_resubmit.get(), 0);
    final ServerName worker1 = ServerName.valueOf("worker1,1,1");
    assertEquals(tot_mgr_resubmit.get(), 0);
    SplitLogTask slt = new SplitLogTask.Resigned(worker1, this.mode);
    assertEquals(tot_mgr_resubmit.get(), 0);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());
    ZKUtil.checkExists(zkw, tasknode);
    // Could be small race here.
    if (tot_mgr_resubmit.get() == 0) {
      waitForCounter(tot_mgr_resubmit, 0, 1, to/2);
    }
    assertEquals(tot_mgr_resubmit.get(), 1);

    byte[] taskstate = ZKUtil.getData(zkw, tasknode);
    slt = SplitLogTask.parseFrom(taskstate);
    assertTrue(slt.isUnassigned(DUMMY_MASTER));
  }

  @Test (timeout=180000)
  public void testUnassignedTimeout() throws Exception {
    LOG.info("TestUnassignedTimeout - iff all tasks are unassigned then" +
        " resubmit");

    // create an orphan task in OWNED state
    String tasknode1 = ZKSplitLog.getEncodedNodeName(zkw, "orphan/1");
    final ServerName worker1 = ServerName.valueOf("worker1,1,1");
    SplitLogTask slt = new SplitLogTask.Owned(worker1, this.mode);
    zkw.getRecoverableZooKeeper().create(tasknode1, slt.toByteArray(), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    waitForCounter(tot_mgr_orphan_task_acquired, 0, 1, to/2);

    // submit another task which will stay in unassigned mode
    TaskBatch batch = new TaskBatch();
    submitTaskAndWait(batch, "foo/1");

    // keep updating the orphan owned node every to/2 seconds
    for (int i = 0; i < (3 * to)/100; i++) {
      Thread.sleep(100);
      final ServerName worker2 = ServerName.valueOf("worker1,1,1");
      slt = new SplitLogTask.Owned(worker2, this.mode);
      ZKUtil.setData(zkw, tasknode1, slt.toByteArray());
    }

    // since we have stopped heartbeating the owned node therefore it should
    // get resubmitted
    LOG.info("waiting for manager to resubmit the orphan task");
    waitForCounter(tot_mgr_resubmit, 0, 1, to + to/2);

    // now all the nodes are unassigned. manager should post another rescan
    waitForCounter(tot_mgr_resubmit_unassigned, 0, 1, 2 * to + to/2);
  }

  @Test (timeout=180000)
  public void testDeadWorker() throws Exception {
    LOG.info("testDeadWorker");

    conf.setLong("hbase.splitlog.max.resubmit", 0);
    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    int version = ZKUtil.checkExists(zkw, tasknode);
    final ServerName worker1 = ServerName.valueOf("worker1,1,1");
    SplitLogTask slt = new SplitLogTask.Owned(worker1, this.mode);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());
    if (tot_mgr_heartbeat.get() == 0) waitForCounter(tot_mgr_heartbeat, 0, 1, to/2);
    slm.handleDeadWorker(worker1);
    if (tot_mgr_resubmit.get() == 0) waitForCounter(tot_mgr_resubmit, 0, 1, to+to/2);
    if (tot_mgr_resubmit_dead_server_task.get() == 0) {
      waitForCounter(tot_mgr_resubmit_dead_server_task, 0, 1, to + to/2);
    }

    int version1 = ZKUtil.checkExists(zkw, tasknode);
    assertTrue(version1 > version);
    byte[] taskstate = ZKUtil.getData(zkw, tasknode);
    slt = SplitLogTask.parseFrom(taskstate);
    assertTrue(slt.isUnassigned(DUMMY_MASTER));
    return;
  }

  @Test (timeout=180000)
  public void testWorkerCrash() throws Exception {
    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    TaskBatch batch = new TaskBatch();

    String tasknode = submitTaskAndWait(batch, "foo/1");
    final ServerName worker1 = ServerName.valueOf("worker1,1,1");

    SplitLogTask slt = new SplitLogTask.Owned(worker1, this.mode);
    ZKUtil.setData(zkw, tasknode, slt.toByteArray());
    if (tot_mgr_heartbeat.get() == 0) waitForCounter(tot_mgr_heartbeat, 0, 1, to/2);

    // Not yet resubmitted.
    Assert.assertEquals(0, tot_mgr_resubmit.get());

    // This server becomes dead
    Mockito.when(sm.isServerOnline(worker1)).thenReturn(false);

    Thread.sleep(1300); // The timeout checker is done every 1000 ms (hardcoded).

    // It has been resubmitted
    Assert.assertEquals(1, tot_mgr_resubmit.get());
  }

  @Test (timeout=180000)
  public void testEmptyLogDir() throws Exception {
    LOG.info("testEmptyLogDir");
    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path emptyLogDirPath = new Path(fs.getWorkingDirectory(),
        UUID.randomUUID().toString());
    fs.mkdirs(emptyLogDirPath);
    slm.splitLogDistributed(emptyLogDirPath);
    assertFalse(fs.exists(emptyLogDirPath));
  }

  @Test (timeout = 60000)
  public void testLogFilesAreArchived() throws Exception {
    LOG.info("testLogFilesAreArchived");
    final SplitLogManager slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Path dir = TEST_UTIL.getDataTestDirOnTestFS("testLogFilesAreArchived");
    conf.set(HConstants.HBASE_DIR, dir.toString());
    Path logDirPath = new Path(dir, UUID.randomUUID().toString());
    fs.mkdirs(logDirPath);
    // create an empty log file
    String logFile = ServerName.valueOf("foo", 1, 1).toString();
    fs.create(new Path(logDirPath, logFile)).close();

    // spin up a thread mocking split done.
    new Thread() {
      @Override
      public void run() {
        boolean done = false;
        while (!done) {
          for (Map.Entry<String, Task> entry : slm.getTasks().entrySet()) {
            final ServerName worker1 = ServerName.valueOf("worker1,1,1");
            SplitLogTask slt = new SplitLogTask.Done(worker1, RecoveryMode.LOG_SPLITTING);
            try {
              ZKUtil.setData(zkw, entry.getKey(), slt.toByteArray());
            } catch (KeeperException e) {
              LOG.warn(e);
            }
            done = true;
          }
        }
      };
    }.start();

    slm.splitLogDistributed(logDirPath);

    assertFalse(fs.exists(logDirPath));
  }

  /**
   * The following test case is aiming to test the situation when distributedLogReplay is turned off
   * and restart a cluster there should no recovery regions in ZK left.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testRecoveryRegionRemovedFromZK() throws Exception {
    LOG.info("testRecoveryRegionRemovedFromZK");
    conf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, false);
    String nodePath =
        ZKUtil.joinZNode(zkw.recoveringRegionsZNode,
          HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    ZKUtil.createSetData(zkw, nodePath, ZKUtil.positionToByteArray(0L));

    slm = new SplitLogManager(ds, conf, stopper, master, DUMMY_MASTER);
    slm.removeStaleRecoveringRegions(null);

    List<String> recoveringRegions =
        zkw.getRecoverableZooKeeper().getChildren(zkw.recoveringRegionsZNode, false);

    assertTrue("Recovery regions isn't cleaned", recoveringRegions.isEmpty());
  }

  @Test(timeout=60000)
  public void testGetPreviousRecoveryMode() throws Exception {
    LOG.info("testGetPreviousRecoveryMode");
    SplitLogCounters.resetCounters();
    Configuration testConf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    testConf.setBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY, true);

    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "testRecovery"),
      new SplitLogTask.Unassigned(
        ServerName.valueOf("mgr,1,1"), RecoveryMode.LOG_SPLITTING).toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    slm = new SplitLogManager(ds, testConf, stopper, master, DUMMY_MASTER);
    LOG.info("Mode1=" + slm.getRecoveryMode());
    assertTrue(slm.isLogSplitting());
    zkw.getRecoverableZooKeeper().delete(ZKSplitLog.getEncodedNodeName(zkw, "testRecovery"), -1);
    LOG.info("Mode2=" + slm.getRecoveryMode());
    slm.setRecoveryMode(false);
    LOG.info("Mode3=" + slm.getRecoveryMode());
    assertTrue("Mode4=" + slm.getRecoveryMode(), slm.isLogReplaying());
  }
}