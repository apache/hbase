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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_MAX_SPLITTER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({RegionServerTests.class, MediumTests.class})
public class TestSplitLogWorker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSplitLogWorker.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSplitLogWorker.class);
  private static final int WAIT_TIME = 15000;
  private final ServerName MANAGER = ServerName.valueOf("manager,1,1");
  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private DummyServer ds;
  private ZKWatcher zkw;
  private SplitLogWorker slw;
  private ExecutorService executorService;

  static class DummyServer implements Server {
    private ZKWatcher zkw;
    private Configuration conf;
    private CoordinatedStateManager cm;

    public DummyServer(ZKWatcher zkw, Configuration conf) {
      this.zkw = zkw;
      this.conf = conf;
      cm = new ZkCoordinatedStateManager(this);
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
    public ZKWatcher getZooKeeper() {
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
    public Connection getConnection() {
      return null;
    }

    @Override
    public ChoreService getChoreService() {
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

    @Override
    public AsyncClusterConnection getAsyncClusterConnection() {
      return null;
    }
  }

  private void waitForCounter(LongAdder ctr, long oldval, long newval, long timems)
      throws Exception {
    assertTrue("ctr=" + ctr.sum() + ", oldval=" + oldval + ", newval=" + newval,
      waitForCounterBoolean(ctr, oldval, newval, timems));
  }

  private boolean waitForCounterBoolean(final LongAdder ctr, final long oldval, long newval,
      long timems) throws Exception {

    return waitForCounterBoolean(ctr, oldval, newval, timems, true);
  }

  private boolean waitForCounterBoolean(final LongAdder ctr, final long oldval, final long newval,
      long timems, boolean failIfTimeout) throws Exception {

    long timeWaited = TEST_UTIL.waitFor(timems, 10, failIfTimeout,
      new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
            return (ctr.sum() >= newval);
      }
    });

    if( timeWaited > 0) {
      // when not timed out
      assertEquals(newval, ctr.sum());
    }
    return true;
  }

  @Before
  public void setup() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();
    zkw = new ZKWatcher(TEST_UTIL.getConfiguration(),
        "split-log-worker-tests", null);
    ds = new DummyServer(zkw, conf);
    ZKUtil.deleteChildrenRecursively(zkw, zkw.getZNodePaths().baseZNode);
    ZKUtil.createAndFailSilent(zkw, zkw.getZNodePaths().baseZNode);
    assertThat(ZKUtil.checkExists(zkw, zkw.getZNodePaths().baseZNode), not(is(-1)));
    LOG.debug(zkw.getZNodePaths().baseZNode + " created");
    ZKUtil.createAndFailSilent(zkw, zkw.getZNodePaths().splitLogZNode);
    assertThat(ZKUtil.checkExists(zkw, zkw.getZNodePaths().splitLogZNode), not(is(-1)));

    LOG.debug(zkw.getZNodePaths().splitLogZNode + " created");
    ZKUtil.createAndFailSilent(zkw, zkw.getZNodePaths().rsZNode);
    assertThat(ZKUtil.checkExists(zkw, zkw.getZNodePaths().rsZNode), not(is(-1)));

    SplitLogCounters.resetCounters();
    executorService = new ExecutorService("TestSplitLogWorker");
    executorService.startExecutorService(ExecutorType.RS_LOG_REPLAY_OPS, 10);
  }

  @After
  public void teardown() throws Exception {
    if (executorService != null) {
      executorService.shutdown();
    }
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
    final ServerName RS = ServerName.valueOf("rs,1,1");
    RegionServerServices mockedRS = getRegionServer(RS);
    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, TATAS),
      new SplitLogTask.Unassigned(ServerName.valueOf("mgr,1,1")).toByteArray(),
        Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    SplitLogWorker slw =
        new SplitLogWorker(ds, TEST_UTIL.getConfiguration(), mockedRS, neverEndingTask);
    slw.start();
    try {
      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, WAIT_TIME);
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
      slw.worker.join(WAIT_TIME);
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
    final ServerName SVR1 = ServerName.valueOf("svr1,1,1");
    final ServerName SVR2 = ServerName.valueOf("svr2,1,1");
    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, TRFT),
      new SplitLogTask.Unassigned(MANAGER).toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    RegionServerServices mockedRS1 = getRegionServer(SVR1);
    RegionServerServices mockedRS2 = getRegionServer(SVR2);
    SplitLogWorker slw1 =
        new SplitLogWorker(ds, TEST_UTIL.getConfiguration(), mockedRS1, neverEndingTask);
    SplitLogWorker slw2 =
        new SplitLogWorker(ds, TEST_UTIL.getConfiguration(), mockedRS2, neverEndingTask);
    slw1.start();
    slw2.start();
    try {
      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, WAIT_TIME);
      // Assert that either the tot_wkr_failed_to_grab_task_owned count was set of if
      // not it, that we fell through to the next counter in line and it was set.
      assertTrue(waitForCounterBoolean(SplitLogCounters.tot_wkr_failed_to_grab_task_owned, 0, 1,
          WAIT_TIME, false) ||
        SplitLogCounters.tot_wkr_failed_to_grab_task_lost_race.sum() == 1);
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
    final ServerName SRV = ServerName.valueOf("tpt_svr,1,1");
    final String PATH = ZKSplitLog.getEncodedNodeName(zkw, "tpt_task");
    RegionServerServices mockedRS = getRegionServer(SRV);
    SplitLogWorker slw =
        new SplitLogWorker(ds, TEST_UTIL.getConfiguration(), mockedRS, neverEndingTask);
    slw.start();
    try {
      Thread.yield(); // let the worker start
      Thread.sleep(1000);
      waitForCounter(SplitLogCounters.tot_wkr_task_grabing, 0, 1, WAIT_TIME);

      // this time create a task node after starting the splitLogWorker
      zkw.getRecoverableZooKeeper().create(PATH,
        new SplitLogTask.Unassigned(MANAGER).toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, WAIT_TIME);
      assertEquals(1, slw.getTaskReadySeq());
      byte [] bytes = ZKUtil.getData(zkw, PATH);
      SplitLogTask slt = SplitLogTask.parseFrom(bytes);
      assertTrue(slt.isOwned(SRV));
      slt = new SplitLogTask.Owned(MANAGER);
      ZKUtil.setData(zkw, PATH, slt.toByteArray());
      waitForCounter(SplitLogCounters.tot_wkr_preempt_task, 0, 1, WAIT_TIME);
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  @Test
  public void testMultipleTasks() throws Exception {
    LOG.info("testMultipleTasks");
    SplitLogCounters.resetCounters();
    final ServerName SRV = ServerName.valueOf("tmt_svr,1,1");
    final String PATH1 = ZKSplitLog.getEncodedNodeName(zkw, "tmt_task");
    RegionServerServices mockedRS = getRegionServer(SRV);
    SplitLogWorker slw =
        new SplitLogWorker(ds, TEST_UTIL.getConfiguration(), mockedRS, neverEndingTask);
    slw.start();
    try {
      Thread.yield(); // let the worker start
      Thread.sleep(100);
      waitForCounter(SplitLogCounters.tot_wkr_task_grabing, 0, 1, WAIT_TIME);

      SplitLogTask unassignedManager =
        new SplitLogTask.Unassigned(MANAGER);
      zkw.getRecoverableZooKeeper().create(PATH1, unassignedManager.toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, WAIT_TIME);
      // now the worker is busy doing the above task

      // create another task
      final String PATH2 = ZKSplitLog.getEncodedNodeName(zkw, "tmt_task_2");
      zkw.getRecoverableZooKeeper().create(PATH2, unassignedManager.toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

      // preempt the first task, have it owned by another worker
      final ServerName anotherWorker = ServerName.valueOf("another-worker,1,1");
      SplitLogTask slt = new SplitLogTask.Owned(anotherWorker);
      ZKUtil.setData(zkw, PATH1, slt.toByteArray());
      waitForCounter(SplitLogCounters.tot_wkr_preempt_task, 0, 1, WAIT_TIME);

      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 1, 2, WAIT_TIME);
      assertEquals(2, slw.getTaskReadySeq());
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
    final ServerName SRV = ServerName.valueOf("svr,1,1");
    RegionServerServices mockedRS = getRegionServer(SRV);
    slw = new SplitLogWorker(ds, TEST_UTIL.getConfiguration(), mockedRS, neverEndingTask);
    slw.start();
    Thread.yield(); // let the worker start
    Thread.sleep(100);

    String task = ZKSplitLog.getEncodedNodeName(zkw, "task");
    SplitLogTask slt = new SplitLogTask.Unassigned(MANAGER);
    zkw.getRecoverableZooKeeper().create(task,slt.toByteArray(), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);

    waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 1, WAIT_TIME);
    // now the worker is busy doing the above task

    // preempt the task, have it owned by another worker
    ZKUtil.setData(zkw, task, slt.toByteArray());
    waitForCounter(SplitLogCounters.tot_wkr_preempt_task, 0, 1, WAIT_TIME);

    // create a RESCAN node
    String rescan = ZKSplitLog.getEncodedNodeName(zkw, "RESCAN");
    rescan = zkw.getRecoverableZooKeeper().create(rescan, slt.toByteArray(), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT_SEQUENTIAL);

    waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 1, 2, WAIT_TIME);
    // RESCAN node might not have been processed if the worker became busy
    // with the above task. preempt the task again so that now the RESCAN
    // node is processed
    ZKUtil.setData(zkw, task, slt.toByteArray());
    waitForCounter(SplitLogCounters.tot_wkr_preempt_task, 1, 2, WAIT_TIME);
    waitForCounter(SplitLogCounters.tot_wkr_task_acquired_rescan, 0, 1, WAIT_TIME);

    List<String> nodes = ZKUtil.listChildrenNoWatch(zkw, zkw.getZNodePaths().splitLogZNode);
    LOG.debug(Objects.toString(nodes));
    int num = 0;
    for (String node : nodes) {
      num++;
      if (node.startsWith("RESCAN")) {
        String name = ZKSplitLog.getEncodedNodeName(zkw, node);
        String fn = ZKSplitLog.getFileName(name);
        byte [] data = ZKUtil.getData(zkw,
                ZNodePaths.joinZNode(zkw.getZNodePaths().splitLogZNode, fn));
        slt = SplitLogTask.parseFrom(data);
        assertTrue(slt.toString(), slt.isDone(SRV));
      }
    }
    assertEquals(2, num);
  }

  @Test
  public void testAcquireMultiTasks() throws Exception {
    LOG.info("testAcquireMultiTasks");
    SplitLogCounters.resetCounters();
    final String TATAS = "tatas";
    final ServerName RS = ServerName.valueOf("rs,1,1");
    final int maxTasks = 3;
    Configuration testConf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    testConf.setInt(HBASE_SPLIT_WAL_MAX_SPLITTER, maxTasks);
    RegionServerServices mockedRS = getRegionServer(RS);
    for (int i = 0; i < maxTasks; i++) {
      zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, TATAS + i),
        new SplitLogTask.Unassigned(ServerName.valueOf("mgr,1,1")).toByteArray(),
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    SplitLogWorker slw = new SplitLogWorker(ds, testConf, mockedRS, neverEndingTask);
    slw.start();
    try {
      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, maxTasks, WAIT_TIME);
      for (int i = 0; i < maxTasks; i++) {
        byte[] bytes = ZKUtil.getData(zkw, ZKSplitLog.getEncodedNodeName(zkw, TATAS + i));
        SplitLogTask slt = SplitLogTask.parseFrom(bytes);
        assertTrue(slt.isOwned(RS));
      }
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  /**
   * The test checks SplitLogWorker should not spawn more splitters than expected num of tasks per
   * RS
   * @throws Exception
   */
  @Test
  public void testAcquireMultiTasksByAvgTasksPerRS() throws Exception {
    LOG.info("testAcquireMultiTasks");
    SplitLogCounters.resetCounters();
    final String TATAS = "tatas";
    final ServerName RS = ServerName.valueOf("rs,1,1");
    final ServerName RS2 = ServerName.valueOf("rs,1,2");
    final int maxTasks = 3;
    Configuration testConf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    testConf.setInt(HBASE_SPLIT_WAL_MAX_SPLITTER, maxTasks);
    RegionServerServices mockedRS = getRegionServer(RS);

    // create two RS nodes
    String rsPath = ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, RS.getServerName());
    zkw.getRecoverableZooKeeper().create(rsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    rsPath = ZNodePaths.joinZNode(zkw.getZNodePaths().rsZNode, RS2.getServerName());
    zkw.getRecoverableZooKeeper().create(rsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

    for (int i = 0; i < maxTasks; i++) {
      zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, TATAS + i),
        new SplitLogTask.Unassigned(ServerName.valueOf("mgr,1,1")).toByteArray(),
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    SplitLogWorker slw = new SplitLogWorker(ds, testConf, mockedRS, neverEndingTask);
    slw.start();
    try {
      int acquiredTasks = 0;
      waitForCounter(SplitLogCounters.tot_wkr_task_acquired, 0, 2, WAIT_TIME);
      for (int i = 0; i < maxTasks; i++) {
        byte[] bytes = ZKUtil.getData(zkw, ZKSplitLog.getEncodedNodeName(zkw, TATAS + i));
        SplitLogTask slt = SplitLogTask.parseFrom(bytes);
        if (slt.isOwned(RS)) {
          acquiredTasks++;
        }
      }
      assertEquals(2, acquiredTasks);
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  /**
   * Create a mocked region server service instance
   */
  private RegionServerServices getRegionServer(ServerName name) {

    RegionServerServices mockedServer = mock(RegionServerServices.class);
    when(mockedServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    when(mockedServer.getServerName()).thenReturn(name);
    when(mockedServer.getZooKeeper()).thenReturn(zkw);
    when(mockedServer.isStopped()).thenReturn(false);
    when(mockedServer.getExecutorService()).thenReturn(executorService);

    return mockedServer;
  }

}
