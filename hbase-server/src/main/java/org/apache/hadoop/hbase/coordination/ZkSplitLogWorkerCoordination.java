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

package org.apache.hadoop.hbase.coordination;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker.TaskExecutor;
import org.apache.hadoop.hbase.regionserver.handler.FinishRegionRecoveringHandler;
import org.apache.hadoop.hbase.regionserver.handler.WALSplitterHandler;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper based implementation of {@link SplitLogWorkerCoordination}
 * It listen for changes in ZooKeeper and
 *
 */
@InterfaceAudience.Private
public class ZkSplitLogWorkerCoordination extends ZooKeeperListener implements
    SplitLogWorkerCoordination {

  private static final Log LOG = LogFactory.getLog(ZkSplitLogWorkerCoordination.class);

  private static final int checkInterval = 5000; // 5 seconds
  private static final int FAILED_TO_OWN_TASK = -1;

  private  SplitLogWorker worker;

  private TaskExecutor splitTaskExecutor;

  private final Object taskReadyLock = new Object();
  volatile int taskReadySeq = 0;
  private volatile String currentTask = null;
  private int currentVersion;
  private volatile boolean shouldStop = false;
  private final Object grabTaskLock = new Object();
  private boolean workerInGrabTask = false;
  private int reportPeriod;
  private RegionServerServices server = null;
  protected final AtomicInteger tasksInProgress = new AtomicInteger(0);
  private int maxConcurrentTasks = 0;

  private final ZkCoordinatedStateManager manager;

  public ZkSplitLogWorkerCoordination(ZkCoordinatedStateManager zkCoordinatedStateManager,
      ZooKeeperWatcher watcher) {
    super(watcher);
    manager = zkCoordinatedStateManager;

  }

  /**
   * Override handler from {@link ZooKeeperListener}
   */
  @Override
  public void nodeChildrenChanged(String path) {
    if (path.equals(watcher.splitLogZNode)) {
      LOG.debug("tasks arrived or departed");
      synchronized (taskReadyLock) {
        taskReadySeq++;
        taskReadyLock.notify();
      }
    }
  }

  /**
   * Override handler from {@link ZooKeeperListener}
   */
  @Override
  public void nodeDataChanged(String path) {
    // there will be a self generated dataChanged event every time attemptToOwnTask()
    // heartbeats the task znode by upping its version
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change
        String taskpath = currentTask;
        if (taskpath != null && taskpath.equals(path)) {
          getDataSetWatchAsync();
        }
      }
    }
  }

  /**
   * Override setter from {@link SplitLogWorkerCoordination}
   */
  @Override
  public void init(RegionServerServices server, Configuration conf,
      TaskExecutor splitExecutor, SplitLogWorker worker) {
    this.server = server;
    this.worker = worker;
    this.splitTaskExecutor = splitExecutor;
    maxConcurrentTasks = conf.getInt("hbase.regionserver.wal.max.splitters", DEFAULT_MAX_SPLITTERS);
    reportPeriod =
        conf.getInt("hbase.splitlog.report.period",
          conf.getInt(HConstants.HBASE_SPLITLOG_MANAGER_TIMEOUT,
            ZKSplitLogManagerCoordination.DEFAULT_TIMEOUT) / 3);
  }

  /* Support functions for Zookeeper async callback */

  void getDataSetWatchFailure(String path) {
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change but that's ok
        String taskpath = currentTask;
        if (taskpath != null && taskpath.equals(path)) {
          LOG.info("retrying data watch on " + path);
          SplitLogCounters.tot_wkr_get_data_retry.incrementAndGet();
          getDataSetWatchAsync();
        } else {
          // no point setting a watch on the task which this worker is not
          // working upon anymore
        }
      }
    }
  }

  public void getDataSetWatchAsync() {
    watcher.getRecoverableZooKeeper().getZooKeeper()
        .getData(currentTask, watcher, new GetDataAsyncCallback(), null);
    SplitLogCounters.tot_wkr_get_data_queued.incrementAndGet();
  }

  void getDataSetWatchSuccess(String path, byte[] data) {
    SplitLogTask slt;
    try {
      slt = SplitLogTask.parseFrom(data);
    } catch (DeserializationException e) {
      LOG.warn("Failed parse", e);
      return;
    }
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change but that's ok
        String taskpath = currentTask;
        if (taskpath != null && taskpath.equals(path)) {
          ServerName serverName = manager.getServer().getServerName();
          // have to compare data. cannot compare version because then there
          // will be race with attemptToOwnTask()
          // cannot just check whether the node has been transitioned to
          // UNASSIGNED because by the time this worker sets the data watch
          // the node might have made two transitions - from owned by this
          // worker to unassigned to owned by another worker
          if (!slt.isOwned(serverName) && !slt.isDone(serverName) && !slt.isErr(serverName)
              && !slt.isResigned(serverName)) {
            LOG.info("task " + taskpath + " preempted from " + serverName
                + ", current task state and owner=" + slt.toString());
            worker.stopTask();
          }
        }
      }
    }
  }

  /**
   * try to grab a 'lock' on the task zk node to own and execute the task.
   * <p>
   * @param path zk node for the task
   */
  private void grabTask(String path) {
    Stat stat = new Stat();
    byte[] data;
    synchronized (grabTaskLock) {
      currentTask = path;
      workerInGrabTask = true;
      if (Thread.interrupted()) {
        return;
      }
    }
    try {
      try {
        if ((data = ZKUtil.getDataNoWatch(watcher, path, stat)) == null) {
          SplitLogCounters.tot_wkr_failed_to_grab_task_no_data.incrementAndGet();
          return;
        }
      } catch (KeeperException e) {
        LOG.warn("Failed to get data for znode " + path, e);
        SplitLogCounters.tot_wkr_failed_to_grab_task_exception.incrementAndGet();
        return;
      }
      SplitLogTask slt;
      try {
        slt = SplitLogTask.parseFrom(data);
      } catch (DeserializationException e) {
        LOG.warn("Failed parse data for znode " + path, e);
        SplitLogCounters.tot_wkr_failed_to_grab_task_exception.incrementAndGet();
        return;
      }
      if (!slt.isUnassigned()) {
        SplitLogCounters.tot_wkr_failed_to_grab_task_owned.incrementAndGet();
        return;
      }

      currentVersion =
          attemptToOwnTask(true, watcher, server.getServerName(), path,
            slt.getMode(), stat.getVersion());
      if (currentVersion < 0) {
        SplitLogCounters.tot_wkr_failed_to_grab_task_lost_race.incrementAndGet();
        return;
      }

      if (ZKSplitLog.isRescanNode(watcher, currentTask)) {
        ZkSplitLogWorkerCoordination.ZkSplitTaskDetails splitTaskDetails =
            new ZkSplitLogWorkerCoordination.ZkSplitTaskDetails();
        splitTaskDetails.setTaskNode(currentTask);
        splitTaskDetails.setCurTaskZKVersion(new MutableInt(currentVersion));

        endTask(new SplitLogTask.Done(server.getServerName(), slt.getMode()),
          SplitLogCounters.tot_wkr_task_acquired_rescan, splitTaskDetails);
        return;
      }

      LOG.info("worker " + server.getServerName() + " acquired task " + path);
      SplitLogCounters.tot_wkr_task_acquired.incrementAndGet();
      getDataSetWatchAsync();

      submitTask(path, slt.getMode(), currentVersion, reportPeriod);

      // after a successful submit, sleep a little bit to allow other RSs to grab the rest tasks
      try {
        int sleepTime = RandomUtils.nextInt(500) + 500;
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while yielding for other region servers", e);
        Thread.currentThread().interrupt();
      }
    } finally {
      synchronized (grabTaskLock) {
        workerInGrabTask = false;
        // clear the interrupt from stopTask() otherwise the next task will
        // suffer
        Thread.interrupted();
      }
    }
  }

  /**
   * Submit a log split task to executor service
   * @param curTask task to submit
   * @param curTaskZKVersion current version of task
   */
  void submitTask(final String curTask, final RecoveryMode mode, final int curTaskZKVersion,
      final int reportPeriod) {
    final MutableInt zkVersion = new MutableInt(curTaskZKVersion);

    CancelableProgressable reporter = new CancelableProgressable() {
      private long last_report_at = 0;

      @Override
      public boolean progress() {
        long t = EnvironmentEdgeManager.currentTime();
        if ((t - last_report_at) > reportPeriod) {
          last_report_at = t;
          int latestZKVersion =
              attemptToOwnTask(false, watcher, server.getServerName(), curTask,
                mode, zkVersion.intValue());
          if (latestZKVersion < 0) {
            LOG.warn("Failed to heartbeat the task" + curTask);
            return false;
          }
          zkVersion.setValue(latestZKVersion);
        }
        return true;
      }
    };
    ZkSplitLogWorkerCoordination.ZkSplitTaskDetails splitTaskDetails =
        new ZkSplitLogWorkerCoordination.ZkSplitTaskDetails();
    splitTaskDetails.setTaskNode(curTask);
    splitTaskDetails.setCurTaskZKVersion(zkVersion);

    WALSplitterHandler hsh =
        new WALSplitterHandler(server, this, splitTaskDetails, reporter,
            this.tasksInProgress, splitTaskExecutor, mode);
    server.getExecutorService().submit(hsh);
  }

  /**
   * This function calculates how many splitters it could create based on expected average tasks per
   * RS and the hard limit upper bound(maxConcurrentTasks) set by configuration. <br>
   * At any given time, a RS allows spawn MIN(Expected Tasks/RS, Hard Upper Bound)
   * @param numTasks current total number of available tasks
   */
  private int calculateAvailableSplitters(int numTasks) {
    // at lease one RS(itself) available
    int availableRSs = 1;
    try {
      List<String> regionServers =
          ZKUtil.listChildrenNoWatch(watcher, watcher.rsZNode);
      availableRSs = Math.max(availableRSs, (regionServers == null) ? 0 : regionServers.size());
    } catch (KeeperException e) {
      // do nothing
      LOG.debug("getAvailableRegionServers got ZooKeeper exception", e);
    }

    int expectedTasksPerRS = (numTasks / availableRSs) + ((numTasks % availableRSs == 0) ? 0 : 1);
    expectedTasksPerRS = Math.max(1, expectedTasksPerRS); // at least be one
    // calculate how many more splitters we could spawn
    return Math.min(expectedTasksPerRS, maxConcurrentTasks)
        - this.tasksInProgress.get();
  }

  /**
   * Try to own the task by transitioning the zk node data from UNASSIGNED to OWNED.
   * <p>
   * This method is also used to periodically heartbeat the task progress by transitioning the node
   * from OWNED to OWNED.
   * <p>
   * @param isFirstTime shows whther it's the first attempt.
   * @param zkw zk wathcer
   * @param server name
   * @param task to own
   * @param taskZKVersion version of the task in zk
   * @return non-negative integer value when task can be owned by current region server otherwise -1
   */
  protected static int attemptToOwnTask(boolean isFirstTime, ZooKeeperWatcher zkw,
      ServerName server, String task, RecoveryMode mode, int taskZKVersion) {
    int latestZKVersion = FAILED_TO_OWN_TASK;
    try {
      SplitLogTask slt = new SplitLogTask.Owned(server, mode);
      Stat stat = zkw.getRecoverableZooKeeper().setData(task, slt.toByteArray(), taskZKVersion);
      if (stat == null) {
        LOG.warn("zk.setData() returned null for path " + task);
        SplitLogCounters.tot_wkr_task_heartbeat_failed.incrementAndGet();
        return FAILED_TO_OWN_TASK;
      }
      latestZKVersion = stat.getVersion();
      SplitLogCounters.tot_wkr_task_heartbeat.incrementAndGet();
      return latestZKVersion;
    } catch (KeeperException e) {
      if (!isFirstTime) {
        if (e.code().equals(KeeperException.Code.NONODE)) {
          LOG.warn("NONODE failed to assert ownership for " + task, e);
        } else if (e.code().equals(KeeperException.Code.BADVERSION)) {
          LOG.warn("BADVERSION failed to assert ownership for " + task, e);
        } else {
          LOG.warn("failed to assert ownership for " + task, e);
        }
      }
    } catch (InterruptedException e1) {
      LOG.warn("Interrupted while trying to assert ownership of " + task + " "
          + StringUtils.stringifyException(e1));
      Thread.currentThread().interrupt();
    }
    SplitLogCounters.tot_wkr_task_heartbeat_failed.incrementAndGet();
    return FAILED_TO_OWN_TASK;
  }

  /**
   * Wait for tasks to become available at /hbase/splitlog zknode. Grab a task one at a time. This
   * policy puts an upper-limit on the number of simultaneous log splitting that could be happening
   * in a cluster.
   * <p>
   * Synchronization using {@link #taskReadyLock} ensures that it will try to grab every task that
   * has been put up
   * @throws InterruptedException
   */
  @Override
  public void taskLoop() throws InterruptedException {
    while (!shouldStop) {
      int seq_start = taskReadySeq;
      List<String> paths = null;
      paths = getTaskList();
      if (paths == null) {
        LOG.warn("Could not get tasks, did someone remove " + watcher.splitLogZNode
            + " ... worker thread exiting.");
        return;
      }
      // pick meta wal firstly
      int offset = (int) (Math.random() * paths.size());
      for (int i = 0; i < paths.size(); i++) {
        if (DefaultWALProvider.isMetaFile(paths.get(i))) {
          offset = i;
          break;
        }
      }
      int numTasks = paths.size();
      for (int i = 0; i < numTasks; i++) {
        int idx = (i + offset) % paths.size();
        // don't call ZKSplitLog.getNodeName() because that will lead to
        // double encoding of the path name
        if (this.calculateAvailableSplitters(numTasks) > 0) {
          grabTask(ZKUtil.joinZNode(watcher.splitLogZNode, paths.get(idx)));
        } else {
          LOG.debug("Current region server " + server.getServerName() + " has "
              + this.tasksInProgress.get() + " tasks in progress and can't take more.");
          break;
        }
        if (shouldStop) {
          return;
        }
      }
      SplitLogCounters.tot_wkr_task_grabing.incrementAndGet();
      synchronized (taskReadyLock) {
        while (seq_start == taskReadySeq) {
          taskReadyLock.wait(checkInterval);
          if (server != null) {
            // check to see if we have stale recovering regions in our internal memory state
            Map<String, Region> recoveringRegions = server.getRecoveringRegions();
            if (!recoveringRegions.isEmpty()) {
              // Make a local copy to prevent ConcurrentModificationException when other threads
              // modify recoveringRegions
              List<String> tmpCopy = new ArrayList<String>(recoveringRegions.keySet());
              int listSize = tmpCopy.size();
              for (int i = 0; i < listSize; i++) {
                String region = tmpCopy.get(i);
                String nodePath = ZKUtil.joinZNode(watcher.recoveringRegionsZNode, region);
                try {
                  if (ZKUtil.checkExists(watcher, nodePath) == -1) {
                    server.getExecutorService().submit(
                      new FinishRegionRecoveringHandler(server, region, nodePath));
                  } else {
                    // current check is a defensive(or redundant) mechanism to prevent us from
                    // having stale recovering regions in our internal RS memory state while
                    // zookeeper(source of truth) says differently. We stop at the first good one
                    // because we should not have a single instance such as this in normal case so
                    // check the first one is good enough.
                    break;
                  }
                } catch (KeeperException e) {
                  // ignore zookeeper error
                  LOG.debug("Got a zookeeper when trying to open a recovering region", e);
                  break;
                }
              }
            }
          }
        }
      }
    }
  }

  private List<String> getTaskList() throws InterruptedException {
    List<String> childrenPaths = null;
    long sleepTime = 1000;
    // It will be in loop till it gets the list of children or
    // it will come out if worker thread exited.
    while (!shouldStop) {
      try {
        childrenPaths =
            ZKUtil.listChildrenAndWatchForNewChildren(watcher,
              watcher.splitLogZNode);
        if (childrenPaths != null) {
          return childrenPaths;
        }
      } catch (KeeperException e) {
        LOG.warn("Could not get children of znode " + watcher.splitLogZNode, e);
      }
      LOG.debug("Retry listChildren of znode " + watcher.splitLogZNode
          + " after sleep for " + sleepTime + "ms!");
      Thread.sleep(sleepTime);
    }
    return childrenPaths;
  }

  @Override
  public void markCorrupted(Path rootDir, String name, FileSystem fs) {
    ZKSplitLog.markCorrupted(rootDir, name, fs);
  }

  @Override
  public boolean isReady() throws InterruptedException {
    int result = -1;
    try {
      result = ZKUtil.checkExists(watcher, watcher.splitLogZNode);
    } catch (KeeperException e) {
      // ignore
      LOG.warn("Exception when checking for " + watcher.splitLogZNode
          + " ... retrying", e);
    }
    if (result == -1) {
      LOG.info(watcher.splitLogZNode
          + " znode does not exist, waiting for master to create");
      Thread.sleep(1000);
    }
    return (result != -1);
  }

  @Override
  public int getTaskReadySeq() {
    return taskReadySeq;
  }

  @Override
  public void registerListener() {
    watcher.registerListener(this);
  }

  @Override
  public void removeListener() {
    watcher.unregisterListener(this);
  }


  @Override
  public void stopProcessingTasks() {
    this.shouldStop = true;

  }

  @Override
  public boolean isStop() {
    return shouldStop;
  }

  @Override
  public RegionStoreSequenceIds getRegionFlushedSequenceId(String failedServerName, String key)
      throws IOException {
    return ZKSplitLog.getRegionFlushedSequenceId(watcher, failedServerName, key);
  }

  /**
   * Asynchronous handler for zk get-data-set-watch on node results.
   */
  class GetDataAsyncCallback implements AsyncCallback.DataCallback {
    private final Log LOG = LogFactory.getLog(GetDataAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      SplitLogCounters.tot_wkr_get_data_result.incrementAndGet();
      if (rc != 0) {
        LOG.warn("getdata rc = " + KeeperException.Code.get(rc) + " " + path);
        getDataSetWatchFailure(path);
        return;
      }
      data = watcher.getRecoverableZooKeeper().removeMetaData(data);
      getDataSetWatchSuccess(path, data);
    }
  }

  /*
   * Next part is related to WALSplitterHandler
   */
  /**
   * endTask() can fail and the only way to recover out of it is for the
   * {@link org.apache.hadoop.hbase.master.SplitLogManager} to timeout the task node.
   * @param slt
   * @param ctr
   */
  @Override
  public void endTask(SplitLogTask slt, AtomicLong ctr, SplitTaskDetails details) {
    ZkSplitTaskDetails zkDetails = (ZkSplitTaskDetails) details;
    String task = zkDetails.getTaskNode();
    int taskZKVersion = zkDetails.getCurTaskZKVersion().intValue();
    try {
      if (ZKUtil.setData(watcher, task, slt.toByteArray(), taskZKVersion)) {
        LOG.info("successfully transitioned task " + task + " to final state " + slt);
        ctr.incrementAndGet();
        return;
      }
      LOG.warn("failed to transistion task " + task + " to end state " + slt
          + " because of version mismatch ");
    } catch (KeeperException.BadVersionException bve) {
      LOG.warn("transisition task " + task + " to " + slt + " failed because of version mismatch",
        bve);
    } catch (KeeperException.NoNodeException e) {
      LOG.fatal(
        "logic error - end task " + task + " " + slt + " failed because task doesn't exist", e);
    } catch (KeeperException e) {
      LOG.warn("failed to end task, " + task + " " + slt, e);
    }
    SplitLogCounters.tot_wkr_final_transition_failed.incrementAndGet();
  }

  /**
   * When ZK-based implementation wants to complete the task, it needs to know task znode and
   * current znode cversion (needed for subsequent update operation).
   */
  public static class ZkSplitTaskDetails implements SplitTaskDetails {
    private String taskNode;
    private MutableInt curTaskZKVersion;

    public ZkSplitTaskDetails() {
    }

    public ZkSplitTaskDetails(String taskNode, MutableInt curTaskZKVersion) {
      this.taskNode = taskNode;
      this.curTaskZKVersion = curTaskZKVersion;
    }

    public String getTaskNode() {
      return taskNode;
    }

    public void setTaskNode(String taskNode) {
      this.taskNode = taskNode;
    }

    public MutableInt getCurTaskZKVersion() {
      return curTaskZKVersion;
    }

    public void setCurTaskZKVersion(MutableInt curTaskZKVersion) {
      this.curTaskZKVersion = curTaskZKVersion;
    }

    @Override
    public String getWALFile() {
      return ZKSplitLog.getFileName(taskNode);
    }
  }

}
