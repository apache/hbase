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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.handler.HLogSplitterHandler;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * This worker is spawned in every regionserver (should we also spawn one in
 * the master?). The Worker waits for log splitting tasks to be put up by the
 * {@link SplitLogManager} running in the master and races with other workers
 * in other serves to acquire those tasks. The coordination is done via
 * zookeeper. All the action takes place at /hbase/splitlog znode.
 * <p>
 * If a worker has successfully moved the task from state UNASSIGNED to
 * OWNED then it owns the task. It keeps heart beating the manager by
 * periodically moving the task from UNASSIGNED to OWNED state. On success it
 * moves the task to TASK_DONE. On unrecoverable error it moves task state to
 * ERR. If it cannot continue but wants the master to retry the task then it
 * moves the task state to RESIGNED.
 * <p>
 * The manager can take a task away from a worker by moving the task from
 * OWNED to UNASSIGNED. In the absence of a global lock there is a
 * unavoidable race here - a worker might have just finished its task when it
 * is stripped of its ownership. Here we rely on the idempotency of the log
 * splitting task for correctness
 */
@InterfaceAudience.Private
public class SplitLogWorker extends ZooKeeperListener implements Runnable {
  public static final int DEFAULT_MAX_SPLITTERS = 2;

  private static final Log LOG = LogFactory.getLog(SplitLogWorker.class);
  private static final int checkInterval = 5000; // 5 seconds
  private static final int FAILED_TO_OWN_TASK = -1;

  Thread worker;
  private final ServerName serverName;
  private final TaskExecutor splitTaskExecutor;
  // thread pool which executes recovery work
  private final ExecutorService executorService;

  private final Object taskReadyLock = new Object();
  volatile int taskReadySeq = 0;
  private volatile String currentTask = null;
  private int currentVersion;
  private volatile boolean exitWorker;
  private final Object grabTaskLock = new Object();
  private boolean workerInGrabTask = false;
  private final int report_period;
  private RegionServerServices server = null;
  private Configuration conf = null;
  protected final AtomicInteger tasksInProgress = new AtomicInteger(0);
  private int maxConcurrentTasks = 0;

  public SplitLogWorker(ZooKeeperWatcher watcher, Configuration conf, RegionServerServices server,
      TaskExecutor splitTaskExecutor) {
    super(watcher);
    this.server = server;
    this.serverName = server.getServerName();
    this.splitTaskExecutor = splitTaskExecutor;
    report_period = conf.getInt("hbase.splitlog.report.period",
      conf.getInt("hbase.splitlog.manager.timeout", SplitLogManager.DEFAULT_TIMEOUT) / 3);
    this.conf = conf;
    this.executorService = this.server.getExecutorService();
    this.maxConcurrentTasks =
        conf.getInt("hbase.regionserver.wal.max.splitters", DEFAULT_MAX_SPLITTERS);
  }

  public SplitLogWorker(final ZooKeeperWatcher watcher, final Configuration conf,
      RegionServerServices server, final LastSequenceId sequenceIdChecker) {
    this(watcher, conf, server, new TaskExecutor() {
      @Override
      public Status exec(String filename, RecoveryMode mode, CancelableProgressable p) {
        Path rootdir;
        FileSystem fs;
        try {
          rootdir = FSUtils.getRootDir(conf);
          fs = rootdir.getFileSystem(conf);
        } catch (IOException e) {
          LOG.warn("could not find root dir or fs", e);
          return Status.RESIGNED;
        }
        // TODO have to correctly figure out when log splitting has been
        // interrupted or has encountered a transient error and when it has
        // encountered a bad non-retry-able persistent error.
        try {
          if (!HLogSplitter.splitLogFile(rootdir, fs.getFileStatus(new Path(rootdir, filename)),
            fs, conf, p, sequenceIdChecker, watcher, mode)) {
            return Status.PREEMPTED;
          }
        } catch (InterruptedIOException iioe) {
          LOG.warn("log splitting of " + filename + " interrupted, resigning", iioe);
          return Status.RESIGNED;
        } catch (IOException e) {
          Throwable cause = e.getCause();
          if (e instanceof RetriesExhaustedException && (cause instanceof NotServingRegionException 
                  || cause instanceof ConnectException 
                  || cause instanceof SocketTimeoutException)) {
            LOG.warn("log replaying of " + filename + " can't connect to the target regionserver, "
            		+ "resigning", e);
            return Status.RESIGNED;
          } else if (cause instanceof InterruptedException) {
            LOG.warn("log splitting of " + filename + " interrupted, resigning", e);
            return Status.RESIGNED;
          } else if(cause instanceof KeeperException) {
            LOG.warn("log splitting of " + filename + " hit ZooKeeper issue, resigning", e);
            return Status.RESIGNED;
          }
          LOG.warn("log splitting of " + filename + " failed, returning error", e);
          return Status.ERR;
        }
        return Status.DONE;
      }
    });
  }

  @Override
  public void run() {
    try {
      LOG.info("SplitLogWorker " + this.serverName + " starting");
      this.watcher.registerListener(this);
      // pre-initialize a new connection for splitlogworker configuration
      HConnectionManager.getConnection(conf);

      // wait for master to create the splitLogZnode
      int res = -1;
      while (res == -1 && !exitWorker) {
        try {
          res = ZKUtil.checkExists(watcher, watcher.splitLogZNode);
        } catch (KeeperException e) {
          // ignore
          LOG.warn("Exception when checking for " + watcher.splitLogZNode  + " ... retrying", e);
        }
        if (res == -1) {
          try {
            LOG.info(watcher.splitLogZNode + " znode does not exist, waiting for master to create");
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.debug("Interrupted while waiting for " + watcher.splitLogZNode
                + (exitWorker ? "" : " (ERROR: exitWorker is not set, " +
                "exiting anyway)"));
            exitWorker = true;
            break;
          }
        }
      }

      if (!exitWorker) {
        taskLoop();
      }
    } catch (Throwable t) {
      // only a logical error can cause here. Printing it out
      // to make debugging easier
      LOG.error("unexpected error ", t);
    } finally {
      LOG.info("SplitLogWorker " + this.serverName + " exiting");
    }
  }

  /**
   * Wait for tasks to become available at /hbase/splitlog zknode. Grab a task
   * one at a time. This policy puts an upper-limit on the number of
   * simultaneous log splitting that could be happening in a cluster.
   * <p>
   * Synchronization using {@link #taskReadyLock} ensures that it will
   * try to grab every task that has been put up
   */
  private void taskLoop() {
    while (!exitWorker) {
      int seq_start = taskReadySeq;
      List<String> paths = getTaskList();
      if (paths == null) {
        LOG.warn("Could not get tasks, did someone remove " +
            this.watcher.splitLogZNode + " ... worker thread exiting.");
        return;
      }
      // pick meta wal firstly
      int offset = (int) (Math.random() * paths.size());
      for(int i = 0; i < paths.size(); i ++){
        if(HLogUtil.isMetaFile(paths.get(i))) {
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
          LOG.debug("Current region server " + this.serverName + " has "
              + this.tasksInProgress.get() + " tasks in progress and can't take more.");
          break;
        }
        if (exitWorker) {
          return;
        }
      }
      SplitLogCounters.tot_wkr_task_grabing.incrementAndGet();
      synchronized (taskReadyLock) {
        while (seq_start == taskReadySeq) {
          try {
            taskReadyLock.wait(checkInterval);
            if (this.server != null) {
              // check to see if we have stale recovering regions in our internal memory state
              Map<String, HRegion> recoveringRegions = this.server.getRecoveringRegions();
              if (!recoveringRegions.isEmpty()) {
                // Make a local copy to prevent ConcurrentModificationException when other threads
                // modify recoveringRegions
                List<String> tmpCopy = new ArrayList<String>(recoveringRegions.keySet());
                for (String region : tmpCopy) {
                  String nodePath = ZKUtil.joinZNode(this.watcher.recoveringRegionsZNode, region);
                  try {
                    if (ZKUtil.checkExists(this.watcher, nodePath) == -1) {
                      HRegion r = recoveringRegions.remove(region);
                      if (r != null) {
                        r.setRecovering(false);
                      }
                      LOG.debug("Mark recovering region:" + region + " up.");
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
          } catch (InterruptedException e) {
            LOG.info("SplitLogWorker interrupted while waiting for task," +
                " exiting: " + e.toString() + (exitWorker ? "" :
                " (ERROR: exitWorker is not set, exiting anyway)"));
            exitWorker = true;
            return;
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
        if ((data = ZKUtil.getDataNoWatch(this.watcher, path, stat)) == null) {
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

      currentVersion = attemptToOwnTask(true, watcher, serverName, path, slt.getMode(), 
        stat.getVersion());
      if (currentVersion < 0) {
        SplitLogCounters.tot_wkr_failed_to_grab_task_lost_race.incrementAndGet();
        return;
      }

      if (ZKSplitLog.isRescanNode(watcher, currentTask)) {
        HLogSplitterHandler.endTask(watcher, new SplitLogTask.Done(this.serverName, slt.getMode()),
          SplitLogCounters.tot_wkr_task_acquired_rescan, currentTask, currentVersion);
        return;
      }

      LOG.info("worker " + serverName + " acquired task " + path);
      SplitLogCounters.tot_wkr_task_acquired.incrementAndGet();
      getDataSetWatchAsync();

      submitTask(path, slt.getMode(), currentVersion, this.report_period);

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
   * Try to own the task by transitioning the zk node data from UNASSIGNED to OWNED.
   * <p>
   * This method is also used to periodically heartbeat the task progress by transitioning the node
   * from OWNED to OWNED.
   * <p>
   * @param isFirstTime
   * @param zkw
   * @param server
   * @param task
   * @param taskZKVersion
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
      LOG.warn("Interrupted while trying to assert ownership of " +
          task + " " + StringUtils.stringifyException(e1));
      Thread.currentThread().interrupt();
    }
    SplitLogCounters.tot_wkr_task_heartbeat_failed.incrementAndGet();
    return FAILED_TO_OWN_TASK;
  }

  /**
   * This function calculates how many splitters it could create based on expected average tasks per
   * RS and the hard limit upper bound(maxConcurrentTasks) set by configuration. <br>
   * At any given time, a RS allows spawn MIN(Expected Tasks/RS, Hard Upper Bound)
   * @param numTasks current total number of available tasks
   * @return
   */
  private int calculateAvailableSplitters(int numTasks) {
    // at lease one RS(itself) available
    int availableRSs = 1;
    try {
      List<String> regionServers = ZKUtil.listChildrenNoWatch(watcher, watcher.rsZNode);
      availableRSs = Math.max(availableRSs, (regionServers == null) ? 0 : regionServers.size());
    } catch (KeeperException e) {
      // do nothing
      LOG.debug("getAvailableRegionServers got ZooKeeper exception", e);
    }

    int expectedTasksPerRS = (numTasks / availableRSs) + ((numTasks % availableRSs == 0) ? 0 : 1);
    expectedTasksPerRS = Math.max(1, expectedTasksPerRS); // at least be one
    // calculate how many more splitters we could spawn
    return Math.min(expectedTasksPerRS, this.maxConcurrentTasks) - this.tasksInProgress.get();
  }

  /**
   * Submit a log split task to executor service
   * @param curTask
   * @param curTaskZKVersion
   */
  void submitTask(final String curTask, final RecoveryMode mode, final int curTaskZKVersion, 
    final int reportPeriod) {
    final MutableInt zkVersion = new MutableInt(curTaskZKVersion);

    CancelableProgressable reporter = new CancelableProgressable() {
      private long last_report_at = 0;

      @Override
      public boolean progress() {
        long t = EnvironmentEdgeManager.currentTimeMillis();
        if ((t - last_report_at) > reportPeriod) {
          last_report_at = t;
          int latestZKVersion =
              attemptToOwnTask(false, watcher, serverName, curTask, mode, zkVersion.intValue());
          if (latestZKVersion < 0) {
            LOG.warn("Failed to heartbeat the task" + curTask);
            return false;
          }
          zkVersion.setValue(latestZKVersion);
        }
        return true;
      }
    };
    
    HLogSplitterHandler hsh = new HLogSplitterHandler(this.server, curTask, zkVersion, reporter, 
      this.tasksInProgress, this.splitTaskExecutor, mode);
    this.executorService.submit(hsh);
  }

  void getDataSetWatchAsync() {
    this.watcher.getRecoverableZooKeeper().getZooKeeper().
      getData(currentTask, this.watcher,
      new GetDataAsyncCallback(), null);
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
          // have to compare data. cannot compare version because then there
          // will be race with attemptToOwnTask()
          // cannot just check whether the node has been transitioned to
          // UNASSIGNED because by the time this worker sets the data watch
          // the node might have made two transitions - from owned by this
          // worker to unassigned to owned by another worker
          if (! slt.isOwned(this.serverName) &&
              ! slt.isDone(this.serverName) &&
              ! slt.isErr(this.serverName) &&
              ! slt.isResigned(this.serverName)) {
            LOG.info("task " + taskpath + " preempted from " +
                serverName + ", current task state and owner=" + slt.toString());
            stopTask();
          }
        }
      }
    }
  }

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

  @Override
  public void nodeDataChanged(String path) {
    // there will be a self generated dataChanged event every time attemptToOwnTask()
    // heartbeats the task znode by upping its version
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change
        String taskpath = currentTask;
        if (taskpath!= null && taskpath.equals(path)) {
          getDataSetWatchAsync();
        }
      }
    }
  }


  private List<String> getTaskList() {
    List<String> childrenPaths = null;
    long sleepTime = 1000;
    // It will be in loop till it gets the list of children or
    // it will come out if worker thread exited.
    while (!exitWorker) {
      try {
        childrenPaths = ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
            this.watcher.splitLogZNode);
        if (childrenPaths != null) {
          return childrenPaths;
        }
      } catch (KeeperException e) {
        LOG.warn("Could not get children of znode "
            + this.watcher.splitLogZNode, e);
      }
      try {
        LOG.debug("Retry listChildren of znode " + this.watcher.splitLogZNode
            + " after sleep for " + sleepTime + "ms!");
        Thread.sleep(sleepTime);
      } catch (InterruptedException e1) {
        LOG.warn("Interrupted while trying to get task list ...", e1);
        Thread.currentThread().interrupt();
      }
    }
    return childrenPaths;
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if(path.equals(watcher.splitLogZNode)) {
      LOG.debug("tasks arrived or departed");
      synchronized (taskReadyLock) {
        taskReadySeq++;
        taskReadyLock.notify();
      }
    }
  }

  /**
   * If the worker is doing a task i.e. splitting a log file then stop the task.
   * It doesn't exit the worker thread.
   */
  void stopTask() {
    LOG.info("Sending interrupt to stop the worker thread");
    worker.interrupt(); // TODO interrupt often gets swallowed, do what else?
  }


  /**
   * start the SplitLogWorker thread
   */
  public void start() {
    worker = new Thread(null, this, "SplitLogWorker-" + serverName);
    exitWorker = false;
    worker.start();
  }

  /**
   * stop the SplitLogWorker thread
   */
  public void stop() {
    exitWorker = true;
    stopTask();
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

  /**
   * Objects implementing this interface actually do the task that has been
   * acquired by a {@link SplitLogWorker}. Since there isn't a water-tight
   * guarantee that two workers will not be executing the same task therefore it
   * is better to have workers prepare the task and then have the
   * {@link SplitLogManager} commit the work in SplitLogManager.TaskFinisher
   */
  public interface TaskExecutor {
    enum Status {
      DONE(),
      ERR(),
      RESIGNED(),
      PREEMPTED()
    }
    Status exec(String name, RecoveryMode mode, CancelableProgressable p);
  }
}
