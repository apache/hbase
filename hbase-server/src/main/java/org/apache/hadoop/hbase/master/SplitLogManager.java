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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.master.SplitLogManager.ResubmitDirective.CHECK;
import static org.apache.hadoop.hbase.master.SplitLogManager.ResubmitDirective.FORCE;
import static org.apache.hadoop.hbase.master.SplitLogManager.TerminationStatus.DELETED;
import static org.apache.hadoop.hbase.master.SplitLogManager.TerminationStatus.FAILURE;
import static org.apache.hadoop.hbase.master.SplitLogManager.TerminationStatus.IN_PROGRESS;
import static org.apache.hadoop.hbase.master.SplitLogManager.TerminationStatus.SUCCESS;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.SplitLogManagerCoordination;
import org.apache.hadoop.hbase.coordination.SplitLogManagerCoordination.SplitLogManagerDetails;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Distributes the task of log splitting to the available region servers.
 * Coordination happens via coordination engine. For every log file that has to be split a
 * task is created. SplitLogWorkers race to grab a task.
 *
 * <p>SplitLogManager monitors the tasks that it creates using the
 * timeoutMonitor thread. If a task's progress is slow then
 * {@link SplitLogManagerCoordination#checkTasks} will take away the
 * task from the owner {@link org.apache.hadoop.hbase.regionserver.SplitLogWorker} 
 * and the task will be up for grabs again. When the task is done then it is 
 * deleted by SplitLogManager.
 *
 * <p>Clients call {@link #splitLogDistributed(Path)} to split a region server's
 * log files. The caller thread waits in this method until all the log files
 * have been split.
 *
 * <p>All the coordination calls made by this class are asynchronous. This is mainly
 * to help reduce response time seen by the callers.
 *
 * <p>There is race in this design between the SplitLogManager and the
 * SplitLogWorker. SplitLogManager might re-queue a task that has in reality
 * already been completed by a SplitLogWorker. We rely on the idempotency of
 * the log splitting task for correctness.
 *
 * <p>It is also assumed that every log splitting task is unique and once
 * completed (either with success or with error) it will be not be submitted
 * again. If a task is resubmitted then there is a risk that old "delete task"
 * can delete the re-submission.
 */
@InterfaceAudience.Private
public class SplitLogManager {
  private static final Log LOG = LogFactory.getLog(SplitLogManager.class);

  private Server server;

  private final Stoppable stopper;
  private final Configuration conf;
  private final ChoreService choreService;

  public static final int DEFAULT_UNASSIGNED_TIMEOUT = (3 * 60 * 1000); // 3 min

  private long unassignedTimeout;
  private long lastTaskCreateTime = Long.MAX_VALUE;
  private long checkRecoveringTimeThreshold = 15000; // 15 seconds
  private final List<Pair<Set<ServerName>, Boolean>> failedRecoveringRegionDeletions = Collections
      .synchronizedList(new ArrayList<Pair<Set<ServerName>, Boolean>>());

  /**
   * In distributedLogReplay mode, we need touch both splitlog and recovering-regions znodes in one
   * operation. So the lock is used to guard such cases.
   */
  protected final ReentrantLock recoveringRegionLock = new ReentrantLock();

  private final ConcurrentMap<String, Task> tasks = new ConcurrentHashMap<String, Task>();
  private TimeoutMonitor timeoutMonitor;

  private volatile Set<ServerName> deadWorkers = null;
  private final Object deadWorkersLock = new Object();

  /**
   * Its OK to construct this object even when region-servers are not online. It does lookup the
   * orphan tasks in coordination engine but it doesn't block waiting for them to be done.
   * @param server the server instance
   * @param conf the HBase configuration
   * @param stopper the stoppable in case anything is wrong
   * @param master the master services
   * @param serverName the master server name
   * @throws IOException
   */
  public SplitLogManager(Server server, Configuration conf, Stoppable stopper,
      MasterServices master, ServerName serverName) throws IOException {
    this.server = server;
    this.conf = conf;
    this.stopper = stopper;
    this.choreService = new ChoreService(serverName.toString() + "_splitLogManager_");
    if (server.getCoordinatedStateManager() != null) {
      SplitLogManagerCoordination coordination =
          ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
              .getSplitLogManagerCoordination();
      Set<String> failedDeletions = Collections.synchronizedSet(new HashSet<String>());
      SplitLogManagerDetails details =
          new SplitLogManagerDetails(tasks, master, failedDeletions, serverName);
      coordination.init();
      coordination.setDetails(details);
      // Determine recovery mode
    }
    this.unassignedTimeout =
        conf.getInt("hbase.splitlog.manager.unassigned.timeout", DEFAULT_UNASSIGNED_TIMEOUT);
    this.timeoutMonitor =
        new TimeoutMonitor(conf.getInt("hbase.splitlog.manager.timeoutmonitor.period", 1000),
            stopper);
    choreService.scheduleChore(timeoutMonitor);
  }

  private FileStatus[] getFileList(List<Path> logDirs, PathFilter filter) throws IOException {
    return getFileList(conf, logDirs, filter);
  }

  /**
   * Get a list of paths that need to be split given a set of server-specific directories and
   * optinally  a filter.
   *
   * See {@link DefaultWALProvider#getServerNameFromWALDirectoryName} for more info on directory
   * layout.
   *
   * Should be package-private, but is needed by
   * {@link org.apache.hadoop.hbase.wal.WALSplitter#split(Path, Path, Path, FileSystem,
   *     Configuration, WALFactory)} for tests.
   */
  @VisibleForTesting
  public static FileStatus[] getFileList(final Configuration conf, final List<Path> logDirs,
      final PathFilter filter)
      throws IOException {
    List<FileStatus> fileStatus = new ArrayList<FileStatus>();
    for (Path logDir : logDirs) {
      final FileSystem fs = logDir.getFileSystem(conf);
      if (!fs.exists(logDir)) {
        LOG.warn(logDir + " doesn't exist. Nothing to do!");
        continue;
      }
      FileStatus[] logfiles = FSUtils.listStatus(fs, logDir, filter);
      if (logfiles == null || logfiles.length == 0) {
        LOG.info(logDir + " is empty dir, no logs to split");
      } else {
        Collections.addAll(fileStatus, logfiles);
      }
    }
    FileStatus[] a = new FileStatus[fileStatus.size()];
    return fileStatus.toArray(a);
  }

  /**
   * @param logDir one region sever wal dir path in .logs
   * @throws IOException if there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   * @throws IOException
   */
  public long splitLogDistributed(final Path logDir) throws IOException {
    List<Path> logDirs = new ArrayList<Path>();
    logDirs.add(logDir);
    return splitLogDistributed(logDirs);
  }

  /**
   * The caller will block until all the log files of the given region server have been processed -
   * successfully split or an error is encountered - by an available worker region server. This
   * method must only be called after the region servers have been brought online.
   * @param logDirs List of log dirs to split
   * @throws IOException If there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   */
  public long splitLogDistributed(final List<Path> logDirs) throws IOException {
    if (logDirs.isEmpty()) {
      return 0;
    }
    Set<ServerName> serverNames = new HashSet<ServerName>();
    for (Path logDir : logDirs) {
      try {
        ServerName serverName = DefaultWALProvider.getServerNameFromWALDirectoryName(logDir);
        if (serverName != null) {
          serverNames.add(serverName);
        }
      } catch (IllegalArgumentException e) {
        // ignore invalid format error.
        LOG.warn("Cannot parse server name from " + logDir);
      }
    }
    return splitLogDistributed(serverNames, logDirs, null);
  }

  /**
   * The caller will block until all the hbase:meta log files of the given region server have been
   * processed - successfully split or an error is encountered - by an available worker region
   * server. This method must only be called after the region servers have been brought online.
   * @param logDirs List of log dirs to split
   * @param filter the Path filter to select specific files for considering
   * @throws IOException If there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   */
  public long splitLogDistributed(final Set<ServerName> serverNames, final List<Path> logDirs,
      PathFilter filter) throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus("Doing distributed log split in " +
      logDirs + " for serverName=" + serverNames);
    FileStatus[] logfiles = getFileList(logDirs, filter);
    status.setStatus("Checking directory contents...");
    LOG.debug("Scheduling batch of logs to split");
    SplitLogCounters.tot_mgr_log_split_batch_start.incrementAndGet();
    LOG.info("started splitting " + logfiles.length + " logs in " + logDirs +
      " for " + serverNames);
    long t = EnvironmentEdgeManager.currentTime();
    long totalSize = 0;
    TaskBatch batch = new TaskBatch();
    Boolean isMetaRecovery = (filter == null) ? null : false;
    for (FileStatus lf : logfiles) {
      // TODO If the log file is still being written to - which is most likely
      // the case for the last log file - then its length will show up here
      // as zero. The size of such a file can only be retrieved after
      // recover-lease is done. totalSize will be under in most cases and the
      // metrics that it drives will also be under-reported.
      totalSize += lf.getLen();
      String pathToLog = FSUtils.removeRootPath(lf.getPath(), conf);
      if (!enqueueSplitTask(pathToLog, batch)) {
        throw new IOException("duplicate log split scheduled for " + lf.getPath());
      }
    }
    waitForSplittingCompletion(batch, status);
    // remove recovering regions
    if (filter == MasterFileSystem.META_FILTER /* reference comparison */) {
      // we split meta regions and user regions separately therefore logfiles are either all for
      // meta or user regions but won't for both( we could have mixed situations in tests)
      isMetaRecovery = true;
    }
    removeRecoveringRegions(serverNames, isMetaRecovery);

    if (batch.done != batch.installed) {
      batch.isDead = true;
      SplitLogCounters.tot_mgr_log_split_batch_err.incrementAndGet();
      LOG.warn("error while splitting logs in " + logDirs + " installed = " + batch.installed
          + " but only " + batch.done + " done");
      String msg = "error or interrupted while splitting logs in " + logDirs + " Task = " + batch;
      status.abort(msg);
      throw new IOException(msg);
    }
    for (Path logDir : logDirs) {
      status.setStatus("Cleaning up log directory...");
      final FileSystem fs = logDir.getFileSystem(conf);
      try {
        if (fs.exists(logDir) && !fs.delete(logDir, false)) {
          LOG.warn("Unable to delete log src dir. Ignoring. " + logDir);
        }
      } catch (IOException ioe) {
        FileStatus[] files = fs.listStatus(logDir);
        if (files != null && files.length > 0) {
          LOG.warn("returning success without actually splitting and "
              + "deleting all the log files in path " + logDir);
        } else {
          LOG.warn("Unable to delete log src dir. Ignoring. " + logDir, ioe);
        }
      }
      SplitLogCounters.tot_mgr_log_split_batch_success.incrementAndGet();
    }
    String msg =
        "finished splitting (more than or equal to) " + totalSize + " bytes in " + batch.installed
            + " log files in " + logDirs + " in "
            + (EnvironmentEdgeManager.currentTime() - t) + "ms";
    status.markComplete(msg);
    LOG.info(msg);
    return totalSize;
  }

  /**
   * Add a task entry to coordination if it is not already there.
   * @param taskname the path of the log to be split
   * @param batch the batch this task belongs to
   * @return true if a new entry is created, false if it is already there.
   */
  boolean enqueueSplitTask(String taskname, TaskBatch batch) {
    lastTaskCreateTime = EnvironmentEdgeManager.currentTime();
    String task =
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitLogManagerCoordination().prepareTask(taskname);
    Task oldtask = createTaskIfAbsent(task, batch);
    if (oldtask == null) {
      // publish the task in the coordination engine
      ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
          .getSplitLogManagerCoordination().submitTask(task);
      return true;
    }
    return false;
  }

  private void waitForSplittingCompletion(TaskBatch batch, MonitoredTask status) {
    synchronized (batch) {
      while ((batch.done + batch.error) != batch.installed) {
        try {
          status.setStatus("Waiting for distributed tasks to finish. " + " scheduled="
              + batch.installed + " done=" + batch.done + " error=" + batch.error);
          int remaining = batch.installed - (batch.done + batch.error);
          int actual = activeTasks(batch);
          if (remaining != actual) {
            LOG.warn("Expected " + remaining + " active tasks, but actually there are " + actual);
          }
          int remainingTasks =
              ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
                  .getSplitLogManagerCoordination().remainingTasksInCoordination();
          if (remainingTasks >= 0 && actual > remainingTasks) {
            LOG.warn("Expected at least" + actual + " tasks remaining, but actually there are "
                + remainingTasks);
          }
          if (remainingTasks == 0 || actual == 0) {
            LOG.warn("No more task remaining, splitting "
                + "should have completed. Remaining tasks is " + remainingTasks
                + ", active tasks in map " + actual);
            if (remainingTasks == 0 && actual == 0) {
              return;
            }
          }
          batch.wait(100);
          if (stopper.isStopped()) {
            LOG.warn("Stopped while waiting for log splits to be completed");
            return;
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for log splits to be completed");
          Thread.currentThread().interrupt();
          return;
        }
      }
    }
  }

  @VisibleForTesting
  ConcurrentMap<String, Task> getTasks() {
    return tasks;
  }

  private int activeTasks(final TaskBatch batch) {
    int count = 0;
    for (Task t : tasks.values()) {
      if (t.batch == batch && t.status == TerminationStatus.IN_PROGRESS) {
        count++;
      }
    }
    return count;

  }

  /**
   * It removes recovering regions under /hbase/recovering-regions/[encoded region name] so that the
   * region server hosting the region can allow reads to the recovered region
   * @param serverNames servers which are just recovered
   * @param isMetaRecovery whether current recovery is for the meta region on
   *          <code>serverNames<code>
   */
  private void removeRecoveringRegions(final Set<ServerName> serverNames, Boolean isMetaRecovery) {
    if (!isLogReplaying()) {
      // the function is only used in WALEdit direct replay mode
      return;
    }

    Set<String> recoveredServerNameSet = new HashSet<String>();
    if (serverNames != null) {
      for (ServerName tmpServerName : serverNames) {
        recoveredServerNameSet.add(tmpServerName.getServerName());
      }
    }

    try {
      this.recoveringRegionLock.lock();
      ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
          .getSplitLogManagerCoordination().removeRecoveringRegions(recoveredServerNameSet,
            isMetaRecovery);
    } catch (IOException e) {
      LOG.warn("removeRecoveringRegions got exception. Will retry", e);
      if (serverNames != null && !serverNames.isEmpty()) {
        this.failedRecoveringRegionDeletions.add(new Pair<Set<ServerName>, Boolean>(serverNames,
            isMetaRecovery));
      }
    } finally {
      this.recoveringRegionLock.unlock();
    }
  }

  /**
   * It removes stale recovering regions under /hbase/recovering-regions/[encoded region name]
   * during master initialization phase.
   * @param failedServers A set of known failed servers
   * @throws IOException
   */
  void removeStaleRecoveringRegions(final Set<ServerName> failedServers) throws IOException,
      InterruptedIOException {
    Set<String> knownFailedServers = new HashSet<String>();
    if (failedServers != null) {
      for (ServerName tmpServerName : failedServers) {
        knownFailedServers.add(tmpServerName.getServerName());
      }
    }

    this.recoveringRegionLock.lock();
    try {
      ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
          .getSplitLogManagerCoordination().removeStaleRecoveringRegions(knownFailedServers);
    } finally {
      this.recoveringRegionLock.unlock();
    }
  }

  /**
   * @param path
   * @param batch
   * @return null on success, existing task on error
   */
  private Task createTaskIfAbsent(String path, TaskBatch batch) {
    Task oldtask;
    // batch.installed is only changed via this function and
    // a single thread touches batch.installed.
    Task newtask = new Task();
    newtask.batch = batch;
    oldtask = tasks.putIfAbsent(path, newtask);
    if (oldtask == null) {
      batch.installed++;
      return null;
    }
    // new task was not used.
    synchronized (oldtask) {
      if (oldtask.isOrphan()) {
        if (oldtask.status == SUCCESS) {
          // The task is already done. Do not install the batch for this
          // task because it might be too late for setDone() to update
          // batch.done. There is no need for the batch creator to wait for
          // this task to complete.
          return (null);
        }
        if (oldtask.status == IN_PROGRESS) {
          oldtask.batch = batch;
          batch.installed++;
          LOG.debug("Previously orphan task " + path + " is now being waited upon");
          return null;
        }
        while (oldtask.status == FAILURE) {
          LOG.debug("wait for status of task " + path + " to change to DELETED");
          SplitLogCounters.tot_mgr_wait_for_zk_delete.incrementAndGet();
          try {
            oldtask.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted when waiting for znode delete callback");
            // fall through to return failure
            break;
          }
        }
        if (oldtask.status != DELETED) {
          LOG.warn("Failure because previously failed task"
              + " state still present. Waiting for znode delete callback" + " path=" + path);
          return oldtask;
        }
        // reinsert the newTask and it must succeed this time
        Task t = tasks.putIfAbsent(path, newtask);
        if (t == null) {
          batch.installed++;
          return null;
        }
        LOG.fatal("Logic error. Deleted task still present in tasks map");
        assert false : "Deleted task still present in tasks map";
        return t;
      }
      LOG.warn("Failure because two threads can't wait for the same task; path=" + path);
      return oldtask;
    }
  }

  Task findOrCreateOrphanTask(String path) {
    Task orphanTask = new Task();
    Task task;
    task = tasks.putIfAbsent(path, orphanTask);
    if (task == null) {
      LOG.info("creating orphan task " + path);
      SplitLogCounters.tot_mgr_orphan_task_acquired.incrementAndGet();
      task = orphanTask;
    }
    return task;
  }

  public void stop() {
    if (choreService != null) {
      choreService.shutdown();
    }
    if (timeoutMonitor != null) {
      timeoutMonitor.cancel(true);
    }
  }

  void handleDeadWorker(ServerName workerName) {
    // resubmit the tasks on the TimeoutMonitor thread. Makes it easier
    // to reason about concurrency. Makes it easier to retry.
    synchronized (deadWorkersLock) {
      if (deadWorkers == null) {
        deadWorkers = new HashSet<ServerName>(100);
      }
      deadWorkers.add(workerName);
    }
    LOG.info("dead splitlog worker " + workerName);
  }

  void handleDeadWorkers(Set<ServerName> serverNames) {
    synchronized (deadWorkersLock) {
      if (deadWorkers == null) {
        deadWorkers = new HashSet<ServerName>(100);
      }
      deadWorkers.addAll(serverNames);
    }
    LOG.info("dead splitlog workers " + serverNames);
  }

  /**
   * This function is to set recovery mode from outstanding split log tasks from before or current
   * configuration setting
   * @param isForInitialization
   * @throws IOException throws if it's impossible to set recovery mode
   */
  public void setRecoveryMode(boolean isForInitialization) throws IOException {
    ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getSplitLogManagerCoordination().setRecoveryMode(isForInitialization);

  }

  public void markRegionsRecovering(ServerName server, Set<HRegionInfo> userRegions)
      throws InterruptedIOException, IOException {
    if (userRegions == null || (!isLogReplaying())) {
      return;
    }
    try {
      this.recoveringRegionLock.lock();
      // mark that we're creating recovering regions
      ((BaseCoordinatedStateManager) this.server.getCoordinatedStateManager())
          .getSplitLogManagerCoordination().markRegionsRecovering(server, userRegions);
    } finally {
      this.recoveringRegionLock.unlock();
    }

  }

  /**
   * @return whether log is replaying
   */
  public boolean isLogReplaying() {
    CoordinatedStateManager m = server.getCoordinatedStateManager();
    if (m == null) return false;
    return ((BaseCoordinatedStateManager)m).getSplitLogManagerCoordination().isReplaying();
  }

  /**
   * @return whether log is splitting
   */
  public boolean isLogSplitting() {
    if (server.getCoordinatedStateManager() == null) return false;
    return ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getSplitLogManagerCoordination().isSplitting();
  }

  /**
   * @return the current log recovery mode
   */
  public RecoveryMode getRecoveryMode() {
    return ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
        .getSplitLogManagerCoordination().getRecoveryMode();
  }

  /**
   * Keeps track of the batch of tasks submitted together by a caller in splitLogDistributed().
   * Clients threads use this object to wait for all their tasks to be done.
   * <p>
   * All access is synchronized.
   */
  @InterfaceAudience.Private
  public static class TaskBatch {
    public int installed = 0;
    public int done = 0;
    public int error = 0;
    public volatile boolean isDead = false;

    @Override
    public String toString() {
      return ("installed = " + installed + " done = " + done + " error = " + error);
    }
  }

  /**
   * in memory state of an active task.
   */
  @InterfaceAudience.Private
  public static class Task {
    public volatile long last_update;
    public volatile int last_version;
    public volatile ServerName cur_worker_name;
    public volatile TaskBatch batch;
    public volatile TerminationStatus status;
    public volatile int incarnation;
    public final AtomicInteger unforcedResubmits = new AtomicInteger();
    public volatile boolean resubmitThresholdReached;

    @Override
    public String toString() {
      return ("last_update = " + last_update + " last_version = " + last_version
          + " cur_worker_name = " + cur_worker_name + " status = " + status + " incarnation = "
          + incarnation + " resubmits = " + unforcedResubmits.get() + " batch = " + batch);
    }

    public Task() {
      incarnation = 0;
      last_version = -1;
      status = IN_PROGRESS;
      setUnassigned();
    }

    public boolean isOrphan() {
      return (batch == null || batch.isDead);
    }

    public boolean isUnassigned() {
      return (cur_worker_name == null);
    }

    public void heartbeatNoDetails(long time) {
      last_update = time;
    }

    public void heartbeat(long time, int version, ServerName worker) {
      last_version = version;
      last_update = time;
      cur_worker_name = worker;
    }

    public void setUnassigned() {
      cur_worker_name = null;
      last_update = -1;
    }
  }

  /**
   * Periodically checks all active tasks and resubmits the ones that have timed out
   */
  private class TimeoutMonitor extends ScheduledChore {
    private long lastLog = 0;

    public TimeoutMonitor(final int period, Stoppable stopper) {
      super("SplitLogManager Timeout Monitor", stopper, period);
    }

    @Override
    protected void chore() {
      int resubmitted = 0;
      int unassigned = 0;
      int tot = 0;
      boolean found_assigned_task = false;
      Set<ServerName> localDeadWorkers;

      synchronized (deadWorkersLock) {
        localDeadWorkers = deadWorkers;
        deadWorkers = null;
      }

      for (Map.Entry<String, Task> e : tasks.entrySet()) {
        String path = e.getKey();
        Task task = e.getValue();
        ServerName cur_worker = task.cur_worker_name;
        tot++;
        // don't easily resubmit a task which hasn't been picked up yet. It
        // might be a long while before a SplitLogWorker is free to pick up a
        // task. This is because a SplitLogWorker picks up a task one at a
        // time. If we want progress when there are no region servers then we
        // will have to run a SplitLogWorker thread in the Master.
        if (task.isUnassigned()) {
          unassigned++;
          continue;
        }
        found_assigned_task = true;
        if (localDeadWorkers != null && localDeadWorkers.contains(cur_worker)) {
          SplitLogCounters.tot_mgr_resubmit_dead_server_task.incrementAndGet();
          if (((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
              .getSplitLogManagerCoordination().resubmitTask(path, task, FORCE)) {
            resubmitted++;
          } else {
            handleDeadWorker(cur_worker);
            LOG.warn("Failed to resubmit task " + path + " owned by dead " + cur_worker
                + ", will retry.");
          }
        } else if (((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitLogManagerCoordination().resubmitTask(path, task, CHECK)) {
          resubmitted++;
        }
      }
      if (tot > 0) {
        long now = EnvironmentEdgeManager.currentTime();
        if (now > lastLog + 5000) {
          lastLog = now;
          LOG.info("total tasks = " + tot + " unassigned = " + unassigned + " tasks=" + tasks);
        }
      }
      if (resubmitted > 0) {
        LOG.info("resubmitted " + resubmitted + " out of " + tot + " tasks");
      }
      // If there are pending tasks and all of them have been unassigned for
      // some time then put up a RESCAN node to ping the workers.
      // ZKSplitlog.DEFAULT_UNASSIGNED_TIMEOUT is of the order of minutes
      // because a. it is very unlikely that every worker had a
      // transient error when trying to grab the task b. if there are no
      // workers then all tasks wills stay unassigned indefinitely and the
      // manager will be indefinitely creating RESCAN nodes. TODO may be the
      // master should spawn both a manager and a worker thread to guarantee
      // that there is always one worker in the system
      if (tot > 0
          && !found_assigned_task
          && ((EnvironmentEdgeManager.currentTime() - lastTaskCreateTime) > unassignedTimeout)) {
        for (Map.Entry<String, Task> e : tasks.entrySet()) {
          String key = e.getKey();
          Task task = e.getValue();
          // we have to do task.isUnassigned() check again because tasks might
          // have been asynchronously assigned. There is no locking required
          // for these checks ... it is OK even if tryGetDataSetWatch() is
          // called unnecessarily for a taskpath
          if (task.isUnassigned() && (task.status != FAILURE)) {
            // We just touch the znode to make sure its still there
            ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
                .getSplitLogManagerCoordination().checkTaskStillAvailable(key);
          }
        }
        ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
            .getSplitLogManagerCoordination().checkTasks();
        SplitLogCounters.tot_mgr_resubmit_unassigned.incrementAndGet();
        LOG.debug("resubmitting unassigned task(s) after timeout");
      }
      Set<String> failedDeletions =
          ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
              .getSplitLogManagerCoordination().getDetails().getFailedDeletions();
      // Retry previously failed deletes
      if (failedDeletions.size() > 0) {
        List<String> tmpPaths = new ArrayList<String>(failedDeletions);
        for (String tmpPath : tmpPaths) {
          // deleteNode is an async call
          ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
              .getSplitLogManagerCoordination().deleteTask(tmpPath);
        }
        failedDeletions.removeAll(tmpPaths);
      }

      // Garbage collect left-over
      long timeInterval =
          EnvironmentEdgeManager.currentTime()
              - ((BaseCoordinatedStateManager) server.getCoordinatedStateManager())
                  .getSplitLogManagerCoordination().getLastRecoveryTime();
      if (!failedRecoveringRegionDeletions.isEmpty()
          || (tot == 0 && tasks.size() == 0 && (timeInterval > checkRecoveringTimeThreshold))) {
        // inside the function there have more checks before GC anything
        if (!failedRecoveringRegionDeletions.isEmpty()) {
          List<Pair<Set<ServerName>, Boolean>> previouslyFailedDeletions =
              new ArrayList<Pair<Set<ServerName>, Boolean>>(failedRecoveringRegionDeletions);
          failedRecoveringRegionDeletions.removeAll(previouslyFailedDeletions);
          for (Pair<Set<ServerName>, Boolean> failedDeletion : previouslyFailedDeletions) {
            removeRecoveringRegions(failedDeletion.getFirst(), failedDeletion.getSecond());
          }
        } else {
          removeRecoveringRegions(null, null);
        }
      }
    }
  }

  public enum ResubmitDirective {
    CHECK(), FORCE();
  }

  public enum TerminationStatus {
    IN_PROGRESS("in_progress"), SUCCESS("success"), FAILURE("failure"), DELETED("deleted");

    String statusMsg;

    TerminationStatus(String msg) {
      statusMsg = msg;
    }

    @Override
    public String toString() {
      return statusMsg;
    }
  }
}
