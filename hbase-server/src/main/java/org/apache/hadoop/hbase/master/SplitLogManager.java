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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.master.SplitLogManager.TaskFinisher.Status;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.google.common.annotations.VisibleForTesting;

/**
 * Distributes the task of log splitting to the available region servers.
 * Coordination happens via zookeeper. For every log file that has to be split a
 * znode is created under <code>/hbase/splitlog</code>. SplitLogWorkers race to grab a task.
 *
 * <p>SplitLogManager monitors the task znodes that it creates using the
 * timeoutMonitor thread. If a task's progress is slow then
 * {@link #resubmit(String, Task, ResubmitDirective)} will take away the task from the owner
 * {@link SplitLogWorker} and the task will be up for grabs again. When the task is done then the
 * task's znode is deleted by SplitLogManager.
 *
 * <p>Clients call {@link #splitLogDistributed(Path)} to split a region server's
 * log files. The caller thread waits in this method until all the log files
 * have been split.
 *
 * <p>All the zookeeper calls made by this class are asynchronous. This is mainly
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
public class SplitLogManager extends ZooKeeperListener {
  private static final Log LOG = LogFactory.getLog(SplitLogManager.class);

  public static final int DEFAULT_TIMEOUT = 120000;
  public static final int DEFAULT_ZK_RETRIES = 3;
  public static final int DEFAULT_MAX_RESUBMIT = 3;
  public static final int DEFAULT_UNASSIGNED_TIMEOUT = (3 * 60 * 1000); //3 min

  private final Stoppable stopper;
  private final MasterServices master;
  private final ServerName serverName;
  private final TaskFinisher taskFinisher;
  private FileSystem fs;
  private Configuration conf;

  private long zkretries;
  private long resubmit_threshold;
  private long timeout;
  private long unassignedTimeout;
  private long lastTaskCreateTime = Long.MAX_VALUE;
  public boolean ignoreZKDeleteForTesting = false;
  private volatile long lastRecoveringNodeCreationTime = 0;
  // When lastRecoveringNodeCreationTime is older than the following threshold, we'll check
  // whether to GC stale recovering znodes
  private long checkRecoveringTimeThreshold = 15000; // 15 seconds
  private final List<Pair<Set<ServerName>, Boolean>> failedRecoveringRegionDeletions = Collections
      .synchronizedList(new ArrayList<Pair<Set<ServerName>, Boolean>>());

  /**
   * In distributedLogReplay mode, we need touch both splitlog and recovering-regions znodes in one
   * operation. So the lock is used to guard such cases.
   */
  protected final ReentrantLock recoveringRegionLock = new ReentrantLock();

  private volatile RecoveryMode recoveryMode;
  private volatile boolean isDrainingDone = false;

  private final ConcurrentMap<String, Task> tasks = new ConcurrentHashMap<String, Task>();
  private TimeoutMonitor timeoutMonitor;

  private volatile Set<ServerName> deadWorkers = null;
  private final Object deadWorkersLock = new Object();

  private Set<String> failedDeletions = null;

  /**
   * Wrapper around {@link #SplitLogManager(ZooKeeperWatcher zkw, Configuration conf,
   *   Stoppable stopper, MasterServices master, ServerName serverName,
   *   boolean masterRecovery, TaskFinisher tf)}
   * that provides a task finisher for copying recovered edits to their final destination.
   * The task finisher has to be robust because it can be arbitrarily restarted or called
   * multiple times.
   *
   * @param zkw the ZK watcher
   * @param conf the HBase configuration
   * @param stopper the stoppable in case anything is wrong
   * @param master the master services
   * @param serverName the master server name
   * @param masterRecovery an indication if the master is in recovery
   * @throws KeeperException
   * @throws InterruptedIOException
   */
  public SplitLogManager(ZooKeeperWatcher zkw, final Configuration conf,
      Stoppable stopper, MasterServices master, ServerName serverName, boolean masterRecovery)
      throws InterruptedIOException, KeeperException {
    this(zkw, conf, stopper, master, serverName, masterRecovery, new TaskFinisher() {
      @Override
      public Status finish(ServerName workerName, String logfile) {
        try {
          HLogSplitter.finishSplitLogFile(logfile, conf);
        } catch (IOException e) {
          LOG.warn("Could not finish splitting of log file " + logfile, e);
          return Status.ERR;
        }
        return Status.DONE;
      }
    });
  }

  /**
   * Its OK to construct this object even when region-servers are not online. It
   * does lookup the orphan tasks in zk but it doesn't block waiting for them
   * to be done.
   *
   * @param zkw the ZK watcher
   * @param conf the HBase configuration
   * @param stopper the stoppable in case anything is wrong
   * @param master the master services
   * @param serverName the master server name
   * @param masterRecovery an indication if the master is in recovery
   * @param tf task finisher
   * @throws KeeperException
   * @throws InterruptedIOException
   */
  public SplitLogManager(ZooKeeperWatcher zkw, Configuration conf,
        Stoppable stopper, MasterServices master,
        ServerName serverName, boolean masterRecovery, TaskFinisher tf)
      throws InterruptedIOException, KeeperException {
    super(zkw);
    this.taskFinisher = tf;
    this.conf = conf;
    this.stopper = stopper;
    this.master = master;
    this.zkretries = conf.getLong("hbase.splitlog.zk.retries", DEFAULT_ZK_RETRIES);
    this.resubmit_threshold = conf.getLong("hbase.splitlog.max.resubmit", DEFAULT_MAX_RESUBMIT);
    this.timeout = conf.getInt("hbase.splitlog.manager.timeout", DEFAULT_TIMEOUT);
    this.unassignedTimeout =
      conf.getInt("hbase.splitlog.manager.unassigned.timeout", DEFAULT_UNASSIGNED_TIMEOUT);

    // Determine recovery mode
    setRecoveryMode(true);

    LOG.info("Timeout=" + timeout + ", unassigned timeout=" + unassignedTimeout +
      ", distributedLogReplay=" + (this.recoveryMode == RecoveryMode.LOG_REPLAY));

    this.serverName = serverName;
    this.timeoutMonitor = new TimeoutMonitor(
      conf.getInt("hbase.splitlog.manager.timeoutmonitor.period", 1000), stopper);

    this.failedDeletions = Collections.synchronizedSet(new HashSet<String>());

    if (!masterRecovery) {
      Threads.setDaemonThreadRunning(timeoutMonitor.getThread(), serverName
          + ".splitLogManagerTimeoutMonitor");
    }
    // Watcher can be null during tests with Mock'd servers.
    if (this.watcher != null) {
      this.watcher.registerListener(this);
      lookForOrphans();
    }
  }

  private FileStatus[] getFileList(List<Path> logDirs, PathFilter filter) throws IOException {
    List<FileStatus> fileStatus = new ArrayList<FileStatus>();
    for (Path hLogDir : logDirs) {
      this.fs = hLogDir.getFileSystem(conf);
      if (!fs.exists(hLogDir)) {
        LOG.warn(hLogDir + " doesn't exist. Nothing to do!");
        continue;
      }
      FileStatus[] logfiles = FSUtils.listStatus(fs, hLogDir, filter);
      if (logfiles == null || logfiles.length == 0) {
        LOG.info(hLogDir + " is empty dir, no logs to split");
      } else {
        Collections.addAll(fileStatus, logfiles);
      }
    }
    FileStatus[] a = new FileStatus[fileStatus.size()];
    return fileStatus.toArray(a);
  }

  /**
   * @param logDir
   *            one region sever hlog dir path in .logs
   * @throws IOException
   *             if there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   * @throws IOException
   */
  public long splitLogDistributed(final Path logDir) throws IOException {
    List<Path> logDirs = new ArrayList<Path>();
    logDirs.add(logDir);
    return splitLogDistributed(logDirs);
  }

  /**
   * The caller will block until all the log files of the given region server
   * have been processed - successfully split or an error is encountered - by an
   * available worker region server. This method must only be called after the
   * region servers have been brought online.
   *
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
        ServerName serverName = HLogUtil.getServerNameFromHLogDirectoryName(logDir);
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
   * The caller will block until all the hbase:meta log files of the given region server
   * have been processed - successfully split or an error is encountered - by an
   * available worker region server. This method must only be called after the
   * region servers have been brought online.
   *
   * @param logDirs List of log dirs to split
   * @param filter the Path filter to select specific files for considering
   * @throws IOException If there was an error while splitting any log file
   * @return cumulative size of the logfiles split
   */
  public long splitLogDistributed(final Set<ServerName> serverNames, final List<Path> logDirs,
      PathFilter filter) throws IOException {
    MonitoredTask status = TaskMonitor.get().createStatus(
          "Doing distributed log split in " + logDirs);
    FileStatus[] logfiles = getFileList(logDirs, filter);
    status.setStatus("Checking directory contents...");
    LOG.debug("Scheduling batch of logs to split");
    SplitLogCounters.tot_mgr_log_split_batch_start.incrementAndGet();
    LOG.info("started splitting " + logfiles.length + " logs in " + logDirs);
    long t = EnvironmentEdgeManager.currentTimeMillis();
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
    // remove recovering regions from ZK
    if (filter == MasterFileSystem.META_FILTER /* reference comparison */) {
      // we split meta regions and user regions separately therefore logfiles are either all for
      // meta or user regions but won't for both( we could have mixed situations in tests)
      isMetaRecovery = true;
    }
    this.removeRecoveringRegionsFromZK(serverNames, isMetaRecovery);

    if (batch.done != batch.installed) {
      batch.isDead = true;
      SplitLogCounters.tot_mgr_log_split_batch_err.incrementAndGet();
      LOG.warn("error while splitting logs in " + logDirs +
      " installed = " + batch.installed + " but only " + batch.done + " done");
      String msg = "error or interrupted while splitting logs in "
        + logDirs + " Task = " + batch;
      status.abort(msg);
      throw new IOException(msg);
    }
    for(Path logDir: logDirs){
      status.setStatus("Cleaning up log directory...");
      try {
        if (fs.exists(logDir) && !fs.delete(logDir, false)) {
          LOG.warn("Unable to delete log src dir. Ignoring. " + logDir);
        }
      } catch (IOException ioe) {
        FileStatus[] files = fs.listStatus(logDir);
        if (files != null && files.length > 0) {
          LOG.warn("returning success without actually splitting and " +
              "deleting all the log files in path " + logDir);
        } else {
          LOG.warn("Unable to delete log src dir. Ignoring. " + logDir, ioe);
        }
      }
      SplitLogCounters.tot_mgr_log_split_batch_success.incrementAndGet();
    }
    String msg = "finished splitting (more than or equal to) " + totalSize +
        " bytes in " + batch.installed + " log files in " + logDirs + " in " +
        (EnvironmentEdgeManager.currentTimeMillis() - t) + "ms";
    status.markComplete(msg);
    LOG.info(msg);
    return totalSize;
  }

  /**
   * Add a task entry to splitlog znode if it is not already there.
   *
   * @param taskname the path of the log to be split
   * @param batch the batch this task belongs to
   * @return true if a new entry is created, false if it is already there.
   */
  boolean enqueueSplitTask(String taskname, TaskBatch batch) {
    SplitLogCounters.tot_mgr_log_split_start.incrementAndGet();
    // This is a znode path under the splitlog dir with the rest of the path made up of an
    // url encoding of the passed in log to split.
    String path = ZKSplitLog.getEncodedNodeName(watcher, taskname);
    lastTaskCreateTime = EnvironmentEdgeManager.currentTimeMillis();
    Task oldtask = createTaskIfAbsent(path, batch);
    if (oldtask == null) {
      // publish the task in zk
      createNode(path, zkretries);
      return true;
    }
    return false;
  }

  private void waitForSplittingCompletion(TaskBatch batch, MonitoredTask status) {
    synchronized (batch) {
      while ((batch.done + batch.error) != batch.installed) {
        try {
          status.setStatus("Waiting for distributed tasks to finish. "
              + " scheduled=" + batch.installed
              + " done=" + batch.done
              + " error=" + batch.error);
          int remaining = batch.installed - (batch.done + batch.error);
          int actual = activeTasks(batch);
          if (remaining != actual) {
            LOG.warn("Expected " + remaining
              + " active tasks, but actually there are " + actual);
          }
          int remainingInZK = remainingTasksInZK();
          if (remainingInZK >= 0 && actual > remainingInZK) {
            LOG.warn("Expected at least" + actual
              + " tasks in ZK, but actually there are " + remainingInZK);
          }
          if (remainingInZK == 0 || actual == 0) {
            LOG.warn("No more task remaining (ZK or task map), splitting "
              + "should have completed. Remaining tasks in ZK " + remainingInZK
              + ", active tasks in map " + actual);
            if (remainingInZK == 0 && actual == 0) {
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
    for (Task t: tasks.values()) {
      if (t.batch == batch && t.status == TerminationStatus.IN_PROGRESS) {
        count++;
      }
    }
    return count;
  }

  private int remainingTasksInZK() {
    int count = 0;
    try {
      List<String> tasks =
        ZKUtil.listChildrenNoWatch(watcher, watcher.splitLogZNode);
      if (tasks != null) {
        for (String t: tasks) {
          if (!ZKSplitLog.isRescanNode(watcher, t)) {
            count++;
          }
        }
      }
    } catch (KeeperException ke) {
      LOG.warn("Failed to check remaining tasks", ke);
      count = -1;
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
  private void
      removeRecoveringRegionsFromZK(final Set<ServerName> serverNames, Boolean isMetaRecovery) {
    if (this.recoveryMode != RecoveryMode.LOG_REPLAY) {
      // the function is only used in WALEdit direct replay mode
      return;
    }

    final String metaEncodeRegionName = HRegionInfo.FIRST_META_REGIONINFO.getEncodedName();
    int count = 0;
    Set<String> recoveredServerNameSet = new HashSet<String>();
    if (serverNames != null) {
      for (ServerName tmpServerName : serverNames) {
        recoveredServerNameSet.add(tmpServerName.getServerName());
      }
    }

    try {
      this.recoveringRegionLock.lock();

      List<String> tasks = ZKUtil.listChildrenNoWatch(watcher, watcher.splitLogZNode);
      if (tasks != null) {
        for (String t : tasks) {
          if (!ZKSplitLog.isRescanNode(watcher, t)) {
            count++;
          }
        }
      }
      if (count == 0 && this.master.isInitialized()
          && !this.master.getServerManager().areDeadServersInProgress()) {
        // no splitting work items left
        deleteRecoveringRegionZNodes(watcher, null);
        // reset lastRecoveringNodeCreationTime because we cleared all recovering znodes at
        // this point.
        lastRecoveringNodeCreationTime = Long.MAX_VALUE;
      } else if (!recoveredServerNameSet.isEmpty()) {
        // remove recovering regions which doesn't have any RS associated with it
        List<String> regions = ZKUtil.listChildrenNoWatch(watcher, watcher.recoveringRegionsZNode);
        if (regions != null) {
          for (String region : regions) {
            if(isMetaRecovery != null) {
              if ((isMetaRecovery && !region.equalsIgnoreCase(metaEncodeRegionName))
                  || (!isMetaRecovery && region.equalsIgnoreCase(metaEncodeRegionName))) {
                // skip non-meta regions when recovering the meta region or
                // skip the meta region when recovering user regions
                continue;
              }
            }
            String nodePath = ZKUtil.joinZNode(watcher.recoveringRegionsZNode, region);
            List<String> failedServers = ZKUtil.listChildrenNoWatch(watcher, nodePath);
            if (failedServers == null || failedServers.isEmpty()) {
              ZKUtil.deleteNode(watcher, nodePath);
              continue;
            }
            if (recoveredServerNameSet.containsAll(failedServers)) {
              ZKUtil.deleteNodeRecursively(watcher, nodePath);
            } else {
              for (String failedServer : failedServers) {
                if (recoveredServerNameSet.contains(failedServer)) {
                  String tmpPath = ZKUtil.joinZNode(nodePath, failedServer);
                  ZKUtil.deleteNode(watcher, tmpPath);
                }
              }
            }
          }
        }
      }
    } catch (KeeperException ke) {
      LOG.warn("removeRecoveringRegionsFromZK got zookeeper exception. Will retry", ke);
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
   * @throws KeeperException
   */
  void removeStaleRecoveringRegionsFromZK(final Set<ServerName> failedServers)
      throws KeeperException {

    Set<String> knownFailedServers = new HashSet<String>();
    if (failedServers != null) {
      for (ServerName tmpServerName : failedServers) {
        knownFailedServers.add(tmpServerName.getServerName());
      }
    }

    this.recoveringRegionLock.lock();
    try {
      List<String> tasks = ZKUtil.listChildrenNoWatch(watcher, watcher.splitLogZNode);
      if (tasks != null) {
        for (String t : tasks) {
          byte[] data = ZKUtil.getData(this.watcher, ZKUtil.joinZNode(watcher.splitLogZNode, t));
          if (data != null) {
            SplitLogTask slt = null;
            try {
              slt = SplitLogTask.parseFrom(data);
            } catch (DeserializationException e) {
              LOG.warn("Failed parse data for znode " + t, e);
            }
            if (slt != null && slt.isDone()) {
              continue;
            }
          }
          // decode the file name
          t = ZKSplitLog.getFileName(t);
          ServerName serverName = HLogUtil.getServerNameFromHLogDirectoryName(new Path(t));
          if (serverName != null) {
            knownFailedServers.add(serverName.getServerName());
          } else {
            LOG.warn("Found invalid WAL log file name:" + t);
          }
        }
      }

      // remove recovering regions which doesn't have any RS associated with it
      List<String> regions = ZKUtil.listChildrenNoWatch(watcher, watcher.recoveringRegionsZNode);
      if (regions != null) {
        for (String region : regions) {
          String nodePath = ZKUtil.joinZNode(watcher.recoveringRegionsZNode, region);
          List<String> regionFailedServers = ZKUtil.listChildrenNoWatch(watcher, nodePath);
          if (regionFailedServers == null || regionFailedServers.isEmpty()) {
            ZKUtil.deleteNode(watcher, nodePath);
            continue;
          }
          boolean needMoreRecovery = false;
          for (String tmpFailedServer : regionFailedServers) {
            if (knownFailedServers.contains(tmpFailedServer)) {
              needMoreRecovery = true;
              break;
            }
          }
          if (!needMoreRecovery) {
            ZKUtil.deleteNodeRecursively(watcher, nodePath);
          }
        }
      }
    } finally {
      this.recoveringRegionLock.unlock();
    }
  }

  public static void deleteRecoveringRegionZNodes(ZooKeeperWatcher watcher, List<String> regions) {
    try {
      if (regions == null) {
        // remove all children under /home/recovering-regions
        LOG.info("Garbage collecting all recovering regions.");
        ZKUtil.deleteChildrenRecursively(watcher, watcher.recoveringRegionsZNode);
      } else {
        for (String curRegion : regions) {
          String nodePath = ZKUtil.joinZNode(watcher.recoveringRegionsZNode, curRegion);
          ZKUtil.deleteNodeRecursively(watcher, nodePath);
        }
      }
    } catch (KeeperException e) {
      LOG.warn("Cannot remove recovering regions from ZooKeeper", e);
    }
  }

  private void setDone(String path, TerminationStatus status) {
    Task task = tasks.get(path);
    if (task == null) {
      if (!ZKSplitLog.isRescanNode(watcher, path)) {
        SplitLogCounters.tot_mgr_unacquired_orphan_done.incrementAndGet();
        LOG.debug("unacquired orphan task is done " + path);
      }
    } else {
      synchronized (task) {
        if (task.status == IN_PROGRESS) {
          if (status == SUCCESS) {
            SplitLogCounters.tot_mgr_log_split_success.incrementAndGet();
            LOG.info("Done splitting " + path);
          } else {
            SplitLogCounters.tot_mgr_log_split_err.incrementAndGet();
            LOG.warn("Error splitting " + path);
          }
          task.status = status;
          if (task.batch != null) {
            synchronized (task.batch) {
              if (status == SUCCESS) {
                task.batch.done++;
              } else {
                task.batch.error++;
              }
              task.batch.notify();
            }
          }
        }
      }
    }
    // delete the task node in zk. It's an async
    // call and no one is blocked waiting for this node to be deleted. All
    // task names are unique (log.<timestamp>) there is no risk of deleting
    // a future task.
    // if a deletion fails, TimeoutMonitor will retry the same deletion later
    deleteNode(path, zkretries);
    return;
  }

  private void createNode(String path, Long retry_count) {
    SplitLogTask slt = new SplitLogTask.Unassigned(serverName, this.recoveryMode);
    ZKUtil.asyncCreate(this.watcher, path, slt.toByteArray(), new CreateAsyncCallback(), retry_count);
    SplitLogCounters.tot_mgr_node_create_queued.incrementAndGet();
    return;
  }

  private void createNodeSuccess(String path) {
    LOG.debug("put up splitlog task at znode " + path);
    getDataSetWatch(path, zkretries);
  }

  private void createNodeFailure(String path) {
    // TODO the Manager should split the log locally instead of giving up
    LOG.warn("failed to create task node" + path);
    setDone(path, FAILURE);
  }


  private void getDataSetWatch(String path, Long retry_count) {
    this.watcher.getRecoverableZooKeeper().getZooKeeper().
        getData(path, this.watcher,
        new GetDataAsyncCallback(true), retry_count);
    SplitLogCounters.tot_mgr_get_data_queued.incrementAndGet();
  }

  private void tryGetDataSetWatch(String path) {
    // A negative retry count will lead to ignoring all error processing.
    this.watcher.getRecoverableZooKeeper().getZooKeeper().
        getData(path, this.watcher,
        new GetDataAsyncCallback(false), Long.valueOf(-1) /* retry count */);
    SplitLogCounters.tot_mgr_get_data_queued.incrementAndGet();
  }

  private void getDataSetWatchSuccess(String path, byte[] data, int version)
  throws DeserializationException {
    if (data == null) {
      if (version == Integer.MIN_VALUE) {
        // assume all done. The task znode suddenly disappeared.
        setDone(path, SUCCESS);
        return;
      }
      SplitLogCounters.tot_mgr_null_data.incrementAndGet();
      LOG.fatal("logic error - got null data " + path);
      setDone(path, FAILURE);
      return;
    }
    data = this.watcher.getRecoverableZooKeeper().removeMetaData(data);
    SplitLogTask slt = SplitLogTask.parseFrom(data);
    if (slt.isUnassigned()) {
      LOG.debug("task not yet acquired " + path + " ver = " + version);
      handleUnassignedTask(path);
    } else if (slt.isOwned()) {
      heartbeat(path, version, slt.getServerName());
    } else if (slt.isResigned()) {
      LOG.info("task " + path + " entered state: " + slt.toString());
      resubmitOrFail(path, FORCE);
    } else if (slt.isDone()) {
      LOG.info("task " + path + " entered state: " + slt.toString());
      if (taskFinisher != null && !ZKSplitLog.isRescanNode(watcher, path)) {
        if (taskFinisher.finish(slt.getServerName(), ZKSplitLog.getFileName(path)) == Status.DONE) {
          setDone(path, SUCCESS);
        } else {
          resubmitOrFail(path, CHECK);
        }
      } else {
        setDone(path, SUCCESS);
      }
    } else if (slt.isErr()) {
      LOG.info("task " + path + " entered state: " + slt.toString());
      resubmitOrFail(path, CHECK);
    } else {
      LOG.fatal("logic error - unexpected zk state for path = " + path + " data = " + slt.toString());
      setDone(path, FAILURE);
    }
  }

  private void getDataSetWatchFailure(String path) {
    LOG.warn("failed to set data watch " + path);
    setDone(path, FAILURE);
  }

  /**
   * It is possible for a task to stay in UNASSIGNED state indefinitely - say
   * SplitLogManager wants to resubmit a task. It forces the task to UNASSIGNED
   * state but it dies before it could create the RESCAN task node to signal
   * the SplitLogWorkers to pick up the task. To prevent this scenario the
   * SplitLogManager resubmits all orphan and UNASSIGNED tasks at startup.
   *
   * @param path
   */
  private void handleUnassignedTask(String path) {
    if (ZKSplitLog.isRescanNode(watcher, path)) {
      return;
    }
    Task task = findOrCreateOrphanTask(path);
    if (task.isOrphan() && (task.incarnation == 0)) {
      LOG.info("resubmitting unassigned orphan task " + path);
      // ignore failure to resubmit. The timeout-monitor will handle it later
      // albeit in a more crude fashion
      resubmit(path, task, FORCE);
    }
  }

  /**
   * Helper function to check whether to abandon retries in ZooKeeper AsyncCallback functions
   * @param statusCode integer value of a ZooKeeper exception code
   * @param action description message about the retried action
   * @return true when need to abandon retries otherwise false
   */
  private boolean needAbandonRetries(int statusCode, String action) {
    if (statusCode == KeeperException.Code.SESSIONEXPIRED.intValue()) {
      LOG.error("ZK session expired. Master is expected to shut down. Abandoning retries for "
          + "action=" + action);
      return true;
    }
    return false;
  }

  private void heartbeat(String path, int new_version, ServerName workerName) {
    Task task = findOrCreateOrphanTask(path);
    if (new_version != task.last_version) {
      if (task.isUnassigned()) {
        LOG.info("task " + path + " acquired by " + workerName);
      }
      task.heartbeat(EnvironmentEdgeManager.currentTimeMillis(), new_version, workerName);
      SplitLogCounters.tot_mgr_heartbeat.incrementAndGet();
    } else {
      // duplicate heartbeats - heartbeats w/o zk node version
      // changing - are possible. The timeout thread does
      // getDataSetWatch() just to check whether a node still
      // exists or not
    }
    return;
  }

  private boolean resubmit(String path, Task task, ResubmitDirective directive) {
    // its ok if this thread misses the update to task.deleted. It will fail later
    if (task.status != IN_PROGRESS) {
      return false;
    }
    int version;
    if (directive != FORCE) {
      // We're going to resubmit:
      //  1) immediately if the worker server is now marked as dead
      //  2) after a configurable timeout if the server is not marked as dead but has still not
      //       finished the task. This allows to continue if the worker cannot actually handle it,
      //       for any reason.
      final long time = EnvironmentEdgeManager.currentTimeMillis() - task.last_update;
      final boolean alive = master.getServerManager() != null ?
          master.getServerManager().isServerOnline(task.cur_worker_name) : true;
      if (alive && time < timeout) {
        LOG.trace("Skipping the resubmit of " + task.toString() + "  because the server " +
            task.cur_worker_name + " is not marked as dead, we waited for " + time +
            " while the timeout is " + timeout);
        return false;
      }
      if (task.unforcedResubmits.get() >= resubmit_threshold) {
        if (!task.resubmitThresholdReached) {
          task.resubmitThresholdReached = true;
          SplitLogCounters.tot_mgr_resubmit_threshold_reached.incrementAndGet();
          LOG.info("Skipping resubmissions of task " + path +
              " because threshold " + resubmit_threshold + " reached");
        }
        return false;
      }
      // race with heartbeat() that might be changing last_version
      version = task.last_version;
    } else {
      SplitLogCounters.tot_mgr_resubmit_force.incrementAndGet();
      version = -1;
    }
    LOG.info("resubmitting task " + path);
    task.incarnation++;
    try {
      // blocking zk call but this is done from the timeout thread
      SplitLogTask slt = new SplitLogTask.Unassigned(this.serverName, this.recoveryMode);
      if (ZKUtil.setData(this.watcher, path, slt.toByteArray(), version) == false) {
        LOG.debug("failed to resubmit task " + path +
            " version changed");
        task.heartbeatNoDetails(EnvironmentEdgeManager.currentTimeMillis());
        return false;
      }
    } catch (NoNodeException e) {
      LOG.warn("failed to resubmit because znode doesn't exist " + path +
          " task done (or forced done by removing the znode)");
      try {
        getDataSetWatchSuccess(path, null, Integer.MIN_VALUE);
      } catch (DeserializationException e1) {
        LOG.debug("Failed to re-resubmit task " + path + " because of deserialization issue", e1);
        task.heartbeatNoDetails(EnvironmentEdgeManager.currentTimeMillis());
        return false;
      }
      return false;
    } catch (KeeperException.BadVersionException e) {
      LOG.debug("failed to resubmit task " + path + " version changed");
      task.heartbeatNoDetails(EnvironmentEdgeManager.currentTimeMillis());
      return false;
    } catch (KeeperException e) {
      SplitLogCounters.tot_mgr_resubmit_failed.incrementAndGet();
      LOG.warn("failed to resubmit " + path, e);
      return false;
    }
    // don't count forced resubmits
    if (directive != FORCE) {
      task.unforcedResubmits.incrementAndGet();
    }
    task.setUnassigned();
    createRescanNode(Long.MAX_VALUE);
    SplitLogCounters.tot_mgr_resubmit.incrementAndGet();
    return true;
  }

  private void resubmitOrFail(String path, ResubmitDirective directive) {
    if (resubmit(path, findOrCreateOrphanTask(path), directive) == false) {
      setDone(path, FAILURE);
    }
  }

  private void deleteNode(String path, Long retries) {
    SplitLogCounters.tot_mgr_node_delete_queued.incrementAndGet();
    // Once a task znode is ready for delete, that is it is in the TASK_DONE
    // state, then no one should be writing to it anymore. That is no one
    // will be updating the znode version any more.
    this.watcher.getRecoverableZooKeeper().getZooKeeper().
      delete(path, -1, new DeleteAsyncCallback(),
        retries);
  }

  private void deleteNodeSuccess(String path) {
    if (ignoreZKDeleteForTesting) {
      return;
    }
    Task task;
    task = tasks.remove(path);
    if (task == null) {
      if (ZKSplitLog.isRescanNode(watcher, path)) {
        SplitLogCounters.tot_mgr_rescan_deleted.incrementAndGet();
      }
      SplitLogCounters.tot_mgr_missing_state_in_delete.incrementAndGet();
      LOG.debug("deleted task without in memory state " + path);
      return;
    }
    synchronized (task) {
      task.status = DELETED;
      task.notify();
    }
    SplitLogCounters.tot_mgr_task_deleted.incrementAndGet();
  }

  private void deleteNodeFailure(String path) {
    LOG.info("Failed to delete node " + path + " and will retry soon.");
    return;
  }

  /**
   * signal the workers that a task was resubmitted by creating the
   * RESCAN node.
   * @throws KeeperException
   */
  private void createRescanNode(long retries) {
    // The RESCAN node will be deleted almost immediately by the
    // SplitLogManager as soon as it is created because it is being
    // created in the DONE state. This behavior prevents a buildup
    // of RESCAN nodes. But there is also a chance that a SplitLogWorker
    // might miss the watch-trigger that creation of RESCAN node provides.
    // Since the TimeoutMonitor will keep resubmitting UNASSIGNED tasks
    // therefore this behavior is safe.
    lastTaskCreateTime = EnvironmentEdgeManager.currentTimeMillis();
    SplitLogTask slt = new SplitLogTask.Done(this.serverName, this.recoveryMode);
    this.watcher.getRecoverableZooKeeper().getZooKeeper().
      create(ZKSplitLog.getRescanNode(watcher), slt.toByteArray(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,
        new CreateRescanAsyncCallback(), Long.valueOf(retries));
  }

  private void createRescanSuccess(String path) {
    SplitLogCounters.tot_mgr_rescan.incrementAndGet();
    getDataSetWatch(path, zkretries);
  }

  private void createRescanFailure() {
    LOG.fatal("logic failure, rescan failure must not happen");
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
      return  null;
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
          LOG.warn("Failure because previously failed task" +
              " state still present. Waiting for znode delete callback" +
              " path=" + path);
          return oldtask;
        }
        // reinsert the newTask and it must succeed this time
        Task t = tasks.putIfAbsent(path, newtask);
        if (t == null) {
          batch.installed++;
          return  null;
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

  @Override
  public void nodeDataChanged(String path) {
    Task task;
    task = tasks.get(path);
    if (task != null || ZKSplitLog.isRescanNode(watcher, path)) {
      if (task != null) {
        task.heartbeatNoDetails(EnvironmentEdgeManager.currentTimeMillis());
      }
      getDataSetWatch(path, zkretries);
    }
  }

  public void stop() {
    if (timeoutMonitor != null) {
      timeoutMonitor.interrupt();
    }
  }

  private void lookForOrphans() {
    List<String> orphans;
    try {
       orphans = ZKUtil.listChildrenNoWatch(this.watcher,
          this.watcher.splitLogZNode);
      if (orphans == null) {
        LOG.warn("could not get children of " + this.watcher.splitLogZNode);
        return;
      }
    } catch (KeeperException e) {
      LOG.warn("could not get children of " + this.watcher.splitLogZNode +
          " " + StringUtils.stringifyException(e));
      return;
    }
    int rescan_nodes = 0;
    for (String path : orphans) {
      String nodepath = ZKUtil.joinZNode(watcher.splitLogZNode, path);
      if (ZKSplitLog.isRescanNode(watcher, nodepath)) {
        rescan_nodes++;
        LOG.debug("found orphan rescan node " + path);
      } else {
        LOG.info("found orphan task " + path);
      }
      getDataSetWatch(nodepath, zkretries);
    }
    LOG.info("Found " + (orphans.size() - rescan_nodes) + " orphan tasks and " +
        rescan_nodes + " rescan nodes");
  }

  /**
   * Create znodes /hbase/recovering-regions/[region_ids...]/[failed region server names ...] for
   * all regions of the passed in region servers
   * @param serverName the name of a region server
   * @param userRegions user regiones assigned on the region server
   */
  void markRegionsRecoveringInZK(final ServerName serverName, Set<HRegionInfo> userRegions)
      throws KeeperException {
    if (userRegions == null || (this.recoveryMode != RecoveryMode.LOG_REPLAY)) {
      return;
    }

    try {
      this.recoveringRegionLock.lock();
      // mark that we're creating recovering znodes
      this.lastRecoveringNodeCreationTime = EnvironmentEdgeManager.currentTimeMillis();

      for (HRegionInfo region : userRegions) {
        String regionEncodeName = region.getEncodedName();
        long retries = this.zkretries;

        do {
          String nodePath = ZKUtil.joinZNode(watcher.recoveringRegionsZNode, regionEncodeName);
          long lastRecordedFlushedSequenceId = -1;
          try {
            long lastSequenceId = this.master.getServerManager().getLastFlushedSequenceId(
              regionEncodeName.getBytes());

            /*
             * znode layout: .../region_id[last known flushed sequence id]/failed server[last known
             * flushed sequence id for the server]
             */
            byte[] data = ZKUtil.getData(this.watcher, nodePath);
            if (data == null) {
              ZKUtil.createSetData(this.watcher, nodePath,
                ZKUtil.positionToByteArray(lastSequenceId));
            } else {
              lastRecordedFlushedSequenceId = SplitLogManager.parseLastFlushedSequenceIdFrom(data);
              if (lastRecordedFlushedSequenceId < lastSequenceId) {
                // update last flushed sequence id in the region level
                ZKUtil.setData(this.watcher, nodePath, ZKUtil.positionToByteArray(lastSequenceId));
              }
            }
            // go one level deeper with server name
            nodePath = ZKUtil.joinZNode(nodePath, serverName.getServerName());
            if (lastSequenceId <= lastRecordedFlushedSequenceId) {
              // the newly assigned RS failed even before any flush to the region
              lastSequenceId = lastRecordedFlushedSequenceId;
            }
            ZKUtil.createSetData(this.watcher, nodePath,
              ZKUtil.regionSequenceIdsToByteArray(lastSequenceId, null));
            LOG.debug("Mark region " + regionEncodeName + " recovering from failed region server "
                + serverName);

            // break retry loop
            break;
          } catch (KeeperException e) {
            // ignore ZooKeeper exceptions inside retry loop
            if (retries <= 1) {
              throw e;
            }
            // wait a little bit for retry
            try {
              Thread.sleep(20);
            } catch (Exception ignoreE) {
              // ignore
            }
          }
        } while ((--retries) > 0 && (!this.stopper.isStopped()));
      }
    } finally {
      this.recoveringRegionLock.unlock();
    }
  }

  /**
   * @param bytes - Content of a failed region server or recovering region znode.
   * @return long - The last flushed sequence Id for the region server
   */
  public static long parseLastFlushedSequenceIdFrom(final byte[] bytes) {
    long lastRecordedFlushedSequenceId = -1l;
    try {
      lastRecordedFlushedSequenceId = ZKUtil.parseHLogPositionFrom(bytes);
    } catch (DeserializationException e) {
      lastRecordedFlushedSequenceId = -1l;
      LOG.warn("Can't parse last flushed sequence Id", e);
    }
    return lastRecordedFlushedSequenceId;
  }

  /**
   * check if /hbase/recovering-regions/<current region encoded name> exists. Returns true if exists
   * and set watcher as well.
   * @param zkw
   * @param regionEncodedName region encode name
   * @return true when /hbase/recovering-regions/<current region encoded name> exists
   * @throws KeeperException
   */
  public static boolean
      isRegionMarkedRecoveringInZK(ZooKeeperWatcher zkw, String regionEncodedName)
          throws KeeperException {
    boolean result = false;
    String nodePath = ZKUtil.joinZNode(zkw.recoveringRegionsZNode, regionEncodedName);

    byte[] node = ZKUtil.getDataAndWatch(zkw, nodePath);
    if (node != null) {
      result = true;
    }
    return result;
  }

  /**
   * This function is used in distributedLogReplay to fetch last flushed sequence id from ZK
   * @param zkw
   * @param serverName
   * @param encodedRegionName
   * @return the last flushed sequence ids recorded in ZK of the region for <code>serverName<code>
   * @throws IOException
   */
  public static RegionStoreSequenceIds getRegionFlushedSequenceId(ZooKeeperWatcher zkw,
      String serverName, String encodedRegionName) throws IOException {
    // when SplitLogWorker recovers a region by directly replaying unflushed WAL edits,
    // last flushed sequence Id changes when newly assigned RS flushes writes to the region.
    // If the newly assigned RS fails again(a chained RS failures scenario), the last flushed
    // sequence Id name space (sequence Id only valid for a particular RS instance), changes
    // when different newly assigned RS flushes the region.
    // Therefore, in this mode we need to fetch last sequence Ids from ZK where we keep history of
    // last flushed sequence Id for each failed RS instance.
    RegionStoreSequenceIds result = null;
    String nodePath = ZKUtil.joinZNode(zkw.recoveringRegionsZNode, encodedRegionName);
    nodePath = ZKUtil.joinZNode(nodePath, serverName);
    try {
      byte[] data = ZKUtil.getData(zkw, nodePath);
      if (data != null) {
        result = ZKUtil.parseRegionStoreSequenceIds(data);
      }
    } catch (KeeperException e) {
      throw new IOException("Cannot get lastFlushedSequenceId from ZooKeeper for server="
          + serverName + "; region=" + encodedRegionName, e);
    } catch (DeserializationException e) {
      LOG.warn("Can't parse last flushed sequence Id from znode:" + nodePath, e);
    }
    return result;
  }

  /**
   * This function is to set recovery mode from outstanding split log tasks from before or
   * current configuration setting
   * @param isForInitialization
   * @throws KeeperException
   * @throws InterruptedIOException
   */
  public void setRecoveryMode(boolean isForInitialization) throws KeeperException {
    if(this.isDrainingDone) {
      // when there is no outstanding splitlogtask after master start up, we already have up to date
      // recovery mode
      return;
    }
    if(this.watcher == null) {
      // when watcher is null(testing code) and recovery mode can only be LOG_SPLITTING
      this.isDrainingDone = true;
      this.recoveryMode = RecoveryMode.LOG_SPLITTING;
      return;
    }
    boolean hasSplitLogTask = false;
    boolean hasRecoveringRegions = false;
    RecoveryMode previousRecoveryMode = RecoveryMode.UNKNOWN;
    RecoveryMode recoveryModeInConfig = (isDistributedLogReplay(conf)) ?
      RecoveryMode.LOG_REPLAY : RecoveryMode.LOG_SPLITTING;

    // Firstly check if there are outstanding recovering regions
    List<String> regions = ZKUtil.listChildrenNoWatch(watcher, watcher.recoveringRegionsZNode);
    if (regions != null && !regions.isEmpty()) {
      hasRecoveringRegions = true;
      previousRecoveryMode = RecoveryMode.LOG_REPLAY;
    }
    if (previousRecoveryMode == RecoveryMode.UNKNOWN) {
      // Secondly check if there are outstanding split log task
      List<String> tasks = ZKUtil.listChildrenNoWatch(watcher, watcher.splitLogZNode);
      if (tasks != null && !tasks.isEmpty()) {
        hasSplitLogTask = true;
        if (isForInitialization) {
          // during initialization, try to get recovery mode from splitlogtask
          for (String task : tasks) {
            try {
              byte[] data = ZKUtil.getData(this.watcher,
                ZKUtil.joinZNode(watcher.splitLogZNode, task));
              if (data == null) continue;
              SplitLogTask slt = SplitLogTask.parseFrom(data);
              previousRecoveryMode = slt.getMode();
              if (previousRecoveryMode == RecoveryMode.UNKNOWN) {
                // created by old code base where we don't set recovery mode in splitlogtask
                // we can safely set to LOG_SPLITTING because we're in master initialization code
                // before SSH is enabled & there is no outstanding recovering regions
                previousRecoveryMode = RecoveryMode.LOG_SPLITTING;
              }
              break;
            } catch (DeserializationException e) {
              LOG.warn("Failed parse data for znode " + task, e);
            }
          }
        }
      }
    }

    synchronized(this) {
      if(this.isDrainingDone) {
        return;
      }
      if (!hasSplitLogTask && !hasRecoveringRegions) {
        this.isDrainingDone = true;
        this.recoveryMode = recoveryModeInConfig;
        return;
      } else if (!isForInitialization) {
        // splitlogtask hasn't drained yet, keep existing recovery mode
        return;
      }

      if (previousRecoveryMode != RecoveryMode.UNKNOWN) {
        this.isDrainingDone = (previousRecoveryMode == recoveryModeInConfig);
        this.recoveryMode = previousRecoveryMode;
      } else {
        this.recoveryMode = recoveryModeInConfig;
      }
    }
  }

  public RecoveryMode getRecoveryMode() {
    return this.recoveryMode;
  }

  /**
   * Returns if distributed log replay is turned on or not
   * @param conf
   * @return true when distributed log replay is turned on
   */
  private boolean isDistributedLogReplay(Configuration conf) {
    boolean dlr = conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY,
      HConstants.DEFAULT_DISTRIBUTED_LOG_REPLAY_CONFIG);
    int version = conf.getInt(HFile.FORMAT_VERSION_KEY, HFile.MAX_FORMAT_VERSION);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Distributed log replay=" + dlr + ", " + HFile.FORMAT_VERSION_KEY + "=" + version);
    }
    // For distributed log replay, hfile version must be 3 at least; we need tag support.
    return dlr && (version >= 3);
  }

  /**
   * Keeps track of the batch of tasks submitted together by a caller in splitLogDistributed().
   * Clients threads use this object to wait for all their tasks to be done.
   * <p>
   * All access is synchronized.
   */
  static class TaskBatch {
    int installed = 0;
    int done = 0;
    int error = 0;
    volatile boolean isDead = false;

    @Override
    public String toString() {
      return ("installed = " + installed + " done = " + done + " error = " + error);
    }
  }

  /**
   * in memory state of an active task.
   */
  static class Task {
    volatile long last_update;
    volatile int last_version;
    volatile ServerName cur_worker_name;
    volatile TaskBatch batch;
    volatile TerminationStatus status;
    volatile int incarnation;
    final AtomicInteger unforcedResubmits = new AtomicInteger();
    volatile boolean resubmitThresholdReached;

    @Override
    public String toString() {
      return ("last_update = " + last_update +
          " last_version = " + last_version +
          " cur_worker_name = " + cur_worker_name +
          " status = " + status +
          " incarnation = " + incarnation +
          " resubmits = " + unforcedResubmits.get() +
          " batch = " + batch);
    }

    Task() {
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
   * Periodically checks all active tasks and resubmits the ones that have timed
   * out
   */
  private class TimeoutMonitor extends Chore {
    private long lastLog = 0;

    public TimeoutMonitor(final int period, Stoppable stopper) {
      super("SplitLogManager Timeout Monitor", period, stopper);
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
          if (resubmit(path, task, FORCE)) {
            resubmitted++;
          } else {
            handleDeadWorker(cur_worker);
            LOG.warn("Failed to resubmit task " + path + " owned by dead " +
                cur_worker + ", will retry.");
          }
        } else if (resubmit(path, task, CHECK)) {
          resubmitted++;
        }
      }
      if (tot > 0) {
        long now = EnvironmentEdgeManager.currentTimeMillis();
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
      if (tot > 0 && !found_assigned_task &&
          ((EnvironmentEdgeManager.currentTimeMillis() - lastTaskCreateTime) >
          unassignedTimeout)) {
        for (Map.Entry<String, Task> e : tasks.entrySet()) {
          String path = e.getKey();
          Task task = e.getValue();
          // we have to do task.isUnassigned() check again because tasks might
          // have been asynchronously assigned. There is no locking required
          // for these checks ... it is OK even if tryGetDataSetWatch() is
          // called unnecessarily for a task
          if (task.isUnassigned() && (task.status != FAILURE)) {
            // We just touch the znode to make sure its still there
            tryGetDataSetWatch(path);
          }
        }
        createRescanNode(Long.MAX_VALUE);
        SplitLogCounters.tot_mgr_resubmit_unassigned.incrementAndGet();
        LOG.debug("resubmitting unassigned task(s) after timeout");
      }

      // Retry previously failed deletes
      if (failedDeletions.size() > 0) {
        List<String> tmpPaths = new ArrayList<String>(failedDeletions);
        for (String tmpPath : tmpPaths) {
          // deleteNode is an async call
          deleteNode(tmpPath, zkretries);
        }
        failedDeletions.removeAll(tmpPaths);
      }

      // Garbage collect left-over /hbase/recovering-regions/... znode
      long timeInterval = EnvironmentEdgeManager.currentTimeMillis()
          - lastRecoveringNodeCreationTime;
      if (!failedRecoveringRegionDeletions.isEmpty()
          || (tot == 0 && tasks.size() == 0 && (timeInterval > checkRecoveringTimeThreshold))) {
        // inside the function there have more checks before GC anything
        if (!failedRecoveringRegionDeletions.isEmpty()) {
          List<Pair<Set<ServerName>, Boolean>> previouslyFailedDeletions =
              new ArrayList<Pair<Set<ServerName>, Boolean>>(failedRecoveringRegionDeletions);
          failedRecoveringRegionDeletions.removeAll(previouslyFailedDeletions);
          for (Pair<Set<ServerName>, Boolean> failedDeletion : previouslyFailedDeletions) {
            removeRecoveringRegionsFromZK(failedDeletion.getFirst(), failedDeletion.getSecond());
          }
        } else {
          removeRecoveringRegionsFromZK(null, null);
        }
      }
    }
  }

  /**
   * Asynchronous handler for zk create node results.
   * Retries on failures.
   */
  class CreateAsyncCallback implements AsyncCallback.StringCallback {
    private final Log LOG = LogFactory.getLog(CreateAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      SplitLogCounters.tot_mgr_node_create_result.incrementAndGet();
      if (rc != 0) {
        if (needAbandonRetries(rc, "Create znode " + path)) {
          createNodeFailure(path);
          return;
        }
        if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
          // What if there is a delete pending against this pre-existing
          // znode? Then this soon-to-be-deleted task znode must be in TASK_DONE
          // state. Only operations that will be carried out on this node by
          // this manager are get-znode-data, task-finisher and delete-znode.
          // And all code pieces correctly handle the case of suddenly
          // disappearing task-znode.
          LOG.debug("found pre-existing znode " + path);
          SplitLogCounters.tot_mgr_node_already_exists.incrementAndGet();
        } else {
          Long retry_count = (Long)ctx;
          LOG.warn("create rc =" + KeeperException.Code.get(rc) + " for " +
              path + " remaining retries=" + retry_count);
          if (retry_count == 0) {
            SplitLogCounters.tot_mgr_node_create_err.incrementAndGet();
            createNodeFailure(path);
          } else {
            SplitLogCounters.tot_mgr_node_create_retry.incrementAndGet();
            createNode(path, retry_count - 1);
          }
          return;
        }
      }
      createNodeSuccess(path);
    }
  }

  /**
   * Asynchronous handler for zk get-data-set-watch on node results.
   * Retries on failures.
   */
  class GetDataAsyncCallback implements AsyncCallback.DataCallback {
    private final Log LOG = LogFactory.getLog(GetDataAsyncCallback.class);
    private boolean completeTaskOnNoNode;

    /**
     * @param completeTaskOnNoNode Complete the task if the znode cannot be found.
     * Since in-memory task creation and znode creation are not atomic, there might be
     * a race where there is a task in memory but the znode is not created yet (TimeoutMonitor).
     * In this case completeTaskOnNoNode should be set to false. See HBASE-11217.
     */
    public GetDataAsyncCallback(boolean completeTaskOnNoNode) {
      this.completeTaskOnNoNode = completeTaskOnNoNode;
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data,
        Stat stat) {
      SplitLogCounters.tot_mgr_get_data_result.incrementAndGet();
      if (rc != 0) {
        if (needAbandonRetries(rc, "GetData from znode " + path)) {
          return;
        }
        if (rc == KeeperException.Code.NONODE.intValue()) {
          SplitLogCounters.tot_mgr_get_data_nonode.incrementAndGet();
          LOG.warn("task znode " + path + " vanished.");
          if (completeTaskOnNoNode) {
            // The task znode has been deleted. Must be some pending delete
            // that deleted the task. Assume success because a task-znode is
            // is only deleted after TaskFinisher is successful.
            try {
              getDataSetWatchSuccess(path, null, Integer.MIN_VALUE);
            } catch (DeserializationException e) {
              LOG.warn("Deserialization problem", e);
            }
          }
          return;
        }
        Long retry_count = (Long) ctx;

        if (retry_count < 0) {
          LOG.warn("getdata rc = " + KeeperException.Code.get(rc) + " " +
              path + ". Ignoring error. No error handling. No retrying.");
          return;
        }
        LOG.warn("getdata rc = " + KeeperException.Code.get(rc) + " " +
            path + " remaining retries=" + retry_count);
        if (retry_count == 0) {
          SplitLogCounters.tot_mgr_get_data_err.incrementAndGet();
          getDataSetWatchFailure(path);
        } else {
          SplitLogCounters.tot_mgr_get_data_retry.incrementAndGet();
          getDataSetWatch(path, retry_count - 1);
        }
        return;
      }
      try {
        getDataSetWatchSuccess(path, data, stat.getVersion());
      } catch (DeserializationException e) {
        LOG.warn("Deserialization problem", e);
      }
      return;
    }
  }

  /**
   * Asynchronous handler for zk delete node results.
   * Retries on failures.
   */
  class DeleteAsyncCallback implements AsyncCallback.VoidCallback {
    private final Log LOG = LogFactory.getLog(DeleteAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx) {
      SplitLogCounters.tot_mgr_node_delete_result.incrementAndGet();
      if (rc != 0) {
        if (needAbandonRetries(rc, "Delete znode " + path)) {
          failedDeletions.add(path);
          return;
        }
        if (rc != KeeperException.Code.NONODE.intValue()) {
          SplitLogCounters.tot_mgr_node_delete_err.incrementAndGet();
          Long retry_count = (Long) ctx;
          LOG.warn("delete rc=" + KeeperException.Code.get(rc) + " for " +
              path + " remaining retries=" + retry_count);
          if (retry_count == 0) {
            LOG.warn("delete failed " + path);
            failedDeletions.add(path);
            deleteNodeFailure(path);
          } else {
            deleteNode(path, retry_count - 1);
          }
          return;
        } else {
          LOG.info(path +
            " does not exist. Either was created but deleted behind our" +
            " back by another pending delete OR was deleted" +
            " in earlier retry rounds. zkretries = " + (Long) ctx);
        }
      } else {
        LOG.debug("deleted " + path);
      }
      deleteNodeSuccess(path);
    }
  }

  /**
   * Asynchronous handler for zk create RESCAN-node results.
   * Retries on failures.
   * <p>
   * A RESCAN node is created using PERSISTENT_SEQUENTIAL flag. It is a signal
   * for all the {@link SplitLogWorker}s to rescan for new tasks.
   */
  class CreateRescanAsyncCallback implements AsyncCallback.StringCallback {
    private final Log LOG = LogFactory.getLog(CreateRescanAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      if (rc != 0) {
        if (needAbandonRetries(rc, "CreateRescan znode " + path)) {
          return;
        }
        Long retry_count = (Long)ctx;
        LOG.warn("rc=" + KeeperException.Code.get(rc) + " for "+ path +
            " remaining retries=" + retry_count);
        if (retry_count == 0) {
          createRescanFailure();
        } else {
          createRescanNode(retry_count - 1);
        }
        return;
      }
      // path is the original arg, name is the actual name that was created
      createRescanSuccess(name);
    }
  }

  /**
   * {@link SplitLogManager} can use objects implementing this interface to
   * finish off a partially done task by {@link SplitLogWorker}. This provides
   * a serialization point at the end of the task processing. Must be
   * restartable and idempotent.
   */
  public interface TaskFinisher {
    /**
     * status that can be returned finish()
     */
    enum Status {
      /**
       * task completed successfully
       */
      DONE(),
      /**
       * task completed with error
       */
      ERR();
    }
    /**
     * finish the partially done task. workername provides clue to where the
     * partial results of the partially done tasks are present. taskname is the
     * name of the task that was put up in zookeeper.
     * <p>
     * @param workerName
     * @param taskname
     * @return DONE if task completed successfully, ERR otherwise
     */
    Status finish(ServerName workerName, String taskname);
  }

  enum ResubmitDirective {
    CHECK(),
    FORCE();
  }

  enum TerminationStatus {
    IN_PROGRESS("in_progress"),
    SUCCESS("success"),
    FAILURE("failure"),
    DELETED("deleted");

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
