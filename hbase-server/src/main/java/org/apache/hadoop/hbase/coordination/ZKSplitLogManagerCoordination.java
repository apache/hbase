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

package org.apache.hadoop.hbase.coordination;

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
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination.TaskFinisher.Status;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.SplitLogManager.ResubmitDirective;
import org.apache.hadoop.hbase.master.SplitLogManager.Task;
import org.apache.hadoop.hbase.master.SplitLogManager.TerminationStatus;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WALSplitter;
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

/**
 * ZooKeeper based implementation of
 * {@link SplitLogManagerCoordination}
 */
@InterfaceAudience.Private
public class ZKSplitLogManagerCoordination extends ZooKeeperListener implements
    SplitLogManagerCoordination {

  public static class ZkSplitLogManagerDetails extends SplitLogManagerDetails {

    ZkSplitLogManagerDetails(ConcurrentMap<String, Task> tasks, MasterServices master,
        Set<String> failedDeletions, ServerName serverName) {
      super(tasks, master, failedDeletions, serverName);
    }
  }

  public static final int DEFAULT_TIMEOUT = 120000;
  public static final int DEFAULT_ZK_RETRIES = 3;
  public static final int DEFAULT_MAX_RESUBMIT = 3;

  private static final Log LOG = LogFactory.getLog(SplitLogManagerCoordination.class);

  private Server server;
  private long zkretries;
  private long resubmitThreshold;
  private long timeout;
  private TaskFinisher taskFinisher;

  SplitLogManagerDetails details;

  // When lastRecoveringNodeCreationTime is older than the following threshold, we'll check
  // whether to GC stale recovering znodes
  private volatile long lastRecoveringNodeCreationTime = 0;
  private Configuration conf;
  public boolean ignoreZKDeleteForTesting = false;

  private RecoveryMode recoveryMode;

  private boolean isDrainingDone = false;

  public ZKSplitLogManagerCoordination(final CoordinatedStateManager manager,
      ZooKeeperWatcher watcher) {
    super(watcher);
    taskFinisher = new TaskFinisher() {
      @Override
      public Status finish(ServerName workerName, String logfile) {
        try {
          WALSplitter.finishSplitLogFile(logfile, manager.getServer().getConfiguration());
        } catch (IOException e) {
          LOG.warn("Could not finish splitting of log file " + logfile, e);
          return Status.ERR;
        }
        return Status.DONE;
      }
    };
    this.server = manager.getServer();
    this.conf = server.getConfiguration();
  }

  @Override
  public void init() throws IOException {
    this.zkretries = conf.getLong("hbase.splitlog.zk.retries", DEFAULT_ZK_RETRIES);
    this.resubmitThreshold = conf.getLong("hbase.splitlog.max.resubmit", DEFAULT_MAX_RESUBMIT);
    this.timeout = conf.getInt(HConstants.HBASE_SPLITLOG_MANAGER_TIMEOUT, DEFAULT_TIMEOUT);
    setRecoveryMode(true);
    if (this.watcher != null) {
      this.watcher.registerListener(this);
      lookForOrphans();
    }
  }

  @Override
  public String prepareTask(String taskname) {
    return ZKSplitLog.getEncodedNodeName(watcher, taskname);
  }

  @Override
  public int remainingTasksInCoordination() {
    int count = 0;
    try {
      List<String> tasks = ZKUtil.listChildrenNoWatch(watcher, watcher.splitLogZNode);
      if (tasks != null) {
        int listSize = tasks.size();
        for (int i = 0; i < listSize; i++) {
          if (!ZKSplitLog.isRescanNode(tasks.get(i))) {
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
   * It is possible for a task to stay in UNASSIGNED state indefinitely - say SplitLogManager wants
   * to resubmit a task. It forces the task to UNASSIGNED state but it dies before it could create
   * the RESCAN task node to signal the SplitLogWorkers to pick up the task. To prevent this
   * scenario the SplitLogManager resubmits all orphan and UNASSIGNED tasks at startup.
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
      resubmitTask(path, task, FORCE);
    }
  }

  @Override
  public void deleteTask(String path) {
    deleteNode(path, zkretries);
  }

  @Override
  public boolean resubmitTask(String path, Task task, ResubmitDirective directive) {
    // its ok if this thread misses the update to task.deleted. It will fail later
    if (task.status != IN_PROGRESS) {
      return false;
    }
    int version;
    if (directive != FORCE) {
      // We're going to resubmit:
      // 1) immediately if the worker server is now marked as dead
      // 2) after a configurable timeout if the server is not marked as dead but has still not
      // finished the task. This allows to continue if the worker cannot actually handle it,
      // for any reason.
      final long time = EnvironmentEdgeManager.currentTime() - task.last_update;
      final boolean alive =
          details.getMaster().getServerManager() != null ? details.getMaster().getServerManager()
              .isServerOnline(task.cur_worker_name) : true;
      if (alive && time < timeout) {
        LOG.trace("Skipping the resubmit of " + task.toString() + "  because the server "
            + task.cur_worker_name + " is not marked as dead, we waited for " + time
            + " while the timeout is " + timeout);
        return false;
      }

      if (task.unforcedResubmits.get() >= resubmitThreshold) {
        if (!task.resubmitThresholdReached) {
          task.resubmitThresholdReached = true;
          SplitLogCounters.tot_mgr_resubmit_threshold_reached.incrementAndGet();
          LOG.info("Skipping resubmissions of task " + path + " because threshold "
              + resubmitThreshold + " reached");
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
    boolean result = resubmit(this.details.getServerName(), path, version);
    if (!result) {
      task.heartbeatNoDetails(EnvironmentEdgeManager.currentTime());
      return false;
    }
    // don't count forced resubmits
    if (directive != FORCE) {
      task.unforcedResubmits.incrementAndGet();
    }
    task.setUnassigned();
    rescan(Long.MAX_VALUE);
    SplitLogCounters.tot_mgr_resubmit.incrementAndGet();
    return true;
  }


  @Override
  public void checkTasks() {
    rescan(Long.MAX_VALUE);
  };

  /**
   * signal the workers that a task was resubmitted by creating the RESCAN node.
   */
  private void rescan(long retries) {
    // The RESCAN node will be deleted almost immediately by the
    // SplitLogManager as soon as it is created because it is being
    // created in the DONE state. This behavior prevents a buildup
    // of RESCAN nodes. But there is also a chance that a SplitLogWorker
    // might miss the watch-trigger that creation of RESCAN node provides.
    // Since the TimeoutMonitor will keep resubmitting UNASSIGNED tasks
    // therefore this behavior is safe.
    SplitLogTask slt = new SplitLogTask.Done(this.details.getServerName(), getRecoveryMode());
    this.watcher
        .getRecoverableZooKeeper()
        .getZooKeeper()
        .create(ZKSplitLog.getRescanNode(watcher), slt.toByteArray(), Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL_SEQUENTIAL, new CreateRescanAsyncCallback(), Long.valueOf(retries));
  }

  @Override
  public void submitTask(String path) {
    createNode(path, zkretries);
  }

  @Override
  public void checkTaskStillAvailable(String path) {
    // A negative retry count will lead to ignoring all error processing.
    this.watcher
        .getRecoverableZooKeeper()
        .getZooKeeper()
        .getData(path, this.watcher, new GetDataAsyncCallback(),
          Long.valueOf(-1) /* retry count */);
    SplitLogCounters.tot_mgr_get_data_queued.incrementAndGet();
  }

  /**
   * It removes recovering regions under /hbase/recovering-regions/[encoded region name] so that the
   * region server hosting the region can allow reads to the recovered region
   * @param recoveredServerNameSet servers which are just recovered
   * @param isMetaRecovery whether current recovery is for the meta region on
   *          <code>serverNames<code>
   */
  @Override
  public void removeRecoveringRegions(final Set<String> recoveredServerNameSet,
      Boolean isMetaRecovery)
  throws IOException {
    final String metaEncodeRegionName = HRegionInfo.FIRST_META_REGIONINFO.getEncodedName();
    int count = 0;
    try {
      List<String> tasks = ZKUtil.listChildrenNoWatch(watcher, watcher.splitLogZNode);
      if (tasks != null) {
        int listSize = tasks.size();
        for (int i = 0; i < listSize; i++) {
          if (!ZKSplitLog.isRescanNode(tasks.get(i))) {
            count++;
          }
        }
      }
      if (count == 0 && this.details.getMaster().isInitialized()
          && !this.details.getMaster().getServerManager().areDeadServersInProgress()) {
        // No splitting work items left
        ZKSplitLog.deleteRecoveringRegionZNodes(watcher, null);
        // reset lastRecoveringNodeCreationTime because we cleared all recovering znodes at
        // this point.
        lastRecoveringNodeCreationTime = Long.MAX_VALUE;
      } else if (!recoveredServerNameSet.isEmpty()) {
        // Remove recovering regions which don't have any RS associated with it
        List<String> regions = ZKUtil.listChildrenNoWatch(watcher, watcher.recoveringRegionsZNode);
        if (regions != null) {
          int listSize = regions.size();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Processing recovering " + regions + " and servers "  +
                recoveredServerNameSet + ", isMetaRecovery=" + isMetaRecovery);
          }
          for (int i = 0; i < listSize; i++) {
            String region = regions.get(i);
            if (isMetaRecovery != null) {
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
              int tmpFailedServerSize = failedServers.size();
              for (int j = 0; j < tmpFailedServerSize; j++) {
                String failedServer = failedServers.get(j);
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
      throw new IOException(ke);
    }
  }

  private void deleteNode(String path, Long retries) {
    SplitLogCounters.tot_mgr_node_delete_queued.incrementAndGet();
    // Once a task znode is ready for delete, that is it is in the TASK_DONE
    // state, then no one should be writing to it anymore. That is no one
    // will be updating the znode version any more.
    this.watcher.getRecoverableZooKeeper().getZooKeeper()
        .delete(path, -1, new DeleteAsyncCallback(), retries);
  }

  private void deleteNodeSuccess(String path) {
    if (ignoreZKDeleteForTesting) {
      return;
    }
    Task task;
    task = details.getTasks().remove(path);
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

  private void createRescanSuccess(String path) {
    SplitLogCounters.tot_mgr_rescan.incrementAndGet();
    getDataSetWatch(path, zkretries);
  }

  private void createRescanFailure() {
    LOG.fatal("logic failure, rescan failure must not happen");
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

  private void createNode(String path, Long retry_count) {
    SplitLogTask slt = new SplitLogTask.Unassigned(details.getServerName(), getRecoveryMode());
    ZKUtil.asyncCreate(this.watcher, path, slt.toByteArray(), new CreateAsyncCallback(),
      retry_count);
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
    this.watcher.getRecoverableZooKeeper().getZooKeeper()
        .getData(path, this.watcher, new GetDataAsyncCallback(), retry_count);
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
      LOG.fatal("logic error - unexpected zk state for path = " + path + " data = "
          + slt.toString());
      setDone(path, FAILURE);
    }
  }

  private void resubmitOrFail(String path, ResubmitDirective directive) {
    if (resubmitTask(path, findOrCreateOrphanTask(path), directive) == false) {
      setDone(path, FAILURE);
    }
  }

  private void getDataSetWatchFailure(String path) {
    LOG.warn("failed to set data watch " + path);
    setDone(path, FAILURE);
  }

  private void setDone(String path, TerminationStatus status) {
    Task task = details.getTasks().get(path);
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

  Task findOrCreateOrphanTask(String path) {
    Task orphanTask = new Task();
    Task task;
    task = details.getTasks().putIfAbsent(path, orphanTask);
    if (task == null) {
      LOG.info("creating orphan task " + path);
      SplitLogCounters.tot_mgr_orphan_task_acquired.incrementAndGet();
      task = orphanTask;
    }
    return task;
  }

  private void heartbeat(String path, int new_version, ServerName workerName) {
    Task task = findOrCreateOrphanTask(path);
    if (new_version != task.last_version) {
      if (task.isUnassigned()) {
        LOG.info("task " + path + " acquired by " + workerName);
      }
      task.heartbeat(EnvironmentEdgeManager.currentTime(), new_version, workerName);
      SplitLogCounters.tot_mgr_heartbeat.incrementAndGet();
    } else {
      // duplicate heartbeats - heartbeats w/o zk node version
      // changing - are possible. The timeout thread does
      // getDataSetWatch() just to check whether a node still
      // exists or not
    }
    return;
  }

  private void lookForOrphans() {
    List<String> orphans;
    try {
      orphans = ZKUtil.listChildrenNoWatch(this.watcher, this.watcher.splitLogZNode);
      if (orphans == null) {
        LOG.warn("could not get children of " + this.watcher.splitLogZNode);
        return;
      }
    } catch (KeeperException e) {
      LOG.warn("could not get children of " + this.watcher.splitLogZNode + " "
          + StringUtils.stringifyException(e));
      return;
    }
    int rescan_nodes = 0;
    int listSize = orphans.size();
    for (int i = 0; i < listSize; i++) {
      String path = orphans.get(i);
      String nodepath = ZKUtil.joinZNode(watcher.splitLogZNode, path);
      if (ZKSplitLog.isRescanNode(watcher, nodepath)) {
        rescan_nodes++;
        LOG.debug("found orphan rescan node " + path);
      } else {
        LOG.info("found orphan task " + path);
      }
      getDataSetWatch(nodepath, zkretries);
    }
    LOG.info("Found " + (orphans.size() - rescan_nodes) + " orphan tasks and " + rescan_nodes
        + " rescan nodes");
  }

  /**
   * Create znodes /hbase/recovering-regions/[region_ids...]/[failed region server names ...] for
   * all regions of the passed in region servers
   * @param serverName the name of a region server
   * @param userRegions user regiones assigned on the region server
   */
  @Override
  public void markRegionsRecovering(final ServerName serverName, Set<HRegionInfo> userRegions)
      throws IOException, InterruptedIOException {
    this.lastRecoveringNodeCreationTime = EnvironmentEdgeManager.currentTime();
    for (HRegionInfo region : userRegions) {
      String regionEncodeName = region.getEncodedName();
      long retries = this.zkretries;

      do {
        String nodePath = ZKUtil.joinZNode(watcher.recoveringRegionsZNode, regionEncodeName);
        long lastRecordedFlushedSequenceId = -1;
        try {
          long lastSequenceId =
              this.details.getMaster().getServerManager()
                  .getLastFlushedSequenceId(regionEncodeName.getBytes()).getLastFlushedSequenceId();

          /*
           * znode layout: .../region_id[last known flushed sequence id]/failed server[last known
           * flushed sequence id for the server]
           */
          byte[] data = ZKUtil.getData(this.watcher, nodePath);
          if (data == null) {
            ZKUtil
                .createSetData(this.watcher, nodePath, ZKUtil.positionToByteArray(lastSequenceId));
          } else {
            lastRecordedFlushedSequenceId =
                ZKSplitLog.parseLastFlushedSequenceIdFrom(data);
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
          if (LOG.isDebugEnabled()) {
            LOG.debug("Marked " + regionEncodeName + " recovering from " + serverName +
              ": " + nodePath);
          }
          // break retry loop
          break;
        } catch (KeeperException e) {
          // ignore ZooKeeper exceptions inside retry loop
          if (retries <= 1) {
            throw new IOException(e);
          }
          // wait a little bit for retry
          try {
            Thread.sleep(20);
          } catch (InterruptedException e1) {
            throw new InterruptedIOException();
          }
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        }
      } while ((--retries) > 0);
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    Task task;
    task = details.getTasks().get(path);
    if (task != null || ZKSplitLog.isRescanNode(watcher, path)) {
      if (task != null) {
        task.heartbeatNoDetails(EnvironmentEdgeManager.currentTime());
      }
      getDataSetWatch(path, zkretries);
    }
  }

  /**
   * ZooKeeper implementation of
   * {@link org.apache.hadoop.hbase.coordination.
   * SplitLogManagerCoordination#removeStaleRecoveringRegions(Set)}
   */
  @Override
  public void removeStaleRecoveringRegions(final Set<String> knownFailedServers)
      throws IOException, InterruptedIOException {

    try {
      List<String> tasks = ZKUtil.listChildrenNoWatch(watcher, watcher.splitLogZNode);
      if (tasks != null) {
        int listSize = tasks.size();
        for (int i = 0; i < listSize; i++) {
          String t = tasks.get(i);
          byte[] data;
          try {
            data = ZKUtil.getData(this.watcher, ZKUtil.joinZNode(watcher.splitLogZNode, t));
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
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
          ServerName serverName = DefaultWALProvider.getServerNameFromWALDirectoryName(new Path(t));
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
        int listSize = regions.size();
        for (int i = 0; i < listSize; i++) {
          String nodePath = ZKUtil.joinZNode(watcher.recoveringRegionsZNode, regions.get(i));
          List<String> regionFailedServers = ZKUtil.listChildrenNoWatch(watcher, nodePath);
          if (regionFailedServers == null || regionFailedServers.isEmpty()) {
            ZKUtil.deleteNode(watcher, nodePath);
            continue;
          }
          boolean needMoreRecovery = false;
          int tmpFailedServerSize = regionFailedServers.size();
          for (int j = 0; j < tmpFailedServerSize; j++) {
            if (knownFailedServers.contains(regionFailedServers.get(j))) {
              needMoreRecovery = true;
              break;
            }
          }
          if (!needMoreRecovery) {
            ZKUtil.deleteNodeRecursively(watcher, nodePath);
          }
        }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized boolean isReplaying() {
    return this.recoveryMode == RecoveryMode.LOG_REPLAY;
  }

  @Override
  public synchronized boolean isSplitting() {
    return this.recoveryMode == RecoveryMode.LOG_SPLITTING;
  }

  private List<String> listSplitLogTasks() throws KeeperException {
    List<String> taskOrRescanList = ZKUtil.listChildrenNoWatch(watcher, watcher.splitLogZNode);
    if (taskOrRescanList == null || taskOrRescanList.isEmpty()) {
      return Collections.<String> emptyList();
    }
    List<String> taskList = new ArrayList<String>();
    for (String taskOrRescan : taskOrRescanList) {
      // Remove rescan nodes
      if (!ZKSplitLog.isRescanNode(taskOrRescan)) {
        taskList.add(taskOrRescan);
      }
    }
    return taskList;
  }

  /**
   * This function is to set recovery mode from outstanding split log tasks from before or current
   * configuration setting
   * @param isForInitialization
   * @throws IOException
   */
  @Override
  public void setRecoveryMode(boolean isForInitialization) throws IOException {
    synchronized(this) {
      if (this.isDrainingDone) {
        // when there is no outstanding splitlogtask after master start up, we already have up to
        // date recovery mode
        return;
      }
    }
    if (this.watcher == null) {
      // when watcher is null(testing code) and recovery mode can only be LOG_SPLITTING
      synchronized(this) {
        this.isDrainingDone = true;
        this.recoveryMode = RecoveryMode.LOG_SPLITTING;
      }
      return;
    }
    boolean hasSplitLogTask = false;
    boolean hasRecoveringRegions = false;
    RecoveryMode previousRecoveryMode = RecoveryMode.UNKNOWN;
    RecoveryMode recoveryModeInConfig =
        (isDistributedLogReplay(conf)) ? RecoveryMode.LOG_REPLAY : RecoveryMode.LOG_SPLITTING;

    // Firstly check if there are outstanding recovering regions
    try {
      List<String> regions = ZKUtil.listChildrenNoWatch(watcher, watcher.recoveringRegionsZNode);
      if (regions != null && !regions.isEmpty()) {
        hasRecoveringRegions = true;
        previousRecoveryMode = RecoveryMode.LOG_REPLAY;
      }
      if (previousRecoveryMode == RecoveryMode.UNKNOWN) {
        // Secondly check if there are outstanding split log task
        List<String> tasks = listSplitLogTasks();
        if (!tasks.isEmpty()) {
          hasSplitLogTask = true;
          if (isForInitialization) {
            // during initialization, try to get recovery mode from splitlogtask
            int listSize = tasks.size();
            for (int i = 0; i < listSize; i++) {
              String task = tasks.get(i);
              try {
                byte[] data =
                    ZKUtil.getData(this.watcher, ZKUtil.joinZNode(watcher.splitLogZNode, task));
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
              } catch (InterruptedException e) {
                throw new InterruptedIOException();
              }
            }
          }
        }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }

    synchronized (this) {
      if (this.isDrainingDone) {
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

  /**
   * Returns if distributed log replay is turned on or not
   * @param conf
   * @return true when distributed log replay is turned on
   */
  private boolean isDistributedLogReplay(Configuration conf) {
    boolean dlr =
        conf.getBoolean(HConstants.DISTRIBUTED_LOG_REPLAY_KEY,
          HConstants.DEFAULT_DISTRIBUTED_LOG_REPLAY_CONFIG);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Distributed log replay=" + dlr);
    }
    return dlr;
  }

  private boolean resubmit(ServerName serverName, String path, int version) {
    try {
      // blocking zk call but this is done from the timeout thread
      SplitLogTask slt =
          new SplitLogTask.Unassigned(this.details.getServerName(), getRecoveryMode());
      if (ZKUtil.setData(this.watcher, path, slt.toByteArray(), version) == false) {
        LOG.debug("failed to resubmit task " + path + " version changed");
        return false;
      }
    } catch (NoNodeException e) {
      LOG.warn("failed to resubmit because znode doesn't exist " + path
          + " task done (or forced done by removing the znode)");
      try {
        getDataSetWatchSuccess(path, null, Integer.MIN_VALUE);
      } catch (DeserializationException e1) {
        LOG.debug("Failed to re-resubmit task " + path + " because of deserialization issue", e1);
        return false;
      }
      return false;
    } catch (KeeperException.BadVersionException e) {
      LOG.debug("failed to resubmit task " + path + " version changed");
      return false;
    } catch (KeeperException e) {
      SplitLogCounters.tot_mgr_resubmit_failed.incrementAndGet();
      LOG.warn("failed to resubmit " + path, e);
      return false;
    }
    return true;
  }


  /**
   * {@link org.apache.hadoop.hbase.master.SplitLogManager} can use objects implementing this
   * interface to finish off a partially done task by
   * {@link org.apache.hadoop.hbase.regionserver.SplitLogWorker}. This provides a
   * serialization point at the end of the task processing. Must be restartable and idempotent.
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
     * finish the partially done task. workername provides clue to where the partial results of the
     * partially done tasks are present. taskname is the name of the task that was put up in
     * zookeeper.
     * <p>
     * @param workerName
     * @param taskname
     * @return DONE if task completed successfully, ERR otherwise
     */
    Status finish(ServerName workerName, String taskname);
  }

  /**
   * Asynchronous handler for zk create node results. Retries on failures.
   */
  public class CreateAsyncCallback implements AsyncCallback.StringCallback {
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
          Long retry_count = (Long) ctx;
          LOG.warn("create rc =" + KeeperException.Code.get(rc) + " for " + path
              + " remaining retries=" + retry_count);
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
   * Asynchronous handler for zk get-data-set-watch on node results. Retries on failures.
   */
  public class GetDataAsyncCallback implements AsyncCallback.DataCallback {
    private final Log LOG = LogFactory.getLog(GetDataAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      SplitLogCounters.tot_mgr_get_data_result.incrementAndGet();
      if (rc != 0) {
        if (needAbandonRetries(rc, "GetData from znode " + path)) {
          return;
        }
        if (rc == KeeperException.Code.NONODE.intValue()) {
          SplitLogCounters.tot_mgr_get_data_nonode.incrementAndGet();
          LOG.warn("task znode " + path + " vanished or not created yet.");
          // ignore since we should not end up in a case where there is in-memory task,
          // but no znode. The only case is between the time task is created in-memory
          // and the znode is created. See HBASE-11217.
          return;
        }
        Long retry_count = (Long) ctx;

        if (retry_count < 0) {
          LOG.warn("getdata rc = " + KeeperException.Code.get(rc) + " " + path
              + ". Ignoring error. No error handling. No retrying.");
          return;
        }
        LOG.warn("getdata rc = " + KeeperException.Code.get(rc) + " " + path
            + " remaining retries=" + retry_count);
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
   * Asynchronous handler for zk delete node results. Retries on failures.
   */
  public class DeleteAsyncCallback implements AsyncCallback.VoidCallback {
    private final Log LOG = LogFactory.getLog(DeleteAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx) {
      SplitLogCounters.tot_mgr_node_delete_result.incrementAndGet();
      if (rc != 0) {
        if (needAbandonRetries(rc, "Delete znode " + path)) {
          details.getFailedDeletions().add(path);
          return;
        }
        if (rc != KeeperException.Code.NONODE.intValue()) {
          SplitLogCounters.tot_mgr_node_delete_err.incrementAndGet();
          Long retry_count = (Long) ctx;
          LOG.warn("delete rc=" + KeeperException.Code.get(rc) + " for " + path
              + " remaining retries=" + retry_count);
          if (retry_count == 0) {
            LOG.warn("delete failed " + path);
            details.getFailedDeletions().add(path);
            deleteNodeFailure(path);
          } else {
            deleteNode(path, retry_count - 1);
          }
          return;
        } else {
          LOG.info(path + " does not exist. Either was created but deleted behind our"
              + " back by another pending delete OR was deleted"
              + " in earlier retry rounds. zkretries = " + ctx);
        }
      } else {
        LOG.debug("deleted " + path);
      }
      deleteNodeSuccess(path);
    }
  }

  /**
   * Asynchronous handler for zk create RESCAN-node results. Retries on failures.
   * <p>
   * A RESCAN node is created using PERSISTENT_SEQUENTIAL flag. It is a signal for all the
   * {@link org.apache.hadoop.hbase.regionserver.SplitLogWorker}s to rescan for new tasks.
   */
  public class CreateRescanAsyncCallback implements AsyncCallback.StringCallback {
    private final Log LOG = LogFactory.getLog(CreateRescanAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      if (rc != 0) {
        if (needAbandonRetries(rc, "CreateRescan znode " + path)) {
          return;
        }
        Long retry_count = (Long) ctx;
        LOG.warn("rc=" + KeeperException.Code.get(rc) + " for " + path + " remaining retries="
            + retry_count);
        if (retry_count == 0) {
          createRescanFailure();
        } else {
          rescan(retry_count - 1);
        }
        return;
      }
      // path is the original arg, name is the actual name that was created
      createRescanSuccess(name);
    }
  }

  @Override
  public void setDetails(SplitLogManagerDetails details) {
    this.details = details;
  }

  @Override
  public SplitLogManagerDetails getDetails() {
    return details;
  }

  @Override
  public synchronized RecoveryMode getRecoveryMode() {
    return recoveryMode;
  }

  @Override
  public long getLastRecoveryTime() {
    return lastRecoveringNodeCreationTime;
  }

  /**
   * Temporary function that is used by unit tests only
   */
  public void setIgnoreDeleteForTesting(boolean b) {
    ignoreZKDeleteForTesting = b;
  }
}
