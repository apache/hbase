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
import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.coordination.ZKSplitLogManagerCoordination.TaskFinisher.Status;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.master.SplitLogManager.ResubmitDirective;
import org.apache.hadoop.hbase.master.SplitLogManager.Task;
import org.apache.hadoop.hbase.master.SplitLogManager.TerminationStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WALSplitUtil;
import org.apache.hadoop.hbase.zookeeper.ZKListener;
import org.apache.hadoop.hbase.zookeeper.ZKMetadata;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper based implementation of
 * {@link SplitLogManagerCoordination}
 */
@InterfaceAudience.Private
public class ZKSplitLogManagerCoordination extends ZKListener implements
    SplitLogManagerCoordination {

  public static final int DEFAULT_TIMEOUT = 120000;
  public static final int DEFAULT_ZK_RETRIES = 3;
  public static final int DEFAULT_MAX_RESUBMIT = 3;

  private static final Logger LOG = LoggerFactory.getLogger(SplitLogManagerCoordination.class);

  private final TaskFinisher taskFinisher;
  private final Configuration conf;

  private long zkretries;
  private long resubmitThreshold;
  private long timeout;

  SplitLogManagerDetails details;

  public boolean ignoreZKDeleteForTesting = false;

  public ZKSplitLogManagerCoordination(Configuration conf, ZKWatcher watcher) {
    super(watcher);
    this.conf = conf;
    taskFinisher = new TaskFinisher() {
      @Override
      public Status finish(ServerName workerName, String logfile) {
        try {
          WALSplitUtil.finishSplitLogFile(logfile, conf);
        } catch (IOException e) {
          LOG.warn("Could not finish splitting of log file " + logfile, e);
          return Status.ERR;
        }
        return Status.DONE;
      }
    };
  }

  @Override
  public void init() throws IOException {
    this.zkretries = conf.getLong("hbase.splitlog.zk.retries", DEFAULT_ZK_RETRIES);
    this.resubmitThreshold = conf.getLong("hbase.splitlog.max.resubmit", DEFAULT_MAX_RESUBMIT);
    this.timeout = conf.getInt(HConstants.HBASE_SPLITLOG_MANAGER_TIMEOUT, DEFAULT_TIMEOUT);
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
      List<String> tasks = ZKUtil.listChildrenNoWatch(watcher,
              watcher.getZNodePaths().splitLogZNode);
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
    if (task.isOrphan() && (task.incarnation.get() == 0)) {
      LOG.info("Resubmitting unassigned orphan task " + path);
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
          SplitLogCounters.tot_mgr_resubmit_threshold_reached.increment();
          LOG.info("Skipping resubmissions of task " + path + " because threshold "
              + resubmitThreshold + " reached");
        }
        return false;
      }
      // race with heartbeat() that might be changing last_version
      version = task.last_version;
    } else {
      SplitLogCounters.tot_mgr_resubmit_force.increment();
      version = -1;
    }
    LOG.info("Resubmitting task " + path);
    task.incarnation.incrementAndGet();
    boolean result = resubmit(path, version);
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
    SplitLogCounters.tot_mgr_resubmit.increment();
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
    SplitLogTask slt = new SplitLogTask.Done(this.details.getServerName());
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
    SplitLogCounters.tot_mgr_get_data_queued.increment();
  }

  private void deleteNode(String path, Long retries) {
    SplitLogCounters.tot_mgr_node_delete_queued.increment();
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
        SplitLogCounters.tot_mgr_rescan_deleted.increment();
      }
      SplitLogCounters.tot_mgr_missing_state_in_delete.increment();
      LOG.debug("Deleted task without in memory state " + path);
      return;
    }
    synchronized (task) {
      task.status = DELETED;
      task.notify();
    }
    SplitLogCounters.tot_mgr_task_deleted.increment();
  }

  private void deleteNodeFailure(String path) {
    LOG.info("Failed to delete node " + path + " and will retry soon.");
    return;
  }

  private void createRescanSuccess(String path) {
    SplitLogCounters.tot_mgr_rescan.increment();
    getDataSetWatch(path, zkretries);
  }

  private void createRescanFailure() {
    LOG.error(HBaseMarkers.FATAL, "logic failure, rescan failure must not happen");
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
    SplitLogTask slt = new SplitLogTask.Unassigned(details.getServerName());
    ZKUtil.asyncCreate(this.watcher, path, slt.toByteArray(), new CreateAsyncCallback(),
      retry_count);
    SplitLogCounters.tot_mgr_node_create_queued.increment();
    return;
  }

  private void createNodeSuccess(String path) {
    LOG.debug("Put up splitlog task at znode " + path);
    getDataSetWatch(path, zkretries);
  }

  private void createNodeFailure(String path) {
    // TODO the Manager should split the log locally instead of giving up
    LOG.warn("Failed to create task node " + path);
    setDone(path, FAILURE);
  }

  private void getDataSetWatch(String path, Long retry_count) {
    this.watcher.getRecoverableZooKeeper().getZooKeeper()
        .getData(path, this.watcher, new GetDataAsyncCallback(), retry_count);
    SplitLogCounters.tot_mgr_get_data_queued.increment();
  }

  private void getDataSetWatchSuccess(String path, byte[] data, int version)
      throws DeserializationException {
    if (data == null) {
      if (version == Integer.MIN_VALUE) {
        // assume all done. The task znode suddenly disappeared.
        setDone(path, SUCCESS);
        return;
      }
      SplitLogCounters.tot_mgr_null_data.increment();
      LOG.error(HBaseMarkers.FATAL, "logic error - got null data " + path);
      setDone(path, FAILURE);
      return;
    }
    data = ZKMetadata.removeMetaData(data);
    SplitLogTask slt = SplitLogTask.parseFrom(data);
    if (slt.isUnassigned()) {
      LOG.debug("Task not yet acquired " + path + ", ver=" + version);
      handleUnassignedTask(path);
    } else if (slt.isOwned()) {
      heartbeat(path, version, slt.getServerName());
    } else if (slt.isResigned()) {
      LOG.info("Task " + path + " entered state=" + slt.toString());
      resubmitOrFail(path, FORCE);
    } else if (slt.isDone()) {
      LOG.info("Task " + path + " entered state=" + slt.toString());
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
      LOG.info("Task " + path + " entered state=" + slt.toString());
      resubmitOrFail(path, CHECK);
    } else {
      LOG.error(HBaseMarkers.FATAL, "logic error - unexpected zk state for path = "
          + path + " data = " + slt.toString());
      setDone(path, FAILURE);
    }
  }

  private void resubmitOrFail(String path, ResubmitDirective directive) {
    if (resubmitTask(path, findOrCreateOrphanTask(path), directive) == false) {
      setDone(path, FAILURE);
    }
  }

  private void getDataSetWatchFailure(String path) {
    LOG.warn("Failed to set data watch " + path);
    setDone(path, FAILURE);
  }

  private void setDone(String path, TerminationStatus status) {
    Task task = details.getTasks().get(path);
    if (task == null) {
      if (!ZKSplitLog.isRescanNode(watcher, path)) {
        SplitLogCounters.tot_mgr_unacquired_orphan_done.increment();
        LOG.debug("Unacquired orphan task is done " + path);
      }
    } else {
      synchronized (task) {
        if (task.status == IN_PROGRESS) {
          if (status == SUCCESS) {
            SplitLogCounters.tot_mgr_log_split_success.increment();
            LOG.info("Done splitting " + path);
          } else {
            SplitLogCounters.tot_mgr_log_split_err.increment();
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

  private Task findOrCreateOrphanTask(String path) {
    return computeIfAbsent(details.getTasks(), path, Task::new, () -> {
      LOG.info("Creating orphan task " + path);
      SplitLogCounters.tot_mgr_orphan_task_acquired.increment();
    });
  }

  private void heartbeat(String path, int new_version, ServerName workerName) {
    Task task = findOrCreateOrphanTask(path);
    if (new_version != task.last_version) {
      if (task.isUnassigned()) {
        LOG.info("Task " + path + " acquired by " + workerName);
      }
      task.heartbeat(EnvironmentEdgeManager.currentTime(), new_version, workerName);
      SplitLogCounters.tot_mgr_heartbeat.increment();
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
      orphans = ZKUtil.listChildrenNoWatch(this.watcher,
              this.watcher.getZNodePaths().splitLogZNode);
      if (orphans == null) {
        LOG.warn("Could not get children of " + this.watcher.getZNodePaths().splitLogZNode);
        return;
      }
    } catch (KeeperException e) {
      LOG.warn("Could not get children of " + this.watcher.getZNodePaths().splitLogZNode + " "
          + StringUtils.stringifyException(e));
      return;
    }
    int rescan_nodes = 0;
    int listSize = orphans.size();
    for (int i = 0; i < listSize; i++) {
      String path = orphans.get(i);
      String nodepath = ZNodePaths.joinZNode(watcher.getZNodePaths().splitLogZNode, path);
      if (ZKSplitLog.isRescanNode(watcher, nodepath)) {
        rescan_nodes++;
        LOG.debug("Found orphan rescan node " + path);
      } else {
        LOG.info("Found orphan task " + path);
      }
      getDataSetWatch(nodepath, zkretries);
    }
    LOG.info("Found " + (orphans.size() - rescan_nodes) + " orphan tasks and " + rescan_nodes
        + " rescan nodes");
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

  private boolean resubmit(String path, int version) {
    try {
      // blocking zk call but this is done from the timeout thread
      SplitLogTask slt =
          new SplitLogTask.Unassigned(this.details.getServerName());
      if (ZKUtil.setData(this.watcher, path, slt.toByteArray(), version) == false) {
        LOG.debug("Failed to resubmit task " + path + " version changed");
        return false;
      }
    } catch (NoNodeException e) {
      LOG.warn("Failed to resubmit because znode doesn't exist " + path
          + " task done (or forced done by removing the znode)");
      try {
        getDataSetWatchSuccess(path, null, Integer.MIN_VALUE);
      } catch (DeserializationException e1) {
        LOG.debug("Failed to re-resubmit task " + path + " because of deserialization issue", e1);
        return false;
      }
      return false;
    } catch (KeeperException.BadVersionException e) {
      LOG.debug("Failed to resubmit task " + path + " version changed");
      return false;
    } catch (KeeperException e) {
      SplitLogCounters.tot_mgr_resubmit_failed.increment();
      LOG.warn("Failed to resubmit " + path, e);
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
    private final Logger LOG = LoggerFactory.getLogger(CreateAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      SplitLogCounters.tot_mgr_node_create_result.increment();
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
          LOG.debug("Found pre-existing znode " + path);
          SplitLogCounters.tot_mgr_node_already_exists.increment();
        } else {
          Long retry_count = (Long) ctx;
          LOG.warn("Create rc=" + KeeperException.Code.get(rc) + " for " + path
              + " remaining retries=" + retry_count);
          if (retry_count == 0) {
            SplitLogCounters.tot_mgr_node_create_err.increment();
            createNodeFailure(path);
          } else {
            SplitLogCounters.tot_mgr_node_create_retry.increment();
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
    private final Logger LOG = LoggerFactory.getLogger(GetDataAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      SplitLogCounters.tot_mgr_get_data_result.increment();
      if (rc != 0) {
        if (needAbandonRetries(rc, "GetData from znode " + path)) {
          return;
        }
        if (rc == KeeperException.Code.NONODE.intValue()) {
          SplitLogCounters.tot_mgr_get_data_nonode.increment();
          LOG.warn("Task znode " + path + " vanished or not created yet.");
          // ignore since we should not end up in a case where there is in-memory task,
          // but no znode. The only case is between the time task is created in-memory
          // and the znode is created. See HBASE-11217.
          return;
        }
        Long retry_count = (Long) ctx;

        if (retry_count < 0) {
          LOG.warn("Getdata rc=" + KeeperException.Code.get(rc) + " " + path
              + ". Ignoring error. No error handling. No retrying.");
          return;
        }
        LOG.warn("Getdata rc=" + KeeperException.Code.get(rc) + " " + path
            + " remaining retries=" + retry_count);
        if (retry_count == 0) {
          SplitLogCounters.tot_mgr_get_data_err.increment();
          getDataSetWatchFailure(path);
        } else {
          SplitLogCounters.tot_mgr_get_data_retry.increment();
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
    private final Logger LOG = LoggerFactory.getLogger(DeleteAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx) {
      SplitLogCounters.tot_mgr_node_delete_result.increment();
      if (rc != 0) {
        if (needAbandonRetries(rc, "Delete znode " + path)) {
          details.getFailedDeletions().add(path);
          return;
        }
        if (rc != KeeperException.Code.NONODE.intValue()) {
          SplitLogCounters.tot_mgr_node_delete_err.increment();
          Long retry_count = (Long) ctx;
          LOG.warn("Delete rc=" + KeeperException.Code.get(rc) + " for " + path
              + " remaining retries=" + retry_count);
          if (retry_count == 0) {
            LOG.warn("Delete failed " + path);
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
        LOG.debug("Deleted " + path);
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
    private final Logger LOG = LoggerFactory.getLogger(CreateRescanAsyncCallback.class);

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

  /**
   * Temporary function that is used by unit tests only
   */
  public void setIgnoreDeleteForTesting(boolean b) {
    ignoreZKDeleteForTesting = b;
  }
}
