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
package org.apache.hadoop.hbase.regionserver.handler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker.TaskExecutor;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker.TaskExecutor.Status;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Handles log splitting a wal
 */
@InterfaceAudience.Private
public class HLogSplitterHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(HLogSplitterHandler.class);
  private final ServerName serverName;
  private final String curTask;
  private final String wal;
  private final ZooKeeperWatcher zkw;
  private final CancelableProgressable reporter;
  private final AtomicInteger inProgressTasks;
  private final MutableInt curTaskZKVersion;
  private final TaskExecutor splitTaskExecutor;
  private final RecoveryMode mode;

  public HLogSplitterHandler(final Server server, String curTask,
      final MutableInt curTaskZKVersion,
      CancelableProgressable reporter,
      AtomicInteger inProgressTasks, TaskExecutor splitTaskExecutor, RecoveryMode mode) {
	  super(server, EventType.RS_LOG_REPLAY);
    this.curTask = curTask;
    this.wal = ZKSplitLog.getFileName(curTask);
    this.reporter = reporter;
    this.inProgressTasks = inProgressTasks;
    this.inProgressTasks.incrementAndGet();
    this.serverName = server.getServerName();
    this.zkw = server.getZooKeeper();
    this.curTaskZKVersion = curTaskZKVersion;
    this.splitTaskExecutor = splitTaskExecutor;
    this.mode = mode;
  }

  @Override
  public void process() throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      Status status = this.splitTaskExecutor.exec(wal, mode, reporter);
      switch (status) {
      case DONE:
        endTask(zkw, new SplitLogTask.Done(this.serverName, this.mode),
          SplitLogCounters.tot_wkr_task_done, curTask, curTaskZKVersion.intValue());
        break;
      case PREEMPTED:
        SplitLogCounters.tot_wkr_preempt_task.incrementAndGet();
        LOG.warn("task execution preempted " + curTask);
        break;
      case ERR:
        if (server != null && !server.isStopped()) {
          endTask(zkw, new SplitLogTask.Err(this.serverName, this.mode),
            SplitLogCounters.tot_wkr_task_err, curTask, curTaskZKVersion.intValue());
          break;
        }
        // if the RS is exiting then there is probably a tons of stuff
        // that can go wrong. Resign instead of signaling error.
        //$FALL-THROUGH$
      case RESIGNED:
        if (server != null && server.isStopped()) {
          LOG.info("task execution interrupted because worker is exiting " + curTask);
        }
        endTask(zkw, new SplitLogTask.Resigned(this.serverName, this.mode),
          SplitLogCounters.tot_wkr_task_resigned, curTask, curTaskZKVersion.intValue());
        break;
      }
    } finally {
      LOG.info("worker " + serverName + " done with task " + curTask + " in "
          + (System.currentTimeMillis() - startTime) + "ms");
      this.inProgressTasks.decrementAndGet();
    }
  }
  
  /**
   * endTask() can fail and the only way to recover out of it is for the
   * {@link SplitLogManager} to timeout the task node.
   * @param slt
   * @param ctr
   */
  public static void endTask(ZooKeeperWatcher zkw, SplitLogTask slt, AtomicLong ctr, String task,
      int taskZKVersion) {
    try {
      if (ZKUtil.setData(zkw, task, slt.toByteArray(), taskZKVersion)) {
        LOG.info("successfully transitioned task " + task + " to final state " + slt);
        ctr.incrementAndGet();
        return;
      }
      LOG.warn("failed to transistion task " + task + " to end state " + slt
          + " because of version mismatch ");
    } catch (KeeperException.BadVersionException bve) {
      LOG.warn("transisition task " + task + " to " + slt
          + " failed because of version mismatch", bve);
    } catch (KeeperException.NoNodeException e) {
      LOG.fatal(
        "logic error - end task " + task + " " + slt
          + " failed because task doesn't exist", e);
    } catch (KeeperException e) {
      LOG.warn("failed to end task, " + task + " " + slt, e);
    }
    SplitLogCounters.tot_wkr_final_transition_failed.incrementAndGet();
  }
}
