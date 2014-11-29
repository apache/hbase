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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker.TaskExecutor;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker.TaskExecutor.Status;
import org.apache.hadoop.hbase.util.CancelableProgressable;

/**
 * Handles log splitting a wal
 */
@InterfaceAudience.Private
public class HLogSplitterHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(HLogSplitterHandler.class);
  private final ServerName serverName;
  private final CancelableProgressable reporter;
  private final AtomicInteger inProgressTasks;
  private final TaskExecutor splitTaskExecutor;
  private final RecoveryMode mode;
  private final SplitLogWorkerCoordination.SplitTaskDetails splitTaskDetails;
  private final SplitLogWorkerCoordination coordination;


  public HLogSplitterHandler(final Server server, SplitLogWorkerCoordination coordination,
      SplitLogWorkerCoordination.SplitTaskDetails splitDetails, CancelableProgressable reporter,
      AtomicInteger inProgressTasks, TaskExecutor splitTaskExecutor, RecoveryMode mode) {
    super(server, EventType.RS_LOG_REPLAY);
    this.splitTaskDetails = splitDetails;
    this.coordination = coordination;
    this.reporter = reporter;
    this.inProgressTasks = inProgressTasks;
    this.inProgressTasks.incrementAndGet();
    this.serverName = server.getServerName();
    this.splitTaskExecutor = splitTaskExecutor;
    this.mode = mode;
  }

  @Override
  public void process() throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      Status status = this.splitTaskExecutor.exec(splitTaskDetails.getWALFile(), mode, reporter);
      switch (status) {
      case DONE:
        coordination.endTask(new SplitLogTask.Done(this.serverName,this.mode),
          SplitLogCounters.tot_wkr_task_done, splitTaskDetails);
        break;
      case PREEMPTED:
        SplitLogCounters.tot_wkr_preempt_task.incrementAndGet();
        LOG.warn("task execution prempted " + splitTaskDetails.getWALFile());
        break;
      case ERR:
        if (server != null && !server.isStopped()) {
          coordination.endTask(new SplitLogTask.Err(this.serverName, this.mode),
            SplitLogCounters.tot_wkr_task_err, splitTaskDetails);
          break;
        }
        // if the RS is exiting then there is probably a tons of stuff
        // that can go wrong. Resign instead of signaling error.
        //$FALL-THROUGH$
      case RESIGNED:
        if (server != null && server.isStopped()) {
          LOG.info("task execution interrupted because worker is exiting "
              + splitTaskDetails.toString());
        }
        coordination.endTask(new SplitLogTask.Resigned(this.serverName, this.mode),
          SplitLogCounters.tot_wkr_task_resigned, splitTaskDetails);
        break;
      }
    } finally {
      LOG.info("worker " + serverName + " done with task " + splitTaskDetails.toString() + " in "
          + (System.currentTimeMillis() - startTime) + "ms");
      this.inProgressTasks.decrementAndGet();
    }
  }
}
