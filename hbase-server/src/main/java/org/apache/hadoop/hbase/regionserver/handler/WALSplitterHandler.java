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

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SplitLogCounters;
import org.apache.hadoop.hbase.SplitLogTask;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker.TaskExecutor;
import org.apache.hadoop.hbase.regionserver.SplitLogWorker.TaskExecutor.Status;
import org.apache.hadoop.hbase.util.CancelableProgressable;

/**
 * Handles log splitting a wal
 */
@InterfaceAudience.Private
public class WALSplitterHandler extends EventHandler {
  private static final Logger LOG = LoggerFactory.getLogger(WALSplitterHandler.class);
  private final ServerName serverName;
  private final CancelableProgressable reporter;
  private final AtomicInteger inProgressTasks;
  private final TaskExecutor splitTaskExecutor;
  private final SplitLogWorkerCoordination.SplitTaskDetails splitTaskDetails;
  private final SplitLogWorkerCoordination coordination;


  public WALSplitterHandler(final Server server, SplitLogWorkerCoordination coordination,
      SplitLogWorkerCoordination.SplitTaskDetails splitDetails, CancelableProgressable reporter,
      AtomicInteger inProgressTasks, TaskExecutor splitTaskExecutor) {
    super(server, EventType.RS_LOG_REPLAY);
    this.splitTaskDetails = splitDetails;
    this.coordination = coordination;
    this.reporter = reporter;
    this.inProgressTasks = inProgressTasks;
    this.inProgressTasks.incrementAndGet();
    this.serverName = server.getServerName();
    this.splitTaskExecutor = splitTaskExecutor;
  }


  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="SF_SWITCH_FALLTHROUGH",
      justification="Intentional")
  @Override
  public void process() throws IOException {
    long startTime = System.currentTimeMillis();
    Status status = null;
    try {
      status = this.splitTaskExecutor.exec(splitTaskDetails.getWALFile(), reporter);
      boolean wasCounterIncremented = false;
      switch (status) {
      case DONE:
        coordination.endTask(new SplitLogTask.Done(this.serverName),
          SplitLogCounters.tot_wkr_task_done, splitTaskDetails);
        break;
      case PREEMPTED:
          SplitLogCounters.tot_wkr_preempt_task.increment();
          wasCounterIncremented = true;
          // Preempted state can currently be returned either when task is preempted, or when
          // there's a particular kind of error (e.g. some ZK/HDFS errors, in my observation).
          // In the latter case, master-side split task will get stuck if we don't update the
          // status. Treat preemption as error to be on the safe side.
          LOG.warn("task execution preempted; treating as error " + splitTaskDetails.getWALFile());
          //$FALL-THROUGH$
      case ERR:
          if (server != null && !server.isStopped()) {
            coordination.endTask(new SplitLogTask.Err(this.serverName), wasCounterIncremented
              ? null : SplitLogCounters.tot_wkr_task_err, splitTaskDetails);
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
          coordination.endTask(new SplitLogTask.Resigned(this.serverName), wasCounterIncremented
            ? null : SplitLogCounters.tot_wkr_task_resigned, splitTaskDetails);
          break;
      }
    } finally {
      LOG.info("Worker " + serverName + " done with task " + splitTaskDetails.toString() + " in "
          + (System.currentTimeMillis() - startTime) + "ms. Status = " + status);
      this.inProgressTasks.decrementAndGet();
    }
  }
}
