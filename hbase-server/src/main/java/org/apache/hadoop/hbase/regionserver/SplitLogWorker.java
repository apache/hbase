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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.coordination.BaseCoordinatedStateManager;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.SplitLogTask.RecoveryMode;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * This worker is spawned in every regionserver, including master. The Worker waits for log
 * splitting tasks to be put up by the {@link org.apache.hadoop.hbase.master.SplitLogManager} 
 * running in the master and races with other workers in other serves to acquire those tasks. 
 * The coordination is done via coordination engine.
 * <p>
 * If a worker has successfully moved the task from state UNASSIGNED to OWNED then it owns the task.
 * It keeps heart beating the manager by periodically moving the task from UNASSIGNED to OWNED
 * state. On success it moves the task to TASK_DONE. On unrecoverable error it moves task state to
 * ERR. If it cannot continue but wants the master to retry the task then it moves the task state to
 * RESIGNED.
 * <p>
 * The manager can take a task away from a worker by moving the task from OWNED to UNASSIGNED. In
 * the absence of a global lock there is a unavoidable race here - a worker might have just finished
 * its task when it is stripped of its ownership. Here we rely on the idempotency of the log
 * splitting task for correctness
 */
@InterfaceAudience.Private
public class SplitLogWorker implements Runnable {

  private static final Log LOG = LogFactory.getLog(SplitLogWorker.class);

  Thread worker;
  // thread pool which executes recovery work
  private SplitLogWorkerCoordination coordination;
  private Configuration conf;
  private RegionServerServices server;
  public SplitLogWorker(Server hserver, Configuration conf, RegionServerServices server,
      TaskExecutor splitTaskExecutor) {
    this.server = server;
    this.conf = conf;
    this.coordination =
        ((BaseCoordinatedStateManager) hserver.getCoordinatedStateManager())
            .getSplitLogWorkerCoordination();
    this.server = server;
    coordination.init(server, conf, splitTaskExecutor, this);
  }

  public SplitLogWorker(final Server hserver, final Configuration conf,
      final RegionServerServices server, final LastSequenceId sequenceIdChecker) {
    this(server, conf, server, new TaskExecutor() {
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
            fs, conf, p, sequenceIdChecker, server.getCoordinatedStateManager(), mode)) {
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
      LOG.info("SplitLogWorker " + server.getServerName() + " starting");
      coordination.registerListener();
      // wait for Coordination Engine is ready
      boolean res = false;
      while (!res && !coordination.isStop()) {
        res = coordination.isReady();
      }
      if (!coordination.isStop()) {
        coordination.taskLoop();
      }
    } catch (Throwable t) {
      if (ExceptionUtil.isInterrupt(t)) {
        LOG.info("SplitLogWorker interrupted. Exiting. " + (coordination.isStop() ? "" :
            " (ERROR: exitWorker is not set, exiting anyway)"));
      } else {
        // only a logical error can cause here. Printing it out
        // to make debugging easier
        LOG.error("unexpected error ", t);
      }
    } finally {
      coordination.removeListener();
      LOG.info("SplitLogWorker " + server.getServerName() + " exiting");
    }
  }
  /**
   * If the worker is doing a task i.e. splitting a log file then stop the task.
   * It doesn't exit the worker thread.
   */
  public void stopTask() {
    LOG.info("Sending interrupt to stop the worker thread");
    worker.interrupt(); // TODO interrupt often gets swallowed, do what else?
  }

  /**
   * start the SplitLogWorker thread
   */
  public void start() {
    worker = new Thread(null, this, "SplitLogWorker-" + server.getServerName().toShortString());
    worker.start();
  }

  /**
   * stop the SplitLogWorker thread
   */
  public void stop() {
    coordination.stopProcessingTasks();
    stopTask();
  }

  /**
   * Objects implementing this interface actually do the task that has been
   * acquired by a {@link SplitLogWorker}. Since there isn't a water-tight
   * guarantee that two workers will not be executing the same task therefore it
   * is better to have workers prepare the task and then have the
   * {@link org.apache.hadoop.hbase.master.SplitLogManager} commit the work in 
   * SplitLogManager.TaskFinisher
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

  /**
   * Returns the number of tasks processed by coordination.
   * This method is used by tests only
   */
  @VisibleForTesting
  public int getTaskReadySeq() {
    return coordination.getTaskReadySeq();
  }
}
