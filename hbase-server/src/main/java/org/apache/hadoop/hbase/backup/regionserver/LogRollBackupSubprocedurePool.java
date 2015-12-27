/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.backup.regionserver;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.errorhandling.ForeignException;

/**
 * Handle running each of the individual tasks for completing a backup procedure
 * on a regionserver.
 */
public class LogRollBackupSubprocedurePool implements Closeable, Abortable {
  private static final Log LOG = LogFactory.getLog(LogRollBackupSubprocedurePool.class);

  /** Maximum number of concurrent snapshot region tasks that can run concurrently */
  private static final String CONCURENT_BACKUP_TASKS_KEY = "hbase.backup.region.concurrentTasks";
  private static final int DEFAULT_CONCURRENT_BACKUP_TASKS = 3;

  private final ExecutorCompletionService<Void> taskPool;
  private final ThreadPoolExecutor executor;
  private volatile boolean aborted;
  private final List<Future<Void>> futures = new ArrayList<Future<Void>>();
  private final String name;

  public LogRollBackupSubprocedurePool(String name, Configuration conf) {
    // configure the executor service
    long keepAlive =
        conf.getLong(LogRollRegionServerProcedureManager.BACKUP_TIMEOUT_MILLIS_KEY,
          LogRollRegionServerProcedureManager.BACKUP_TIMEOUT_MILLIS_DEFAULT);
    int threads = conf.getInt(CONCURENT_BACKUP_TASKS_KEY, DEFAULT_CONCURRENT_BACKUP_TASKS);
    this.name = name;
    executor =
        new ThreadPoolExecutor(1, threads, keepAlive, TimeUnit.SECONDS,
          new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory("rs(" + name
            + ")-backup-pool"));
    taskPool = new ExecutorCompletionService<Void>(executor);
  }

  /**
   * Submit a task to the pool.
   */
  public void submitTask(final Callable<Void> task) {
    Future<Void> f = this.taskPool.submit(task);
    futures.add(f);
  }

  /**
   * Wait for all of the currently outstanding tasks submitted via {@link #submitTask(Callable)}
   * @return <tt>true</tt> on success, <tt>false</tt> otherwise
   * @throws ForeignException exception
   */
  public boolean waitForOutstandingTasks() throws ForeignException {
    LOG.debug("Waiting for backup procedure to finish.");

    try {
      for (Future<Void> f : futures) {
        f.get();
      }
      return true;
    } catch (InterruptedException e) {
      if (aborted) {
        throw new ForeignException("Interrupted and found to be aborted while waiting for tasks!",
            e);
      }
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ForeignException) {
        throw (ForeignException) e.getCause();
      }
      throw new ForeignException(name, e.getCause());
    } finally {
      // close off remaining tasks
      for (Future<Void> f : futures) {
        if (!f.isDone()) {
          f.cancel(true);
        }
      }
    }
    return false;
  }

  /**
   * Attempt to cleanly shutdown any running tasks - allows currently running tasks to cleanly
   * finish
   */
  @Override
  public void close() {
    executor.shutdown();
  }

  @Override
  public void abort(String why, Throwable e) {
    if (this.aborted) {
      return;
    }

    this.aborted = true;
    LOG.warn("Aborting because: " + why, e);
    this.executor.shutdownNow();
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }
}