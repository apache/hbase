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
package org.apache.hadoop.hbase.procedure.flush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManager;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.procedure.SubprocedureFactory;
import org.apache.hadoop.hbase.procedure.ZKProcedureMemberRpcs;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This manager class handles flushing of the regions for table on a {@link HRegionServer}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class RegionServerFlushTableProcedureManager extends RegionServerProcedureManager {
  private static final Log LOG = LogFactory.getLog(RegionServerFlushTableProcedureManager.class);

  private static final String CONCURENT_FLUSH_TASKS_KEY =
      "hbase.flush.procedure.region.concurrentTasks";
  private static final int DEFAULT_CONCURRENT_FLUSH_TASKS = 3;

  public static final String FLUSH_REQUEST_THREADS_KEY =
      "hbase.flush.procedure.region.pool.threads";
  public static final int FLUSH_REQUEST_THREADS_DEFAULT = 10;

  public static final String FLUSH_TIMEOUT_MILLIS_KEY =
      "hbase.flush.procedure.region.timeout";
  public static final long FLUSH_TIMEOUT_MILLIS_DEFAULT = 60000;

  public static final String FLUSH_REQUEST_WAKE_MILLIS_KEY =
      "hbase.flush.procedure.region.wakefrequency";
  private static final long FLUSH_REQUEST_WAKE_MILLIS_DEFAULT = 500;

  private RegionServerServices rss;
  private ProcedureMemberRpcs memberRpcs;
  private ProcedureMember member;

  /**
   * Exposed for testing.
   * @param conf HBase configuration.
   * @param server region server.
   * @param memberRpc use specified memberRpc instance
   * @param procMember use specified ProcedureMember
   */
   RegionServerFlushTableProcedureManager(Configuration conf, HRegionServer server,
      ProcedureMemberRpcs memberRpc, ProcedureMember procMember) {
    this.rss = server;
    this.memberRpcs = memberRpc;
    this.member = procMember;
  }

  public RegionServerFlushTableProcedureManager() {}

  /**
   * Start accepting flush table requests.
   */
  @Override
  public void start() {
    LOG.debug("Start region server flush procedure manager " + rss.getServerName().toString());
    this.memberRpcs.start(rss.getServerName().toString(), member);
  }

  /**
   * Close <tt>this</tt> and all running tasks
   * @param force forcefully stop all running tasks
   * @throws IOException
   */
  @Override
  public void stop(boolean force) throws IOException {
    String mode = force ? "abruptly" : "gracefully";
    LOG.info("Stopping region server flush procedure manager " + mode + ".");

    try {
      this.member.close();
    } finally {
      this.memberRpcs.close();
    }
  }

  /**
   * If in a running state, creates the specified subprocedure to flush table regions.
   *
   * Because this gets the local list of regions to flush and not the set the master had,
   * there is a possibility of a race where regions may be missed.
   *
   * @param table
   * @return Subprocedure to submit to the ProcedureMemeber.
   */
  public Subprocedure buildSubprocedure(String table) {

    // don't run the subprocedure if the parent is stop(ping)
    if (rss.isStopping() || rss.isStopped()) {
      throw new IllegalStateException("Can't start flush region subprocedure on RS: "
          + rss.getServerName() + ", because stopping/stopped!");
    }

    // check to see if this server is hosting any regions for the table
    List<Region> involvedRegions;
    try {
      involvedRegions = getRegionsToFlush(table);
    } catch (IOException e1) {
      throw new IllegalStateException("Failed to figure out if there is region to flush.", e1);
    }

    // We need to run the subprocedure even if we have no relevant regions.  The coordinator
    // expects participation in the procedure and without sending message the master procedure
    // will hang and fail.

    LOG.debug("Launching subprocedure to flush regions for " + table);
    ForeignExceptionDispatcher exnDispatcher = new ForeignExceptionDispatcher(table);
    Configuration conf = rss.getConfiguration();
    long timeoutMillis = conf.getLong(FLUSH_TIMEOUT_MILLIS_KEY,
        FLUSH_TIMEOUT_MILLIS_DEFAULT);
    long wakeMillis = conf.getLong(FLUSH_REQUEST_WAKE_MILLIS_KEY,
        FLUSH_REQUEST_WAKE_MILLIS_DEFAULT);

    FlushTableSubprocedurePool taskManager =
        new FlushTableSubprocedurePool(rss.getServerName().toString(), conf, rss);
    return new FlushTableSubprocedure(member, exnDispatcher, wakeMillis,
      timeoutMillis, involvedRegions, table, taskManager);
  }

  /**
   * Get the list of regions to flush for the table on this server
   *
   * It is possible that if a region moves somewhere between the calls
   * we'll miss the region.
   *
   * @param table
   * @return the list of online regions. Empty list is returned if no regions.
   * @throws IOException
   */
  private List<Region> getRegionsToFlush(String table) throws IOException {
    return rss.getOnlineRegions(TableName.valueOf(table));
  }

  public class FlushTableSubprocedureBuilder implements SubprocedureFactory {

    @Override
    public Subprocedure buildSubprocedure(String name, byte[] data) {
      // The name of the procedure instance from the master is the table name.
      return RegionServerFlushTableProcedureManager.this.buildSubprocedure(name);
    }

  }

  /**
   * We use the FlushTableSubprocedurePool, a class specific thread pool instead of
   * {@link org.apache.hadoop.hbase.executor.ExecutorService}.
   *
   * It uses a {@link java.util.concurrent.ExecutorCompletionService} which provides queuing of
   * completed tasks which lets us efficiently cancel pending tasks upon the earliest operation
   * failures.
   */
  static class FlushTableSubprocedurePool {
    private final Abortable abortable;
    private final ExecutorCompletionService<Void> taskPool;
    private final ThreadPoolExecutor executor;
    private volatile boolean stopped;
    private final List<Future<Void>> futures = new ArrayList<Future<Void>>();
    private final String name;

    FlushTableSubprocedurePool(String name, Configuration conf, Abortable abortable) {
      this.abortable = abortable;
      // configure the executor service
      long keepAlive = conf.getLong(
        RegionServerFlushTableProcedureManager.FLUSH_TIMEOUT_MILLIS_KEY,
        RegionServerFlushTableProcedureManager.FLUSH_TIMEOUT_MILLIS_DEFAULT);
      int threads = conf.getInt(CONCURENT_FLUSH_TASKS_KEY, DEFAULT_CONCURRENT_FLUSH_TASKS);
      this.name = name;
      executor = new ThreadPoolExecutor(1, threads, keepAlive, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory("rs("
              + name + ")-flush-proc-pool"));
      taskPool = new ExecutorCompletionService<Void>(executor);
    }

    boolean hasTasks() {
      return futures.size() != 0;
    }

    /**
     * Submit a task to the pool.
     *
     * NOTE: all must be submitted before you can safely {@link #waitForOutstandingTasks()}.
     */
    void submitTask(final Callable<Void> task) {
      Future<Void> f = this.taskPool.submit(task);
      futures.add(f);
    }

    /**
     * Wait for all of the currently outstanding tasks submitted via {@link #submitTask(Callable)}.
     * This *must* be called after all tasks are submitted via submitTask.
     *
     * @return <tt>true</tt> on success, <tt>false</tt> otherwise
     * @throws InterruptedException
     */
    boolean waitForOutstandingTasks() throws ForeignException, InterruptedException {
      LOG.debug("Waiting for local region flush to finish.");

      int sz = futures.size();
      try {
        // Using the completion service to process the futures.
        for (int i = 0; i < sz; i++) {
          Future<Void> f = taskPool.take();
          f.get();
          if (!futures.remove(f)) {
            LOG.warn("unexpected future" + f);
          }
          LOG.debug("Completed " + (i+1) + "/" + sz +  " local region flush tasks.");
        }
        LOG.debug("Completed " + sz +  " local region flush tasks.");
        return true;
      } catch (InterruptedException e) {
        LOG.warn("Got InterruptedException in FlushSubprocedurePool", e);
        if (!stopped) {
          Thread.currentThread().interrupt();
          throw new ForeignException("FlushSubprocedurePool", e);
        }
        // we are stopped so we can just exit.
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ForeignException) {
          LOG.warn("Rethrowing ForeignException from FlushSubprocedurePool", e);
          throw (ForeignException)e.getCause();
        } else if (cause instanceof DroppedSnapshotException) {
          // we have to abort the region server according to contract of flush
          abortable.abort("Received DroppedSnapshotException, aborting", cause);
        }
        LOG.warn("Got Exception in FlushSubprocedurePool", e);
        throw new ForeignException(name, e.getCause());
      } finally {
        cancelTasks();
      }
      return false;
    }

    /**
     * This attempts to cancel out all pending and in progress tasks. Does not interrupt the running
     * tasks itself. An ongoing HRegion.flush() should not be interrupted (see HBASE-13877).
     * @throws InterruptedException
     */
    void cancelTasks() throws InterruptedException {
      Collection<Future<Void>> tasks = futures;
      LOG.debug("cancelling " + tasks.size() + " flush region tasks " + name);
      for (Future<Void> f: tasks) {
        f.cancel(false);
      }

      // evict remaining tasks and futures from taskPool.
      futures.clear();
      while (taskPool.poll() != null) {}
      stop();
    }

    /**
     * Gracefully shutdown the thread pool. An ongoing HRegion.flush() should not be
     * interrupted (see HBASE-13877)
     */
    void stop() {
      if (this.stopped) return;

      this.stopped = true;
      this.executor.shutdown();
    }
  }

  /**
   * Initialize this region server flush procedure manager
   * Uses a zookeeper based member controller.
   * @param rss region server
   * @throws KeeperException if the zookeeper cannot be reached
   */
  @Override
  public void initialize(RegionServerServices rss) throws KeeperException {
    this.rss = rss;
    ZooKeeperWatcher zkw = rss.getZooKeeper();
    this.memberRpcs = new ZKProcedureMemberRpcs(zkw,
      MasterFlushTableProcedureManager.FLUSH_TABLE_PROCEDURE_SIGNATURE);

    Configuration conf = rss.getConfiguration();
    long keepAlive = conf.getLong(FLUSH_TIMEOUT_MILLIS_KEY, FLUSH_TIMEOUT_MILLIS_DEFAULT);
    int opThreads = conf.getInt(FLUSH_REQUEST_THREADS_KEY, FLUSH_REQUEST_THREADS_DEFAULT);

    // create the actual flush table procedure member
    ThreadPoolExecutor pool = ProcedureMember.defaultPool(rss.getServerName().toString(),
      opThreads, keepAlive);
    this.member = new ProcedureMember(memberRpcs, pool, new FlushTableSubprocedureBuilder());
  }

  @Override
  public String getProcedureSignature() {
    return MasterFlushTableProcedureManager.FLUSH_TABLE_PROCEDURE_SIGNATURE;
  }

}
