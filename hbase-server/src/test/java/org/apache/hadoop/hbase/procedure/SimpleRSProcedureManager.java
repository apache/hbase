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
package org.apache.hadoop.hbase.procedure;

import java.io.Closeable;
import java.io.IOException;
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
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.zookeeper.KeeperException;

public class SimpleRSProcedureManager extends RegionServerProcedureManager {

  private static final Log LOG = LogFactory.getLog(SimpleRSProcedureManager.class);

  private RegionServerServices rss;
  private ProcedureMemberRpcs memberRpcs;
  private ProcedureMember member;

  @Override
  public void initialize(RegionServerServices rss) throws IOException {
    this.rss = rss;
    ZooKeeperWatcher zkw = rss.getZooKeeper();
    this.memberRpcs = new ZKProcedureMemberRpcs(zkw, getProcedureSignature());

    ThreadPoolExecutor pool =
        ProcedureMember.defaultPool(rss.getServerName().toString(), 1);
    this.member = new ProcedureMember(memberRpcs, pool, new SimleSubprocedureBuilder());
    LOG.info("Initialized: " + rss.getServerName().toString());
  }

  @Override
  public void start() {
    this.memberRpcs.start(rss.getServerName().toString(), member);
    LOG.info("Started.");
  }

  @Override
  public void stop(boolean force) throws IOException {
    LOG.info("stop: " + force);
    try {
      this.member.close();
    } finally {
      this.memberRpcs.close();
    }
  }

  @Override
  public String getProcedureSignature() {
    return SimpleMasterProcedureManager.SIMPLE_SIGNATURE;
  }

  /**
   * If in a running state, creates the specified subprocedure for handling a procedure.
   * @return Subprocedure to submit to the ProcedureMemeber.
   */
  public Subprocedure buildSubprocedure(String name) {

    // don't run a procedure if the parent is stop(ping)
    if (rss.isStopping() || rss.isStopped()) {
      throw new IllegalStateException("Can't start procedure on RS: " + rss.getServerName()
          + ", because stopping/stopped!");
    }

    LOG.info("Attempting to run a procedure.");
    ForeignExceptionDispatcher errorDispatcher = new ForeignExceptionDispatcher();
    Configuration conf = rss.getConfiguration();

    SimpleSubprocedurePool taskManager =
        new SimpleSubprocedurePool(rss.getServerName().toString(), conf);
    return new SimpleSubprocedure(rss, member, errorDispatcher, taskManager, name);
  }

  /**
   * Build the actual procedure runner that will do all the 'hard' work
   */
  public class SimleSubprocedureBuilder implements SubprocedureFactory {

    @Override
    public Subprocedure buildSubprocedure(String name, byte[] data) {
      LOG.info("Building procedure: " + name);
      return SimpleRSProcedureManager.this.buildSubprocedure(name);
    }
  }

  public class SimpleSubprocedurePool implements Closeable, Abortable {

    private final ExecutorCompletionService<Void> taskPool;
    private final ThreadPoolExecutor executor;
    private volatile boolean aborted;
    private final List<Future<Void>> futures = new ArrayList<Future<Void>>();
    private final String name;

    public SimpleSubprocedurePool(String name, Configuration conf) {
      this.name = name;
      executor = new ThreadPoolExecutor(1, 1, 500, TimeUnit.SECONDS,
          new LinkedBlockingQueue<Runnable>(),
          new DaemonThreadFactory("rs(" + name + ")-procedure-pool"));
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
     *
     * @return <tt>true</tt> on success, <tt>false</tt> otherwise
     * @throws ForeignException
     */
    public boolean waitForOutstandingTasks() throws ForeignException {
      LOG.debug("Waiting for procedure to finish.");

      try {
        for (Future<Void> f: futures) {
          f.get();
        }
        return true;
      } catch (InterruptedException e) {
        if (aborted) throw new ForeignException(
            "Interrupted and found to be aborted while waiting for tasks!", e);
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        if (e.getCause() instanceof ForeignException) {
          throw (ForeignException) e.getCause();
        }
        throw new ForeignException(name, e.getCause());
      } finally {
        // close off remaining tasks
        for (Future<Void> f: futures) {
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
      if (this.aborted) return;

      this.aborted = true;
      LOG.warn("Aborting because: " + why, e);
      this.executor.shutdownNow();
    }

    @Override
    public boolean isAborted() {
      return this.aborted;
    }
  }

  public class SimpleSubprocedure extends Subprocedure {
    private final RegionServerServices rss;
    private final SimpleSubprocedurePool taskManager;

    public SimpleSubprocedure(RegionServerServices rss, ProcedureMember member,
        ForeignExceptionDispatcher errorListener, SimpleSubprocedurePool taskManager, String name) {
      super(member, name, errorListener, 500, 60000);
      LOG.info("Constructing a SimpleSubprocedure.");
      this.rss = rss;
      this.taskManager = taskManager;
    }

    /**
     * Callable task.
     * TODO. We don't need a thread pool to execute roll log. This can be simplified
     * with no use of subprocedurepool.
     */
    class RSSimpleTask implements Callable<Void> {
      RSSimpleTask() {}

      @Override
      public Void call() throws Exception {
        LOG.info("Execute subprocedure on " + rss.getServerName().toString());
        return null;
      }

    }

    private void execute() throws ForeignException {

      monitor.rethrowException();

      // running a task (e.g., roll log, flush table) on region server
      taskManager.submitTask(new RSSimpleTask());
      monitor.rethrowException();

      // wait for everything to complete.
      taskManager.waitForOutstandingTasks();
      monitor.rethrowException();

    }

    @Override
    public void acquireBarrier() throws ForeignException {
      // do nothing, executing in inside barrier step.
    }

    /**
     * do a log roll.
     */
    @Override
    public byte[] insideBarrier() throws ForeignException {
      execute();
      return SimpleMasterProcedureManager.SIMPLE_DATA.getBytes();
    }

    /**
     * Cancel threads if they haven't finished.
     */
    @Override
    public void cleanup(Exception e) {
      taskManager.abort("Aborting simple subprocedure tasks due to error", e);
    }
  }

}
