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
package org.apache.hadoop.hbase.regionserver.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.errorhandling.ForeignException;
import org.apache.hadoop.hbase.errorhandling.ForeignExceptionDispatcher;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.ProcedureMember;
import org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManager;
import org.apache.hadoop.hbase.procedure.Subprocedure;
import org.apache.hadoop.hbase.procedure.SubprocedureFactory;
import org.apache.hadoop.hbase.procedure.ZKProcedureMemberRpcs;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * This manager class handles the work dealing with snapshots for a {@link HRegionServer}.
 * <p>
 * This provides the mechanism necessary to kick off a online snapshot specific
 * {@link Subprocedure} that is responsible for the regions being served by this region server.
 * If any failures occur with the subprocedure, the RegionSeverSnapshotManager's subprocedure
 * handler, {@link ProcedureMember}, notifies the master's ProcedureCoordinator to abort all
 * others.
 * <p>
 * On startup, requires {@link #start()} to be called.
 * <p>
 * On shutdown, requires {@link #stop(boolean)} to be called
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
@InterfaceStability.Unstable
public class RegionServerSnapshotManager extends RegionServerProcedureManager {
  private static final Log LOG = LogFactory.getLog(RegionServerSnapshotManager.class);

  /** Maximum number of snapshot region tasks that can run concurrently */
  private static final String CONCURENT_SNAPSHOT_TASKS_KEY = "hbase.snapshot.region.concurrentTasks";
  private static final int DEFAULT_CONCURRENT_SNAPSHOT_TASKS = 3;

  /** Conf key for number of request threads to start snapshots on regionservers */
  public static final String SNAPSHOT_REQUEST_THREADS_KEY = "hbase.snapshot.region.pool.threads";
  /** # of threads for snapshotting regions on the rs. */
  public static final int SNAPSHOT_REQUEST_THREADS_DEFAULT = 10;

  /** Conf key for max time to keep threads in snapshot request pool waiting */
  public static final String SNAPSHOT_TIMEOUT_MILLIS_KEY = "hbase.snapshot.region.timeout";
  /** Keep threads alive in request pool for max of 60 seconds */
  public static final long SNAPSHOT_TIMEOUT_MILLIS_DEFAULT = 60000;

  /** Conf key for millis between checks to see if snapshot completed or if there are errors*/
  public static final String SNAPSHOT_REQUEST_WAKE_MILLIS_KEY = "hbase.snapshot.region.wakefrequency";
  /** Default amount of time to check for errors while regions finish snapshotting */
  private static final long SNAPSHOT_REQUEST_WAKE_MILLIS_DEFAULT = 500;

  private RegionServerServices rss;
  private ProcedureMemberRpcs memberRpcs;
  private ProcedureMember member;

  /**
   * Exposed for testing.
   * @param conf HBase configuration.
   * @param parent parent running the snapshot handler
   * @param memberRpc use specified memberRpc instance
   * @param procMember use specified ProcedureMember
   */
   RegionServerSnapshotManager(Configuration conf, HRegionServer parent,
      ProcedureMemberRpcs memberRpc, ProcedureMember procMember) {
    this.rss = parent;
    this.memberRpcs = memberRpc;
    this.member = procMember;
  }

  public RegionServerSnapshotManager() {}

  /**
   * Start accepting snapshot requests.
   */
  @Override
  public void start() {
    LOG.debug("Start Snapshot Manager " + rss.getServerName().toString());
    this.memberRpcs.start(rss.getServerName().toString(), member);
  }

  /**
   * Close <tt>this</tt> and all running snapshot tasks
   * @param force forcefully stop all running tasks
   * @throws IOException
   */
  @Override
  public void stop(boolean force) throws IOException {
    String mode = force ? "abruptly" : "gracefully";
    LOG.info("Stopping RegionServerSnapshotManager " + mode + ".");

    try {
      this.member.close();
    } finally {
      this.memberRpcs.close();
    }
  }

  /**
   * If in a running state, creates the specified subprocedure for handling an online snapshot.
   *
   * Because this gets the local list of regions to snapshot and not the set the master had,
   * there is a possibility of a race where regions may be missed.  This detected by the master in
   * the snapshot verification step.
   *
   * @param snapshot
   * @return Subprocedure to submit to the ProcedureMemeber.
   */
  public Subprocedure buildSubprocedure(SnapshotDescription snapshot) {

    // don't run a snapshot if the parent is stop(ping)
    if (rss.isStopping() || rss.isStopped()) {
      throw new IllegalStateException("Can't start snapshot on RS: " + rss.getServerName()
          + ", because stopping/stopped!");
    }

    // check to see if this server is hosting any regions for the snapshots
    // check to see if we have regions for the snapshot
    List<HRegion> involvedRegions;
    try {
      involvedRegions = getRegionsToSnapshot(snapshot);
    } catch (IOException e1) {
      throw new IllegalStateException("Failed to figure out if we should handle a snapshot - "
          + "something has gone awry with the online regions.", e1);
    }

    // We need to run the subprocedure even if we have no relevant regions.  The coordinator
    // expects participation in the procedure and without sending message the snapshot attempt
    // will hang and fail.

    LOG.debug("Launching subprocedure for snapshot " + snapshot.getName() + " from table "
        + snapshot.getTable());
    ForeignExceptionDispatcher exnDispatcher = new ForeignExceptionDispatcher(snapshot.getName());
    Configuration conf = rss.getConfiguration();
    long timeoutMillis = conf.getLong(SNAPSHOT_TIMEOUT_MILLIS_KEY,
        SNAPSHOT_TIMEOUT_MILLIS_DEFAULT);
    long wakeMillis = conf.getLong(SNAPSHOT_REQUEST_WAKE_MILLIS_KEY,
        SNAPSHOT_REQUEST_WAKE_MILLIS_DEFAULT);

    switch (snapshot.getType()) {
    case FLUSH:
      SnapshotSubprocedurePool taskManager =
        new SnapshotSubprocedurePool(rss.getServerName().toString(), conf, rss);
      return new FlushSnapshotSubprocedure(member, exnDispatcher, wakeMillis,
          timeoutMillis, involvedRegions, snapshot, taskManager);
    case SKIPFLUSH:
        /*
         * This is to take an online-snapshot without force a coordinated flush to prevent pause
         * The snapshot type is defined inside the snapshot description. FlushSnapshotSubprocedure
         * should be renamed to distributedSnapshotSubprocedure, and the flush() behavior can be
         * turned on/off based on the flush type.
         * To minimized the code change, class name is not changed.
         */
        SnapshotSubprocedurePool taskManager2 =
            new SnapshotSubprocedurePool(rss.getServerName().toString(), conf, rss);
        return new FlushSnapshotSubprocedure(member, exnDispatcher, wakeMillis,
            timeoutMillis, involvedRegions, snapshot, taskManager2);

    default:
      throw new UnsupportedOperationException("Unrecognized snapshot type:" + snapshot.getType());
    }
  }

  /**
   * Determine if the snapshot should be handled on this server
   *
   * NOTE: This is racy -- the master expects a list of regionservers.
   * This means if a region moves somewhere between the calls we'll miss some regions.
   * For example, a region move during a snapshot could result in a region to be skipped or done
   * twice.  This is manageable because the {@link MasterSnapshotVerifier} will double check the
   * region lists after the online portion of the snapshot completes and will explicitly fail the
   * snapshot.
   *
   * @param snapshot
   * @return the list of online regions. Empty list is returned if no regions are responsible for
   *         the given snapshot.
   * @throws IOException
   */
  private List<HRegion> getRegionsToSnapshot(SnapshotDescription snapshot) throws IOException {
    List<HRegion> onlineRegions = rss.getOnlineRegions(TableName.valueOf(snapshot.getTable()));
    Iterator<HRegion> iterator = onlineRegions.iterator();
    // remove the non-default regions
    while (iterator.hasNext()) {
      HRegion r = iterator.next();
      if (!RegionReplicaUtil.isDefaultReplica(r.getRegionInfo())) {
        iterator.remove();
      }
    }
    return onlineRegions;
  }

  /**
   * Build the actual snapshot runner that will do all the 'hard' work
   */
  public class SnapshotSubprocedureBuilder implements SubprocedureFactory {

    @Override
    public Subprocedure buildSubprocedure(String name, byte[] data) {
      try {
        // unwrap the snapshot information
        SnapshotDescription snapshot = SnapshotDescription.parseFrom(data);
        return RegionServerSnapshotManager.this.buildSubprocedure(snapshot);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalArgumentException("Could not read snapshot information from request.");
      }
    }

  }

  /**
   * We use the SnapshotSubprocedurePool, a class specific thread pool instead of
   * {@link org.apache.hadoop.hbase.executor.ExecutorService}.
   *
   * It uses a {@link java.util.concurrent.ExecutorCompletionService} which provides queuing of
   * completed tasks which lets us efficiently cancel pending tasks upon the earliest operation
   * failures.
   *
   * HBase's ExecutorService (different from {@link java.util.concurrent.ExecutorService}) isn't
   * really built for coordinated tasks where multiple threads as part of one larger task.  In
   * RS's the HBase Executor services are only used for open and close and not other threadpooled
   * operations such as compactions and replication  sinks.
   */
  static class SnapshotSubprocedurePool {
    private final Abortable abortable;
    private final ExecutorCompletionService<Void> taskPool;
    private final ThreadPoolExecutor executor;
    private volatile boolean stopped;
    private final List<Future<Void>> futures = new ArrayList<Future<Void>>();
    private final String name;

    SnapshotSubprocedurePool(String name, Configuration conf, Abortable abortable) {
      this.abortable = abortable;
      // configure the executor service
      long keepAlive = conf.getLong(
        RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_KEY,
        RegionServerSnapshotManager.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT);
      int threads = conf.getInt(CONCURENT_SNAPSHOT_TASKS_KEY, DEFAULT_CONCURRENT_SNAPSHOT_TASKS);
      this.name = name;
      executor = new ThreadPoolExecutor(1, threads, keepAlive, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(), new DaemonThreadFactory("rs("
              + name + ")-snapshot-pool"));
      taskPool = new ExecutorCompletionService<Void>(executor);
    }

    boolean hasTasks() {
      return futures.size() != 0;
    }

    /**
     * Submit a task to the pool.
     *
     * NOTE: all must be submitted before you can safely {@link #waitForOutstandingTasks()}. This
     * version does not support issuing tasks from multiple concurrent table snapshots requests.
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
     * @throws SnapshotCreationException if the snapshot failed while we were waiting
     */
    boolean waitForOutstandingTasks() throws ForeignException, InterruptedException {
      LOG.debug("Waiting for local region snapshots to finish.");

      int sz = futures.size();
      try {
        // Using the completion service to process the futures that finish first first.
        for (int i = 0; i < sz; i++) {
          Future<Void> f = taskPool.take();
          f.get();
          if (!futures.remove(f)) {
            LOG.warn("unexpected future" + f);
          }
          LOG.debug("Completed " + (i+1) + "/" + sz +  " local region snapshots.");
        }
        LOG.debug("Completed " + sz +  " local region snapshots.");
        return true;
      } catch (InterruptedException e) {
        LOG.warn("Got InterruptedException in SnapshotSubprocedurePool", e);
        if (!stopped) {
          Thread.currentThread().interrupt();
          throw new ForeignException("SnapshotSubprocedurePool", e);
        }
        // we are stopped so we can just exit.
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof ForeignException) {
          LOG.warn("Rethrowing ForeignException from SnapshotSubprocedurePool", e);
          throw (ForeignException)e.getCause();
        } else if (cause instanceof DroppedSnapshotException) {
          // we have to abort the region server according to contract of flush
          abortable.abort("Received DroppedSnapshotException, aborting", cause);
        }
        LOG.warn("Got Exception in SnapshotSubprocedurePool", e);
        throw new ForeignException(name, e.getCause());
      } finally {
        cancelTasks();
      }
      return false;
    }

    /**
     * This attempts to cancel out all pending and in progress tasks (interruptions issues)
     * @throws InterruptedException
     */
    void cancelTasks() throws InterruptedException {
      Collection<Future<Void>> tasks = futures;
      LOG.debug("cancelling " + tasks.size() + " tasks for snapshot " + name);
      for (Future<Void> f: tasks) {
        // TODO Ideally we'd interrupt hbase threads when we cancel.  However it seems that there
        // are places in the HBase code where row/region locks are taken and not released in a
        // finally block.  Thus we cancel without interrupting.  Cancellations will be slower to
        // complete but we won't suffer from unreleased locks due to poor code discipline.
        f.cancel(false);
      }

      // evict remaining tasks and futures from taskPool.
      futures.clear();
      while (taskPool.poll() != null) {}
      stop();
    }

    /**
     * Abruptly shutdown the thread pool.  Call when exiting a region server.
     */
    void stop() {
      if (this.stopped) return;

      this.stopped = true;
      this.executor.shutdown();
    }
  }

  /**
   * Create a default snapshot handler - uses a zookeeper based member controller.
   * @param rss region server running the handler
   * @throws KeeperException if the zookeeper cluster cannot be reached
   */
  @Override
  public void initialize(RegionServerServices rss) throws KeeperException {
    this.rss = rss;
    ZooKeeperWatcher zkw = rss.getZooKeeper();
    this.memberRpcs = new ZKProcedureMemberRpcs(zkw,
        SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION);

    // read in the snapshot request configuration properties
    Configuration conf = rss.getConfiguration();
    long keepAlive = conf.getLong(SNAPSHOT_TIMEOUT_MILLIS_KEY, SNAPSHOT_TIMEOUT_MILLIS_DEFAULT);
    int opThreads = conf.getInt(SNAPSHOT_REQUEST_THREADS_KEY, SNAPSHOT_REQUEST_THREADS_DEFAULT);

    // create the actual snapshot procedure member
    ThreadPoolExecutor pool = ProcedureMember.defaultPool(rss.getServerName().toString(),
      opThreads, keepAlive);
    this.member = new ProcedureMember(memberRpcs, pool, new SnapshotSubprocedureBuilder());
  }

  @Override
  public String getProcedureSignature() {
    return SnapshotManager.ONLINE_SNAPSHOT_CONTROLLER_DESCRIPTION;
  }

}
