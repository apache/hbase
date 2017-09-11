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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;

/**
 * Compact region on request and then run split if appropriate
 */
@InterfaceAudience.Private
public class CompactSplit implements PropagatingConfigurationObserver {
  private static final Log LOG = LogFactory.getLog(CompactSplit.class);

  // Configuration key for the large compaction threads.
  public final static String LARGE_COMPACTION_THREADS =
      "hbase.regionserver.thread.compaction.large";
  public final static int LARGE_COMPACTION_THREADS_DEFAULT = 1;

  // Configuration key for the small compaction threads.
  public final static String SMALL_COMPACTION_THREADS =
      "hbase.regionserver.thread.compaction.small";
  public final static int SMALL_COMPACTION_THREADS_DEFAULT = 1;

  // Configuration key for split threads
  public final static String SPLIT_THREADS = "hbase.regionserver.thread.split";
  public final static int SPLIT_THREADS_DEFAULT = 1;

  public static final String REGION_SERVER_REGION_SPLIT_LIMIT =
      "hbase.regionserver.regionSplitLimit";
  public static final int DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT= 1000;

  private final HRegionServer server;
  private final Configuration conf;

  private final ThreadPoolExecutor longCompactions;
  private final ThreadPoolExecutor shortCompactions;
  private final ThreadPoolExecutor splits;

  private volatile ThroughputController compactionThroughputController;

  /**
   * Splitting should not take place if the total number of regions exceed this.
   * This is not a hard limit to the number of regions but it is a guideline to
   * stop splitting after number of online regions is greater than this.
   */
  private int regionSplitLimit;

  /** @param server */
  CompactSplit(HRegionServer server) {
    super();
    this.server = server;
    this.conf = server.getConfiguration();
    this.regionSplitLimit = conf.getInt(REGION_SERVER_REGION_SPLIT_LIMIT,
        DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT);

    int largeThreads = Math.max(1, conf.getInt(
        LARGE_COMPACTION_THREADS, LARGE_COMPACTION_THREADS_DEFAULT));
    int smallThreads = conf.getInt(
        SMALL_COMPACTION_THREADS, SMALL_COMPACTION_THREADS_DEFAULT);

    int splitThreads = conf.getInt(SPLIT_THREADS, SPLIT_THREADS_DEFAULT);

    // if we have throttle threads, make sure the user also specified size
    Preconditions.checkArgument(largeThreads > 0 && smallThreads > 0);

    final String n = Thread.currentThread().getName();

    StealJobQueue<Runnable> stealJobQueue = new StealJobQueue<Runnable>(COMPARATOR);
    this.longCompactions = new ThreadPoolExecutor(largeThreads, largeThreads,
        60, TimeUnit.SECONDS, stealJobQueue,
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            String name = n + "-longCompactions-" + System.currentTimeMillis();
            return new Thread(r, name);
          }
      });
    this.longCompactions.setRejectedExecutionHandler(new Rejection());
    this.longCompactions.prestartAllCoreThreads();
    this.shortCompactions = new ThreadPoolExecutor(smallThreads, smallThreads,
        60, TimeUnit.SECONDS, stealJobQueue.getStealFromQueue(),
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            String name = n + "-shortCompactions-" + System.currentTimeMillis();
            return new Thread(r, name);
          }
      });
    this.shortCompactions
        .setRejectedExecutionHandler(new Rejection());
    this.splits = (ThreadPoolExecutor)
        Executors.newFixedThreadPool(splitThreads,
            new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            String name = n + "-splits-" + System.currentTimeMillis();
            return new Thread(r, name);
          }
      });

    // compaction throughput controller
    this.compactionThroughputController =
        CompactionThroughputControllerFactory.create(server, conf);
  }

  @Override
  public String toString() {
    return "compaction_queue=("
        + longCompactions.getQueue().size() + ":"
        + shortCompactions.getQueue().size() + ")"
        + ", split_queue=" + splits.getQueue().size();
  }

  public String dumpQueue() {
    StringBuffer queueLists = new StringBuffer();
    queueLists.append("Compaction/Split Queue dump:\n");
    queueLists.append("  LargeCompation Queue:\n");
    BlockingQueue<Runnable> lq = longCompactions.getQueue();
    Iterator<Runnable> it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    if (shortCompactions != null) {
      queueLists.append("\n");
      queueLists.append("  SmallCompation Queue:\n");
      lq = shortCompactions.getQueue();
      it = lq.iterator();
      while (it.hasNext()) {
        queueLists.append("    " + it.next().toString());
        queueLists.append("\n");
      }
    }

    queueLists.append("\n");
    queueLists.append("  Split Queue:\n");
    lq = splits.getQueue();
    it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    return queueLists.toString();
  }

  public synchronized boolean requestSplit(final Region r) {
    // don't split regions that are blocking
    if (shouldSplitRegion() && ((HRegion)r).getCompactPriority() >= Store.PRIORITY_USER) {
      byte[] midKey = ((HRegion)r).checkSplit();
      if (midKey != null) {
        requestSplit(r, midKey);
        return true;
      }
    }
    return false;
  }

  public synchronized void requestSplit(final Region r, byte[] midKey) {
    requestSplit(r, midKey, null);
  }

  /*
   * The User parameter allows the split thread to assume the correct user identity
   */
  public synchronized void requestSplit(final Region r, byte[] midKey, User user) {
    if (midKey == null) {
      LOG.debug("Region " + r.getRegionInfo().getRegionNameAsString() +
        " not splittable because midkey=null");
      if (((HRegion)r).shouldForceSplit()) {
        ((HRegion)r).clearSplit();
      }
      return;
    }
    try {
      this.splits.execute(new SplitRequest(r, midKey, this.server, user));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Splitting " + r + ", " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + r, ree);
    }
  }

  public synchronized void requestCompaction(HRegion region, String why, int priority,
      CompactionLifeCycleTracker tracker, User user) throws IOException {
    requestCompactionInternal(region, why, priority, true, tracker, user);
  }

  public synchronized void requestCompaction(HRegion region, HStore store, String why, int priority,
      CompactionLifeCycleTracker tracker, User user) throws IOException {
    requestCompactionInternal(region, store, why, priority, true, tracker, user);
  }

  private void requestCompactionInternal(HRegion region, String why, int priority,
      boolean selectNow, CompactionLifeCycleTracker tracker, User user) throws IOException {
    // request compaction on all stores
    for (HStore store : region.stores.values()) {
      requestCompactionInternal(region, store, why, priority, selectNow, tracker, user);
    }
  }

  private void requestCompactionInternal(HRegion region, HStore store, String why, int priority,
      boolean selectNow, CompactionLifeCycleTracker tracker, User user) throws IOException {
    if (this.server.isStopped() || (region.getTableDescriptor() != null &&
        !region.getTableDescriptor().isCompactionEnabled())) {
      return;
    }
    Optional<CompactionContext> compaction;
    if (selectNow) {
      compaction = selectCompaction(region, store, priority, tracker, user);
      if (!compaction.isPresent()) {
        // message logged inside
        return;
      }
    } else {
      compaction = Optional.empty();
    }

    RegionServerSpaceQuotaManager spaceQuotaManager =
        this.server.getRegionServerSpaceQuotaManager();
    if (spaceQuotaManager != null &&
        spaceQuotaManager.areCompactionsDisabled(region.getTableDescriptor().getTableName())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Ignoring compaction request for " + region +
            " as an active space quota violation " + " policy disallows compactions.");
      }
      return;
    }

    ThreadPoolExecutor pool;
    if (selectNow) {
      // compaction.get is safe as we will just return if selectNow is true but no compaction is
      // selected
      pool = store.throttleCompaction(compaction.get().getRequest().getSize()) ? longCompactions
          : shortCompactions;
    } else {
      // We assume that most compactions are small. So, put system compactions into small
      // pool; we will do selection there, and move to large pool if necessary.
      pool = shortCompactions;
    }
    pool.execute(new CompactionRunner(store, region, compaction, pool, user));
    region.incrementCompactionsQueuedCount();
    if (LOG.isDebugEnabled()) {
      String type = (pool == shortCompactions) ? "Small " : "Large ";
      LOG.debug(type + "Compaction requested: " + (selectNow ? compaction.toString() : "system")
          + (why != null && !why.isEmpty() ? "; Because: " + why : "") + "; " + this);
    }
  }

  public synchronized void requestSystemCompaction(HRegion region, String why) throws IOException {
    requestCompactionInternal(region, why, Store.NO_PRIORITY, false,
      CompactionLifeCycleTracker.DUMMY, null);
  }

  public synchronized void requestSystemCompaction(HRegion region, HStore store, String why)
      throws IOException {
    requestCompactionInternal(region, store, why, Store.NO_PRIORITY, false,
      CompactionLifeCycleTracker.DUMMY, null);
  }

  private Optional<CompactionContext> selectCompaction(HRegion region, HStore store, int priority,
      CompactionLifeCycleTracker tracker, User user) throws IOException {
    Optional<CompactionContext> compaction = store.requestCompaction(priority, tracker, user);
    if (!compaction.isPresent() && LOG.isDebugEnabled() && region.getRegionInfo() != null) {
      LOG.debug("Not compacting " + region.getRegionInfo().getRegionNameAsString() +
          " because compaction request was cancelled");
    }
    return compaction;
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splits.shutdown();
    longCompactions.shutdown();
    shortCompactions.shutdown();
  }

  private void waitFor(ThreadPoolExecutor t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        LOG.info("Waiting for " + name + " to finish...");
        if (!done) {
          t.shutdownNow();
        }
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted waiting for " + name + " to finish...");
      }
    }
  }

  void join() {
    waitFor(splits, "Split Thread");
    waitFor(longCompactions, "Large Compaction Thread");
    waitFor(shortCompactions, "Small Compaction Thread");
  }

  /**
   * Returns the current size of the queue containing regions that are
   * processed.
   *
   * @return The current size of the regions queue.
   */
  public int getCompactionQueueSize() {
    return longCompactions.getQueue().size() + shortCompactions.getQueue().size();
  }

  public int getLargeCompactionQueueSize() {
    return longCompactions.getQueue().size();
  }


  public int getSmallCompactionQueueSize() {
    return shortCompactions.getQueue().size();
  }

  public int getSplitQueueSize() {
    return splits.getQueue().size();
  }

  private boolean shouldSplitRegion() {
    if(server.getNumberOfOnlineRegions() > 0.9*regionSplitLimit) {
      LOG.warn("Total number of regions is approaching the upper limit " + regionSplitLimit + ". "
          + "Please consider taking a look at http://hbase.apache.org/book.html#ops.regionmgt");
    }
    return (regionSplitLimit > server.getNumberOfOnlineRegions());
  }

  /**
   * @return the regionSplitLimit
   */
  public int getRegionSplitLimit() {
    return this.regionSplitLimit;
  }

  private static final Comparator<Runnable> COMPARATOR =
      new Comparator<Runnable>() {

    private int compare(CompactionRequest r1, CompactionRequest r2) {
      if (r1 == r2) {
        return 0; //they are the same request
      }
      // less first
      int cmp = Integer.compare(r1.getPriority(), r2.getPriority());
      if (cmp != 0) {
        return cmp;
      }
      cmp = Long.compare(r1.getSelectionNanoTime(), r2.getSelectionNanoTime());
      if (cmp != 0) {
        return cmp;
      }

      // break the tie based on hash code
      return System.identityHashCode(r1) - System.identityHashCode(r2);
    }

    @Override
    public int compare(Runnable r1, Runnable r2) {
      // CompactionRunner first
      if (r1 instanceof CompactionRunner) {
        if (!(r2 instanceof CompactionRunner)) {
          return -1;
        }
      } else {
        if (r2 instanceof CompactionRunner) {
          return 1;
        } else {
          // break the tie based on hash code
          return System.identityHashCode(r1) - System.identityHashCode(r2);
        }
      }
      CompactionRunner o1 = (CompactionRunner) r1;
      CompactionRunner o2 = (CompactionRunner) r2;
      // less first
      int cmp = Integer.compare(o1.queuedPriority, o2.queuedPriority);
      if (cmp != 0) {
        return cmp;
      }
      Optional<CompactionContext> c1 = o1.compaction;
      Optional<CompactionContext> c2 = o2.compaction;
      if (c1.isPresent()) {
        return c2.isPresent() ? compare(c1.get().getRequest(), c2.get().getRequest()) : -1;
      } else {
        return c2.isPresent() ? 1 : 0;
      }
    }
  };

  private final class CompactionRunner implements Runnable {
    private final HStore store;
    private final HRegion region;
    private final Optional<CompactionContext> compaction;
    private int queuedPriority;
    private ThreadPoolExecutor parent;
    private User user;
    private long time;

    public CompactionRunner(HStore store, HRegion region, Optional<CompactionContext> compaction,
        ThreadPoolExecutor parent, User user) {
      super();
      this.store = store;
      this.region = region;
      this.compaction = compaction;
      this.queuedPriority = compaction.isPresent() ? compaction.get().getRequest().getPriority()
          : store.getCompactPriority();
      this.parent = parent;
      this.user = user;
      this.time = System.currentTimeMillis();
    }

    @Override
    public String toString() {
      return compaction.map(c -> "Request = " + c.getRequest())
          .orElse("regionName = " + region.toString() + ", storeName = " + store.toString() +
              ", priority = " + queuedPriority + ", time = " + time);
    }

    private void doCompaction(User user) {
      CompactionContext c;
      // Common case - system compaction without a file selection. Select now.
      if (!compaction.isPresent()) {
        int oldPriority = this.queuedPriority;
        this.queuedPriority = this.store.getCompactPriority();
        if (this.queuedPriority > oldPriority) {
          // Store priority decreased while we were in queue (due to some other compaction?),
          // requeue with new priority to avoid blocking potential higher priorities.
          this.parent.execute(this);
          return;
        }
        Optional<CompactionContext> selected;
        try {
          selected = selectCompaction(this.region, this.store, queuedPriority,
            CompactionLifeCycleTracker.DUMMY, user);
        } catch (IOException ex) {
          LOG.error("Compaction selection failed " + this, ex);
          server.checkFileSystem();
          region.decrementCompactionsQueuedCount();
          return;
        }
        if (!selected.isPresent()) {
          region.decrementCompactionsQueuedCount();
          return; // nothing to do
        }
        c = selected.get();
        assert c.hasSelection();
        // Now see if we are in correct pool for the size; if not, go to the correct one.
        // We might end up waiting for a while, so cancel the selection.

        ThreadPoolExecutor pool =
            store.throttleCompaction(c.getRequest().getSize()) ? longCompactions : shortCompactions;

        // Long compaction pool can process small job
        // Short compaction pool should not process large job
        if (this.parent == shortCompactions && pool == longCompactions) {
          this.store.cancelRequestedCompaction(c);
          this.parent = pool;
          this.parent.execute(this);
          return;
        }
      } else {
        c = compaction.get();
      }
      // Finally we can compact something.
      assert c != null;

      c.getRequest().getTracker().beforeExecute(store);
      try {
        // Note: please don't put single-compaction logic here;
        //       put it into region/store/etc. This is CST logic.
        long start = EnvironmentEdgeManager.currentTime();
        boolean completed =
            region.compact(c, store, compactionThroughputController, user);
        long now = EnvironmentEdgeManager.currentTime();
        LOG.info(((completed) ? "Completed" : "Aborted") + " compaction: " +
              this + "; duration=" + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          // degenerate case: blocked regions require recursive enqueues
          if (store.getCompactPriority() <= 0) {
            requestSystemCompaction(region, store, "Recursive enqueue");
          } else {
            // see if the compaction has caused us to exceed max region size
            requestSplit(region);
          }
        }
      } catch (IOException ex) {
        IOException remoteEx =
            ex instanceof RemoteException ? ((RemoteException) ex).unwrapRemoteException() : ex;
        LOG.error("Compaction failed " + this, remoteEx);
        if (remoteEx != ex) {
          LOG.info("Compaction failed at original callstack: " + formatStackTrace(ex));
        }
        region.reportCompactionRequestFailure();
        server.checkFileSystem();
      } catch (Exception ex) {
        LOG.error("Compaction failed " + this, ex);
        region.reportCompactionRequestFailure();
        server.checkFileSystem();
      } finally {
        c.getRequest().getTracker().afterExecute(store);
        region.decrementCompactionsQueuedCount();
        LOG.debug("CompactSplitThread Status: " + CompactSplit.this);
      }
    }

    @Override
    public void run() {
      Preconditions.checkNotNull(server);
      if (server.isStopped()
          || (region.getTableDescriptor() != null && !region.getTableDescriptor().isCompactionEnabled())) {
        region.decrementCompactionsQueuedCount();
        return;
      }
      doCompaction(user);
    }

    private String formatStackTrace(Exception ex) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      pw.flush();
      return sw.toString();
    }
  }

  /**
   * Cleanup class to use when rejecting a compaction request from the queue.
   */
  private static class Rejection implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor pool) {
      if (runnable instanceof CompactionRunner) {
        CompactionRunner runner = (CompactionRunner) runnable;
        LOG.debug("Compaction Rejected: " + runner);
        runner.compaction.ifPresent(c -> runner.store.cancelRequestedCompaction(c));
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void onConfigurationChange(Configuration newConf) {
    // Check if number of large / small compaction threads has changed, and then
    // adjust the core pool size of the thread pools, by using the
    // setCorePoolSize() method. According to the javadocs, it is safe to
    // change the core pool size on-the-fly. We need to reset the maximum
    // pool size, as well.
    int largeThreads = Math.max(1, newConf.getInt(
            LARGE_COMPACTION_THREADS,
            LARGE_COMPACTION_THREADS_DEFAULT));
    if (this.longCompactions.getCorePoolSize() != largeThreads) {
      LOG.info("Changing the value of " + LARGE_COMPACTION_THREADS +
              " from " + this.longCompactions.getCorePoolSize() + " to " +
              largeThreads);
      if(this.longCompactions.getCorePoolSize() < largeThreads) {
        this.longCompactions.setMaximumPoolSize(largeThreads);
        this.longCompactions.setCorePoolSize(largeThreads);
      } else {
        this.longCompactions.setCorePoolSize(largeThreads);
        this.longCompactions.setMaximumPoolSize(largeThreads);
      }
    }

    int smallThreads = newConf.getInt(SMALL_COMPACTION_THREADS,
            SMALL_COMPACTION_THREADS_DEFAULT);
    if (this.shortCompactions.getCorePoolSize() != smallThreads) {
      LOG.info("Changing the value of " + SMALL_COMPACTION_THREADS +
                " from " + this.shortCompactions.getCorePoolSize() + " to " +
                smallThreads);
      if(this.shortCompactions.getCorePoolSize() < smallThreads) {
        this.shortCompactions.setMaximumPoolSize(smallThreads);
        this.shortCompactions.setCorePoolSize(smallThreads);
      } else {
        this.shortCompactions.setCorePoolSize(smallThreads);
        this.shortCompactions.setMaximumPoolSize(smallThreads);
      }
    }

    int splitThreads = newConf.getInt(SPLIT_THREADS,
            SPLIT_THREADS_DEFAULT);
    if (this.splits.getCorePoolSize() != splitThreads) {
      LOG.info("Changing the value of " + SPLIT_THREADS +
                " from " + this.splits.getCorePoolSize() + " to " +
                splitThreads);
      if(this.splits.getCorePoolSize() < splitThreads) {
        this.splits.setMaximumPoolSize(splitThreads);
        this.splits.setCorePoolSize(splitThreads);
      } else {
        this.splits.setCorePoolSize(splitThreads);
        this.splits.setMaximumPoolSize(splitThreads);
      }
    }

    ThroughputController old = this.compactionThroughputController;
    if (old != null) {
      old.stop("configuration change");
    }
    this.compactionThroughputController =
        CompactionThroughputControllerFactory.create(server, newConf);

    // We change this atomically here instead of reloading the config in order that upstream
    // would be the only one with the flexibility to reload the config.
    this.conf.reloadConfiguration();
  }

  protected int getSmallCompactionThreadNum() {
    return this.shortCompactions.getCorePoolSize();
  }

  protected int getLargeCompactionThreadNum() {
    return this.longCompactions.getCorePoolSize();
  }

  protected int getSplitThreadNum() {
    return this.splits.getCorePoolSize();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerChildren(ConfigurationManager manager) {
    // No children to register.
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    // No children to register
  }

  @VisibleForTesting
  public ThroughputController getCompactionThroughputController() {
    return compactionThroughputController;
  }

  @VisibleForTesting
  /**
   * Shutdown the long compaction thread pool.
   * Should only be used in unit test to prevent long compaction thread pool from stealing job
   * from short compaction queue
   */
  void shutdownLongCompactions(){
    this.longCompactions.shutdown();
  }

  public void clearLongCompactionsQueue() {
    longCompactions.getQueue().clear();
  }

  public void clearShortCompactionsQueue() {
    shortCompactions.getQueue().clear();
  }
}
