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

import static org.apache.hadoop.hbase.regionserver.Store.NO_PRIORITY;
import static org.apache.hadoop.hbase.regionserver.Store.PRIORITY_USER;

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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.quotas.RegionServerSpaceQuotaManager;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Compact region on request and then run split if appropriate
 */
@InterfaceAudience.Private
public class CompactSplit implements CompactionRequester, PropagatingConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(CompactSplit.class);

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
  public static final String HBASE_REGION_SERVER_ENABLE_COMPACTION =
      "hbase.regionserver.compaction.enabled";

  private final HRegionServer server;
  private final Configuration conf;
  private volatile ThreadPoolExecutor longCompactions;
  private volatile ThreadPoolExecutor shortCompactions;
  private volatile ThreadPoolExecutor splits;

  private volatile ThroughputController compactionThroughputController;

  private volatile boolean compactionsEnabled;
  /**
   * Splitting should not take place if the total number of regions exceed this.
   * This is not a hard limit to the number of regions but it is a guideline to
   * stop splitting after number of online regions is greater than this.
   */
  private int regionSplitLimit;

  CompactSplit(HRegionServer server) {
    this.server = server;
    this.conf = server.getConfiguration();
    this.compactionsEnabled = this.conf.getBoolean(HBASE_REGION_SERVER_ENABLE_COMPACTION,true);
    createCompactionExecutors();
    createSplitExcecutors();

    // compaction throughput controller
    this.compactionThroughputController =
        CompactionThroughputControllerFactory.create(server, conf);
  }

  private void createSplitExcecutors() {
    final String n = Thread.currentThread().getName();
    int splitThreads = conf.getInt(SPLIT_THREADS, SPLIT_THREADS_DEFAULT);
    this.splits = (ThreadPoolExecutor) Executors.newFixedThreadPool(splitThreads,
      new ThreadFactoryBuilder().setNameFormat(n + "-splits-%d").setDaemon(true).build());
  }

  private void createCompactionExecutors() {
    this.regionSplitLimit =
        conf.getInt(REGION_SERVER_REGION_SPLIT_LIMIT, DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT);

    int largeThreads =
        Math.max(1, conf.getInt(LARGE_COMPACTION_THREADS, LARGE_COMPACTION_THREADS_DEFAULT));
    int smallThreads = conf.getInt(SMALL_COMPACTION_THREADS, SMALL_COMPACTION_THREADS_DEFAULT);

    // if we have throttle threads, make sure the user also specified size
    Preconditions.checkArgument(largeThreads > 0 && smallThreads > 0);

    final String n = Thread.currentThread().getName();

    StealJobQueue<Runnable> stealJobQueue = new StealJobQueue<Runnable>(COMPARATOR);
    this.longCompactions = new ThreadPoolExecutor(largeThreads, largeThreads, 60, TimeUnit.SECONDS,
        stealJobQueue, new ThreadFactoryBuilder().setNameFormat(n + "-longCompactions-%d")
            .setDaemon(true).build());
    this.longCompactions.setRejectedExecutionHandler(new Rejection());
    this.longCompactions.prestartAllCoreThreads();
    this.shortCompactions = new ThreadPoolExecutor(smallThreads, smallThreads, 60, TimeUnit.SECONDS,
        stealJobQueue.getStealFromQueue(), new ThreadFactoryBuilder()
            .setNameFormat(n + "-shortCompactions-%d").setDaemon(true).build());
    this.shortCompactions.setRejectedExecutionHandler(new Rejection());
  }

  @Override
  public String toString() {
    return "compactionQueue=(longCompactions="
        + longCompactions.getQueue().size() + ":shortCompactions="
        + shortCompactions.getQueue().size() + ")"
        + ", splitQueue=" + splits.getQueue().size();
  }

  public String dumpQueue() {
    StringBuilder queueLists = new StringBuilder();
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
    // Don't split regions that are blocking is the default behavior.
    // But in some circumstances, split here is needed to prevent the region size from
    // continuously growing, as well as the number of store files, see HBASE-26242.
    HRegion hr = (HRegion)r;
    try {
      if (shouldSplitRegion() && hr.getCompactPriority() >= PRIORITY_USER) {
        byte[] midKey = hr.checkSplit().orElse(null);
        if (midKey != null) {
          requestSplit(r, midKey);
          return true;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      // We get this sometimes. Not sure why. Catch and return false; no split request.
      LOG.warn("Catching out-of-bounds; region={}, policy={}", hr == null? null: hr.getRegionInfo(),
        hr == null? "null": hr.getCompactPriority(), e);
    }
    return false;
  }

  private synchronized void requestSplit(final Region r, byte[] midKey) {
    requestSplit(r, midKey, null);
  }

  /*
   * The User parameter allows the split thread to assume the correct user identity
   */
  private synchronized void requestSplit(final Region r, byte[] midKey, User user) {
    if (midKey == null) {
      LOG.debug("Region " + r.getRegionInfo().getRegionNameAsString() +
        " not splittable because midkey=null");
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

  private void interrupt() {
    longCompactions.shutdownNow();
    shortCompactions.shutdownNow();
  }

  private void reInitializeCompactionsExecutors() {
    createCompactionExecutors();
  }

  private interface CompactionCompleteTracker {

    default void completed(Store store) {
    }
  }

  private static final CompactionCompleteTracker DUMMY_COMPLETE_TRACKER =
    new CompactionCompleteTracker() {};

  private static final class AggregatingCompleteTracker implements CompactionCompleteTracker {

    private final CompactionLifeCycleTracker tracker;

    private final AtomicInteger remaining;

    public AggregatingCompleteTracker(CompactionLifeCycleTracker tracker, int numberOfStores) {
      this.tracker = tracker;
      this.remaining = new AtomicInteger(numberOfStores);
    }

    @Override
    public void completed(Store store) {
      if (remaining.decrementAndGet() == 0) {
        tracker.completed();
      }
    }
  }

  private CompactionCompleteTracker getCompleteTracker(CompactionLifeCycleTracker tracker,
      IntSupplier numberOfStores) {
    if (tracker == CompactionLifeCycleTracker.DUMMY) {
      // a simple optimization to avoid creating unnecessary objects as usually we do not care about
      // the life cycle of a compaction.
      return DUMMY_COMPLETE_TRACKER;
    } else {
      return new AggregatingCompleteTracker(tracker, numberOfStores.getAsInt());
    }
  }

  @Override
  public synchronized void requestCompaction(HRegion region, String why, int priority,
      CompactionLifeCycleTracker tracker, User user) throws IOException {
    requestCompactionInternal(region, why, priority, true, tracker,
      getCompleteTracker(tracker, () -> region.getTableDescriptor().getColumnFamilyCount()), user);
  }

  @Override
  public synchronized void requestCompaction(HRegion region, HStore store, String why, int priority,
      CompactionLifeCycleTracker tracker, User user) throws IOException {
    requestCompactionInternal(region, store, why, priority, true, tracker,
      getCompleteTracker(tracker, () -> 1), user);
  }

  @Override
  public void switchCompaction(boolean onOrOff) {
    if (onOrOff) {
      // re-create executor pool if compactions are disabled.
      if (!isCompactionsEnabled()) {
        LOG.info("Re-Initializing compactions because user switched on compactions");
        reInitializeCompactionsExecutors();
      }
    } else {
      LOG.info("Interrupting running compactions because user switched off compactions");
      interrupt();
    }
    setCompactionsEnabled(onOrOff);
  }

  private void requestCompactionInternal(HRegion region, String why, int priority,
      boolean selectNow, CompactionLifeCycleTracker tracker,
      CompactionCompleteTracker completeTracker, User user) throws IOException {
    // request compaction on all stores
    for (HStore store : region.stores.values()) {
      requestCompactionInternal(region, store, why, priority, selectNow, tracker, completeTracker,
        user);
    }
  }

  private void requestCompactionInternal(HRegion region, HStore store, String why, int priority,
      boolean selectNow, CompactionLifeCycleTracker tracker,
      CompactionCompleteTracker completeTracker, User user) throws IOException {
    if (this.server.isStopped() || (region.getTableDescriptor() != null &&
        !region.getTableDescriptor().isCompactionEnabled())) {
      return;
    }
    RegionServerSpaceQuotaManager spaceQuotaManager =
        this.server.getRegionServerSpaceQuotaManager();

    if (user != null && !Superusers.isSuperUser(user) && spaceQuotaManager != null
        && spaceQuotaManager.areCompactionsDisabled(region.getTableDescriptor().getTableName())) {
      // Enter here only when:
      // It's a user generated req, the user is super user, quotas enabled, compactions disabled.
      String reason = "Ignoring compaction request for " + region +
          " as an active space quota violation " + " policy disallows compactions.";
      tracker.notExecuted(store, reason);
      completeTracker.completed(store);
      LOG.debug(reason);
      return;
    }

    CompactionContext compaction;
    if (selectNow) {
      Optional<CompactionContext> c =
        selectCompaction(region, store, priority, tracker, completeTracker, user);
      if (!c.isPresent()) {
        // message logged inside
        return;
      }
      compaction = c.get();
    } else {
      compaction = null;
    }

    ThreadPoolExecutor pool;
    if (selectNow) {
      // compaction.get is safe as we will just return if selectNow is true but no compaction is
      // selected
      pool = store.throttleCompaction(compaction.getRequest().getSize()) ? longCompactions
          : shortCompactions;
    } else {
      // We assume that most compactions are small. So, put system compactions into small
      // pool; we will do selection there, and move to large pool if necessary.
      pool = shortCompactions;
    }
    pool.execute(
      new CompactionRunner(store, region, compaction, tracker, completeTracker, pool, user));
    region.incrementCompactionsQueuedCount();
    if (LOG.isDebugEnabled()) {
      String type = (pool == shortCompactions) ? "Small " : "Large ";
      LOG.debug(type + "Compaction requested: " + (selectNow ? compaction.toString() : "system")
          + (why != null && !why.isEmpty() ? "; Because: " + why : "") + "; " + this);
    }
  }

  public synchronized void requestSystemCompaction(HRegion region, String why) throws IOException {
    requestCompactionInternal(region, why, NO_PRIORITY, false, CompactionLifeCycleTracker.DUMMY,
      DUMMY_COMPLETE_TRACKER, null);
  }

  public synchronized void requestSystemCompaction(HRegion region, HStore store, String why)
      throws IOException {
    requestCompactionInternal(region, store, why, NO_PRIORITY, false,
      CompactionLifeCycleTracker.DUMMY, DUMMY_COMPLETE_TRACKER, null);
  }

  private Optional<CompactionContext> selectCompaction(HRegion region, HStore store, int priority,
      CompactionLifeCycleTracker tracker, CompactionCompleteTracker completeTracker, User user)
      throws IOException {
    // don't even select for compaction if disableCompactions is set to true
    if (!isCompactionsEnabled()) {
      LOG.info(String.format("User has disabled compactions"));
      return Optional.empty();
    }
    Optional<CompactionContext> compaction = store.requestCompaction(priority, tracker, user);
    if (!compaction.isPresent() && region.getRegionInfo() != null) {
      String reason = "Not compacting " + region.getRegionInfo().getRegionNameAsString() +
          " because compaction request was cancelled";
      tracker.notExecuted(store, reason);
      completeTracker.completed(store);
      LOG.debug(reason);
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
        t.shutdownNow();
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
    if (server.getNumberOfOnlineRegions() > 0.9 * regionSplitLimit) {
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

    private int compare(CompactionRequestImpl r1, CompactionRequestImpl r2) {
      if (r1 == r2) {
        return 0; //they are the same request
      }
      // less first
      int cmp = Integer.compare(r1.getPriority(), r2.getPriority());
      if (cmp != 0) {
        return cmp;
      }
      cmp = Long.compare(r1.getSelectionTime(), r2.getSelectionTime());
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
      CompactionContext c1 = o1.compaction;
      CompactionContext c2 = o2.compaction;
      if (c1 != null) {
        return c2 != null ? compare(c1.getRequest(), c2.getRequest()) : -1;
      } else {
        return c2 != null ? 1 : 0;
      }
    }
  };

  private final class CompactionRunner implements Runnable {
    private final HStore store;
    private final HRegion region;
    private final CompactionContext compaction;
    private final CompactionLifeCycleTracker tracker;
    private final CompactionCompleteTracker completeTracker;
    private int queuedPriority;
    private ThreadPoolExecutor parent;
    private User user;
    private long time;

    public CompactionRunner(HStore store, HRegion region, CompactionContext compaction,
        CompactionLifeCycleTracker tracker, CompactionCompleteTracker completeTracker,
        ThreadPoolExecutor parent, User user) {
      this.store = store;
      this.region = region;
      this.compaction = compaction;
      this.tracker = tracker;
      this.completeTracker = completeTracker;
      this.queuedPriority =
          compaction != null ? compaction.getRequest().getPriority() : store.getCompactPriority();
      this.parent = parent;
      this.user = user;
      this.time = EnvironmentEdgeManager.currentTime();
    }

    @Override
    public String toString() {
      if (compaction != null) {
        return "Request=" + compaction.getRequest();
      } else {
        return "region=" + region.toString() + ", storeName=" + store.toString() +
            ", priority=" + queuedPriority + ", startTime=" + time;
      }
    }

    private void doCompaction(User user) {
      CompactionContext c;
      // Common case - system compaction without a file selection. Select now.
      if (compaction == null) {
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
          selected = selectCompaction(this.region, this.store, queuedPriority, tracker,
            completeTracker, user);
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
        c = compaction;
      }
      // Finally we can compact something.
      assert c != null;

      tracker.beforeExecution(store);
      try {
        // Note: please don't put single-compaction logic here;
        //       put it into region/store/etc. This is CST logic.
        long start = EnvironmentEdgeManager.currentTime();
        boolean completed =
            region.compact(c, store, compactionThroughputController, user);
        long now = EnvironmentEdgeManager.currentTime();
        LOG.info(((completed) ? "Completed" : "Aborted") + " compaction " +
              this + "; duration=" + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          // degenerate case: blocked regions require recursive enqueues
          if (region.getCompactPriority() < Store.PRIORITY_USER
            && store.getCompactPriority() <= 0) {
            requestSystemCompaction(region, store, "Recursive enqueue");
          } else {
            // see if the compaction has caused us to exceed max region size
            if (!requestSplit(region) && store.getCompactPriority() <= 0) {
              requestSystemCompaction(region, store, "Recursive enqueue");
            }
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
        tracker.afterExecution(store);
        completeTracker.completed(store);
        region.decrementCompactionsQueuedCount();
        LOG.debug("Status {}", CompactSplit.this);
      }
    }

    @Override
    public void run() {
      Preconditions.checkNotNull(server);
      if (server.isStopped() || (region.getTableDescriptor() != null &&
          !region.getTableDescriptor().isCompactionEnabled())) {
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
        if (runner.compaction != null) {
          runner.store.cancelRequestedCompaction(runner.compaction);
        }
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

  public ThroughputController getCompactionThroughputController() {
    return compactionThroughputController;
  }

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

  public boolean isCompactionsEnabled() {
    return compactionsEnabled;
  }

  public void setCompactionsEnabled(boolean compactionsEnabled) {
    this.compactionsEnabled = compactionsEnabled;
    this.conf.set(HBASE_REGION_SERVER_ENABLE_COMPACTION,String.valueOf(compactionsEnabled));
  }

  /**
   * @return the longCompactions thread pool executor
   */
  ThreadPoolExecutor getLongCompactions() {
    return longCompactions;
  }

  /**
   * @return the shortCompactions thread pool executor
   */
  ThreadPoolExecutor getShortCompactions() {
    return shortCompactions;
  }

}
