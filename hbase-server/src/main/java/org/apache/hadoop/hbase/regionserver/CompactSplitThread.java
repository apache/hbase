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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Compact region on request and then run split if appropriate
 */
@InterfaceAudience.Private
public class CompactSplitThread implements CompactionRequestor, PropagatingConfigurationObserver {
  private static final Log LOG = LogFactory.getLog(CompactSplitThread.class);

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
  
  // Configuration keys for merge threads
  public final static String MERGE_THREADS = "hbase.regionserver.thread.merge";
  public final static int MERGE_THREADS_DEFAULT = 1;

  public static final String REGION_SERVER_REGION_SPLIT_LIMIT =
      "hbase.regionserver.regionSplitLimit";
  public static final int DEFAULT_REGION_SERVER_REGION_SPLIT_LIMIT= 1000;
  
  private final HRegionServer server;
  private final Configuration conf;

  private final ThreadPoolExecutor longCompactions;
  private final ThreadPoolExecutor shortCompactions;
  private final ThreadPoolExecutor splits;
  private final ThreadPoolExecutor mergePool;

  private volatile CompactionThroughputController compactionThroughputController;

  /**
   * Splitting should not take place if the total number of regions exceed this.
   * This is not a hard limit to the number of regions but it is a guideline to
   * stop splitting after number of online regions is greater than this.
   */
  private int regionSplitLimit;

  /** @param server */
  CompactSplitThread(HRegionServer server) {
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

    StealJobQueue<Runnable> stealJobQueue = new StealJobQueue<>();
    this.longCompactions = new ThreadPoolExecutor(largeThreads, largeThreads,
        60, TimeUnit.SECONDS, stealJobQueue,
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-longCompactions-" + System.currentTimeMillis());
            return t;
          }
      });
    this.longCompactions.setRejectedExecutionHandler(new Rejection());
    this.longCompactions.prestartAllCoreThreads();
    this.shortCompactions = new ThreadPoolExecutor(smallThreads, smallThreads,
        60, TimeUnit.SECONDS, stealJobQueue.getStealFromQueue(),
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-shortCompactions-" + System.currentTimeMillis());
            return t;
          }
      });
    this.shortCompactions
        .setRejectedExecutionHandler(new Rejection());
    this.splits = (ThreadPoolExecutor)
        Executors.newFixedThreadPool(splitThreads,
            new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-splits-" + System.currentTimeMillis());
            return t;
          }
      });
    int mergeThreads = conf.getInt(MERGE_THREADS, MERGE_THREADS_DEFAULT);
    this.mergePool = (ThreadPoolExecutor) Executors.newFixedThreadPool(
        mergeThreads, new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-merges-" + System.currentTimeMillis());
            return t;
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
        + ", split_queue=" + splits.getQueue().size()
        + ", merge_queue=" + mergePool.getQueue().size();
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

    queueLists.append("\n");
    queueLists.append("  Region Merge Queue:\n");
    lq = mergePool.getQueue();
    it = lq.iterator();
    while (it.hasNext()) {
      queueLists.append("    " + it.next().toString());
      queueLists.append("\n");
    }

    return queueLists.toString();
  }

  public synchronized void requestRegionsMerge(final Region a,
      final Region b, final boolean forcible, long masterSystemTime, User user) {
    try {
      mergePool.execute(new RegionMergeRequest(a, b, this.server, forcible, masterSystemTime,user));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Region merge requested for " + a + "," + b + ", forcible="
            + forcible + ".  " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.warn("Could not execute merge for " + a + "," + b + ", forcible="
          + forcible, ree);
    }
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
        LOG.debug("Split requested for " + r + ".  " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + r, ree);
    }
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final Region r, final String why)
      throws IOException {
    return requestCompaction(r, why, null);
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final Region r, final String why,
      List<Pair<CompactionRequest, Store>> requests) throws IOException {
    return requestCompaction(r, why, Store.NO_PRIORITY, requests, null);
  }

  @Override
  public synchronized CompactionRequest requestCompaction(final Region r, final Store s,
      final String why, CompactionRequest request) throws IOException {
    return requestCompaction(r, s, why, Store.NO_PRIORITY, request, null);
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final Region r, final String why,
      int p, List<Pair<CompactionRequest, Store>> requests, User user) throws IOException {
    return requestCompactionInternal(r, why, p, requests, true, user);
  }

  private List<CompactionRequest> requestCompactionInternal(final Region r, final String why,
      int p, List<Pair<CompactionRequest, Store>> requests, boolean selectNow, User user)
          throws IOException {
    // not a special compaction request, so make our own list
    List<CompactionRequest> ret = null;
    if (requests == null) {
      ret = selectNow ? new ArrayList<CompactionRequest>(r.getStores().size()) : null;
      for (Store s : r.getStores()) {
        CompactionRequest cr = requestCompactionInternal(r, s, why, p, null, selectNow, user);
        if (selectNow) ret.add(cr);
      }
    } else {
      Preconditions.checkArgument(selectNow); // only system requests have selectNow == false
      ret = new ArrayList<CompactionRequest>(requests.size());
      for (Pair<CompactionRequest, Store> pair : requests) {
        ret.add(requestCompaction(r, pair.getSecond(), why, p, pair.getFirst(), user));
      }
    }
    return ret;
  }

  public CompactionRequest requestCompaction(final Region r, final Store s,
      final String why, int priority, CompactionRequest request, User user) throws IOException {
    return requestCompactionInternal(r, s, why, priority, request, true, user);
  }

  public synchronized void requestSystemCompaction(
      final Region r, final String why) throws IOException {
    requestCompactionInternal(r, why, Store.NO_PRIORITY, null, false, null);
  }

  public void requestSystemCompaction(
      final Region r, final Store s, final String why) throws IOException {
    requestCompactionInternal(r, s, why, Store.NO_PRIORITY, null, false, null);
  }

  /**
   * @param r region store belongs to
   * @param s Store to request compaction on
   * @param why Why compaction requested -- used in debug messages
   * @param priority override the default priority (NO_PRIORITY == decide)
   * @param request custom compaction request. Can be <tt>null</tt> in which case a simple
   *          compaction will be used.
   */
  private synchronized CompactionRequest requestCompactionInternal(final Region r, final Store s,
      final String why, int priority, CompactionRequest request, boolean selectNow, User user)
          throws IOException {
    if (this.server.isStopped()
        || (r.getTableDesc() != null && !r.getTableDesc().isCompactionEnabled())) {
      return null;
    }

    CompactionContext compaction = null;
    if (selectNow) {
      compaction = selectCompaction(r, s, priority, request, user);
      if (compaction == null) return null; // message logged inside
    }

    // We assume that most compactions are small. So, put system compactions into small
    // pool; we will do selection there, and move to large pool if necessary.
    ThreadPoolExecutor pool = (selectNow && s.throttleCompaction(compaction.getRequest().getSize()))
      ? longCompactions : shortCompactions;
    pool.execute(new CompactionRunner(s, r, compaction, pool, user));
    if (LOG.isDebugEnabled()) {
      String type = (pool == shortCompactions) ? "Small " : "Large ";
      LOG.debug(type + "Compaction requested: " + (selectNow ? compaction.toString() : "system")
          + (why != null && !why.isEmpty() ? "; Because: " + why : "") + "; " + this);
    }
    return selectNow ? compaction.getRequest() : null;
  }

  private CompactionContext selectCompaction(final Region r, final Store s,
      int priority, CompactionRequest request, User user) throws IOException {
    CompactionContext compaction = s.requestCompaction(priority, request, user);
    if (compaction == null) {
      if(LOG.isDebugEnabled() && r.getRegionInfo() != null) {
        LOG.debug("Not compacting " + r.getRegionInfo().getRegionNameAsString() +
            " because compaction request was cancelled");
      }
      return null;
    }
    assert compaction.hasSelection();
    if (priority != Store.NO_PRIORITY) {
      compaction.getRequest().setPriority(priority);
    }
    return compaction;
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splits.shutdown();
    mergePool.shutdown();
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
    waitFor(mergePool, "Merge Thread");
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

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="EQ_COMPARETO_USE_OBJECT_EQUALS",
      justification="Contrived use of compareTo")
  private class CompactionRunner implements Runnable, Comparable<CompactionRunner> {
    private final Store store;
    private final HRegion region;
    private CompactionContext compaction;
    private int queuedPriority;
    private ThreadPoolExecutor parent;
    private User user;

    public CompactionRunner(Store store, Region region,
        CompactionContext compaction, ThreadPoolExecutor parent, User user) {
      super();
      this.store = store;
      this.region = (HRegion)region;
      this.compaction = compaction;
      this.queuedPriority = (this.compaction == null)
          ? store.getCompactPriority() : compaction.getRequest().getPriority();
      this.parent = parent;
      this.user = user;
    }

    @Override
    public String toString() {
      return (this.compaction != null) ? ("Request = " + compaction.getRequest())
          : ("Store = " + store.toString() + ", pri = " + queuedPriority);
    }

    private void doCompaction(User user) {
      // Common case - system compaction without a file selection. Select now.
      if (this.compaction == null) {
        int oldPriority = this.queuedPriority;
        this.queuedPriority = this.store.getCompactPriority();
        if (this.queuedPriority > oldPriority) {
          // Store priority decreased while we were in queue (due to some other compaction?),
          // requeue with new priority to avoid blocking potential higher priorities.
          this.parent.execute(this);
          return;
        }
        try {
          this.compaction = selectCompaction(this.region, this.store, queuedPriority, null, user);
        } catch (IOException ex) {
          LOG.error("Compaction selection failed " + this, ex);
          server.checkFileSystem();
          return;
        }
        if (this.compaction == null) return; // nothing to do
        // Now see if we are in correct pool for the size; if not, go to the correct one.
        // We might end up waiting for a while, so cancel the selection.
        assert this.compaction.hasSelection();
        ThreadPoolExecutor pool = store.throttleCompaction(
            compaction.getRequest().getSize()) ? longCompactions : shortCompactions;

        // Long compaction pool can process small job
        // Short compaction pool should not process large job
        if (this.parent == shortCompactions && pool == longCompactions) {
          this.store.cancelRequestedCompaction(this.compaction);
          this.compaction = null;
          this.parent = pool;
          this.parent.execute(this);
          return;
        }
      }
      // Finally we can compact something.
      assert this.compaction != null;

      this.compaction.getRequest().beforeExecute();
      try {
        // Note: please don't put single-compaction logic here;
        //       put it into region/store/etc. This is CST logic.
        long start = EnvironmentEdgeManager.currentTime();
        boolean completed =
            region.compact(compaction, store, compactionThroughputController, user);
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
        server.checkFileSystem();
      } catch (Exception ex) {
        LOG.error("Compaction failed " + this, ex);
        server.checkFileSystem();
      } finally {
        LOG.debug("CompactSplitThread Status: " + CompactSplitThread.this);
      }
      this.compaction.getRequest().afterExecute();
    }

    @Override
    public void run() {
      Preconditions.checkNotNull(server);
      if (server.isStopped()
          || (region.getTableDesc() != null && !region.getTableDesc().isCompactionEnabled())) {
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

    @Override
    public int compareTo(CompactionRunner o) {
      // Only compare the underlying request (if any), for queue sorting purposes.
      int compareVal = queuedPriority - o.queuedPriority; // compare priority
      if (compareVal != 0) return compareVal;
      CompactionContext tc = this.compaction, oc = o.compaction;
      // Sort pre-selected (user?) compactions before system ones with equal priority.
      return (tc == null) ? ((oc == null) ? 0 : 1)
          : ((oc == null) ? -1 : tc.getRequest().compareTo(oc.getRequest()));
    }
  }

  /**
   * Cleanup class to use when rejecting a compaction request from the queue.
   */
  private static class Rejection implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor pool) {
      if (runnable instanceof CompactionRunner) {
        CompactionRunner runner = (CompactionRunner)runnable;
        LOG.debug("Compaction Rejected: " + runner);
        runner.store.cancelRequestedCompaction(runner.compaction);
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

    int mergeThreads = newConf.getInt(MERGE_THREADS,
            MERGE_THREADS_DEFAULT);
    if (this.mergePool.getCorePoolSize() != mergeThreads) {
      LOG.info("Changing the value of " + MERGE_THREADS +
                " from " + this.mergePool.getCorePoolSize() + " to " +
                mergeThreads);
      if(this.mergePool.getCorePoolSize() < mergeThreads) {
        this.mergePool.setMaximumPoolSize(mergeThreads);
        this.mergePool.setCorePoolSize(mergeThreads);
      } else {
        this.mergePool.setCorePoolSize(mergeThreads);
        this.mergePool.setMaximumPoolSize(mergeThreads);
      }
    }

    CompactionThroughputController old = this.compactionThroughputController;
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

  protected int getMergeThreadNum() {
    return this.mergePool.getCorePoolSize();
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
  public CompactionThroughputController getCompactionThroughputController() {
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
}
