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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;

import com.google.common.base.Preconditions;

/**
 * Compact region on request and then run split if appropriate
 */
@InterfaceAudience.Private
public class CompactSplitThread implements CompactionRequestor {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);

  private final HRegionServer server;
  private final Configuration conf;

  private final ThreadPoolExecutor largeCompactions;
  private final ThreadPoolExecutor smallCompactions;
  private final ThreadPoolExecutor splits;

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
    this.regionSplitLimit = conf.getInt("hbase.regionserver.regionSplitLimit",
        Integer.MAX_VALUE);

    int largeThreads = Math.max(1, conf.getInt(
        "hbase.regionserver.thread.compaction.large", 1));
    int smallThreads = conf.getInt(
        "hbase.regionserver.thread.compaction.small", 1);

    int splitThreads = conf.getInt("hbase.regionserver.thread.split", 1);

    // if we have throttle threads, make sure the user also specified size
    Preconditions.checkArgument(largeThreads > 0 && smallThreads > 0);

    final String n = Thread.currentThread().getName();

    this.largeCompactions = new ThreadPoolExecutor(largeThreads, largeThreads,
        60, TimeUnit.SECONDS, new PriorityBlockingQueue<Runnable>(),
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-largeCompactions-" + System.currentTimeMillis());
            return t;
          }
      });
    this.largeCompactions.setRejectedExecutionHandler(new Rejection());
    this.smallCompactions = new ThreadPoolExecutor(smallThreads, smallThreads,
        60, TimeUnit.SECONDS, new PriorityBlockingQueue<Runnable>(),
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(n + "-smallCompactions-" + System.currentTimeMillis());
            return t;
          }
      });
    this.smallCompactions
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
  }

  @Override
  public String toString() {
    return "compaction_queue=("
        + largeCompactions.getQueue().size() + ":"
        + smallCompactions.getQueue().size() + ")"
        + ", split_queue=" + splits.getQueue().size();
  }
  
  public String dumpQueue() {
    StringBuffer queueLists = new StringBuffer();
    queueLists.append("Compaction/Split Queue dump:\n");
    queueLists.append("  LargeCompation Queue:\n");
    BlockingQueue<Runnable> lq = largeCompactions.getQueue();
    Iterator it = lq.iterator();
    while(it.hasNext()){
      queueLists.append("    "+it.next().toString());
      queueLists.append("\n");
    }
    
    if( smallCompactions != null ){
      queueLists.append("\n");
      queueLists.append("  SmallCompation Queue:\n");
      lq = smallCompactions.getQueue();
      it = lq.iterator();
      while(it.hasNext()){
        queueLists.append("    "+it.next().toString());
        queueLists.append("\n");
      }
    }
    
    queueLists.append("\n");
    queueLists.append("  Split Queue:\n");
    lq = splits.getQueue();
    it = lq.iterator();
    while(it.hasNext()){
      queueLists.append("    "+it.next().toString());
      queueLists.append("\n");
    }
    
    return queueLists.toString();
  }

  public synchronized boolean requestSplit(final HRegion r) {
    // don't split regions that are blocking
    if (shouldSplitRegion() && r.getCompactPriority() >= Store.PRIORITY_USER) {
      byte[] midKey = r.checkSplit();
      if (midKey != null) {
        requestSplit(r, midKey);
        return true;
      }
    }
    return false;
  }

  public synchronized void requestSplit(final HRegion r, byte[] midKey) {
    if (midKey == null) {
      LOG.debug("Region " + r.getRegionNameAsString() +
        " not splittable because midkey=null");
      return;
    }
    try {
      this.splits.execute(new SplitRequest(r, midKey, this.server));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Split requested for " + r + ".  " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + r, ree);
    }
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final HRegion r, final String why)
      throws IOException {
    return requestCompaction(r, why, null);
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final HRegion r, final String why,
      List<Pair<CompactionRequest, Store>> requests) throws IOException {
    return requestCompaction(r, why, Store.NO_PRIORITY, requests);
  }

  @Override
  public synchronized CompactionRequest requestCompaction(final HRegion r, final Store s,
      final String why, CompactionRequest request) throws IOException {
    return requestCompaction(r, s, why, Store.NO_PRIORITY, request);
  }

  @Override
  public synchronized List<CompactionRequest> requestCompaction(final HRegion r, final String why,
      int p, List<Pair<CompactionRequest, Store>> requests) throws IOException {
    // not a special compaction request, so make our own list
    List<CompactionRequest> ret;
    if (requests == null) {
      ret = new ArrayList<CompactionRequest>(r.getStores().size());
      for (Store s : r.getStores().values()) {
        ret.add(requestCompaction(r, s, why, p, null));
      }
    } else {
      ret = new ArrayList<CompactionRequest>(requests.size());
      for (Pair<CompactionRequest, Store> pair : requests) {
        ret.add(requestCompaction(r, pair.getSecond(), why, p, pair.getFirst()));
      }
    }
    return ret;
  }

  /**
   * @param r HRegion store belongs to
   * @param s Store to request compaction on
   * @param why Why compaction requested -- used in debug messages
   * @param priority override the default priority (NO_PRIORITY == decide)
   * @param request custom compaction request. Can be <tt>null</tt> in which case a simple
   *          compaction will be used.
   */
  public synchronized CompactionRequest requestCompaction(final HRegion r, final Store s,
      final String why, int priority, CompactionRequest request) throws IOException {
    if (this.server.isStopped()) {
      return null;
    }
    CompactionContext compaction = s.requestCompaction(priority, request);
    if (compaction == null) {
      if(LOG.isDebugEnabled()) {
        LOG.debug("Not compacting " + r.getRegionNameAsString() + 
            " because compaction request was cancelled");
      }
      return null;
    }

    assert compaction.hasSelection();
    if (priority != Store.NO_PRIORITY) {
      compaction.getRequest().setPriority(priority);
    }
    ThreadPoolExecutor pool = s.throttleCompaction(compaction.getRequest().getSize())
      ? largeCompactions : smallCompactions;
    pool.execute(new CompactionRunner(s, r, compaction));
    if (LOG.isDebugEnabled()) {
      String type = (pool == smallCompactions) ? "Small " : "Large ";
      LOG.debug(type + "Compaction requested: " + compaction
          + (why != null && !why.isEmpty() ? "; Because: " + why : "")
          + "; " + this);
    }
    return compaction.getRequest();
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splits.shutdown();
    largeCompactions.shutdown();
    smallCompactions.shutdown();
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
    waitFor(largeCompactions, "Large Compaction Thread");
    waitFor(smallCompactions, "Small Compaction Thread");
  }

  /**
   * Returns the current size of the queue containing regions that are
   * processed.
   *
   * @return The current size of the regions queue.
   */
  public int getCompactionQueueSize() {
    return largeCompactions.getQueue().size() + smallCompactions.getQueue().size();
  }

  private boolean shouldSplitRegion() {
    return (regionSplitLimit > server.getNumberOfOnlineRegions());
  }

  /**
   * @return the regionSplitLimit
   */
  public int getRegionSplitLimit() {
    return this.regionSplitLimit;
  }

  private class CompactionRunner implements Runnable, Comparable<CompactionRunner> {
    private final Store store;
    private final HRegion region;
    private final CompactionContext compaction;

    public CompactionRunner(Store store, HRegion region, CompactionContext compaction) {
      super();
      this.store = store;
      this.region = region;
      this.compaction = compaction;
    }

    @Override
    public void run() {
      Preconditions.checkNotNull(server);
      if (server.isStopped()) {
        return;
      }
      this.compaction.getRequest().beforeExecute();
      try {
        // Note: please don't put single-compaction logic here;
        //       put it into region/store/etc. This is CST logic.
        long start = EnvironmentEdgeManager.currentTimeMillis();
        boolean completed = region.compact(compaction, store);
        long now = EnvironmentEdgeManager.currentTimeMillis();
        LOG.info(((completed) ? "Completed" : "Aborted") + " compaction: " +
              this + "; duration=" + StringUtils.formatTimeDiff(now, start));
        if (completed) {
          // degenerate case: blocked regions require recursive enqueues
          if (store.getCompactPriority() <= 0) {
            requestCompaction(region, store, "Recursive enqueue", null);
          } else {
            // see if the compaction has caused us to exceed max region size
            requestSplit(region);
          }
        }
      } catch (IOException ex) {
        LOG.error("Compaction failed " + this, RemoteExceptionHandler.checkIOException(ex));
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
    public int compareTo(CompactionRunner o) {
      // Only compare the underlying request, for queue sorting purposes.
      return this.compaction.getRequest().compareTo(o.compaction.getRequest());
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
}
