/**
 * Copyright 2010 The Apache Software Foundation
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

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Compact region on request and then run split if appropriate
 */
public class CompactSplitThread implements ConfigurationObserver {
  static final Log LOG = LogFactory.getLog(CompactSplitThread.class);

  private final HRegionServer server;
  private Configuration conf;

  private final ThreadPoolExecutor largeCompactions;
  private final ThreadPoolExecutor smallCompactions;
  private final ThreadPoolExecutor splits;

  /* The default priority for user-specified compaction requests.
   * The user gets top priority unless we have blocking compactions (Pri <= 0)
   */
  public static final int PRIORITY_USER = 1;
  public static final int NO_PRIORITY = Integer.MIN_VALUE;

  /** @param server */
  CompactSplitThread(HRegionServer server) {
    super();
    this.server = server;
    this.conf = server.conf;
    Preconditions.checkArgument(this.server != null && this.conf != null);

    int largeThreads = Math.max(1, conf.getInt(
            HConstants.LARGE_COMPACTION_THREADS,
            HConstants.DEFAULT_LARGE_COMPACTION_THREADS));
    int smallThreads = conf.getInt(HConstants.SMALL_COMPACTION_THREADS,
            HConstants.DEFAULT_SMALL_COMPACTION_THREADS);
    int splitThreads = conf.getInt(HConstants.SPLIT_THREADS,
            HConstants.DEFAULT_SPLIT_THREADS);

    Preconditions.checkArgument(largeThreads > 0 && smallThreads > 0);

    CompactionRequest.Rejection rejectionHandler = new CompactionRequest.Rejection();
    this.largeCompactions = new ThreadPoolExecutor(largeThreads, largeThreads,
        60, TimeUnit.SECONDS, new PriorityBlockingQueue<Runnable>(),
        new DaemonThreadFactory("large-compaction-thread-"), rejectionHandler);
    this.smallCompactions = new ThreadPoolExecutor(smallThreads, smallThreads,
        60, TimeUnit.SECONDS, new PriorityBlockingQueue<Runnable>(),
        new DaemonThreadFactory("small-compaction-thread-"), rejectionHandler);

    this.splits = (ThreadPoolExecutor) Executors
        .newFixedThreadPool(splitThreads);
  }

  @Override
  public String toString() {
    return "compaction_queue="
        + (smallCompactions != null ? "("
            + largeCompactions.getQueue().size() + ":"
            + smallCompactions.getQueue().size() + ")"
            : largeCompactions.getQueue().size())
        + ", split_queue=" + splits.getQueue().size();
  }

  public synchronized boolean requestSplit(final HRegion r) {
    // don't split regions that are blocking
    if (r.getCompactPriority() >= PRIORITY_USER) {
      byte[] midKey = r.checkSplit();
      if (midKey != null) {
        requestSplit(r, midKey);
        return true;
      }
    }
    return false;
  }

  public synchronized void requestSplit(final HRegion r, byte[] midKey) {
    try {
      this.splits.execute(new SplitRequest(r, midKey));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Split requested for " + r + ".  " + this);
      }
    } catch (RejectedExecutionException ree) {
      LOG.info("Could not execute split for " + r, ree);
    }
  }

  /**
   * @param r HRegion store belongs to
   * @param why Why compaction requested -- used in debug messages
   */
  public synchronized void requestCompaction(final HRegion r,
      final String why) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, why, NO_PRIORITY);
    }
  }

  public synchronized void requestCompaction(final HRegion r, final Store s,
      final String why) {
    requestCompaction(r, s, why, NO_PRIORITY);
  }

  public synchronized void requestCompaction(final HRegion r, final String why,
      int p) {
    for(Store s : r.getStores().values()) {
      requestCompaction(r, s, why, p);
    }
  }
  
  /**
   * @param r HRegion store belongs to
   * @param s Store to request compaction on
   * @param why Why compaction requested -- used in debug messages
   * @param priority override the default priority (NO_PRIORITY == decide)
   */
  public synchronized void requestCompaction(final HRegion r, final Store s,
      final String why, int priority) {

    if (this.server.stopRequestedAtStageOne.get()) {
      return;
    }

    CompactionRequest cr = s.requestCompaction();
    if (cr != null) {
      cr.setServer(this.server);
      if (priority != NO_PRIORITY) {
        cr.setPriority(priority);
      }
      // smallCompactions: like the 10 items or less line at Walmart
      ThreadPoolExecutor pool = s.throttleCompaction(cr.getSize())
        ? largeCompactions : smallCompactions;
      pool.execute(cr);
      if (LOG.isDebugEnabled()) {
        LOG.debug((pool == smallCompactions) ? "Small " : "Large "
            + "Compaction requested: " + cr
            + (why != null && !why.isEmpty() ? "; Because: " + why : "")
            + "; " + this);
      }
    }
  }

  /**
   * Only interrupt once it's done with a run through the work loop.
   */
  void interruptIfNecessary() {
    splits.shutdown();
    largeCompactions.shutdown();
    if (smallCompactions != null)
      smallCompactions.shutdown();
  }

  private void waitFor(ThreadPoolExecutor t, String name) {
    boolean done = false;
    while (!done) {
      try {
        done = t.awaitTermination(60, TimeUnit.SECONDS);
        LOG.debug("Waiting for " + name + " to finish...");
      } catch (InterruptedException ie) {
        LOG.debug("Interrupted waiting for " + name + " to finish...");
      }
    }
  }

  void join() {
    waitFor(splits, "Split Thread");
    waitFor(largeCompactions, "Large Compaction Thread");
    if (smallCompactions != null) {
      waitFor(smallCompactions, "Small Compaction Thread");
    }
  }

  /**
   * Returns the current size of the queue containing regions that are
   * processed.
   *
   * @return The current size of the regions queue.
   */
  public int getCompactionQueueSize() {
    int size = largeCompactions.getQueue().size();
    if (smallCompactions != null)
      size += smallCompactions.getQueue().size();
    return size;
  }

  @Override
  public void notifyOnChange(Configuration newConf) {
    // Check if number of large / small compaction threads has changed, and then
    // adjust the core pool size of the thread pools, by using the
    // setCorePoolSize() method. According to the javadocs, it is safe to
    // change the core pool size on-the-fly. We need to reset the maximum
    // pool size, as well.
    int largeThreads = Math.max(1, newConf.getInt(
            HConstants.LARGE_COMPACTION_THREADS,
            HConstants.DEFAULT_LARGE_COMPACTION_THREADS));
    if (this.largeCompactions.getCorePoolSize() != largeThreads) {
      LOG.info("Changing the value of " + HConstants.LARGE_COMPACTION_THREADS +
              " from " + this.largeCompactions.getCorePoolSize() + " to " +
              largeThreads);
      this.largeCompactions.setMaximumPoolSize(largeThreads);
      this.largeCompactions.setCorePoolSize(largeThreads);
    }

    int smallThreads = newConf.getInt(HConstants.SMALL_COMPACTION_THREADS,
            HConstants.DEFAULT_SMALL_COMPACTION_THREADS);
    if (this.smallCompactions.getCorePoolSize() != smallThreads) {
      LOG.info("Changing the value of " + HConstants.SMALL_COMPACTION_THREADS +
                " from " + this.smallCompactions.getCorePoolSize() + " to " +
                smallThreads);
      this.smallCompactions.setMaximumPoolSize(smallThreads);
      this.smallCompactions.setCorePoolSize(smallThreads);
    }

    this.conf = newConf;
  }

  /**
   * Helper method for tests to check if the number of small compaction threads
   * change on-the-fly.
   *
   * @return
   */
  protected int getSmallCompactionThreadNum() {
    return this.smallCompactions.getCorePoolSize();
  }

  /**
   * Helper method for tests to check if the number of large compaction threads
   * change on-the-fly.
   *
   * @return
   */
  protected int getLargeCompactionThreadNum() {
    return this.largeCompactions.getCorePoolSize();
  }
}
