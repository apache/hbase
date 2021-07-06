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
package org.apache.hadoop.hbase.regionserver;

import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputControllerService;
import org.apache.hadoop.hbase.util.StealJobQueue;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class help manage compaction thread pools and compaction throughput controller,
 * @see CompactSplit
 * @see org.apache.hadoop.hbase.compactionserver.CompactionThreadManager
 */
@InterfaceAudience.Private
public class CompactThreadControl {
  private static final Logger LOG = LoggerFactory.getLogger(CompactThreadControl.class);
  private volatile ThreadPoolExecutor longCompactions;
  private volatile ThreadPoolExecutor shortCompactions;
  private volatile ThroughputController compactionThroughputController;
  private BiConsumer<Runnable, ThreadPoolExecutor> rejection;

  public CompactThreadControl(ThroughputControllerService server, int largeThreads,
      int smallThreads, Comparator<Runnable> cmp,
      BiConsumer<Runnable, ThreadPoolExecutor> rejection) {
    createCompactionExecutors(largeThreads, smallThreads, cmp);

    // compaction throughput controller
    this.compactionThroughputController =
        CompactionThroughputControllerFactory.create(server, server.getConfiguration());
    // compaction throughput controller
    this.rejection = rejection;
  }

  /**
   * Cleanup class to use when rejecting a compaction request from the queue.
   */
  private class Rejection implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor pool) {
      rejection.accept(runnable, pool);
    }
  }

  void createCompactionExecutors(int largeThreads, int smallThreads, Comparator<Runnable> cmp) {
    // if we have throttle threads, make sure the user also specified size
    Preconditions.checkArgument(largeThreads > 0 && smallThreads > 0);

    final String n = Thread.currentThread().getName();

    StealJobQueue<Runnable> stealJobQueue = new StealJobQueue<Runnable>(cmp);
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
    return "compactionQueue=(longCompactions=" + longCompactions.getQueue().size()
        + ":shortCompactions=" + shortCompactions.getQueue().size() + ")";
  }

  public StringBuilder dumpQueue() {
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
    return queueLists;
  }

  public ThreadPoolExecutor getLongCompactions() {
    return longCompactions;
  }

  public ThreadPoolExecutor getShortCompactions() {
    return shortCompactions;
  }

  void setCompactionThroughputController(ThroughputController compactionThroughputController) {
    this.compactionThroughputController = compactionThroughputController;
  }

  public ThroughputController getCompactionThroughputController() {
    return compactionThroughputController;
  }

  public void waitForStop() {
    waitForPoolStop(longCompactions, "Large Compaction Thread");
    waitForPoolStop(shortCompactions, "Small Compaction Thread");
  }

  private void waitForPoolStop(ThreadPoolExecutor t, String name) {
    if (t == null) {
      return;
    }
    try {
      t.shutdown();
      t.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted waiting for " + name + " to finish...");
      Thread.currentThread().interrupt();
      t.shutdownNow();
    }
  }

}
