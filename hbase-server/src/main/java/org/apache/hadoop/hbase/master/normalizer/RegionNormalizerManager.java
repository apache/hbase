/*
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
package org.apache.hadoop.hbase.master.normalizer;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.zookeeper.RegionNormalizerTracker;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class encapsulates the details of the {@link RegionNormalizer} subsystem.
 */
@InterfaceAudience.Private
public class RegionNormalizerManager implements PropagatingConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(RegionNormalizerManager.class);

  private final RegionNormalizerTracker regionNormalizerTracker;
  private final RegionNormalizerChore regionNormalizerChore;
  private final RegionNormalizerWorkQueue<TableName> workQueue;
  private final RegionNormalizerWorker worker;
  private final ExecutorService pool;

  private final Object startStopLock = new Object();
  private boolean started = false;
  private boolean stopped = false;

  RegionNormalizerManager(
    @NonNull  final RegionNormalizerTracker regionNormalizerTracker,
    @Nullable final RegionNormalizerChore regionNormalizerChore,
    @Nullable final RegionNormalizerWorkQueue<TableName> workQueue,
    @Nullable final RegionNormalizerWorker worker
  ) {
    this.regionNormalizerTracker = regionNormalizerTracker;
    this.regionNormalizerChore = regionNormalizerChore;
    this.workQueue = workQueue;
    this.worker = worker;
    this.pool = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("normalizer-worker-%d")
      .setUncaughtExceptionHandler(
        (thread, throwable) ->
          LOG.error("Uncaught exception, worker thread likely terminated.", throwable))
      .build());
  }

  @Override
  public void registerChildren(ConfigurationManager manager) {
    if (worker != null) {
      manager.registerObserver(worker);
    }
  }

  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    if (worker != null) {
      manager.deregisterObserver(worker);
    }
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    // no configuration managed here directly.
  }

  public void start() {
    synchronized (startStopLock) {
      if (started) {
        return;
      }
      regionNormalizerTracker.start();
      if (worker != null) {
        // worker will be null when master is in maintenance mode.
        pool.submit(worker);
      }
      started = true;
    }
  }

  public void stop() {
    synchronized (startStopLock) {
      if (!started) {
        throw new IllegalStateException("calling `stop` without first calling `start`.");
      }
      if (stopped) {
        return;
      }
      pool.shutdownNow(); // shutdownNow to interrupt the worker thread sitting on `take()`
      regionNormalizerTracker.stop();
      stopped = true;
    }
  }

  public ScheduledChore getRegionNormalizerChore() {
    return regionNormalizerChore;
  }

  /**
   * Return {@code true} if region normalizer is on, {@code false} otherwise
   */
  public boolean isNormalizerOn() {
    return regionNormalizerTracker.isNormalizerOn();
  }

  /**
   * Set region normalizer on/off
   * @param normalizerOn whether normalizer should be on or off
   */
  public void setNormalizerOn(boolean normalizerOn) {
    try {
      regionNormalizerTracker.setNormalizerOn(normalizerOn);
    } catch (KeeperException e) {
      LOG.warn("Error flipping normalizer switch", e);
    }
  }

  /**
   * Call-back for the case where plan couldn't be executed due to constraint violation,
   * such as namespace quota.
   * @param type type of plan that was skipped.
   */
  public void planSkipped(NormalizationPlan.PlanType type) {
    // TODO: this appears to be used only for testing.
    if (worker != null) {
      worker.planSkipped(type);
    }
  }

  /**
   * Retrieve a count of the number of times plans of type {@code type} were submitted but skipped.
   * @param type type of plan for which skipped count is to be returned
   */
  public long getSkippedCount(NormalizationPlan.PlanType type) {
    // TODO: this appears to be used only for testing.
    return worker == null ? 0 : worker.getSkippedCount(type);
  }

  /**
   * Return the number of times a {@link SplitNormalizationPlan} has been submitted.
   */
  public long getSplitPlanCount() {
    return worker == null ? 0 : worker.getSplitPlanCount();
  }

  /**
   * Return the number of times a {@link MergeNormalizationPlan} has been submitted.
   */
  public long getMergePlanCount() {
    return worker == null ? 0 : worker.getMergePlanCount();
  }

  /**
   * Submit tables for normalization.
   * @param tables   a list of tables to submit.
   * @param isHighPriority {@code true} when these requested tables should skip to the front of
   *   the queue.
   * @return {@code true} when work was queued, {@code false} otherwise.
   */
  public boolean normalizeRegions(List<TableName> tables, boolean isHighPriority) {
    if (workQueue == null) {
      return false;
    }
    if (isHighPriority) {
      workQueue.putAllFirst(tables);
    } else {
      workQueue.putAll(tables);
    }
    return true;
  }
}
