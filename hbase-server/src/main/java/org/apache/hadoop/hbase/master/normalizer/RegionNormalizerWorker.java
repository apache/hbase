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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.RateLimiter;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * Consumes normalization request targets ({@link TableName}s) off the
 * {@link RegionNormalizerWorkQueue}, dispatches them to the {@link RegionNormalizer}, and executes
 * the resulting {@link NormalizationPlan}s.
 */
@InterfaceAudience.Private
class RegionNormalizerWorker implements PropagatingConfigurationObserver, Runnable {
  public static final String HBASE_TABLE_NORMALIZATION_ENABLED =
    "hbase.table.normalization.enabled";
  private static final Logger LOG = LoggerFactory.getLogger(RegionNormalizerWorker.class);

  static final String RATE_LIMIT_BYTES_PER_SEC_KEY =
    "hbase.normalizer.throughput.max_bytes_per_sec";
  private static final long RATE_UNLIMITED_BYTES = 1_000_000_000_000L; // 1TB/sec

  static final String CUMULATIVE_SIZE_LIMIT_MB_KEY = "hbase.normalizer.plans_size_limit.mb";
  static final long DEFAULT_CUMULATIVE_SIZE_LIMIT_MB = Long.MAX_VALUE;

  private final MasterServices masterServices;
  private final RegionNormalizer regionNormalizer;
  private final RegionNormalizerWorkQueue<TableName> workQueue;
  private final RateLimiter rateLimiter;

  private final long[] skippedCount;
  private final boolean defaultNormalizerTableLevel;
  private long splitPlanCount;
  private long mergePlanCount;
  private final AtomicLong cumulativePlansSizeLimitMb;
  private final ExecutorService pool;

  RegionNormalizerWorker(final Configuration configuration, final MasterServices masterServices,
    final RegionNormalizer regionNormalizer, final RegionNormalizerWorkQueue<TableName> workQueue) {
    this.masterServices = masterServices;
    this.regionNormalizer = regionNormalizer;
    this.workQueue = workQueue;
    this.skippedCount = new long[NormalizationPlan.PlanType.values().length];
    this.splitPlanCount = 0;
    this.mergePlanCount = 0;
    this.rateLimiter = loadRateLimiter(configuration);
    this.defaultNormalizerTableLevel = extractDefaultNormalizerValue(configuration);
    this.cumulativePlansSizeLimitMb = new AtomicLong(
      configuration.getLong(CUMULATIVE_SIZE_LIMIT_MB_KEY, DEFAULT_CUMULATIVE_SIZE_LIMIT_MB));
    this.pool = Executors.newFixedThreadPool(
      configuration.getInt(HConstants.HBASE_NORMALIZER_WORKER_POOL_THREADS,
        HConstants.HBASE_NORMALIZER_WORKER_POOL_THREADS_DEFAULT),
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("table-normalizer-worker-%d")
        .setUncaughtExceptionHandler(
          (thread, throwable) -> LOG.error("Uncaught exception, worker thread likely terminated.",
            throwable)).build());
  }

  private boolean extractDefaultNormalizerValue(final Configuration configuration) {
    String s = configuration.get(HBASE_TABLE_NORMALIZATION_ENABLED);
    return Boolean.parseBoolean(s);
  }

  @Override
  public void registerChildren(ConfigurationManager manager) {
    if (regionNormalizer instanceof ConfigurationObserver) {
      final ConfigurationObserver observer = (ConfigurationObserver) regionNormalizer;
      manager.registerObserver(observer);
    }
  }

  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    if (regionNormalizer instanceof ConfigurationObserver) {
      final ConfigurationObserver observer = (ConfigurationObserver) regionNormalizer;
      manager.deregisterObserver(observer);
    }
  }

  private static long logLongConfigurationUpdated(final String key, final long oldValue,
    final long newValue) {
    if (oldValue != newValue) {
      LOG.info("Updated configuration for key '{}' from {} to {}", key, oldValue, newValue);
    }
    return newValue;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    rateLimiter.setRate(loadRateLimit(conf));
    cumulativePlansSizeLimitMb.set(
      logLongConfigurationUpdated(CUMULATIVE_SIZE_LIMIT_MB_KEY, cumulativePlansSizeLimitMb.get(),
        conf.getLong(CUMULATIVE_SIZE_LIMIT_MB_KEY, DEFAULT_CUMULATIVE_SIZE_LIMIT_MB)));
  }

  private static RateLimiter loadRateLimiter(final Configuration configuration) {
    return RateLimiter.create(loadRateLimit(configuration));
  }

  private static long loadRateLimit(final Configuration configuration) {
    long rateLimitBytes =
      configuration.getLongBytes(RATE_LIMIT_BYTES_PER_SEC_KEY, RATE_UNLIMITED_BYTES);
    long rateLimitMbs = rateLimitBytes / 1_000_000L;
    if (rateLimitMbs <= 0) {
      LOG.warn("Configured value {}={} is <= 1MB. Falling back to default.",
        RATE_LIMIT_BYTES_PER_SEC_KEY, rateLimitBytes);
      rateLimitBytes = RATE_UNLIMITED_BYTES;
      rateLimitMbs = RATE_UNLIMITED_BYTES / 1_000_000L;
    }
    LOG.info("Normalizer rate limit set to {}",
      rateLimitBytes == RATE_UNLIMITED_BYTES ? "unlimited" : rateLimitMbs + " MB/sec");
    return rateLimitMbs;
  }

  /**
   * @see RegionNormalizerManager#planSkipped(NormalizationPlan.PlanType)
   */
  void planSkipped(NormalizationPlan.PlanType type) {
    synchronized (skippedCount) {
      // updates come here via procedure threads, so synchronize access to this counter.
      skippedCount[type.ordinal()]++;
    }
  }

  /**
   * @see RegionNormalizerManager#getSkippedCount(NormalizationPlan.PlanType)
   */
  long getSkippedCount(NormalizationPlan.PlanType type) {
    return skippedCount[type.ordinal()];
  }

  /**
   * @see RegionNormalizerManager#getSplitPlanCount()
   */
  long getSplitPlanCount() {
    return splitPlanCount;
  }

  /**
   * @see RegionNormalizerManager#getMergePlanCount()
   */
  long getMergePlanCount() {
    return mergePlanCount;
  }

  /**
   * Used in test only. This field is exposed to the test, as opposed to tracking the current
   * configuration value beside the RateLimiter instance and managing synchronization to keep the
   * two in sync.
   */
  RateLimiter getRateLimiter() {
    return rateLimiter;
  }

  @Override
  public void run() {
    while (true) {
      if (Thread.interrupted()) {
        LOG.debug("interrupt detected. terminating.");
        pool.shutdown();
        break;
      }
      final TableName tableName;
      try {
        tableName = workQueue.take();
      } catch (InterruptedException e) {
        LOG.debug("interrupt detected. terminating.");
        pool.shutdown();
        break;
      }

      pool.submit(() -> {
        final List<NormalizationPlan> plans = calculatePlans(tableName);
        submitPlans(plans);
      });
    }
  }

  private List<NormalizationPlan> calculatePlans(final TableName tableName) {
    if (masterServices.skipRegionManagementAction("region normalizer")) {
      return Collections.emptyList();
    }

    final TableDescriptor tblDesc;
    try {
      tblDesc = masterServices.getTableDescriptors().get(tableName);
      boolean normalizationEnabled;
      if (tblDesc != null) {
        String defined = tblDesc.getValue(TableDescriptorBuilder.NORMALIZATION_ENABLED);
        if (defined != null) {
          normalizationEnabled = tblDesc.isNormalizationEnabled();
        } else {
          normalizationEnabled = this.defaultNormalizerTableLevel;
        }
        if (!normalizationEnabled) {
          LOG.debug("Skipping table {} because normalization is disabled in its table properties "
            + "and normalization is also disabled at table level by default", tableName);
          return Collections.emptyList();
        }
      }
    } catch (IOException e) {
      LOG.debug("Skipping table {} because unable to access its table descriptor.", tableName, e);
      return Collections.emptyList();
    }

    List<NormalizationPlan> plans = regionNormalizer.computePlansForTable(tblDesc);

    plans = truncateForSize(plans);

    if (CollectionUtils.isEmpty(plans)) {
      LOG.debug("No normalization required for table {}.", tableName);
      return Collections.emptyList();
    }
    return plans;
  }

  private List<NormalizationPlan> truncateForSize(List<NormalizationPlan> plans) {
    if (cumulativePlansSizeLimitMb.get() != DEFAULT_CUMULATIVE_SIZE_LIMIT_MB) {
      List<NormalizationPlan> maybeTruncatedPlans = new ArrayList<>(plans.size());
      long totalCumulativeSizeMb = 0;
      long truncatedCumulativeSizeMb = 0;
      for (NormalizationPlan plan : plans) {
        totalCumulativeSizeMb += plan.getPlanSizeMb();
        if (totalCumulativeSizeMb <= cumulativePlansSizeLimitMb.get()) {
          truncatedCumulativeSizeMb += plan.getPlanSizeMb();
          maybeTruncatedPlans.add(plan);
        }
      }
      if (maybeTruncatedPlans.size() != plans.size()) {
        LOG.debug(
          "Truncating list of normalization plans that RegionNormalizerWorker will process "
            + "because of {}. Original list had {} plan(s), new list has {} plan(s). "
            + "Original list covered regions with cumulative size {} mb, "
            + "new list covers regions with cumulative size {} mb.",
          CUMULATIVE_SIZE_LIMIT_MB_KEY, plans.size(), maybeTruncatedPlans.size(),
          totalCumulativeSizeMb, truncatedCumulativeSizeMb);
      }
      return maybeTruncatedPlans;
    } else {
      return plans;
    }
  }

  private void submitPlans(final List<NormalizationPlan> plans) {
    // as of this writing, `plan.submit()` is non-blocking and uses Async Admin APIs to submit
    // task, so there's no artificial rate-limiting of merge/split requests due to this serial loop.
    for (NormalizationPlan plan : plans) {
      switch (plan.getType()) {
        case MERGE: {
          submitMergePlan((MergeNormalizationPlan) plan);
          break;
        }
        case SPLIT: {
          submitSplitPlan((SplitNormalizationPlan) plan);
          break;
        }
        case NONE:
          LOG.debug("Nothing to do for {} with PlanType=NONE. Ignoring.", plan);
          planSkipped(plan.getType());
          break;
        default:
          LOG.warn("Plan {} is of an unrecognized PlanType. Ignoring.", plan);
          planSkipped(plan.getType());
          break;
      }
    }
  }

  /**
   * Interacts with {@link MasterServices} in order to execute a plan.
   */
  private void submitMergePlan(final MergeNormalizationPlan plan) {
    final int totalSizeMb;
    try {
      final long totalSizeMbLong = plan.getNormalizationTargets().stream()
        .mapToLong(NormalizationTarget::getRegionSizeMb).reduce(0, Math::addExact);
      totalSizeMb = Math.toIntExact(totalSizeMbLong);
    } catch (ArithmeticException e) {
      LOG.debug("Sum of merge request size overflows rate limiter data type. {}", plan);
      planSkipped(plan.getType());
      return;
    }

    final RegionInfo[] infos = plan.getNormalizationTargets().stream()
      .map(NormalizationTarget::getRegionInfo).toArray(RegionInfo[]::new);
    final long pid;
    try {
      pid = masterServices.mergeRegions(infos, false, HConstants.NO_NONCE, HConstants.NO_NONCE);
    } catch (IOException e) {
      LOG.info("failed to submit plan {}.", plan, e);
      planSkipped(plan.getType());
      return;
    }
    mergePlanCount++;
    LOG.info("Submitted {} resulting in pid {}", plan, pid);
    final long rateLimitedSecs = Math.round(rateLimiter.acquire(Math.max(1, totalSizeMb)));
    LOG.debug("Rate limiting delayed the worker by {}", Duration.ofSeconds(rateLimitedSecs));
  }

  /**
   * Interacts with {@link MasterServices} in order to execute a plan.
   */
  private void submitSplitPlan(final SplitNormalizationPlan plan) {
    final int totalSizeMb;
    try {
      totalSizeMb = Math.toIntExact(plan.getSplitTarget().getRegionSizeMb());
    } catch (ArithmeticException e) {
      LOG.debug("Split request size overflows rate limiter data type. {}", plan);
      planSkipped(plan.getType());
      return;
    }
    final RegionInfo info = plan.getSplitTarget().getRegionInfo();
    final long rateLimitedSecs = Math.round(rateLimiter.acquire(Math.max(1, totalSizeMb)));
    LOG.debug("Rate limiting delayed this operation by {}", Duration.ofSeconds(rateLimitedSecs));

    final long pid;
    try {
      pid = masterServices.splitRegion(info, null, HConstants.NO_NONCE, HConstants.NO_NONCE);
    } catch (IOException e) {
      LOG.info("failed to submit plan {}.", plan, e);
      planSkipped(plan.getType());
      return;
    }
    splitPlanCount++;
    LOG.info("Submitted {} resulting in pid {}", plan, pid);
  }
}
