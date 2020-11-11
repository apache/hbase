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

import static org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils.isEmpty;
import java.io.IOException;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of region normalizer. Logic in use:
 * <ol>
 *   <li>Get all regions of a given table</li>
 *   <li>Get avg size S of the regions in the table (by total size of store files reported in
 *     RegionMetrics)</li>
 *   <li>For each region R0, if R0 is bigger than S * 2, it is kindly requested to split.</li>
 *   <li>Otherwise, for the next region in the chain R1, if R0 + R1 is smaller then S, R0 and R1
 *     are kindly requested to merge.</li>
 * </ol>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
class SimpleRegionNormalizer implements RegionNormalizer, ConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleRegionNormalizer.class);

  static final String SPLIT_ENABLED_KEY = "hbase.normalizer.split.enabled";
  static final boolean DEFAULT_SPLIT_ENABLED = true;
  static final String MERGE_ENABLED_KEY = "hbase.normalizer.merge.enabled";
  static final boolean DEFAULT_MERGE_ENABLED = true;
  // TODO: after HBASE-24416, `min.region.count` only applies to merge plans; should
  //  deprecate/rename the configuration key.
  static final String MIN_REGION_COUNT_KEY = "hbase.normalizer.min.region.count";
  static final int DEFAULT_MIN_REGION_COUNT = 3;
  static final String MERGE_MIN_REGION_AGE_DAYS_KEY = "hbase.normalizer.merge.min_region_age.days";
  static final int DEFAULT_MERGE_MIN_REGION_AGE_DAYS = 3;
  static final String MERGE_MIN_REGION_SIZE_MB_KEY = "hbase.normalizer.merge.min_region_size.mb";
  static final int DEFAULT_MERGE_MIN_REGION_SIZE_MB = 1;

  private MasterServices masterServices;
  private NormalizerConfiguration normalizerConfiguration;

  public SimpleRegionNormalizer() {
    masterServices = null;
    normalizerConfiguration = new NormalizerConfiguration();
  }

  @Override
  public Configuration getConf() {
    return normalizerConfiguration.getConf();
  }

  @Override
  public void setConf(final Configuration conf) {
    if (conf == null) {
      return;
    }
    normalizerConfiguration = new NormalizerConfiguration(conf, normalizerConfiguration);
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    LOG.debug("Updating configuration parameters according to new configuration instance.");
    setConf(conf);
  }

  private static int parseMinRegionCount(final Configuration conf) {
    final int parsedValue = conf.getInt(MIN_REGION_COUNT_KEY, DEFAULT_MIN_REGION_COUNT);
    final int settledValue = Math.max(1, parsedValue);
    if (parsedValue != settledValue) {
      warnInvalidValue(MIN_REGION_COUNT_KEY, parsedValue, settledValue);
    }
    return settledValue;
  }

  private static Period parseMergeMinRegionAge(final Configuration conf) {
    final int parsedValue =
      conf.getInt(MERGE_MIN_REGION_AGE_DAYS_KEY, DEFAULT_MERGE_MIN_REGION_AGE_DAYS);
    final int settledValue = Math.max(0, parsedValue);
    if (parsedValue != settledValue) {
      warnInvalidValue(MERGE_MIN_REGION_AGE_DAYS_KEY, parsedValue, settledValue);
    }
    return Period.ofDays(settledValue);
  }

  private static long parseMergeMinRegionSizeMb(final Configuration conf) {
    final long parsedValue =
      conf.getLong(MERGE_MIN_REGION_SIZE_MB_KEY, DEFAULT_MERGE_MIN_REGION_SIZE_MB);
    final long settledValue = Math.max(0, parsedValue);
    if (parsedValue != settledValue) {
      warnInvalidValue(MERGE_MIN_REGION_SIZE_MB_KEY, parsedValue, settledValue);
    }
    return settledValue;
  }

  private static <T> void warnInvalidValue(final String key, final T parsedValue,
    final T settledValue) {
    LOG.warn("Configured value {}={} is invalid. Setting value to {}.",
      key, parsedValue, settledValue);
  }

  private static <T> void logConfigurationUpdated(final String key, final T oldValue,
    final T newValue) {
    if (!Objects.equals(oldValue, newValue)) {
      LOG.info("Updated configuration for key '{}' from {} to {}", key, oldValue, newValue);
    }
  }

  /**
   * Return this instance's configured value for {@value #SPLIT_ENABLED_KEY}.
   */
  public boolean isSplitEnabled() {
    return normalizerConfiguration.isSplitEnabled();
  }

  /**
   * Return this instance's configured value for {@value #MERGE_ENABLED_KEY}.
   */
  public boolean isMergeEnabled() {
    return normalizerConfiguration.isMergeEnabled();
  }

  /**
   * Return this instance's configured value for {@value #MIN_REGION_COUNT_KEY}.
   */
  public int getMinRegionCount() {
    return normalizerConfiguration.getMinRegionCount();
  }

  /**
   * Return this instance's configured value for {@value #MERGE_MIN_REGION_AGE_DAYS_KEY}.
   */
  public Period getMergeMinRegionAge() {
    return normalizerConfiguration.getMergeMinRegionAge();
  }

  /**
   * Return this instance's configured value for {@value #MERGE_MIN_REGION_SIZE_MB_KEY}.
   */
  public long getMergeMinRegionSizeMb() {
    return normalizerConfiguration.getMergeMinRegionSizeMb();
  }

  @Override
  public void setMasterServices(final MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  @Override
  public List<NormalizationPlan> computePlansForTable(final TableName table) {
    if (table == null) {
      return Collections.emptyList();
    }
    if (table.isSystemTable()) {
      LOG.debug("Normalization of system table {} isn't allowed", table);
      return Collections.emptyList();
    }

    final boolean proceedWithSplitPlanning = proceedWithSplitPlanning();
    final boolean proceedWithMergePlanning = proceedWithMergePlanning();
    if (!proceedWithMergePlanning && !proceedWithSplitPlanning) {
      LOG.debug("Both split and merge are disabled. Skipping normalization of table: {}", table);
      return Collections.emptyList();
    }

    final NormalizeContext ctx = new NormalizeContext(table);
    if (isEmpty(ctx.getTableRegions())) {
      return Collections.emptyList();
    }

    LOG.debug("Computing normalization plan for table:  {}, number of regions: {}", table,
      ctx.getTableRegions().size());

    final List<NormalizationPlan> plans = new ArrayList<>();
    if (proceedWithSplitPlanning) {
      plans.addAll(computeSplitNormalizationPlans(ctx));
    }
    if (proceedWithMergePlanning) {
      plans.addAll(computeMergeNormalizationPlans(ctx));
    }

    LOG.debug("Computed {} normalization plans for table {}", plans.size(), table);
    return plans;
  }

  /**
   * @return size of region in MB and if region is not found than -1
   */
  private long getRegionSizeMB(RegionInfo hri) {
    ServerName sn =
      masterServices.getAssignmentManager().getRegionStates().getRegionServerOfRegion(hri);
    RegionMetrics regionLoad =
      masterServices.getServerManager().getLoad(sn).getRegionMetrics().get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.debug("{} was not found in RegionsLoad", hri.getRegionNameAsString());
      return -1;
    }
    return (long) regionLoad.getStoreFileSize().get(Size.Unit.MEGABYTE);
  }

  private boolean isMasterSwitchEnabled(final MasterSwitchType masterSwitchType) {
    return masterServices.isSplitOrMergeEnabled(masterSwitchType);
  }

  private boolean proceedWithSplitPlanning() {
    return isSplitEnabled() && isMasterSwitchEnabled(MasterSwitchType.SPLIT);
  }

  private boolean proceedWithMergePlanning() {
    return isMergeEnabled() && isMasterSwitchEnabled(MasterSwitchType.MERGE);
  }

  /**
   * @param tableRegions regions of table to normalize
   * @return average region size depending on
   * @see org.apache.hadoop.hbase.client.TableDescriptor#getNormalizerTargetRegionCount()
   * Also make sure tableRegions contains regions of the same table
   */
  private double getAverageRegionSizeMb(final List<RegionInfo> tableRegions) {
    if (isEmpty(tableRegions)) {
      throw new IllegalStateException(
        "Cannot calculate average size of a table without any regions.");
    }
    final int regionCount = tableRegions.size();
    final long totalSizeMb = tableRegions.stream()
      .mapToLong(this::getRegionSizeMB)
      .sum();
    TableName table = tableRegions.get(0).getTable();
    int targetRegionCount = -1;
    long targetRegionSize = -1;
    try {
      TableDescriptor tableDescriptor = masterServices.getTableDescriptors().get(table);
      if (tableDescriptor != null && LOG.isDebugEnabled()) {
        targetRegionCount = tableDescriptor.getNormalizerTargetRegionCount();
        targetRegionSize = tableDescriptor.getNormalizerTargetRegionSize();
        LOG.debug("Table {} configured with target region count {}, target region size {}", table,
          targetRegionCount, targetRegionSize);
      }
    } catch (IOException e) {
      LOG.warn("TableDescriptor for {} unavailable, table-level target region count and size"
        + " configurations cannot be considered.", table, e);
    }

    double avgRegionSize;
    if (targetRegionSize > 0) {
      avgRegionSize = targetRegionSize;
    } else if (targetRegionCount > 0) {
      avgRegionSize = totalSizeMb / (double) targetRegionCount;
    } else {
      avgRegionSize = totalSizeMb / (double) regionCount;
    }

    LOG.debug("Table {}, total aggregated regions size: {} and average region size {}", table,
      totalSizeMb, avgRegionSize);
    return avgRegionSize;
  }

  /**
   * Determine if a {@link RegionInfo} should be considered for a merge operation.
   * </p>
   * Callers beware: for safe concurrency, be sure to pass in the local instance of
   * {@link NormalizerConfiguration}, don't use {@code this}'s instance.
   */
  private boolean skipForMerge(
    final NormalizerConfiguration normalizerConfiguration,
    final RegionStates regionStates,
    final RegionInfo regionInfo
  ) {
    final RegionState state = regionStates.getRegionState(regionInfo);
    final String name = regionInfo.getEncodedName();
    return
      logTraceReason(
        () -> state == null,
        "skipping merge of region {} because no state information is available.", name)
        || logTraceReason(
          () -> !Objects.equals(state.getState(), RegionState.State.OPEN),
          "skipping merge of region {} because it is not open.", name)
        || logTraceReason(
          () -> !isOldEnoughForMerge(normalizerConfiguration, regionInfo),
          "skipping merge of region {} because it is not old enough.", name)
        || logTraceReason(
          () -> !isLargeEnoughForMerge(normalizerConfiguration, regionInfo),
          "skipping merge region {} because it is not large enough.", name);
  }

  /**
   * Computes the merge plans that should be executed for this table to converge average region
   * towards target average or target region count.
   */
  private List<NormalizationPlan> computeMergeNormalizationPlans(final NormalizeContext ctx) {
    final NormalizerConfiguration configuration = normalizerConfiguration;
    if (ctx.getTableRegions().size() < configuration.getMinRegionCount()) {
      LOG.debug("Table {} has {} regions, required min number of regions for normalizer to run"
          + " is {}, not computing merge plans.", ctx.getTableName(),
        ctx.getTableRegions().size(), configuration.getMinRegionCount());
      return Collections.emptyList();
    }

    final long avgRegionSizeMb = (long) ctx.getAverageRegionSizeMb();
    if (avgRegionSizeMb < configuration.getMergeMinRegionSizeMb()) {
      return Collections.emptyList();
    }
    LOG.debug("Computing normalization plan for table {}. average region size: {}, number of"
      + " regions: {}.", ctx.getTableName(), avgRegionSizeMb, ctx.getTableRegions().size());

    // this nested loop walks the table's region chain once, looking for contiguous sequences of
    // regions that meet the criteria for merge. The outer loop tracks the starting point of the
    // next sequence, the inner loop looks for the end of that sequence. A single sequence becomes
    // an instance of MergeNormalizationPlan.

    final List<NormalizationPlan> plans = new LinkedList<>();
    final List<NormalizationTarget> rangeMembers = new LinkedList<>();
    long sumRangeMembersSizeMb;
    int current = 0;
    for (int rangeStart = 0;
         rangeStart < ctx.getTableRegions().size() - 1 && current < ctx.getTableRegions().size();) {
      // walk the region chain looking for contiguous sequences of regions that can be merged.
      rangeMembers.clear();
      sumRangeMembersSizeMb = 0;
      for (current = rangeStart; current < ctx.getTableRegions().size(); current++) {
        final RegionInfo regionInfo = ctx.getTableRegions().get(current);
        final long regionSizeMb = getRegionSizeMB(regionInfo);
        if (skipForMerge(configuration, ctx.getRegionStates(), regionInfo)) {
          // this region cannot participate in a range. resume the outer loop.
          rangeStart = Math.max(current, rangeStart + 1);
          break;
        }
        if (rangeMembers.isEmpty() // when there are no range members, seed the range with whatever
                                   // we have. this way we're prepared in case the next region is
                                   // 0-size.
          || regionSizeMb == 0 // always add an empty region to the current range.
          || (regionSizeMb + sumRangeMembersSizeMb <= avgRegionSizeMb)) { // add the current region
                                                                          // to the range when
                                                                          // there's capacity
                                                                          // remaining.
          rangeMembers.add(new NormalizationTarget(regionInfo, regionSizeMb));
          sumRangeMembersSizeMb += regionSizeMb;
          continue;
        }
        // we have accumulated enough regions to fill a range. resume the outer loop.
        rangeStart = Math.max(current, rangeStart + 1);
        break;
      }
      if (rangeMembers.size() > 1) {
        plans.add(new MergeNormalizationPlan.Builder().setTargets(rangeMembers).build());
      }
    }
    return plans;
  }

  /**
   * Determine if a region in {@link RegionState} should be considered for a split operation.
   */
  private static boolean skipForSplit(final RegionState state, final RegionInfo regionInfo) {
    final String name = regionInfo.getEncodedName();
    return
      logTraceReason(
        () -> state == null,
        "skipping split of region {} because no state information is available.", name)
        || logTraceReason(
          () -> !Objects.equals(state.getState(), RegionState.State.OPEN),
          "skipping merge of region {} because it is not open.", name);
  }

  /**
   * Computes the split plans that should be executed for this table to converge average region size
   * towards target average or target region count.
   * <br />
   * if the region is > 2 times larger than average, we split it. split
   * is more high priority normalization action than merge.
   */
  private List<NormalizationPlan> computeSplitNormalizationPlans(final NormalizeContext ctx) {
    final double avgRegionSize = ctx.getAverageRegionSizeMb();
    LOG.debug("Table {}, average region size: {}", ctx.getTableName(), avgRegionSize);

    final List<NormalizationPlan> plans = new ArrayList<>();
    for (final RegionInfo hri : ctx.getTableRegions()) {
      if (skipForSplit(ctx.getRegionStates().getRegionState(hri), hri)) {
        continue;
      }
      final long regionSizeMb = getRegionSizeMB(hri);
      if (regionSizeMb > 2 * avgRegionSize) {
        LOG.info("Table {}, large region {} has size {}, more than twice avg size {}, splitting",
          ctx.getTableName(), hri.getRegionNameAsString(), regionSizeMb, avgRegionSize);
        plans.add(new SplitNormalizationPlan(hri, regionSizeMb));
      }
    }
    return plans;
  }

  /**
   * Return {@code true} when {@code regionInfo} has a creation date that is old
   * enough to be considered for a merge operation, {@code false} otherwise.
   */
  private static boolean isOldEnoughForMerge(
    final NormalizerConfiguration normalizerConfiguration,
    final RegionInfo regionInfo
  ) {
    final Instant currentTime = Instant.ofEpochMilli(EnvironmentEdgeManager.currentTime());
    final Instant regionCreateTime = Instant.ofEpochMilli(regionInfo.getRegionId());
    return currentTime.isAfter(
      regionCreateTime.plus(normalizerConfiguration.getMergeMinRegionAge()));
  }

  /**
   * Return {@code true} when {@code regionInfo} has a size that is sufficient
   * to be considered for a merge operation, {@code false} otherwise.
   * </p>
   * Callers beware: for safe concurrency, be sure to pass in the local instance of
   * {@link NormalizerConfiguration}, don't use {@code this}'s instance.
   */
  private boolean isLargeEnoughForMerge(
    final NormalizerConfiguration normalizerConfiguration,
    final RegionInfo regionInfo
  ) {
    return getRegionSizeMB(regionInfo) >= normalizerConfiguration.getMergeMinRegionSizeMb();
  }

  private static boolean logTraceReason(final BooleanSupplier predicate, final String fmtWhenTrue,
    final Object... args) {
    final boolean value = predicate.getAsBoolean();
    if (value) {
      LOG.trace(fmtWhenTrue, args);
    }
    return value;
  }

  /**
   * Holds the configuration values read from {@link Configuration}. Encapsulation in a POJO
   * enables atomic hot-reloading of configs without locks.
   */
  private static final class NormalizerConfiguration {
    private final Configuration conf;
    private final boolean splitEnabled;
    private final boolean mergeEnabled;
    private final int minRegionCount;
    private final Period mergeMinRegionAge;
    private final long mergeMinRegionSizeMb;

    private NormalizerConfiguration() {
      conf = null;
      splitEnabled = DEFAULT_SPLIT_ENABLED;
      mergeEnabled = DEFAULT_MERGE_ENABLED;
      minRegionCount = DEFAULT_MIN_REGION_COUNT;
      mergeMinRegionAge = Period.ofDays(DEFAULT_MERGE_MIN_REGION_AGE_DAYS);
      mergeMinRegionSizeMb = DEFAULT_MERGE_MIN_REGION_SIZE_MB;
    }

    private NormalizerConfiguration(
      final Configuration conf,
      final NormalizerConfiguration currentConfiguration
    ) {
      this.conf = conf;
      splitEnabled = conf.getBoolean(SPLIT_ENABLED_KEY, DEFAULT_SPLIT_ENABLED);
      mergeEnabled = conf.getBoolean(MERGE_ENABLED_KEY, DEFAULT_MERGE_ENABLED);
      minRegionCount = parseMinRegionCount(conf);
      mergeMinRegionAge = parseMergeMinRegionAge(conf);
      mergeMinRegionSizeMb = parseMergeMinRegionSizeMb(conf);
      logConfigurationUpdated(SPLIT_ENABLED_KEY, currentConfiguration.isSplitEnabled(),
        splitEnabled);
      logConfigurationUpdated(MERGE_ENABLED_KEY, currentConfiguration.isMergeEnabled(),
        mergeEnabled);
      logConfigurationUpdated(MIN_REGION_COUNT_KEY, currentConfiguration.getMinRegionCount(),
        minRegionCount);
      logConfigurationUpdated(MERGE_MIN_REGION_AGE_DAYS_KEY,
        currentConfiguration.getMergeMinRegionAge(), mergeMinRegionAge);
      logConfigurationUpdated(MERGE_MIN_REGION_SIZE_MB_KEY,
        currentConfiguration.getMergeMinRegionSizeMb(), mergeMinRegionSizeMb);
    }

    public Configuration getConf() {
      return conf;
    }

    public boolean isSplitEnabled() {
      return splitEnabled;
    }

    public boolean isMergeEnabled() {
      return mergeEnabled;
    }

    public int getMinRegionCount() {
      return minRegionCount;
    }

    public Period getMergeMinRegionAge() {
      return mergeMinRegionAge;
    }

    public long getMergeMinRegionSizeMb() {
      return mergeMinRegionSizeMb;
    }
  }

  /**
   * Inner class caries the state necessary to perform a single invocation of
   * {@link #computePlansForTable(TableName)}. Grabbing this data from the assignment manager
   * up-front allows any computed values to be realized just once.
   */
  private class NormalizeContext {
    private final TableName tableName;
    private final RegionStates regionStates;
    private final List<RegionInfo> tableRegions;
    private final double averageRegionSizeMb;

    public NormalizeContext(final TableName tableName) {
      this.tableName = tableName;
      regionStates = SimpleRegionNormalizer.this.masterServices
        .getAssignmentManager()
        .getRegionStates();
      tableRegions = regionStates.getRegionsOfTable(tableName);
      // The list of regionInfo from getRegionsOfTable() is ordered by regionName.
      // regionName does not necessary guarantee the order by STARTKEY (let's say 'aa1', 'aa1!',
      // in order by regionName, it will be 'aa1!' followed by 'aa1').
      // This could result in normalizer merging non-adjacent regions into one and creates overlaps.
      // In order to avoid that, sort the list by RegionInfo.COMPARATOR.
      // See HBASE-24376
      tableRegions.sort(RegionInfo.COMPARATOR);
      averageRegionSizeMb = SimpleRegionNormalizer.this.getAverageRegionSizeMb(this.tableRegions);
    }

    public TableName getTableName() {
      return tableName;
    }

    public RegionStates getRegionStates() {
      return regionStates;
    }

    public List<RegionInfo> getTableRegions() {
      return tableRegions;
    }

    public double getAverageRegionSizeMb() {
      return averageRegionSizeMb;
    }
  }
}
