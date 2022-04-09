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
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
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
  /**
   * @deprecated since 2.5.0 and will be removed in 4.0.0.
   *   Use {@link SimpleRegionNormalizer#MERGE_MIN_REGION_COUNT_KEY} instead.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-25745">HBASE-25745</a>
   */
  @Deprecated
  static final String MIN_REGION_COUNT_KEY = "hbase.normalizer.min.region.count";
  static final String MERGE_MIN_REGION_COUNT_KEY = "hbase.normalizer.merge.min.region.count";
  static final int DEFAULT_MERGE_MIN_REGION_COUNT = 3;
  static final String MERGE_MIN_REGION_AGE_DAYS_KEY = "hbase.normalizer.merge.min_region_age.days";
  static final int DEFAULT_MERGE_MIN_REGION_AGE_DAYS = 3;
  static final String MERGE_MIN_REGION_SIZE_MB_KEY = "hbase.normalizer.merge.min_region_size.mb";
  static final int DEFAULT_MERGE_MIN_REGION_SIZE_MB = 0;

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

  private static int parseMergeMinRegionCount(final Configuration conf) {
    final int parsedValue = conf.getInt(MERGE_MIN_REGION_COUNT_KEY,
      DEFAULT_MERGE_MIN_REGION_COUNT);
    final int settledValue = Math.max(1, parsedValue);
    if (parsedValue != settledValue) {
      warnInvalidValue(MERGE_MIN_REGION_COUNT_KEY, parsedValue, settledValue);
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
   * Return this instance's configured value for {@value #MERGE_MIN_REGION_COUNT_KEY}.
   */
  public int getMergeMinRegionCount() {
    return normalizerConfiguration.getMergeMinRegionCount();
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
  public List<NormalizationPlan> computePlansForTable(final TableDescriptor tableDescriptor) {
    if (tableDescriptor == null) {
      return Collections.emptyList();
    }
    TableName table = tableDescriptor.getTableName();
    if (table.isSystemTable()) {
      LOG.debug("Normalization of system table {} isn't allowed", table);
      return Collections.emptyList();
    }

    final boolean proceedWithSplitPlanning = proceedWithSplitPlanning(tableDescriptor);
    final boolean proceedWithMergePlanning = proceedWithMergePlanning(tableDescriptor);
    if (!proceedWithMergePlanning && !proceedWithSplitPlanning) {
      LOG.debug("Both split and merge are disabled. Skipping normalization of table: {}", table);
      return Collections.emptyList();
    }

    final NormalizeContext ctx = new NormalizeContext(tableDescriptor);
    if (isEmpty(ctx.getTableRegions())) {
      return Collections.emptyList();
    }

    LOG.debug("Computing normalization plan for table:  {}, number of regions: {}", table,
      ctx.getTableRegions().size());

    final List<NormalizationPlan> plans = new ArrayList<>();
    int splitPlansCount = 0;
    if (proceedWithSplitPlanning) {
      List<NormalizationPlan> splitPlans = computeSplitNormalizationPlans(ctx);
      splitPlansCount = splitPlans.size();
      plans.addAll(splitPlans);
    }
    int mergePlansCount = 0;
    if (proceedWithMergePlanning) {
      List<NormalizationPlan> mergePlans = computeMergeNormalizationPlans(ctx);
      mergePlansCount = mergePlans.size();
      plans.addAll(mergePlans);
    }

    LOG.debug("Computed normalization plans for table {}. Total plans: {}, split plans: {}, " +
        "merge plans: {}", table, plans.size(), splitPlansCount, mergePlansCount);
    return plans;
  }

  /**
   * @return size of region in MB and if region is not found than -1
   */
  private long getRegionSizeMB(RegionInfo hri) {
    ServerName sn =
      masterServices.getAssignmentManager().getRegionStates().getRegionServerOfRegion(hri);
    if (sn == null) {
      LOG.debug("{} region was not found on any Server", hri.getRegionNameAsString());
      return -1;
    }
    ServerMetrics serverMetrics = masterServices.getServerManager().getLoad(sn);
    if (serverMetrics == null) {
      LOG.debug("server {} was not found in ServerManager", sn.getServerName());
      return -1;
    }
    RegionMetrics regionLoad = serverMetrics.getRegionMetrics().get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.debug("{} was not found in RegionsLoad", hri.getRegionNameAsString());
      return -1;
    }
    return (long) regionLoad.getStoreFileSize().get(Size.Unit.MEGABYTE);
  }

  private boolean isMasterSwitchEnabled(final MasterSwitchType masterSwitchType) {
    return masterServices.isSplitOrMergeEnabled(masterSwitchType);
  }

  private boolean proceedWithSplitPlanning(TableDescriptor tableDescriptor) {
    String value = tableDescriptor.getValue(SPLIT_ENABLED_KEY);
    return  (value == null ? isSplitEnabled() : Boolean.parseBoolean(value)) &&
      isMasterSwitchEnabled(MasterSwitchType.SPLIT);
  }

  private boolean proceedWithMergePlanning(TableDescriptor tableDescriptor) {
    String value = tableDescriptor.getValue(MERGE_ENABLED_KEY);
    return (value == null ? isMergeEnabled() : Boolean.parseBoolean(value)) &&
      isMasterSwitchEnabled(MasterSwitchType.MERGE);
  }

  /**
   * Also make sure tableRegions contains regions of the same table
   * @param tableRegions    regions of table to normalize
   * @param tableDescriptor the TableDescriptor
   * @return average region size depending on
   * @see TableDescriptor#getNormalizerTargetRegionCount()
   */
  private double getAverageRegionSizeMb(final List<RegionInfo> tableRegions,
    final TableDescriptor tableDescriptor) {
    if (isEmpty(tableRegions)) {
      throw new IllegalStateException(
        "Cannot calculate average size of a table without any regions.");
    }
    TableName table = tableDescriptor.getTableName();
    double avgRegionSize;
    int targetRegionCount = tableDescriptor.getNormalizerTargetRegionCount();
    long targetRegionSize = tableDescriptor.getNormalizerTargetRegionSize();
    LOG.debug("Table {} configured with target region count {}, target region size {} MB", table,
      targetRegionCount, targetRegionSize);

    if (targetRegionSize > 0) {
      avgRegionSize = targetRegionSize;
    } else {
      final int regionCount = tableRegions.size();
      final long totalSizeMb = tableRegions.stream()
        .mapToLong(this::getRegionSizeMB)
        .sum();
      if (targetRegionCount > 0) {
        avgRegionSize = totalSizeMb / (double) targetRegionCount;
      } else {
        avgRegionSize = totalSizeMb / (double) regionCount;
      }
      LOG.debug("Table {}, total aggregated regions size: {} MB and average region size {} MB",
        table, totalSizeMb, String.format("%.3f", avgRegionSize));
    }

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
    final NormalizeContext ctx,
    final RegionInfo regionInfo
  ) {
    final RegionState state = ctx.getRegionStates().getRegionState(regionInfo);
    final String name = regionInfo.getEncodedName();
    return
      logTraceReason(
        () -> state == null,
        "skipping merge of region {} because no state information is available.", name)
        || logTraceReason(
          () -> !Objects.equals(state.getState(), RegionState.State.OPEN),
          "skipping merge of region {} because it is not open.", name)
        || logTraceReason(
          () -> !isOldEnoughForMerge(normalizerConfiguration, ctx, regionInfo),
          "skipping merge of region {} because it is not old enough.", name)
        || logTraceReason(
          () -> !isLargeEnoughForMerge(normalizerConfiguration, ctx, regionInfo),
          "skipping merge region {} because it is not large enough.", name);
  }

  /**
   * Computes the merge plans that should be executed for this table to converge average region
   * towards target average or target region count.
   */
  private List<NormalizationPlan> computeMergeNormalizationPlans(final NormalizeContext ctx) {
    final NormalizerConfiguration configuration = normalizerConfiguration;
    if (ctx.getTableRegions().size() < configuration.getMergeMinRegionCount(ctx)) {
      LOG.debug("Table {} has {} regions, required min number of regions for normalizer to run"
          + " is {}, not computing merge plans.", ctx.getTableName(),
        ctx.getTableRegions().size(), configuration.getMergeMinRegionCount());
      return Collections.emptyList();
    }

    final long avgRegionSizeMb = (long) ctx.getAverageRegionSizeMb();
    if (avgRegionSizeMb < configuration.getMergeMinRegionSizeMb(ctx)) {
      return Collections.emptyList();
    }
    LOG.debug("Computing normalization plan for table {}. average region size: {} MB, number of"
      + " regions: {}.", ctx.getTableName(), avgRegionSizeMb,
      ctx.getTableRegions().size());

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
        if (skipForMerge(configuration, ctx, regionInfo)) {
          // this region cannot participate in a range. resume the outer loop.
          rangeStart = Math.max(current, rangeStart + 1);
          break;
        }
        if (rangeMembers.isEmpty() // when there are no range members, seed the range with whatever
                                   // we have. this way we're prepared in case the next region is
                                   // 0-size.
          || (rangeMembers.size() == 1 && sumRangeMembersSizeMb == 0) // when there is only one
                                                                      // region and the size is 0,
                                                                      // seed the range with
                                                                      // whatever we have.
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
    LOG.debug("Table {}, average region size: {} MB", ctx.getTableName(),
      String.format("%.3f", avgRegionSize));

    final List<NormalizationPlan> plans = new ArrayList<>();
    for (final RegionInfo hri : ctx.getTableRegions()) {
      if (skipForSplit(ctx.getRegionStates().getRegionState(hri), hri)) {
        continue;
      }
      final long regionSizeMb = getRegionSizeMB(hri);
      if (regionSizeMb > 2 * avgRegionSize) {
        LOG.info("Table {}, large region {} has size {} MB, more than twice avg size {} MB, "
            + "splitting", ctx.getTableName(), hri.getRegionNameAsString(), regionSizeMb,
          String.format("%.3f", avgRegionSize));
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
    final NormalizeContext ctx,
    final RegionInfo regionInfo
  ) {
    final Instant currentTime = Instant.ofEpochMilli(EnvironmentEdgeManager.currentTime());
    final Instant regionCreateTime = Instant.ofEpochMilli(regionInfo.getRegionId());
    return currentTime.isAfter(
      regionCreateTime.plus(normalizerConfiguration.getMergeMinRegionAge(ctx)));
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
    final NormalizeContext ctx,
    final RegionInfo regionInfo
  ) {
    return getRegionSizeMB(regionInfo) >= normalizerConfiguration.getMergeMinRegionSizeMb(ctx);
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
    private final int mergeMinRegionCount;
    private final Period mergeMinRegionAge;
    private final long mergeMinRegionSizeMb;

    private NormalizerConfiguration() {
      conf = null;
      splitEnabled = DEFAULT_SPLIT_ENABLED;
      mergeEnabled = DEFAULT_MERGE_ENABLED;
      mergeMinRegionCount = DEFAULT_MERGE_MIN_REGION_COUNT;
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
      mergeMinRegionCount = parseMergeMinRegionCount(conf);
      mergeMinRegionAge = parseMergeMinRegionAge(conf);
      mergeMinRegionSizeMb = parseMergeMinRegionSizeMb(conf);
      logConfigurationUpdated(SPLIT_ENABLED_KEY, currentConfiguration.isSplitEnabled(),
        splitEnabled);
      logConfigurationUpdated(MERGE_ENABLED_KEY, currentConfiguration.isMergeEnabled(),
        mergeEnabled);
      logConfigurationUpdated(MERGE_MIN_REGION_COUNT_KEY,
        currentConfiguration.getMergeMinRegionCount(), mergeMinRegionCount);
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

    public int getMergeMinRegionCount() {
      return mergeMinRegionCount;
    }

    public int getMergeMinRegionCount(NormalizeContext context) {
      String stringValue = context.getOrDefault(MERGE_MIN_REGION_COUNT_KEY,
        Function.identity(), null);
      if (stringValue == null) {
        stringValue = context.getOrDefault(MIN_REGION_COUNT_KEY, Function.identity(),  null);
        if (stringValue != null) {
          LOG.debug("The config key {} in table descriptor is deprecated. Instead please use {}. "
              + "In future release we will remove the deprecated config.", MIN_REGION_COUNT_KEY,
            MERGE_MIN_REGION_COUNT_KEY);
        }
      }
      final int mergeMinRegionCount = stringValue == null ? 0 : Integer.parseInt(stringValue);
      if (mergeMinRegionCount <= 0) {
        return getMergeMinRegionCount();
      }
      return mergeMinRegionCount;
    }

    public Period getMergeMinRegionAge() {
      return mergeMinRegionAge;
    }

    public Period getMergeMinRegionAge(NormalizeContext context) {
      final int mergeMinRegionAge = context.getOrDefault(MERGE_MIN_REGION_AGE_DAYS_KEY,
        Integer::parseInt, -1);
      if (mergeMinRegionAge < 0) {
        return getMergeMinRegionAge();
      }
      return Period.ofDays(mergeMinRegionAge);
    }

    public long getMergeMinRegionSizeMb() {
      return mergeMinRegionSizeMb;
    }

    public long getMergeMinRegionSizeMb(NormalizeContext context) {
      final long mergeMinRegionSizeMb = context.getOrDefault(MERGE_MIN_REGION_SIZE_MB_KEY,
        Long::parseLong, (long)-1);
      if (mergeMinRegionSizeMb < 0) {
        return getMergeMinRegionSizeMb();
      }
      return mergeMinRegionSizeMb;
    }
  }

  /**
   * Inner class caries the state necessary to perform a single invocation of
   * {@link #computePlansForTable(TableDescriptor)}. Grabbing this data from the assignment manager
   * up-front allows any computed values to be realized just once.
   */
  private class NormalizeContext {
    private final TableName tableName;
    private final RegionStates regionStates;
    private final List<RegionInfo> tableRegions;
    private final double averageRegionSizeMb;
    private final TableDescriptor tableDescriptor;

    public NormalizeContext(final TableDescriptor tableDescriptor) {
      this.tableDescriptor = tableDescriptor;
      tableName = tableDescriptor.getTableName();
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
      averageRegionSizeMb = SimpleRegionNormalizer.this.getAverageRegionSizeMb(this.tableRegions,
        this.tableDescriptor);
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

    public <T> T getOrDefault(String key, Function<String, T> function, T defaultValue) {
      String value = tableDescriptor.getValue(key);
      if (value == null) {
        return defaultValue;
      } else {
        return function.apply(value);
      }
    }
  }
}
