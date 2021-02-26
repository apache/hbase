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
package org.apache.hadoop.hbase.master.normalizer;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin.MasterSwitchType;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Simple implementation of region normalizer. Logic in use:
 * <ol>
 * <li>Get all regions of a given table</li>
 * <li>Get avg size S of the regions in the table (by total size of store files reported in
 * RegionMetrics)</li>
 * <li>For each region R0, if R0 is bigger than S * 2, it is kindly requested to split.</li>
 * <li>Otherwise, for the next region in the chain R1, if R0 + R1 is smaller then S, R0 and R1 are
 * kindly requested to merge.</li>
 * </ol>
 * Region sizes are coarse and approximate on the order of megabytes. Additionally, "empty" regions
 * (less than 1MB, with the previous note) are not merged away. This is by design to prevent
 * normalization from undoing the pre-splitting of a table.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class SimpleRegionNormalizer implements RegionNormalizer {

  private static final Log LOG = LogFactory.getLog(SimpleRegionNormalizer.class);
  static final String SPLIT_ENABLED_KEY = "hbase.normalizer.split.enabled";
  static final boolean DEFAULT_SPLIT_ENABLED = true;
  static final String MERGE_ENABLED_KEY = "hbase.normalizer.merge.enabled";
  static final boolean DEFAULT_MERGE_ENABLED = true;
  // TODO: after HBASE-24416, `min.region.count` only applies to merge plans; should
  // deprecate/rename the configuration key.
  static final String MIN_REGION_COUNT_KEY = "hbase.normalizer.min.region.count";
  static final int DEFAULT_MIN_REGION_COUNT = 3;
  static final String MERGE_MIN_REGION_AGE_DAYS_KEY = "hbase.normalizer.merge.min_region_age.days";
  static final int DEFAULT_MERGE_MIN_REGION_AGE_DAYS = 3;
  static final String MERGE_MIN_REGION_SIZE_MB_KEY = "hbase.normalizer.merge.min_region_size.mb";
  static final int DEFAULT_MERGE_MIN_REGION_SIZE_MB = 1;

  private final long[] skippedCount;
  private Configuration conf;
  private MasterServices masterServices;
  private MasterRpcServices masterRpcServices;
  private boolean splitEnabled;
  private boolean mergeEnabled;
  private int minRegionCount;
  private int mergeMinRegionAge;
  private int mergeMinRegionSizeMb;

  public SimpleRegionNormalizer() {
    skippedCount = new long[NormalizationPlan.PlanType.values().length];
    splitEnabled = DEFAULT_SPLIT_ENABLED;
    mergeEnabled = DEFAULT_MERGE_ENABLED;
    minRegionCount = DEFAULT_MIN_REGION_COUNT;
    mergeMinRegionAge = DEFAULT_MERGE_MIN_REGION_AGE_DAYS;
    mergeMinRegionSizeMb = DEFAULT_MERGE_MIN_REGION_SIZE_MB;
  }

  // Comparator that gives higher priority to region Split plan
  private Comparator<NormalizationPlan> planComparator = new Comparator<NormalizationPlan>() {
    @Override
    public int compare(NormalizationPlan plan, NormalizationPlan plan2) {
      if (plan instanceof SplitNormalizationPlan) {
        return -1;
      }
      if (plan2 instanceof SplitNormalizationPlan) {
        return 1;
      }
      return 0;
    }
  };

  @Override
  public void setMasterRpcServices(MasterRpcServices masterRpcServices) {
    this.masterRpcServices = masterRpcServices;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf == null) {
      return;
    }
    this.conf = conf;
    splitEnabled = conf.getBoolean(SPLIT_ENABLED_KEY, DEFAULT_SPLIT_ENABLED);
    mergeEnabled = conf.getBoolean(MERGE_ENABLED_KEY, DEFAULT_MERGE_ENABLED);
    minRegionCount = parseMinRegionCount(conf);
    mergeMinRegionAge = parseMergeMinRegionAge(conf);
    mergeMinRegionSizeMb = parseMergeMinRegionSizeMb(conf);
  }

  private int parseMergeMinRegionSizeMb(Configuration conf) {
    final int parsedValue =
        conf.getInt(MERGE_MIN_REGION_SIZE_MB_KEY, DEFAULT_MERGE_MIN_REGION_SIZE_MB);
    final int settledValue = Math.max(0, parsedValue);
    if (parsedValue != settledValue) {
      warnInvalidValue(MERGE_MIN_REGION_SIZE_MB_KEY, parsedValue, settledValue);
    }
    return settledValue;
  }

  private int parseMinRegionCount(Configuration conf) {
    final int parsedValue = conf.getInt(MIN_REGION_COUNT_KEY, DEFAULT_MIN_REGION_COUNT);
    final int settledValue = Math.max(1, parsedValue);
    if (parsedValue != settledValue) {
      warnInvalidValue(MIN_REGION_COUNT_KEY, parsedValue, settledValue);
    }
    return settledValue;
  }

  private int parseMergeMinRegionAge(Configuration conf) {
    final int parsedValue =
        conf.getInt(MERGE_MIN_REGION_AGE_DAYS_KEY, DEFAULT_MERGE_MIN_REGION_AGE_DAYS);
    final int settledValue = Math.max(0, parsedValue);
    if (parsedValue != settledValue) {
      warnInvalidValue(MERGE_MIN_REGION_AGE_DAYS_KEY, parsedValue, settledValue);
    }
    return settledValue;
  }

  private void warnInvalidValue(final String key, final int parsedValue, final int settledValue) {
    LOG.warn("Configured value " + key + "=" + parsedValue + " is invalid. Setting value to"
        + settledValue);
  }

  /**
   * Return configured value for MasterSwitchType.SPLIT.
   */
  public boolean isSplitEnabled() {
    return splitEnabled;
  }

  /**
   * Return configured value for MasterSwitchType.MERGE.
   */
  public boolean isMergeEnabled() {
    return mergeEnabled;
  }

  private boolean isMasterSwitchEnabled(MasterSwitchType masterSwitchType) {
    boolean enabled = false;
    try {
      enabled = masterRpcServices.isSplitOrMergeEnabled(null,
        RequestConverter.buildIsSplitOrMergeEnabledRequest(masterSwitchType)).getEnabled();
    } catch (ServiceException e) {
      LOG.debug("Unable to determine whether split or merge is enabled", e);
    }
    return enabled;
  }

  /**
   * Return this instance's configured value for {@link #MIN_REGION_COUNT_KEY}.
   */
  public int getMinRegionCount() {
    return minRegionCount;
  }

  /**
   * Return this instance's configured value for {@link #MERGE_MIN_REGION_AGE_DAYS_KEY}.
   */
  public int getMergeMinRegionAge() {
    return mergeMinRegionAge;
  }

  /**
   * Return this instance's configured value for {@link #MERGE_MIN_REGION_SIZE_MB_KEY}.
   */
  public int getMergeMinRegionSizeMb() {
    return mergeMinRegionSizeMb;
  }

  /**
   * Set the master service.
   * @param masterServices inject instance of MasterServices
   */
  @Override
  public void setMasterServices(final MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  /**
   * Computes next most "urgent" normalization action on the table. Action may be either a split, or
   * a merge, or no action.
   * @param table table to normalize
   * @return normalization plan to execute
   */
  @Override
  public List<NormalizationPlan> computePlansForTable(TableName table) throws HBaseIOException {
    if (table == null) {
      return Collections.emptyList();
    }
    if (table.isSystemTable()) {
      LOG.debug("Normalization of system table " + table + " isn't allowed");
      return Collections.emptyList();
    }

    final boolean proceedWithSplitPlanning = proceedWithSplitPlanning();
    final boolean proceedWithMergePlanning = proceedWithMergePlanning();
    if (!proceedWithMergePlanning && !proceedWithSplitPlanning) {
      LOG.debug("Both split and merge are disabled. Skipping normalization of table: " + table);
      return Collections.emptyList();
    }

    final NormalizeContext ctx = new NormalizeContext(table);
    if (CollectionUtils.isEmpty(ctx.getTableRegions())) {
      return Collections.emptyList();
    }

    LOG.debug("Computing normalization plan for table:  " + table + ", number of regions: "
        + ctx.getTableRegions().size());

    final List<NormalizationPlan> plans = new ArrayList<>();
    if (proceedWithSplitPlanning) {
      plans.addAll(computeSplitNormalizationPlans(ctx));
    }
    if (proceedWithMergePlanning) {
      plans.addAll(computeMergeNormalizationPlans(ctx));
    }

    LOG.debug("Computed " + plans.size() + " normalization plans for table" + table);
    return plans;
  }

  private boolean proceedWithMergePlanning() {
    return isMergeEnabled() && isMasterSwitchEnabled(MasterSwitchType.MERGE);
  }

  private boolean proceedWithSplitPlanning() {
    return isSplitEnabled() && isMasterSwitchEnabled(MasterSwitchType.SPLIT);
  }

  /**
   * @param hri used to calculate region size
   * @return region size in MB and if region is not found than -1
   */
  private long getRegionSizeMB(HRegionInfo hri) {
    ServerName sn =
        masterServices.getAssignmentManager().getRegionStates().getRegionServerOfRegion(hri);
    if (sn == null) {
      LOG.debug(hri.getRegionNameAsString() + " region was not found on any Server");
      return -1;
    }
    ServerLoad load = masterServices.getServerManager().getLoad(sn);
    if (load == null) {
      LOG.debug(sn.getServerName() + " was not found in online servers");
      return -1;
    }
    RegionLoad regionLoad = load.getRegionsLoad().get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.debug(hri.getRegionNameAsString() + " was not found in RegionsLoad");
      return -1;
    }
    return regionLoad.getStorefileSizeMB();
  }

  /**
   * @param tableRegions regions of table to normalize
   * @return average region size Also make sure tableRegions contains regions of the same table
   */
  private double getAverageRegionSizeMb(List<HRegionInfo> tableRegions) {
    if (CollectionUtils.isEmpty(tableRegions)) {
      throw new IllegalStateException(
          "Cannot calculate average size of a table without any regions.");
    }
    final int regionCount = tableRegions.size();
    long totalSizeMb = 0;
    // tableRegions.stream().mapToLong(this::getRegionSizeMB).sum();

    for (HRegionInfo rinfo : tableRegions) {
      totalSizeMb += getRegionSizeMB(rinfo);
    }
    TableName table = tableRegions.get(0).getTable();
    int targetRegionCount = -1;
    long targetRegionSize = -1;
    try {
      HTableDescriptor tableDescriptor = masterServices.getTableDescriptors().get(table);
      if (tableDescriptor != null && LOG.isDebugEnabled()) {
        targetRegionCount = tableDescriptor.getNormalizerTargetRegionCount();
        targetRegionSize = tableDescriptor.getNormalizerTargetRegionSize();
        LOG.debug("Table " + table + " configured with target region count" + targetRegionCount
            + ", target region size " + targetRegionSize);
      }
    } catch (IOException e) {
      LOG.warn(
        "TableDescriptor for " + table + " unavailable, table-level target region count and size"
            + " configurations cannot be considered.",
        e);
    }

    double avgRegionSize;
    if (targetRegionSize > 0) {
      avgRegionSize = targetRegionSize;
    } else if (targetRegionCount > 0) {
      avgRegionSize = totalSizeMb / (double) targetRegionCount;
    } else {
      avgRegionSize = totalSizeMb / (double) regionCount;
    }

    LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb
        + " and average region size " + avgRegionSize);
    return avgRegionSize;
  }

  /**
   * Determine if a {@link HRegionInfo} should be considered for a merge operation.
   */
  private boolean skipForMerge(final RegionStates regionStates, final HRegionInfo regionInfo) {
    boolean regionIsOpen = regionStates.isRegionInState(regionInfo, RegionState.State.OPEN);
    final String name = regionInfo.getEncodedName();
    if (!regionIsOpen) {
      LOG.trace("skipping merge of region " + name + " because it is not open");
      return true;
    }
    if (!isOldEnoughForMerge(regionInfo)) {
      LOG.trace("skipping merge of region " + name + " because it is not old enough.");
      return true;
    }
    if (!isLargeEnoughForMerge(regionInfo)) {
      LOG.trace("skipping merge region " + name + " because it is not large enough.");
      return true;
    }
    return false;
  }

  /**
   * Computes the merge plans that should be executed for this table to converge average region
   * towards target average or target region count.
   */
  private List<NormalizationPlan> computeMergeNormalizationPlans(final NormalizeContext ctx) {
    if (ctx.getTableRegions().size() < minRegionCount) {
      LOG.debug("Table " + ctx.getTableName() + " has " + ctx.getTableRegions().size()
          + " regions, required min number of regions for normalizer to run" + " is "
          + minRegionCount + ", not computing merge plans.");
      return Collections.emptyList();
    }

    final double avgRegionSizeMb = ctx.getAverageRegionSizeMb();
    LOG.debug(
      "Computing normalization plan for table " + ctx.getTableName() + ". average region size: "
          + avgRegionSizeMb + ", number of" + " regions: " + ctx.getTableRegions().size());

    final List<NormalizationPlan> plans = new ArrayList<>();
    for (int candidateIdx = 0; candidateIdx < ctx.getTableRegions().size() - 1; candidateIdx++) {
      final HRegionInfo current = ctx.getTableRegions().get(candidateIdx);
      final HRegionInfo next = ctx.getTableRegions().get(candidateIdx + 1);
      if (skipForMerge(ctx.getRegionStates(), current)
          || skipForMerge(ctx.getRegionStates(), next)) {
        continue;
      }
      final long currentSizeMb = getRegionSizeMB(current);
      final long nextSizeMb = getRegionSizeMB(next);
      if (currentSizeMb + nextSizeMb < avgRegionSizeMb) {
        plans.add(new MergeNormalizationPlan(current, next));
        candidateIdx++;
      }
    }
    return plans;
  }

  /**
   * Computes the split plans that should be executed for this table to converge average region size
   * towards target average or target region count. <br />
   * if the region is > 2 times larger than average, we split it. split is more high priority
   * normalization action than merge.
   */
  private List<NormalizationPlan> computeSplitNormalizationPlans(final NormalizeContext ctx) {
    final double avgRegionSize = ctx.getAverageRegionSizeMb();
    TableName tableName = ctx.getTableName();
    LOG.debug("Table " + tableName + ", average region size: " + avgRegionSize);

    final List<NormalizationPlan> plans = new ArrayList<>();
    for (final HRegionInfo hri : ctx.getTableRegions()) {
      boolean regionIsOpen = ctx.getRegionStates().isRegionInState(hri, RegionState.State.OPEN);
      if (!regionIsOpen) {
        continue;
      }
      final long regionSize = getRegionSizeMB(hri);
      if (regionSize > 2 * avgRegionSize) {
        LOG.info(
          "Table " + tableName + ", large region " + hri.getRegionNameAsString() + " has size "
              + regionSize + ", more than twice avg size " + avgRegionSize + ", splitting");
        plans.add(new SplitNormalizationPlan(hri, null));
      }
    }
    return plans;
  }

  /**
   * Return {@code true} when {@code regionInfo} has a creation date that is old enough to be
   * considered for a merge operation, {@code false} otherwise.
   */
  private boolean isOldEnoughForMerge(final HRegionInfo regionInfo) {
    final Timestamp currentTime = new Timestamp(EnvironmentEdgeManager.currentTime());
    final Timestamp regionCreateTime = new Timestamp(regionInfo.getRegionId());
    return new Timestamp(regionCreateTime.getTime() + TimeUnit.DAYS.toMillis(mergeMinRegionAge))
        .before(currentTime);
  }

  /**
   * Return {@code true} when {@code regionInfo} has a size that is sufficient to be considered for
   * a merge operation, {@code false} otherwise.
   */
  private boolean isLargeEnoughForMerge(final HRegionInfo regionInfo) {
    return getRegionSizeMB(regionInfo) >= mergeMinRegionSizeMb;
  }

  /**
   * Inner class caries the state necessary to perform a single invocation of
   * {@link #computePlansForTable(TableName)}. Grabbing this data from the assignment manager
   * up-front allows any computed values to be realized just once.
   */
  private class NormalizeContext {
    private final TableName tableName;
    private final RegionStates regionStates;
    private final List<HRegionInfo> tableRegions;
    private final double averageRegionSizeMb;

    public NormalizeContext(final TableName tableName) {
      this.tableName = tableName;
      regionStates =
          SimpleRegionNormalizer.this.masterServices.getAssignmentManager().getRegionStates();
      tableRegions = regionStates.getRegionsOfTable(tableName);
      // The list of regionInfo from getRegionsOfTable() is ordered by regionName.
      // regionName does not necessary guarantee the order by STARTKEY (let's say 'aa1', 'aa1!',
      // in order by regionName, it will be 'aa1!' followed by 'aa1').
      // This could result in normalizer merging non-adjacent regions into one and creates overlaps.
      // In order to avoid that, sort the list by RegionInfo.COMPARATOR.
      // See HBASE-24376
      Collections.sort(tableRegions);
      averageRegionSizeMb = SimpleRegionNormalizer.this.getAverageRegionSizeMb(this.tableRegions);
    }

    public TableName getTableName() {
      return tableName;
    }

    public RegionStates getRegionStates() {
      return regionStates;
    }

    public List<HRegionInfo> getTableRegions() {
      return tableRegions;
    }

    public double getAverageRegionSizeMb() {
      return averageRegionSizeMb;
    }
  }
}
