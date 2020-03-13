/*
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;

@InterfaceAudience.Private
public abstract class AbstractRegionNormalizer implements RegionNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRegionNormalizer.class);
  protected MasterServices masterServices;
  protected MasterRpcServices masterRpcServices;

  /**
   * Set the master service.
   * @param masterServices inject instance of MasterServices
   */
  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  @Override
  public void setMasterRpcServices(MasterRpcServices masterRpcServices) {
    this.masterRpcServices = masterRpcServices;
  }

  /**
   * @param hri regioninfo
   * @return size of region in MB and if region is not found than -1
   */
  protected long getRegionSize(RegionInfo hri) {
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

  protected boolean isMergeEnabled() {
    boolean mergeEnabled = true;
    try {
      mergeEnabled = masterRpcServices
          .isSplitOrMergeEnabled(null,
            RequestConverter.buildIsSplitOrMergeEnabledRequest(MasterSwitchType.MERGE))
          .getEnabled();
    } catch (ServiceException e) {
      LOG.warn("Unable to determine whether merge is enabled", e);
    }
    return mergeEnabled;
  }

  protected boolean isSplitEnabled() {
    boolean splitEnabled = true;
    try {
      splitEnabled = masterRpcServices
          .isSplitOrMergeEnabled(null,
            RequestConverter.buildIsSplitOrMergeEnabledRequest(MasterSwitchType.SPLIT))
          .getEnabled();
    } catch (ServiceException se) {
      LOG.warn("Unable to determine whether split is enabled", se);
    }
    return splitEnabled;
  }

  /**
   * @param tableRegions regions of table to normalize
   * @return average region size depending on
   * @see org.apache.hadoop.hbase.client.TableDescriptor#getNormalizerTargetRegionCount()
   * Also make sure tableRegions contains regions of the same table
   */
  protected double getAverageRegionSize(List<RegionInfo> tableRegions) {
    long totalSizeMb = 0;
    int actualRegionCnt = 0;
    for (RegionInfo hri : tableRegions) {
      long regionSize = getRegionSize(hri);
      // don't consider regions that are in bytes for averaging the size.
      if (regionSize > 0) {
        actualRegionCnt++;
        totalSizeMb += regionSize;
      }
    }
    TableName table = tableRegions.get(0).getTable();
    int targetRegionCount = -1;
    long targetRegionSize = -1;
    try {
      TableDescriptor tableDescriptor = masterServices.getTableDescriptors().get(table);
      if (tableDescriptor != null) {
        targetRegionCount = tableDescriptor.getNormalizerTargetRegionCount();
        targetRegionSize = tableDescriptor.getNormalizerTargetRegionSize();
        LOG.debug("Table {}:  target region count is {}, target region size is {}", table,
          targetRegionCount, targetRegionSize);
      }
    } catch (IOException e) {
      LOG.warn(
        "cannot get the target number and target size of table {}, they will be default value -1.",
        table, e);
    }

    double avgRegionSize;
    if (targetRegionSize > 0) {
      avgRegionSize = targetRegionSize;
    } else if (targetRegionCount > 0) {
      avgRegionSize = totalSizeMb / (double) targetRegionCount;
    } else {
      avgRegionSize = actualRegionCnt == 0 ? 0 : totalSizeMb / (double) actualRegionCnt;
    }

    LOG.debug("Table {}, total aggregated regions size: {} and average region size {}", table,
      totalSizeMb, avgRegionSize);
    return avgRegionSize;
  }

  /**
   * Determine if a region in {@link RegionState} should be considered for a merge operation.
   */
  private static boolean skipForMerge(final RegionState state) {
    return state == null || !Objects.equals(state.getState(), RegionState.State.OPEN);
  }

  /**
   * Computes the merge plans that should be executed for this table to converge average region
   * towards target average or target region count
   * @param table table to normalize
   * @return list of merge normalization plans
   */
  protected List<NormalizationPlan> getMergeNormalizationPlan(TableName table) {
    final RegionStates regionStates = masterServices.getAssignmentManager().getRegionStates();
    final List<RegionInfo> tableRegions = regionStates.getRegionsOfTable(table);
    final double avgRegionSize = getAverageRegionSize(tableRegions);
    LOG.debug("Table {}, average region size: {}. Computing normalization plan for table: {}, "
        + "number of regions: {}",
      table, avgRegionSize, table, tableRegions.size());

    final List<NormalizationPlan> plans = new ArrayList<>();
    for (int candidateIdx = 0; candidateIdx < tableRegions.size() - 1; candidateIdx++) {
      final RegionInfo hri = tableRegions.get(candidateIdx);
      final RegionInfo hri2 = tableRegions.get(candidateIdx + 1);
      if (skipForMerge(regionStates.getRegionState(hri))) {
        continue;
      }
      if (skipForMerge(regionStates.getRegionState(hri2))) {
        continue;
      }
      final long regionSize = getRegionSize(hri);
      final long regionSize2 = getRegionSize(hri2);

      if (regionSize >= 0 && regionSize2 >= 0 && regionSize + regionSize2 < avgRegionSize) {
        // at least one of the two regions should be older than MIN_REGION_DURATION days
        plans.add(new MergeNormalizationPlan(hri, hri2));
        candidateIdx++;
      } else {
        LOG.debug("Skipping region {} of table {} with size {}", hri.getRegionNameAsString(), table,
          regionSize);
      }
    }
    return plans;
  }

  /**
   * Determine if a region in {@link RegionState} should be considered for a split operation.
   */
  private static boolean skipForSplit(final RegionState state) {
    return state == null || !Objects.equals(state.getState(), RegionState.State.OPEN);
  }

  /**
   * Computes the split plans that should be executed for this table to converge average region size
   * towards target average or target region count
   * @param table table to normalize
   * @return list of split normalization plans
   */
  protected List<NormalizationPlan> getSplitNormalizationPlan(TableName table) {
    final RegionStates regionStates = masterServices.getAssignmentManager().getRegionStates();
    final List<RegionInfo> tableRegions = regionStates.getRegionsOfTable(table);
    final double avgRegionSize = getAverageRegionSize(tableRegions);
    LOG.debug("Table {}, average region size: {}", table, avgRegionSize);

    final List<NormalizationPlan> plans = new ArrayList<>();
    for (final RegionInfo hri : tableRegions) {
      if (skipForSplit(regionStates.getRegionState(hri))) {
        continue;
      }
      long regionSize = getRegionSize(hri);
      // if the region is > 2 times larger than average, we split it, split
      // is more high priority normalization action than merge.
      if (regionSize > 2 * avgRegionSize) {
        LOG.info("Table {}, large region {} has size {}, more than twice avg size {}, splitting",
          table, hri.getRegionNameAsString(), regionSize, avgRegionSize);
        plans.add(new SplitNormalizationPlan(hri, null));
      }
    }
    return plans;
  }
}
