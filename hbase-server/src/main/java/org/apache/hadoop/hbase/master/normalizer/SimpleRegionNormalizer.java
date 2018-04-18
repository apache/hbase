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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Size;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;

/**
 * Simple implementation of region normalizer.
 *
 * Logic in use:
 *
 *  <ol>
 *  <li> Get all regions of a given table
 *  <li> Get avg size S of each region (by total size of store files reported in RegionMetrics)
 *  <li> Seek every single region one by one. If a region R0 is bigger than S * 2, it is
 *  kindly requested to split. Thereon evaluate the next region R1
 *  <li> Otherwise, if R0 + R1 is smaller than S, R0 and R1 are kindly requested to merge.
 *  Thereon evaluate the next region R2
 *  <li> Otherwise, R1 is evaluated
 * </ol>
 * <p>
 * Region sizes are coarse and approximate on the order of megabytes. Additionally,
 * "empty" regions (less than 1MB, with the previous note) are not merged away. This
 * is by design to prevent normalization from undoing the pre-splitting of a table.
 */
@InterfaceAudience.Private
public class SimpleRegionNormalizer implements RegionNormalizer {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleRegionNormalizer.class);
  private int minRegionCount;
  private MasterServices masterServices;
  private MasterRpcServices masterRpcServices;
  private static long[] skippedCount = new long[NormalizationPlan.PlanType.values().length];

  public SimpleRegionNormalizer() {
    minRegionCount = HBaseConfiguration.create().getInt("hbase.normalizer.min.region.count", 3);
  }
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

  @Override
  public void planSkipped(RegionInfo hri, PlanType type) {
    skippedCount[type.ordinal()]++;
  }

  @Override
  public long getSkippedCount(NormalizationPlan.PlanType type) {
    return skippedCount[type.ordinal()];
  }

  /**
   * Comparator class that gives higher priority to region Split plan.
   */
  static class PlanComparator implements Comparator<NormalizationPlan> {
    @Override
    public int compare(NormalizationPlan plan1, NormalizationPlan plan2) {
      boolean plan1IsSplit = plan1 instanceof SplitNormalizationPlan;
      boolean plan2IsSplit = plan2 instanceof SplitNormalizationPlan;
      if (plan1IsSplit && plan2IsSplit) {
        return 0;
      } else if (plan1IsSplit) {
        return -1;
      } else if (plan2IsSplit) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  private Comparator<NormalizationPlan> planComparator = new PlanComparator();

  /**
   * Computes next most "urgent" normalization action on the table.
   * Action may be either a split, or a merge, or no action.
   *
   * @param table table to normalize
   * @return normalization plan to execute
   */
  @Override
  public List<NormalizationPlan> computePlanForTable(TableName table) throws HBaseIOException {
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of system table " + table + " isn't allowed");
      return null;
    }

    List<NormalizationPlan> plans = new ArrayList<>();
    List<RegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(table);

    //TODO: should we make min number of regions a config param?
    if (tableRegions == null || tableRegions.size() < minRegionCount) {
      int nrRegions = tableRegions == null ? 0 : tableRegions.size();
      LOG.debug("Table " + table + " has " + nrRegions + " regions, required min number"
        + " of regions for normalizer to run is " + minRegionCount + ", not running normalizer");
      return null;
    }

    LOG.debug("Computing normalization plan for table: " + table +
      ", number of regions: " + tableRegions.size());

    long totalSizeMb = 0;
    int acutalRegionCnt = 0;

    for (int i = 0; i < tableRegions.size(); i++) {
      RegionInfo hri = tableRegions.get(i);
      long regionSize = getRegionSize(hri);
      if (regionSize > 0) {
        acutalRegionCnt++;
        totalSizeMb += regionSize;
      }
    }

    double avgRegionSize = acutalRegionCnt == 0 ? 0 : totalSizeMb / (double) acutalRegionCnt;

    LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb);
    LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

    int candidateIdx = 0;
    boolean splitEnabled = true, mergeEnabled = true;
    try {
      splitEnabled = masterRpcServices.isSplitOrMergeEnabled(null,
        RequestConverter.buildIsSplitOrMergeEnabledRequest(MasterSwitchType.SPLIT)).getEnabled();
    } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException e) {
      LOG.debug("Unable to determine whether split is enabled", e);
    }
    try {
      mergeEnabled = masterRpcServices.isSplitOrMergeEnabled(null,
        RequestConverter.buildIsSplitOrMergeEnabledRequest(MasterSwitchType.MERGE)).getEnabled();
    } catch (org.apache.hbase.thirdparty.com.google.protobuf.ServiceException e) {
      LOG.debug("Unable to determine whether split is enabled", e);
    }
    while (candidateIdx < tableRegions.size()) {
      RegionInfo hri = tableRegions.get(candidateIdx);
      long regionSize = getRegionSize(hri);
      // if the region is > 2 times larger than average, we split it, split
      // is more high priority normalization action than merge.
      if (regionSize > 2 * avgRegionSize) {
        if (splitEnabled) {
          LOG.info("Table " + table + ", large region " + hri.getRegionNameAsString() + " has size "
              + regionSize + ", more than twice avg size, splitting");
          plans.add(new SplitNormalizationPlan(hri, null));
        }
      } else {
        if (candidateIdx == tableRegions.size()-1) {
          break;
        }
        if (mergeEnabled) {
          RegionInfo hri2 = tableRegions.get(candidateIdx+1);
          long regionSize2 = getRegionSize(hri2);
          if (regionSize >= 0 && regionSize2 >= 0 && regionSize + regionSize2 < avgRegionSize) {
            LOG.info("Table " + table + ", small region size: " + regionSize
              + " plus its neighbor size: " + regionSize2
              + ", less than the avg size " + avgRegionSize + ", merging them");
            plans.add(new MergeNormalizationPlan(hri, hri2));
            candidateIdx++;
          }
        }
      }
      candidateIdx++;
    }
    if (plans.isEmpty()) {
      LOG.debug("No normalization needed, regions look good for table: " + table);
      return null;
    }
    Collections.sort(plans, planComparator);
    return plans;
  }

  private long getRegionSize(RegionInfo hri) {
    ServerName sn = masterServices.getAssignmentManager().getRegionStates().
      getRegionServerOfRegion(hri);
    RegionMetrics regionLoad = masterServices.getServerManager().getLoad(sn).
      getRegionMetrics().get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.debug(hri.getRegionNameAsString() + " was not found in RegionsLoad");
      return -1;
    }
    return (long) regionLoad.getStoreFileSize().get(Size.Unit.MEGABYTE);
  }
}
