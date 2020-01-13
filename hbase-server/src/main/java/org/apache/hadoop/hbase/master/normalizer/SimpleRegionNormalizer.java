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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple implementation of region normalizer. Logic in use:
 * <ol>
 * <li>Get all regions of a given table
 * <li>Get avg size S of each region (by total size of store files reported in RegionMetrics)
 * <li>Seek every single region one by one. If a region R0 is bigger than S * 2, it is kindly
 * requested to split. Thereon evaluate the next region R1
 * <li>Otherwise, if R0 + R1 is smaller than S, R0 and R1 are kindly requested to merge. Thereon
 * evaluate the next region R2
 * <li>Otherwise, R1 is evaluated
 * </ol>
 * <p>
 * Region sizes are coarse and approximate on the order of megabytes. Additionally, "empty" regions
 * (less than 1MB, with the previous note) are not merged away. This is by design to prevent
 * normalization from undoing the pre-splitting of a table.
 */
@InterfaceAudience.Private
public class SimpleRegionNormalizer extends AbstractRegionNormalizer {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleRegionNormalizer.class);
  private int minRegionCount;
  private static long[] skippedCount = new long[NormalizationPlan.PlanType.values().length];

  public SimpleRegionNormalizer() {
    minRegionCount = HBaseConfiguration.create().getInt("hbase.normalizer.min.region.count", 3);
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
   * Computes next most "urgent" normalization action on the table. Action may be either a split, or
   * a merge, or no action.
   * @param table table to normalize
   * @return normalization plan to execute
   */
  @Override
  public List<NormalizationPlan> computePlanForTable(TableName table) throws HBaseIOException {
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of system table {} isn't allowed", table);
      return null;
    }
    boolean splitEnabled = isSplitEnabled();
    boolean mergeEnabled = isMergeEnabled();
    if (!mergeEnabled && !splitEnabled) {
      LOG.debug("Both split and merge are disabled for table: {}", table);
      return null;
    }
    List<NormalizationPlan> plans = new ArrayList<>();
    List<RegionInfo> tableRegions =
        masterServices.getAssignmentManager().getRegionStates().getRegionsOfTable(table);

    if (tableRegions == null || tableRegions.size() < minRegionCount) {
      int nrRegions = tableRegions == null ? 0 : tableRegions.size();
      LOG.debug("Table {} has {} regions, required min number of regions for normalizer to run is "
          + "{}, not running normalizer",
        table, nrRegions, minRegionCount);
      return null;
    }

    LOG.debug("Computing normalization plan for table:  {}, number of regions: {}", table,
      tableRegions.size());

    if (splitEnabled) {
      List<NormalizationPlan> splitPlans = getSplitNormalizationPlan(table);
      if (splitPlans != null) {
        plans.addAll(splitPlans);
      }
    }

    if (mergeEnabled) {
      List<NormalizationPlan> mergePlans = getMergeNormalizationPlan(table);
      if (mergePlans != null) {
        plans.addAll(mergePlans);
      }
    }
    if (plans.isEmpty()) {
      LOG.debug("No normalization needed, regions look good for table: {}", table);
      return null;
    }
    Collections.sort(plans, planComparator);
    return plans;
  }
}
