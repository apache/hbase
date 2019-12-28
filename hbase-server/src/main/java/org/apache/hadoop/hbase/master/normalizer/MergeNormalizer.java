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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of MergeNormalizer Logic in use:
 * <ol>
 * <li>get all regions of a given table
 * <li>get avg size S of each region (by total size of store files reported in RegionLoad)
 * <li>two regions R1 and its neighbour R2 are merged, if R1 + R2 &lt; S, and all such regions are
 * returned to be merged
 * <li>Otherwise, no action is performed
 * </ol>
 * <p>
 * Region sizes are coarse and approximate on the order of megabytes. Also, empty regions (less than
 * 1MB) are also merged if the age of region is &gt MIN_DURATION_FOR_MERGE (default 2)
 * </p>
 */

@InterfaceAudience.Private
public class MergeNormalizer extends BaseNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(MergeNormalizer.class);
  private static final int MIN_REGION_COUNT = 3;

  @Override
  public List<NormalizationPlan> computePlanForTable(TableName table) throws HBaseIOException {
    if (!shouldNormalize(table)) {
      return null;
    }
    List<NormalizationPlan> plans = new ArrayList<>();
    List<HRegionInfo> tableRegions =
        masterServices.getAssignmentManager().getRegionStates().getRegionsOfTable(table);
    double avgRegionSize = getAvgRegionSize(tableRegions);
    LOG.debug("Table {}, average region size: {}", table, avgRegionSize);
    LOG.debug("Computing normalization plan for table: {}, number of regions: {}", table,
      tableRegions.size());

    int candidateIdx = 0;
    while (candidateIdx < tableRegions.size()) {
      if (candidateIdx == tableRegions.size() - 1) {
        break;
      }
      HRegionInfo hri = tableRegions.get(candidateIdx);
      long regionSize = getRegionSize(hri);
      HRegionInfo hri2 = tableRegions.get(candidateIdx + 1);
      long regionSize2 = getRegionSize(hri2);
      if (regionSize >= 0 && regionSize2 >= 0 && regionSize + regionSize2 < avgRegionSize) {
        // atleast one of the two regions should be older than MIN_REGION_DURATION days
        if (isOldEnoughToMerge(hri) || isOldEnoughToMerge(hri2)) {
          LOG.info(
            "Table {}, small region size: {} plus its neighbor size: {}, less than the avg size "
                + "{}, merging them",
            table, regionSize, regionSize2, avgRegionSize);
          plans.add(new MergeNormalizationPlan(hri, hri2));
          candidateIdx++;
        }
      } else {
        LOG.debug("Skipping region {} of table {}", hri.getRegionId(), table);
      }
      candidateIdx++;
    }
    if (plans.isEmpty()) {
      LOG.debug("No normalization needed, regions look good for table: {}", table);
      return null;
    }
    return plans;
  }

  private boolean isOldEnoughToMerge(HRegionInfo hri) {
    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
    Timestamp hriTime = new Timestamp(hri.getRegionId());
    boolean isOld =
        new Timestamp(hriTime.getTime() + TimeUnit.DAYS.toMillis(getMinimumDurationBeforeMerge()))
            .before(currentTime);
    return isOld;
  }

  private boolean shouldNormalize(TableName table) {
    boolean normalize = false;
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of system table {} isn't allowed", table);
    } else if (!isMergeEnabled()) {
      LOG.debug("Merge disabled for table: {}", table);
    } else {
      List<HRegionInfo> tableRegions =
          masterServices.getAssignmentManager().getRegionStates().getRegionsOfTable(table);
      if (tableRegions == null || tableRegions.size() < MIN_REGION_COUNT) {
        int nrRegions = tableRegions == null ? 0 : tableRegions.size();
        LOG.debug(
          "Table {} has {} regions, required min number of regions for normalizer to run is {} , "
              + "not running normalizer",
          table, nrRegions, MIN_REGION_COUNT);
      } else {
        normalize = true;
      }
    }
    return normalize;
  }

  private int getMinimumDurationBeforeMerge() {
    Configuration entries = HBaseConfiguration.create();
    int minDuration = masterServices.getConfiguration()
      .getInt(HConstants.HBASE_MASTER_DAYS_BEFORE_MERGE, HConstants.DEFAULT_MIN_DAYS_BEFORE_MERGE);
    return minDuration;
  }
}
