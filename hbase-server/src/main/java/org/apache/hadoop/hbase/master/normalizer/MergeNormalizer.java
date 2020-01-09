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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
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
 * Considering the split policy takes care of splitting region we also want a way to merge when
 * regions are too small. It is little different than what
 * {@link org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer} does. Instead of doing
 * splits and merge both to achieve average region size in cluster for a table. We only merge
 * regions(older than defined age) and rely on Split policy for region splits. The goal of this
 * normalizer is to merge small regions to make size of regions close to average size (which is
 * either average size or depends on either target region size or count in that order). Consider
 * region with size 1,2,3,4,10,10,10,5,4,3. If minimum merge age is set to 0 days this algorithm
 * will find the average size as 7.2 assuming we haven't provided target region count or size. Now
 * we will find all those adjacent region which if merged doesn't exceed the average size. so we
 * will merge 1-2, 3-4, 4,3 in our first run. To get best results from this normalizer theoretically
 * we should set target region size between 0.5 to 0.75 of configured maximum file size. If we set
 * min merge age as 3 we create plan as above and see if we have a plan which has both regions as
 * new(age less than 3) we discard such plans and we consider the regions even if one of the region
 * is old enough to be merged.
 * </p>
 */

@InterfaceAudience.Private
public class MergeNormalizer extends AbstractRegionNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(MergeNormalizer.class);

  private int minRegionCount;
  private int minRegionAge;
  private static long[] skippedCount = new long[NormalizationPlan.PlanType.values().length];

  public MergeNormalizer() {
    Configuration conf = HBaseConfiguration.create();
    minRegionCount = conf.getInt("hbase.normalizer.min.region.count", 3);
    minRegionAge = conf.getInt("hbase.normalizer.min.region.merge.age", 3);
  }

  @Override
  public void planSkipped(RegionInfo hri, NormalizationPlan.PlanType type) {
    skippedCount[type.ordinal()]++;
  }

  @Override
  public long getSkippedCount(NormalizationPlan.PlanType type) {
    return skippedCount[type.ordinal()];
  }

  @Override
  public List<NormalizationPlan> computePlanForTable(TableName table) throws HBaseIOException {
    List<NormalizationPlan> plans = new ArrayList<>();
    if (!shouldNormalize(table)) {
      return null;
    }
    // at least one of the two regions should be older than MIN_REGION_AGE days
    List<NormalizationPlan> normalizationPlans = getMergeNormalizationPlan(table);
    for (NormalizationPlan plan : normalizationPlans) {
      if (plan instanceof MergeNormalizationPlan) {
        RegionInfo hri = ((MergeNormalizationPlan) plan).getFirstRegion();
        RegionInfo hri2 = ((MergeNormalizationPlan) plan).getSecondRegion();
        if (isOldEnoughToMerge(hri) || isOldEnoughToMerge(hri2)) {
          plans.add(plan);
        } else {
          LOG.debug("Skipping region {} and {} as they are both new", hri.getEncodedName(),
            hri2.getEncodedName());
        }
      }
    }
    if (plans.isEmpty()) {
      LOG.debug("No normalization needed, regions look good for table: {}", table);
      return null;
    }
    return plans;
  }

  private boolean isOldEnoughToMerge(RegionInfo hri) {
    Timestamp currentTime = new Timestamp(System.currentTimeMillis());
    Timestamp hriTime = new Timestamp(hri.getRegionId());
    boolean isOld =
      new Timestamp(hriTime.getTime() + TimeUnit.DAYS.toMillis(minRegionAge))
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
      List<RegionInfo> tableRegions =
        masterServices.getAssignmentManager().getRegionStates().getRegionsOfTable(table);
      if (tableRegions == null || tableRegions.size() < minRegionCount) {
        int nrRegions = tableRegions == null ? 0 : tableRegions.size();
        LOG.debug(
          "Table {} has {} regions, required min number of regions for normalizer to run is {} , "
            + "not running normalizer",
          table, nrRegions, minRegionCount);
      } else {
        normalize = true;
      }
    }
    return normalize;
  }
}
