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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.normalizer.NormalizationPlan.PlanType;
import org.apache.hadoop.hbase.util.Triple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Simple implementation of region normalizer.
 *
 * Logic in use:
 *
 *  <ol>
 *  <li> get all regions of a given table
 *  <li> get avg size S of each region (by total size of store files reported in RegionLoad)
 *  <li> If biggest region is bigger than S * 2, it is kindly requested to split,
 *    and normalization stops
 *  <li> Otherwise, two smallest region R1 and its smallest neighbor R2 are kindly requested
 *    to merge, if R1 + R1 &lt;  S, and normalization stops
 *  <li> Otherwise, no action is performed
 * </ol>
 * <p>
 * Region sizes are coarse and approximate on the order of megabytes. Additionally,
 * "empty" regions (less than 1MB, with the previous note) are not merged away. This
 * is by design to prevent normalization from undoing the pre-splitting of a table.
 */
@InterfaceAudience.Private
public class SimpleRegionNormalizer implements RegionNormalizer {

  private static final Log LOG = LogFactory.getLog(SimpleRegionNormalizer.class);
  private static final int MIN_REGION_COUNT = 3;
  private MasterServices masterServices;
  private static long[] skippedCount = new long[PlanType.values().length];

  /**
   * Set the master service.
   * @param masterServices inject instance of MasterServices
   */
  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  /*
   * This comparator compares the region size.
   * The second element in the triple is region size while the 3rd element
   * is the index of the region in the underlying List
   */
  private Comparator<Triple<HRegionInfo, Long, Integer>> regionSizeComparator =
      new Comparator<Triple<HRegionInfo, Long, Integer>>() {
    @Override
    public int compare(Triple<HRegionInfo, Long, Integer> pair,
        Triple<HRegionInfo, Long, Integer> pair2) {
      long sz = pair.getSecond();
      long sz2 = pair2.getSecond();
      return (sz < sz2) ? -1 : ((sz == sz2) ? 0 : 1);
    }
  };

  @Override
  public void planSkipped(HRegionInfo hri, PlanType type) {
    skippedCount[type.ordinal()]++;
  }

  @Override
  public long getSkippedCount(NormalizationPlan.PlanType type) {
    return skippedCount[type.ordinal()];
  }

  /**
   * Computes next most "urgent" normalization action on the table.
   * Action may be either a split, or a merge, or no action.
   *
   * @param table table to normalize
   * @param types desired types of NormalizationPlan
   * @return normalization plan to execute
   */
  @Override
  public NormalizationPlan computePlanForTable(TableName table, List<PlanType> types)
      throws HBaseIOException {
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of system table " + table + " isn't allowed");
      return EmptyNormalizationPlan.getInstance();
    }

    List<HRegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(table);

    //TODO: should we make min number of regions a config param?
    if (tableRegions == null || tableRegions.size() < MIN_REGION_COUNT) {
      int nrRegions = tableRegions == null ? 0 : tableRegions.size();
      LOG.debug("Table " + table + " has " + nrRegions + " regions, required min number"
        + " of regions for normalizer to run is " + MIN_REGION_COUNT + ", not running normalizer");
      return EmptyNormalizationPlan.getInstance();
    }

    LOG.debug("Computing normalization plan for table: " + table +
      ", number of regions: " + tableRegions.size());

    long totalSizeMb = 0;

    ArrayList<Triple<HRegionInfo, Long, Integer>> regionsWithSize =
        new ArrayList<Triple<HRegionInfo, Long, Integer>>(tableRegions.size());
    for (int i = 0; i < tableRegions.size(); i++) {
      HRegionInfo hri = tableRegions.get(i);
      long regionSize = getRegionSize(hri);
      regionsWithSize.add(new Triple<HRegionInfo, Long, Integer>(hri, regionSize, i));
      totalSizeMb += regionSize;
    }
    Collections.sort(regionsWithSize, regionSizeComparator);

    Triple<HRegionInfo, Long, Integer> largestRegion = regionsWithSize.get(tableRegions.size()-1);

    double avgRegionSize = totalSizeMb / (double) tableRegions.size();

    LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb);
    LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

    // now; if the largest region is >2 times large than average, we split it, split
    // is more high priority normalization action than merge.
    if (types.contains(PlanType.SPLIT) && largestRegion.getSecond() > 2 * avgRegionSize) {
      LOG.debug("Table " + table + ", largest region "
        + largestRegion.getFirst().getRegionNameAsString() + " has size "
        + largestRegion.getSecond() + ", more than 2 times than avg size, splitting");
      return new SplitNormalizationPlan(largestRegion.getFirst(), null);
    }
    int candidateIdx = 0;
    // look for two successive entries whose indices are adjacent
    while (candidateIdx < tableRegions.size()-1) {
      if (Math.abs(regionsWithSize.get(candidateIdx).getThird() -
        regionsWithSize.get(candidateIdx + 1).getThird()) == 1) {
        break;
      }
      candidateIdx++;
    }
    if (candidateIdx == tableRegions.size()-1) {
      LOG.debug("No neighboring regions found for table: " + table);
      return EmptyNormalizationPlan.getInstance();
    }
    Triple<HRegionInfo, Long, Integer> candidateRegion = regionsWithSize.get(candidateIdx);
    Triple<HRegionInfo, Long, Integer> candidateRegion2 = regionsWithSize.get(candidateIdx+1);
    if (types.contains(PlanType.MERGE) &&
        candidateRegion.getSecond() + candidateRegion2.getSecond() < avgRegionSize) {
      LOG.debug("Table " + table + ", smallest region size: " + candidateRegion.getSecond()
        + " and its smallest neighbor size: " + candidateRegion2.getSecond()
        + ", less than the avg size, merging them");
      return new MergeNormalizationPlan(candidateRegion.getFirst(),
        candidateRegion2.getFirst());
    }
    LOG.debug("No normalization needed, regions look good for table: " + table);
    return EmptyNormalizationPlan.getInstance();
  }

  private long getRegionSize(HRegionInfo hri) {
    ServerName sn = masterServices.getAssignmentManager().getRegionStates().
      getRegionServerOfRegion(hri);
    RegionLoad regionLoad = masterServices.getServerManager().getLoad(sn).
      getRegionsLoad().get(hri.getRegionName());
    return regionLoad.getStorefileSizeMB();
  }
}
