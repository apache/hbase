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
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

/**
 * Simple implementation of region normalizer.
 *
 * Logic in use:
 *
 *  - get all regions of a given table
 *  - get avg size S of each region (by total size of store files reported in RegionLoad)
 *  - If biggest region is bigger than S * 2, it is kindly requested to split,
 *    and normalization stops
 *  - Otherwise, two smallest region R1 and its smallest neighbor R2 are kindly requested
 *    to merge, if R1 + R1 <  S, and normalization stops
 *  - Otherwise, no action is performed
 */
@InterfaceAudience.Private
public class SimpleRegionNormalizer implements RegionNormalizer {
  private static final Log LOG = LogFactory.getLog(SimpleRegionNormalizer.class);
  private MasterServices masterServices;

  /**
   * Set the master service.
   * @param masterServices inject instance of MasterServices
   */
  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  /**
   * Computes next most "urgent" normalization action on the table.
   * Action may be either a split, or a merge, or no action.
   *
   * @param table table to normalize
   * @return normalization plan to execute
   */
  @Override
  public NormalizationPlan computePlanForTable(TableName table)
      throws HBaseIOException {
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of table " + table + " isn't allowed");
      return EmptyNormalizationPlan.getInstance();
    }

    List<HRegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(table);

    //TODO: should we make min number of regions a config param?
    if (tableRegions == null || tableRegions.size() < 3) {
      LOG.debug("Table " + table + " has " + tableRegions.size() + " regions, required min number"
        + " of regions for normalizer to run is 3, not running normalizer");
      return EmptyNormalizationPlan.getInstance();
    }

    LOG.debug("Computing normalization plan for table: " + table +
      ", number of regions: " + tableRegions.size());

    long totalSizeMb = 0;
    Pair<HRegionInfo, Long> largestRegion = new Pair<>();

    // A is a smallest region, B is it's smallest neighbor
    Pair<HRegionInfo, Long> smallestRegion = new Pair<>();
    Pair<HRegionInfo, Long> smallestNeighborOfSmallestRegion;
    int smallestRegionIndex = 0;

    for (int i = 0; i < tableRegions.size(); i++) {
      HRegionInfo hri = tableRegions.get(i);
      long regionSize = getRegionSize(hri);
      totalSizeMb += regionSize;

      if (largestRegion.getFirst() == null || regionSize > largestRegion.getSecond()) {
        largestRegion.setFirst(hri);
        largestRegion.setSecond(regionSize);
      }

      if (smallestRegion.getFirst() == null || regionSize < smallestRegion.getSecond()) {
        smallestRegion.setFirst(hri);
        smallestRegion.setSecond(regionSize);
        smallestRegionIndex = i;
      }
    }

    // now get smallest neighbor of smallest region
    long leftNeighborSize = -1;
    long rightNeighborSize = -1;

    if (smallestRegionIndex > 0) {
      leftNeighborSize = getRegionSize(tableRegions.get(smallestRegionIndex - 1));
    }

    if (smallestRegionIndex < tableRegions.size() - 1) {
      rightNeighborSize = getRegionSize(tableRegions.get(smallestRegionIndex + 1));
    }

    if (leftNeighborSize == -1) {
      smallestNeighborOfSmallestRegion =
        new Pair<>(tableRegions.get(smallestRegionIndex + 1), rightNeighborSize);
    } else if (rightNeighborSize == -1) {
      smallestNeighborOfSmallestRegion =
        new Pair<>(tableRegions.get(smallestRegionIndex - 1), leftNeighborSize);
    } else {
      if (leftNeighborSize < rightNeighborSize) {
        smallestNeighborOfSmallestRegion =
          new Pair<>(tableRegions.get(smallestRegionIndex - 1), leftNeighborSize);
      } else {
        smallestNeighborOfSmallestRegion =
          new Pair<>(tableRegions.get(smallestRegionIndex + 1), rightNeighborSize);
      }
    }

    double avgRegionSize = totalSizeMb / (double) tableRegions.size();

    LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb);
    LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

    // now; if the largest region is >2 times large than average, we split it, split
    // is more high priority normalization action than merge.
    if (largestRegion.getSecond() > 2 * avgRegionSize) {
      LOG.debug("Table " + table + ", largest region "
        + largestRegion.getFirst().getRegionName() + " has size "
        + largestRegion.getSecond() + ", more than 2 times than avg size, splitting");
      return new SplitNormalizationPlan(largestRegion.getFirst(), null);
    } else {
      if ((smallestRegion.getSecond() + smallestNeighborOfSmallestRegion.getSecond()
          < avgRegionSize)) {
        LOG.debug("Table " + table + ", smallest region size: " + smallestRegion.getSecond()
          + " and its smallest neighbor size: " + smallestNeighborOfSmallestRegion.getSecond()
          + ", less than half the avg size, merging them");
        return new MergeNormalizationPlan(smallestRegion.getFirst(),
          smallestNeighborOfSmallestRegion.getFirst());
      } else {
        LOG.debug("No normalization needed, regions look good for table: " + table);
        return EmptyNormalizationPlan.getInstance();
      }
    }
  }

  private long getRegionSize(HRegionInfo hri) {
    ServerName sn = masterServices.getAssignmentManager().getRegionStates().
      getRegionServerOfRegion(hri);
    RegionLoad regionLoad = masterServices.getServerManager().getLoad(sn).
      getRegionsLoad().get(hri.getRegionName());
    return regionLoad.getStorefileSizeMB();
  }
}
