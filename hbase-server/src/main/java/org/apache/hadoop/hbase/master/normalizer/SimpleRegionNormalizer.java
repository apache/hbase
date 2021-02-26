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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseIOException;
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
import org.apache.hadoop.hbase.protobuf.RequestConverter;

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
  private MasterRpcServices masterRpcServices;

  /**
   * Set the master service.
   * @param masterServices inject instance of MasterServices
   */
  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  // Comparator that gives higher priority to region Split plan
  private Comparator<NormalizationPlan> planComparator =
      new Comparator<NormalizationPlan>() {
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

  /**
   * Computes next most "urgent" normalization action on the table.
   * Action may be either a split, or a merge, or no action.
   *
   * @param table table to normalize
   * @return normalization plan to execute
   */
  @Override
  public List<NormalizationPlan> computePlansForTable(TableName table) throws HBaseIOException {
    if (table == null || table.isSystemTable()) {
      LOG.debug("Normalization of system table " + table + " isn't allowed");
      return null;
    }
    boolean splitEnabled = true, mergeEnabled = true;
    splitEnabled = isSplitEnabled();
    mergeEnabled = isMergeEnabled();

    if (!splitEnabled && !mergeEnabled) {
      LOG.debug("Both split and merge are disabled for table: " + table);
      return null;
    }

    List<NormalizationPlan> plans = new ArrayList<NormalizationPlan>();
    List<HRegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
      getRegionsOfTable(table);

    //TODO: should we make min number of regions a config param?
    if (tableRegions == null || tableRegions.size() < MIN_REGION_COUNT) {
      int nrRegions = tableRegions == null ? 0 : tableRegions.size();
      LOG.debug("Table " + table + " has " + nrRegions + " regions, required min number"
        + " of regions for normalizer to run is " + MIN_REGION_COUNT + ", not running normalizer");
      return null;
    }

    LOG.debug("Computing normalization plan for table: " + table +
      ", number of regions: " + tableRegions.size());

    long totalSizeMb = 0;
    int actualRegionCnt = 0;

    for (int i = 0; i < tableRegions.size(); i++) {
      HRegionInfo hri = tableRegions.get(i);
      long regionSize = getRegionSize(hri);
      if (regionSize > 0) {
        actualRegionCnt++;
        totalSizeMb += regionSize;
      }
    }

    int targetRegionCount = -1;
    long targetRegionSize = -1;
    try {
      HTableDescriptor tableDescriptor = masterServices.getTableDescriptors().get(table);
      if(tableDescriptor != null) {
        targetRegionCount =
          tableDescriptor.getNormalizerTargetRegionCount();
        targetRegionSize =
          tableDescriptor.getNormalizerTargetRegionSize();
        LOG.debug("Table " + table + ":  target region count is " + targetRegionCount
            + ", target region size is " + targetRegionSize);
      }
    } catch (IOException e) {
      LOG.warn("cannot get the target number and target size of table " + table
          + ", they will be default value -1.");
    }

    double avgRegionSize;
    if (targetRegionSize > 0) {
      avgRegionSize = targetRegionSize;
    } else if (targetRegionCount > 0) {
      avgRegionSize = totalSizeMb / (double) targetRegionCount;
    } else {
      avgRegionSize = actualRegionCnt == 0 ? 0 : totalSizeMb / (double) actualRegionCnt;
    }

    LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb);
    LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

    int splitCount = 0;
    int mergeCount = 0;
    for (int candidateIdx = 0; candidateIdx < tableRegions.size(); candidateIdx++) {
      HRegionInfo hri = tableRegions.get(candidateIdx);
      long regionSize = getRegionSize(hri);
      // if the region is > 2 times larger than average, we split it, split
      // is more high priority normalization action than merge.
      if (regionSize > 2 * avgRegionSize) {
        if (splitEnabled) {
          LOG.info("Table " + table + ", large region " + hri.getRegionNameAsString() + " has size "
              + regionSize + ", more than twice avg size, splitting");
          plans.add(new SplitNormalizationPlan(hri, null));
          splitCount++;
        }
      } else {
        if (candidateIdx == tableRegions.size()-1) {
          break;
        }
        if (mergeEnabled) {
          HRegionInfo hri2 = tableRegions.get(candidateIdx+1);
          long regionSize2 = getRegionSize(hri2);
          if (regionSize >= 0 && regionSize2 >= 0 && regionSize + regionSize2 < avgRegionSize) {
            LOG.info("Table " + table + ", small region size: " + regionSize
              + " plus its neighbor size: " + regionSize2
              + ", less than the avg size " + avgRegionSize + ", merging them");
            plans.add(new MergeNormalizationPlan(hri, hri2));
            mergeCount++;
            candidateIdx++;
          }
        }
      }
    }
    if (plans.isEmpty()) {
      LOG.debug("No normalization needed, regions look good for table: " + table);
      return null;
    }
    Collections.sort(plans, planComparator);
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("Computed normalization plans for table %s. Total plans %d, split " +
          "plans %d, merge plans %d", table, plans.size(), splitCount, mergeCount));
    }
    return plans;
  }
  /**
   * Return configured value for MasterSwitchType.SPLIT.
   */
  private boolean isSplitEnabled() {
    boolean splitEnabled = true;
    try {
      splitEnabled = masterRpcServices.isSplitOrMergeEnabled(null,
        RequestConverter.buildIsSplitOrMergeEnabledRequest(MasterSwitchType.SPLIT)).getEnabled();
    } catch (ServiceException se) {
      LOG.debug("Unable to determine whether split is enabled", se);
    }
    return splitEnabled;
  }

  /**
   * Return configured value for MasterSwitchType.MERGE.
   */
  private boolean isMergeEnabled() {
    boolean mergeEnabled = true;
    try {
      mergeEnabled = masterRpcServices.isSplitOrMergeEnabled(null,
        RequestConverter.buildIsSplitOrMergeEnabledRequest(MasterSwitchType.MERGE)).getEnabled();
    } catch (ServiceException se) {
      LOG.debug("Unable to determine whether merge is enabled", se);
    }
    return mergeEnabled;
  }

  /**
   * @param hri used to calculate region size
   * @return region size in MB
   */
  private long getRegionSize(HRegionInfo hri) {
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
}
