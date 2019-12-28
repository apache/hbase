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
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseNormalizer implements RegionNormalizer {
  private static final Logger LOG = LoggerFactory.getLogger(BaseNormalizer.class);
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

  protected long getRegionSize(HRegionInfo hri) {
    ServerName sn =
        masterServices.getAssignmentManager().getRegionStates().getRegionServerOfRegion(hri);
    RegionLoad regionLoad =
        masterServices.getServerManager().getLoad(sn).getRegionsLoad().get(hri.getRegionName());
    if (regionLoad == null) {
      LOG.debug("Region {} was not found in RegionsLoad", hri.getRegionNameAsString());
      return -1;
    }
    return regionLoad.getStorefileSizeMB();
  }

  protected boolean isMergeEnabled() {
    boolean mergeEnabled = true;
    try {
      mergeEnabled = masterRpcServices
          .isSplitOrMergeEnabled(null,
            RequestConverter.buildIsSplitOrMergeEnabledRequest(Admin.MasterSwitchType.MERGE))
          .getEnabled();
    } catch (ServiceException se) {
      LOG.warn("Unable to determine whether merge is enabled", se);
    }
    return mergeEnabled;
  }

  protected boolean isSplitEnabled() {
    boolean splitEnabled = true;
    try {
      splitEnabled = masterRpcServices
          .isSplitOrMergeEnabled(null,
            RequestConverter.buildIsSplitOrMergeEnabledRequest(Admin.MasterSwitchType.SPLIT))
          .getEnabled();
    } catch (ServiceException se) {
      LOG.warn("Unable to determine whether merge is enabled", se);
    }
    return splitEnabled;
  }

  protected double getAvgRegionSize(List<HRegionInfo> tableRegions) {
    long totalSizeMb = 0;
    int acutalRegionCnt = 0;
    for (HRegionInfo hri : tableRegions) {
      long regionSize = getRegionSize(hri);
      // don't consider regions that are in bytes for averaging the size.
      if (regionSize > 0) {
        acutalRegionCnt++;
        totalSizeMb += regionSize;
      }
    }

    double avgRegionSize = acutalRegionCnt == 0 ? 0 : totalSizeMb / (double) acutalRegionCnt;
    return avgRegionSize;
  }

  protected List<NormalizationPlan> getMergeNormalizationPlan(TableName table) {
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
        plans.add(new MergeNormalizationPlan(hri, hri2));
        candidateIdx++;
      } else {
        LOG.debug("Skipping region {} of table {} with size {}", hri.getRegionId(), table,
          regionSize);
      }
      candidateIdx++;
    }
    return plans;
  }

  protected List<NormalizationPlan> getSplitNormalizationPlan(TableName table) {
    List<NormalizationPlan> plans = new ArrayList<>();
    List<HRegionInfo> tableRegions =
        masterServices.getAssignmentManager().getRegionStates().getRegionsOfTable(table);
    double avgRegionSize = getAvgRegionSize(tableRegions);
    LOG.debug("Table {}, average region size: {}", table, avgRegionSize);

    int candidateIdx = 0;
    while (candidateIdx < tableRegions.size()) {
      HRegionInfo hri = tableRegions.get(candidateIdx);
      long regionSize = getRegionSize(hri);
      // if the region is > 2 times larger than average, we split it, split
      // is more high priority normalization action than merge.
      if (regionSize > 2 * avgRegionSize) {
        LOG.info("Table " + table + ", large region " + hri.getRegionNameAsString() + " has size "
            + regionSize + ", more than twice avg size, splitting");
        plans.add(new SplitNormalizationPlan(hri, null));
      }
      candidateIdx++;
    }
    return plans;
  }
}
