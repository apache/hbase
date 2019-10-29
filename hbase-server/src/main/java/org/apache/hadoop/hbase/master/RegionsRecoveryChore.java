/*
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

package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.PerClientRandomNonceGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This chore, every time it runs, will try to recover regions with high store ref count
 * by reopening them
 */
@InterfaceAudience.Private
public class RegionsRecoveryChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(RegionsRecoveryChore.class);

  private static final String REGIONS_RECOVERY_CHORE_NAME = "RegionsRecoveryChore";

  private static final String REGIONS_RECOVERY_INTERVAL =
    "hbase.master.regions.recovery.check.interval";

  private static final int DEFAULT_REGIONS_RECOVERY_INTERVAL = 1200 * 1000; // Default 20 min ?

  private static final String ERROR_REOPEN_REIONS_MSG =
    "Error reopening regions with high storeRefCount. ";

  private final HMaster hMaster;
  private final int storeFileRefCountThreshold;

  private static final PerClientRandomNonceGenerator NONCE_GENERATOR =
    new PerClientRandomNonceGenerator();

  /**
   * Construct RegionsRecoveryChore with provided params
   *
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param configuration The configuration params to be used
   * @param hMaster HMaster instance to initiate RegionTableRegions
   */
  RegionsRecoveryChore(final Stoppable stopper, final Configuration configuration,
      final HMaster hMaster) {

    super(REGIONS_RECOVERY_CHORE_NAME, stopper, configuration.getInt(REGIONS_RECOVERY_INTERVAL,
      DEFAULT_REGIONS_RECOVERY_INTERVAL));
    this.hMaster = hMaster;
    this.storeFileRefCountThreshold = configuration.getInt(
      HConstants.STORE_FILE_REF_COUNT_THRESHOLD,
      HConstants.DEFAULT_STORE_FILE_REF_COUNT_THRESHOLD);

  }

  @Override
  protected void chore() {
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Starting up Regions Recovery chore for reopening regions based on storeFileRefCount...");
    }
    try {
      // only if storeFileRefCountThreshold > 0, consider the feature turned on
      if (storeFileRefCountThreshold > 0) {
        final ClusterStatus clusterStatus = hMaster.getClusterStatus();
        final Map<ServerName, ServerLoad> serverMetricsMap =
          clusterStatus.getLiveServersLoad();
        final Map<TableName, List<HRegionInfo>> tableToReopenRegionsMap =
          getTableToRegionsByRefCount(serverMetricsMap);
        if (MapUtils.isNotEmpty(tableToReopenRegionsMap)) {
          for (Map.Entry<TableName, List<HRegionInfo>> tableRegionEntry :
              tableToReopenRegionsMap.entrySet()) {
            TableName tableName = tableRegionEntry.getKey();
            List<HRegionInfo> hRegionInfos = tableRegionEntry.getValue();
            try {
              LOG.warn("Reopening regions due to high storeFileRefCount. " +
                "TableName: {} , noOfRegions: {}", tableName, hRegionInfos.size());
              hMaster.reopenRegions(tableName, hRegionInfos, NONCE_GENERATOR.getNonceGroup(),
                NONCE_GENERATOR.newNonce());
            } catch (IOException e) {
              List<String> regionNames = new ArrayList<>();
              for (HRegionInfo hRegionInfo : hRegionInfos) {
                regionNames.add(hRegionInfo.getRegionNameAsString());
              }
              LOG.error("{} tableName: {}, regionNames: {}", ERROR_REOPEN_REIONS_MSG,
                tableName, regionNames, e);
            }
          }
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Reopening regions with very high storeFileRefCount is disabled. " +
              "Provide threshold value > 0 for {} to enable it.",
            HConstants.STORE_FILE_REF_COUNT_THRESHOLD);
        }
      }
    } catch (Exception e) {
      LOG.error("Error while reopening regions based on storeRefCount threshold", e);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Exiting Regions Recovery chore for reopening regions based on storeFileRefCount...");
    }
  }

  private Map<TableName, List<HRegionInfo>> getTableToRegionsByRefCount(
      final Map<ServerName, ServerLoad> serverMetricsMap) {

    final Map<TableName, List<HRegionInfo>> tableToReopenRegionsMap = new HashMap<>();
    for (ServerLoad serverLoad : serverMetricsMap.values()) {
      Map<byte[], RegionLoad> regionLoadsMap = serverLoad.getRegionsLoad();
      for (RegionLoad regionLoad : regionLoadsMap.values()) {
        // For each region, each store file can have different ref counts
        // We need to find maximum of all such ref counts and if that max count
        // is beyond a threshold value, we should reopen the region.
        // Here, we take max ref count of all store files and not the cumulative
        // count of all store files
        final int maxStoreFileRefCount = regionLoad.getMaxStoreFileRefCount();

        if (maxStoreFileRefCount > storeFileRefCountThreshold) {
          final byte[] regionName = regionLoad.getName();
          prepareTableToReopenRegionsMap(tableToReopenRegionsMap, regionName,
            maxStoreFileRefCount);
        }
      }
    }
    return tableToReopenRegionsMap;

  }

  private void prepareTableToReopenRegionsMap(
      final Map<TableName, List<HRegionInfo>> tableToReopenRegionsMap,
      final byte[] regionName, final int regionStoreRefCount) {

    final HRegionInfo hRegionInfo = hMaster.getAssignmentManager().getRegionInfo(regionName);
    final TableName tableName = hRegionInfo.getTable();
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      // Do not reopen regions of meta table even if it has
      // high store file reference count
      return;
    }
    LOG.warn("Region {} for Table {} has high storeFileRefCount {}, considering it for reopen..",
      hRegionInfo.getRegionNameAsString(), tableName, regionStoreRefCount);
    if (!tableToReopenRegionsMap.containsKey(tableName)) {
      tableToReopenRegionsMap.put(tableName, new ArrayList<HRegionInfo>());
    }
    tableToReopenRegionsMap.get(tableName).add(hRegionInfo);

  }

}
