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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.PRIMARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.SECONDARY;
import static org.apache.hadoop.hbase.favored.FavoredNodesPlan.Position.TERTIARY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.Visitor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.favored.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.favored.FavoredNodesPlan;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used internally for reading meta and constructing datastructures that are
 * then queried, for things like regions to regionservers, table to regions, etc.
 * It also records the favored nodes mapping for regions.
 *
 */
@InterfaceAudience.Private
public class SnapshotOfRegionAssignmentFromMeta {
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotOfRegionAssignmentFromMeta.class
      .getName());

  private final Connection connection;

  /** the table name to region map */
  private final Map<TableName, List<RegionInfo>> tableToRegionMap;
  /** the region to region server map */
  //private final Map<RegionInfo, ServerName> regionToRegionServerMap;
  private Map<RegionInfo, ServerName> regionToRegionServerMap;
  /** the region name to region info map */
  private final Map<String, RegionInfo> regionNameToRegionInfoMap;

  /** the regionServer to region map */
  private final Map<ServerName, List<RegionInfo>> currentRSToRegionMap;
  private final Map<ServerName, List<RegionInfo>> secondaryRSToRegionMap;
  private final Map<ServerName, List<RegionInfo>> teritiaryRSToRegionMap;
  private final Map<ServerName, List<RegionInfo>> primaryRSToRegionMap;
  /** the existing assignment plan in the hbase:meta region */
  private final FavoredNodesPlan existingAssignmentPlan;
  private final Set<TableName> disabledTables;
  private final boolean excludeOfflinedSplitParents;

  public SnapshotOfRegionAssignmentFromMeta(Connection connection) {
    this(connection, new HashSet<>(), false);
  }

  public SnapshotOfRegionAssignmentFromMeta(Connection connection, Set<TableName> disabledTables,
      boolean excludeOfflinedSplitParents) {
    this.connection = connection;
    tableToRegionMap = new HashMap<>();
    regionToRegionServerMap = new HashMap<>();
    currentRSToRegionMap = new HashMap<>();
    primaryRSToRegionMap = new HashMap<>();
    secondaryRSToRegionMap = new HashMap<>();
    teritiaryRSToRegionMap = new HashMap<>();
    regionNameToRegionInfoMap = new TreeMap<>();
    existingAssignmentPlan = new FavoredNodesPlan();
    this.disabledTables = disabledTables;
    this.excludeOfflinedSplitParents = excludeOfflinedSplitParents;
  }

  /**
   * Initialize the region assignment snapshot by scanning the hbase:meta table
   * @throws IOException
   */
  public void initialize() throws IOException {
    LOG.info("Start to scan the hbase:meta for the current region assignment " +
      "snappshot");
    // TODO: at some point this code could live in the MetaTableAccessor
    Visitor v = new Visitor() {
      @Override
      public boolean visit(Result result) throws IOException {
        try {
          if (result ==  null || result.isEmpty()) return true;
          RegionLocations rl = MetaTableAccessor.getRegionLocations(result);
          if (rl == null) return true;
          RegionInfo hri = rl.getRegionLocation(0).getRegionInfo();
          if (hri == null) return true;
          if (hri.getTable() == null) return true;
          if (disabledTables.contains(hri.getTable())) {
            return true;
          }
          // Are we to include split parents in the list?
          if (excludeOfflinedSplitParents && hri.isSplit()) return true;
          HRegionLocation[] hrls = rl.getRegionLocations();

          // Add the current assignment to the snapshot for all replicas
          for (int i = 0; i < hrls.length; i++) {
            if (hrls[i] == null) continue;
            hri = hrls[i].getRegionInfo();
            if (hri == null) continue;
            addAssignment(hri, hrls[i].getServerName());
            addRegion(hri);
          }

          hri = rl.getRegionLocation(0).getRegionInfo();
          // the code below is to handle favored nodes
          byte[] favoredNodes = result.getValue(HConstants.CATALOG_FAMILY,
              FavoredNodeAssignmentHelper.FAVOREDNODES_QUALIFIER);
          if (favoredNodes == null) return true;
          // Add the favored nodes into assignment plan
          ServerName[] favoredServerList =
              FavoredNodeAssignmentHelper.getFavoredNodesList(favoredNodes);
          // Add the favored nodes into assignment plan
          existingAssignmentPlan.updateFavoredNodesMap(hri,
              Arrays.asList(favoredServerList));

          /*
           * Typically there should be FAVORED_NODES_NUM favored nodes for a region in meta. If
           * there is less than FAVORED_NODES_NUM, lets use as much as we can but log a warning.
           */
          if (favoredServerList.length != FavoredNodeAssignmentHelper.FAVORED_NODES_NUM) {
            LOG.warn("Insufficient favored nodes for region " + hri + " fn: " + Arrays
                .toString(favoredServerList));
          }
          for (int i = 0; i < favoredServerList.length; i++) {
            if (i == PRIMARY.ordinal()) addPrimaryAssignment(hri, favoredServerList[i]);
            if (i == SECONDARY.ordinal()) addSecondaryAssignment(hri, favoredServerList[i]);
            if (i == TERTIARY.ordinal()) addTeritiaryAssignment(hri, favoredServerList[i]);
          }
          return true;
        } catch (RuntimeException e) {
          LOG.error("Catche remote exception " + e.getMessage() +
              " when processing" + result);
          throw e;
        }
      }
    };
    // Scan hbase:meta to pick up user regions
    MetaTableAccessor.fullScanRegions(connection, v);
    //regionToRegionServerMap = regions;
    LOG.info("Finished to scan the hbase:meta for the current region assignment" +
      "snapshot");
  }

  private void addRegion(RegionInfo regionInfo) {
    // Process the region name to region info map
    regionNameToRegionInfoMap.put(regionInfo.getRegionNameAsString(), regionInfo);

    // Process the table to region map
    TableName tableName = regionInfo.getTable();
    List<RegionInfo> regionList = tableToRegionMap.get(tableName);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    // Add the current region info into the tableToRegionMap
    regionList.add(regionInfo);
    tableToRegionMap.put(tableName, regionList);
  }

  private void addAssignment(RegionInfo regionInfo, ServerName server) {
    // Process the region to region server map
    regionToRegionServerMap.put(regionInfo, server);

    if (server == null) return;

    // Process the region server to region map
    List<RegionInfo> regionList = currentRSToRegionMap.get(server);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(regionInfo);
    currentRSToRegionMap.put(server, regionList);
  }

  private void addPrimaryAssignment(RegionInfo regionInfo, ServerName server) {
    // Process the region server to region map
    List<RegionInfo> regionList = primaryRSToRegionMap.get(server);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(regionInfo);
    primaryRSToRegionMap.put(server, regionList);
  }

  private void addSecondaryAssignment(RegionInfo regionInfo, ServerName server) {
    // Process the region server to region map
    List<RegionInfo> regionList = secondaryRSToRegionMap.get(server);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(regionInfo);
    secondaryRSToRegionMap.put(server, regionList);
  }

  private void addTeritiaryAssignment(RegionInfo regionInfo, ServerName server) {
    // Process the region server to region map
    List<RegionInfo> regionList = teritiaryRSToRegionMap.get(server);
    if (regionList == null) {
      regionList = new ArrayList<>();
    }
    regionList.add(regionInfo);
    teritiaryRSToRegionMap.put(server, regionList);
  }

  /**
   * Get the regioninfo for a region
   * @return the regioninfo
   */
  public Map<String, RegionInfo> getRegionNameToRegionInfoMap() {
    return this.regionNameToRegionInfoMap;
  }

  /**
   * Get regions for tables
   * @return a mapping from table to regions
   */
  public Map<TableName, List<RegionInfo>> getTableToRegionMap() {
    return tableToRegionMap;
  }

  /**
   * Get region to region server map
   * @return region to region server map
   */
  public Map<RegionInfo, ServerName> getRegionToRegionServerMap() {
    return regionToRegionServerMap;
  }

  /**
   * Get regionserver to region map
   * @return regionserver to region map
   */
  public Map<ServerName, List<RegionInfo>> getRegionServerToRegionMap() {
    return currentRSToRegionMap;
  }

  /**
   * Get the favored nodes plan
   * @return the existing favored nodes plan
   */
  public FavoredNodesPlan getExistingAssignmentPlan() {
    return this.existingAssignmentPlan;
  }

  /**
   * Get the table set
   * @return the table set
   */
  public Set<TableName> getTableSet() {
    return this.tableToRegionMap.keySet();
  }

  public Map<ServerName, List<RegionInfo>> getSecondaryToRegionInfoMap() {
    return this.secondaryRSToRegionMap;
  }

  public Map<ServerName, List<RegionInfo>> getTertiaryToRegionInfoMap() {
    return this.teritiaryRSToRegionMap;
  }

  public Map<ServerName, List<RegionInfo>> getPrimaryToRegionInfoMap() {
    return this.primaryRSToRegionMap;
  }
}
