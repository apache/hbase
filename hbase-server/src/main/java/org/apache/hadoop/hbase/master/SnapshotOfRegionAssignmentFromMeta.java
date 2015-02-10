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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.Visitor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.balancer.FavoredNodeAssignmentHelper;
import org.apache.hadoop.hbase.master.balancer.FavoredNodesPlan;

/**
 * Used internally for reading meta and constructing datastructures that are
 * then queried, for things like regions to regionservers, table to regions, etc.
 * It also records the favored nodes mapping for regions.
 *
 */
@InterfaceAudience.Private
public class SnapshotOfRegionAssignmentFromMeta {
  private static final Log LOG = LogFactory.getLog(SnapshotOfRegionAssignmentFromMeta.class
      .getName());

  private final Connection connection;

  /** the table name to region map */
  private final Map<TableName, List<HRegionInfo>> tableToRegionMap;
  /** the region to region server map */
  //private final Map<HRegionInfo, ServerName> regionToRegionServerMap;
  private Map<HRegionInfo, ServerName> regionToRegionServerMap;
  /** the region name to region info map */
  private final Map<String, HRegionInfo> regionNameToRegionInfoMap;

  /** the regionServer to region map */
  private final Map<ServerName, List<HRegionInfo>> regionServerToRegionMap;
  /** the existing assignment plan in the hbase:meta region */
  private final FavoredNodesPlan existingAssignmentPlan;
  private final Set<TableName> disabledTables;
  private final boolean excludeOfflinedSplitParents;

  public SnapshotOfRegionAssignmentFromMeta(Connection connection) {
    this(connection, new HashSet<TableName>(), false);
  }

  public SnapshotOfRegionAssignmentFromMeta(Connection connection, Set<TableName> disabledTables,
      boolean excludeOfflinedSplitParents) {
    this.connection = connection;
    tableToRegionMap = new HashMap<TableName, List<HRegionInfo>>();
    regionToRegionServerMap = new HashMap<HRegionInfo, ServerName>();
    regionServerToRegionMap = new HashMap<ServerName, List<HRegionInfo>>();
    regionNameToRegionInfoMap = new TreeMap<String, HRegionInfo>();
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
          HRegionInfo hri = rl.getRegionLocation(0).getRegionInfo();
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

  private void addRegion(HRegionInfo regionInfo) {
    // Process the region name to region info map
    regionNameToRegionInfoMap.put(regionInfo.getRegionNameAsString(), regionInfo);

    // Process the table to region map
    TableName tableName = regionInfo.getTable();
    List<HRegionInfo> regionList = tableToRegionMap.get(tableName);
    if (regionList == null) {
      regionList = new ArrayList<HRegionInfo>();
    }
    // Add the current region info into the tableToRegionMap
    regionList.add(regionInfo);
    tableToRegionMap.put(tableName, regionList);
  }

  private void addAssignment(HRegionInfo regionInfo, ServerName server) {
    // Process the region to region server map
    regionToRegionServerMap.put(regionInfo, server);

    if (server == null) return;

    // Process the region server to region map
    List<HRegionInfo> regionList = regionServerToRegionMap.get(server);
    if (regionList == null) {
      regionList = new ArrayList<HRegionInfo>();
    }
    regionList.add(regionInfo);
    regionServerToRegionMap.put(server, regionList);
  }

  /**
   * Get the regioninfo for a region
   * @return the regioninfo
   */
  public Map<String, HRegionInfo> getRegionNameToRegionInfoMap() {
    return this.regionNameToRegionInfoMap;
  }

  /**
   * Get regions for tables
   * @return a mapping from table to regions
   */
  public Map<TableName, List<HRegionInfo>> getTableToRegionMap() {
    return tableToRegionMap;
  }

  /**
   * Get region to region server map
   * @return region to region server map
   */
  public Map<HRegionInfo, ServerName> getRegionToRegionServerMap() {
    return regionToRegionServerMap;
  }

  /**
   * Get regionserver to region map
   * @return regionserver to region map
   */
  public Map<ServerName, List<HRegionInfo>> getRegionServerToRegionMap() {
    return regionServerToRegionMap;
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
}
