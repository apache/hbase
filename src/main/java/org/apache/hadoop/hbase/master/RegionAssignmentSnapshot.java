/**
 * Copyright 2011 The Apache Software Foundation
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;

public class RegionAssignmentSnapshot {
  private static final Log LOG = LogFactory.getLog(RegionAssignmentSnapshot.class
      .getName());

  private Configuration conf;

  /** the table name to region map */
  private final Map<String, List<HRegionInfo>> tableToRegionMap;
  /** the region to region server map */
  private final Map<HRegionInfo, HServerAddress> regionToRegionServerMap;
  /** the region name to region info map */
  private final Map<String, HRegionInfo> regionNameToRegionInfoMap;
  
  private final Map<String, Map<HServerAddress, List<HRegionInfo>>> rsToRegionsPerTable;
  /** the regionServer to region map */
  private final Map<HServerAddress, List<HRegionInfo>> regionServerToRegionMap;
  /** the existing assignment plan in the META region */
  private final AssignmentPlan exsitingAssignmentPlan;
  /** The rack view for the current region server */
  private final AssignmentDomain globalAssignmentDomain;
  
  public RegionAssignmentSnapshot(Configuration conf) {
    this.conf = conf;
    tableToRegionMap = new HashMap<String, List<HRegionInfo>>();
    regionToRegionServerMap = new HashMap<HRegionInfo, HServerAddress>();
    regionServerToRegionMap = new HashMap<HServerAddress, List<HRegionInfo>>();
    regionNameToRegionInfoMap = new TreeMap<String, HRegionInfo>();
    rsToRegionsPerTable = new HashMap<>();
    exsitingAssignmentPlan = new AssignmentPlan();
    globalAssignmentDomain = new AssignmentDomain(conf);
  }

  /**
   * Initialize the region assignment snapshot by scanning the META table
   * @throws IOException
   */
  public void initialize() throws IOException {
    LOG.info("Start to scan the META for the current region assignment " +
        "snapshot");
    
    // Add all the online region servers
    HBaseAdmin admin  = new HBaseAdmin(conf);
    for (HServerInfo serverInfo : admin.getClusterStatus().getServerInfo()) {
      globalAssignmentDomain.addServer(serverInfo.getServerAddress());
    }
    
    MetaScannerVisitor visitor = new MetaScannerVisitor() {
      public boolean processRow(Result result) throws IOException {
        try {
          byte[] region = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.REGIONINFO_QUALIFIER);
          byte[] server = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.SERVER_QUALIFIER);
          // Process the region info
          if (region == null) return true;
          HRegionInfo regionInfo = Writables.getHRegionInfo(region);
          if (regionInfo == null || regionInfo.isSplit()) {
            return true;
          }
          addRegion(regionInfo);
          
          // Process the region server
          if (server == null) return true;
          HServerAddress regionServer = new HServerAddress(Bytes.toString(server));
          
          // Add the current assignment to the snapshot
          addAssignment(regionInfo, regionServer);
          
          // Process the assignment plan
          byte[] favoredNodes = result.getValue(HConstants.CATALOG_FAMILY,
              HConstants.FAVOREDNODES_QUALIFIER);
          if (favoredNodes == null) return true;
          // Add the favored nodes into assignment plan
          List<HServerAddress> favoredServerList =
            RegionPlacement.getFavoredNodesList(favoredNodes);
          exsitingAssignmentPlan.updateAssignmentPlan(regionInfo,
              favoredServerList);
          return true;
        } catch (RuntimeException e) {
          LOG.error("Catche remote exception " + e.getMessage() +
              " when processing" + result);
          throw e;
        }
      }
    };

    // Scan .META. to pick up user regions
    MetaScanner.metaScan(conf, visitor);
    LOG.info("Finished to scan the META for the current region assignment" +
      "snapshot");
  }
  
  private void addRegion(HRegionInfo regionInfo) {
    if (regionInfo == null)
      return;
    // Process the region name to region info map
    regionNameToRegionInfoMap.put(regionInfo.getRegionNameAsString(), regionInfo);
    
    // Process the table to region map
    String tableName = regionInfo.getTableDesc().getNameAsString();
    List<HRegionInfo> regionList = tableToRegionMap.get(tableName);
    if (regionList == null) {
      regionList = new ArrayList<HRegionInfo>();
    }
    // Add the current region info into the tableToRegionMap
    regionList.add(regionInfo);
    tableToRegionMap.put(tableName, regionList);
  }
  
  private void addAssignment(HRegionInfo regionInfo, HServerAddress server) {
    if (server != null && regionInfo != null) {
      // Process the region to region server map
      regionToRegionServerMap.put(regionInfo, server);
      
      // Process the region server to region map
      List<HRegionInfo> regionList = regionServerToRegionMap.get(server);
      if (regionList == null) {
        regionList = new ArrayList<HRegionInfo>();
      }
      regionList.add(regionInfo);
      regionServerToRegionMap.put(server, regionList);

      // update rsToRegionsPerTable accordingly
      String tblName = regionInfo.getTableDesc().getNameAsString();
      Map<HServerAddress, List<HRegionInfo>> assignment = rsToRegionsPerTable.get(tblName);
      if (assignment== null){
        assignment = new HashMap<>();
        rsToRegionsPerTable.put(tblName, assignment);
      }
      List<HRegionInfo> regions = assignment.get(server);
      if (regions == null) {
        regions = new ArrayList<>();
        assignment.put(server, regions);
      }
      regions.add(regionInfo);
    } 
  }

  public Map<String, HRegionInfo> getRegionNameToRegionInfoMap() {
    return this.regionNameToRegionInfoMap;
  }
  
  public Map<String, List<HRegionInfo>> getTableToRegionMap() {
    return tableToRegionMap;
  }

  public Map<HRegionInfo, HServerAddress> getRegionToRegionServerMap() {
    return regionToRegionServerMap;
  }

  public Map<HServerAddress, List<HRegionInfo>> getRegionServerToRegionMap() {
    return regionServerToRegionMap;
  }
  
  public AssignmentPlan getExistingAssignmentPlan() {
    return this.exsitingAssignmentPlan;
  }

  public AssignmentDomain getGlobalAssignmentDomain() {
    return this.globalAssignmentDomain;
  }
  
  public Set<String> getTableSet() {
    return this.tableToRegionMap.keySet();
  }

  /**
   * Returns assignment regionserver->regions per table
   *
   */
  public Map<String, Map<HServerAddress, List<HRegionInfo>>> getRegionServerToRegionsPerTable() {
    return this.rsToRegionsPerTable;
  }
}
