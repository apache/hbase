/**
 * Copyright 2012 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Writable;

import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;

/**
 * AssignmentPlan is a writable object for the region assignment plan.
 * It contains the mapping information between each region and
 * its favored region server list.
 *
 * All the access to this class is thread-safe.
 */
@ThriftStruct
public class AssignmentPlan implements Writable{
  protected static final Log LOG = LogFactory.getLog(
      AssignmentPlan.class.getName());

  private static final int VERSION = 1;

  /** the map between each region and its favored region server list */
  private Map<HRegionInfo, List<HServerAddress>> assignmentMap;

  /** the map between each region and its lasted favored server list update
   * time stamp
  */
  private Map<HRegionInfo, Long> assignmentUpdateTS = new HashMap<>();

  public static enum POSITION {
    PRIMARY,
    SECONDARY,
    TERTIARY;
  };

  public AssignmentPlan() {
    assignmentMap = new HashMap<HRegionInfo, List<HServerAddress>>();
  }

  @ThriftConstructor
  public AssignmentPlan(
      @ThriftField(1) Map<HRegionInfo, List<HServerAddress>> assignmentMap) {
    this.assignmentMap = assignmentMap;
  }

  /**
   * Initialize the assignment plan with the existing primary region server map
   * and the existing secondary/tertiary region server map
   *
   * if any regions cannot find the proper secondary / tertiary region server
   * for whatever reason, just do NOT update the assignment plan for this region
   * @param primaryRSMap
   * @param secondaryAndTertiaryRSMap
   */
  public void initialize(Map<HRegionInfo, HServerAddress> primaryRSMap,
      Map<HRegionInfo, Pair<HServerAddress, HServerAddress>> secondaryAndTertiaryRSMap) {
    for (Map.Entry<HRegionInfo, Pair<HServerAddress, HServerAddress>> entry :
      secondaryAndTertiaryRSMap.entrySet()) {
      // Get the region info and their secondary/tertiary region server
      HRegionInfo regionInfo = entry.getKey();
      Pair<HServerAddress, HServerAddress> secondaryAndTertiaryPair =
        entry.getValue();

      // Get the primary region server
      HServerAddress primaryRS = primaryRSMap.get(regionInfo);
      if (primaryRS == null) {
        LOG.error("No primary region server for region " +
            regionInfo.getRegionNameAsString());
        continue;
      }

      // Update the assignment plan with the favored nodes
      List<HServerAddress> serverList = new ArrayList<HServerAddress>();
      serverList.add(POSITION.PRIMARY.ordinal(), primaryRS);
      serverList.add(POSITION.SECONDARY.ordinal(),
          secondaryAndTertiaryPair.getFirst());
      serverList.add(POSITION.TERTIARY.ordinal(),
          secondaryAndTertiaryPair.getSecond());
      this.updateAssignmentPlan(regionInfo, serverList);
    }
  }

  /**
   * Add an assignment to the plan
   * @param region
   * @param servers
   * @param ts
   */
  public synchronized void updateAssignmentPlan(HRegionInfo region,
      List<HServerAddress> servers, long ts) {
    if (region == null || servers == null || servers.size() ==0)
      return;
    this.assignmentUpdateTS.put(region, Long.valueOf(ts));
    this.assignmentMap.put(region, servers);
    LOG.info("Update the assignment plan for region " +
        region.getRegionNameAsString() + " to favored nodes " +
        RegionPlacement.getFavoredNodes(servers)
        + " at time stamp " + ts + " hashcode " + region.hashCode());
  }

  /**
   * Add an assignment to the plan
   * @param region
   * @param servers
   */
  public synchronized void updateAssignmentPlan(HRegionInfo region,
      List<HServerAddress> servers) {
    if (region == null || servers == null || servers.size() ==0)
      return;
    this.assignmentMap.put(region, servers);
    LOG.info("Update the assignment plan for region " +
        region.getRegionNameAsString() + " ; favored nodes " +
        RegionPlacement.getFavoredNodes(servers));
  }

  /**
   * Remove one assignment from the plan
   * @param region
   */
  public synchronized void removeAssignment(HRegionInfo region) {
    LOG.info("Remove the assignment plan for region " +
      region.getRegionNameAsString() + " to favored nodes "
      + " at time stamp " + System.currentTimeMillis() + " hashcode " +
      region.hashCode());
    this.assignmentMap.remove(region);
    this.assignmentUpdateTS.remove(region);
  }

  /**
   * @param region
   * @return true if there is an assignment plan for the particular region.
   */
  public synchronized boolean hasAssignment(HRegionInfo region) {
    return assignmentMap.containsKey(region);
  }

  /**
   * Returns a reference to the server list in the plan.
   */
  public synchronized List<HServerAddress> getAssignment(HRegionInfo region) {
    return assignmentMap.get(region);
  }

  /**
   * Scans the current assignment map serially to check if there is a region
   * present with the same name.
   *
   * This is a relatively expensive operation. It should be used only when
   * getAssignment() is not working.
   * @param regionName name of the region
   * @return List of favored nodes if found, else null
   */
  public synchronized List<HServerAddress> getAssignmentForRegion(
      byte[] regionName) {
    for (HRegionInfo info : assignmentMap.keySet()) {
      if (Bytes.equals(info.getRegionName(), regionName)) {
        LOG.info("Found entry for " + Bytes.toStringBinary(regionName));
        return getAssignment(info);
      }
    }
    return null;
  }

  /**
   * @param region
   * @return the last update time stamp for the region in the plan
   */
  public synchronized long getAssignmentUpdateTS(HRegionInfo region) {
    Long updateTS = assignmentUpdateTS.get(region);
    if (updateTS == null)
      return Long.MIN_VALUE;
    else
      return updateTS.longValue();
  }

  /**
   * @return the mapping between each region to its favored region server list
   */
  @ThriftField(1)
  public synchronized Map<HRegionInfo, List<HServerAddress>> getAssignmentMap() {
    return this.assignmentMap;
  }

  public Map<HRegionInfo, Long> getAssignmentUpdateTSMap() {
    return this.assignmentUpdateTS;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(VERSION);
    if (this.assignmentMap == null) {
      out.writeInt(0);
    }
    // write the size of the favored assignment map
    out.writeInt(this.assignmentMap.size());
    for (Map.Entry<HRegionInfo,  List<HServerAddress>> entry :
      assignmentMap.entrySet()) {
      // write the region info
      entry.getKey().write(out);
      // write the list of favored server list
      List<HServerAddress> serverList = entry.getValue();
      // write the size of the list
      out.writeInt(serverList.size());
      for (HServerAddress addr : serverList) {
        // write the element of the list
        addr.write(out);
      }
    }
  }

 @Override
  public void readFields(DataInput in) throws IOException{
   int version = in.readInt();
   if (version != VERSION) {
     throw new IOException("The version mismatch for the assignment plan. " +
         "The expected versioin is " + VERSION +
         " but the verion from the assigment plan is " + version);
   }
   // read the favoredAssignmentMap size
   int assignmentMapSize = in.readInt();
   for (int i = 0; i < assignmentMapSize; i++) {
     // read each region info
     HRegionInfo region = new HRegionInfo();
     region.readFields(in);
     // read the size of favored server list
     int serverListSize = in.readInt();
     List<HServerAddress> serverList =
       new ArrayList<HServerAddress>(serverListSize);
     for (int j = 0; j < serverListSize; j++) {
       HServerAddress addr = new HServerAddress();
       addr.readFields(in);
       serverList.add(addr);
     }

     // add the assignment to favoredAssignmentMap
     this.assignmentMap.put(region, serverList);
   }
 }

 @Override
 public boolean equals(Object o) {
   if (this == o) {
     return true;
   }
   if (o == null) {
     return false;
   }
   if (getClass() != o.getClass()) {
     return false;
   }
   // To compare the map from objec o is identical to current assignment map.
   Map<HRegionInfo, List<HServerAddress>> comparedMap=
     ((AssignmentPlan)o).getAssignmentMap();


   // compare the size
   if (comparedMap.size() != this.assignmentMap.size())
     return false;

   // compare each element in the assignment map
   for (Map.Entry<HRegionInfo, List<HServerAddress>> entry :
     comparedMap.entrySet()) {
     List<HServerAddress> serverList = this.assignmentMap.get(entry.getKey());
     if (serverList == null && entry.getValue() != null) {
       return false;
     } else if (!serverList.equals(entry.getValue())) {
       return false;
     }
   }
   return true;
 }

  /**
   * Returns the position of the passed server in the list of favored nodes (the
   * position can be primary, secondary or tertiary)
   *
   * @param favoredNodes
   * @param server
   * @return
   */
  public static AssignmentPlan.POSITION getFavoredServerPosition(
      List<HServerAddress> favoredNodes, HServerAddress server) {
    if (favoredNodes == null || server == null ||
        favoredNodes.size() != HConstants.FAVORED_NODES_NUM) {
      return null;
    }
    for (AssignmentPlan.POSITION p : AssignmentPlan.POSITION.values()) {
      if (favoredNodes.get(p.ordinal()).equals(server)) {
        return p;
      }
    }
    return null;
  }

  /**
   * Replaces a server on the specified position (primary, secondary or
   * tertiary) in the list of favored nodes with a new one.
   *
   * @param favoredNodes
   * @param posOfReplacement
   * @param newServer
   * @return
   */
  public static boolean replaceFavoredNodesServerWithNew(
      List<HServerAddress> favoredNodes, POSITION posOfReplacement,
      HServerAddress newServer) {
    if (favoredNodes == null || newServer == null
        || favoredNodes.size() != HConstants.FAVORED_NODES_NUM) {
      return false;
    }
    favoredNodes.set(posOfReplacement.ordinal(), newServer);
    return true;
  }
}
