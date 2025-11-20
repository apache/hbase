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
package org.apache.hadoop.hbase.master.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracks regions that are currently in transition (RIT) - those not yet in their terminal state.
 */
@InterfaceAudience.Private
public class RegionInTransitionTracker {
  private static final Logger LOG = LoggerFactory.getLogger(RegionInTransitionTracker.class);

  private final List<RegionState.State> DISABLE_TABLE_REGION_STATE =
    Arrays.asList(RegionState.State.OFFLINE, RegionState.State.CLOSED);

  private final List<RegionState.State> ENABLE_TABLE_REGION_STATE =
    Collections.singletonList(RegionState.State.OPEN);

  private final ConcurrentSkipListMap<RegionInfo, RegionStateNode> regionInTransition =
    new ConcurrentSkipListMap<>(RegionInfo.COMPARATOR);

  private TableStateManager tableStateManager;

  public boolean isRegionInTransition(final RegionInfo regionInfo) {
    return regionInTransition.containsKey(regionInfo);
  }

  /**
   * Handles a region whose hosting RegionServer has crashed. When a RegionServer fails, all regions
   * it was hosting are automatically added to the RIT list since they need to be reassigned to
   * other servers.
   */
  public void regionCrashed(RegionStateNode regionStateNode) {
    if (regionStateNode.getRegionInfo().getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      return;
    }

    if (addRegionInTransition(regionStateNode)) {
      LOG.debug("{} added to RIT list because hosting region server is crashed ",
        regionStateNode.getRegionInfo().getEncodedName());
    }
  }

  /**
   * Processes a region state change and updates the RIT tracking accordingly. This is the core
   * method that determines whether a region should be added to or removed from the RIT list based
   * on its current state and the table's enabled/disabled status. This method should be called
   * whenever a region state changes get stored to hbase:meta Note: Only default replicas (replica
   * ID 0) are tracked. Read replicas are ignored.
   * @param regionStateNode the region state node with the current state information
   */
  public void handleRegionStateNodeOperation(RegionStateNode regionStateNode) {
    // only consider default replica for availability
    if (regionStateNode.getRegionInfo().getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      return;
    }

    RegionState.State currentState = regionStateNode.getState();
    boolean isTableEnabled = isIsTableEnabled(regionStateNode.getTable());
    List<RegionState.State> terminalStates =
      isTableEnabled ? ENABLE_TABLE_REGION_STATE : DISABLE_TABLE_REGION_STATE;

    // if region is merged or split it should not be in RIT list
    if (
      currentState == RegionState.State.SPLIT || currentState == RegionState.State.MERGED
        || regionStateNode.getRegionInfo().isSplit()
    ) {
      if (removeRegionInTransition(regionStateNode.getRegionInfo())) {
        LOG.debug("Removed {} from RIT list as it is split or merged",
          regionStateNode.getRegionInfo().getEncodedName());
      }
    } else if (!terminalStates.contains(currentState)) {
      if (addRegionInTransition(regionStateNode)) {
        LOG.debug("{} added to RIT list because it is in-between state, region state : {} ",
          regionStateNode.getRegionInfo().getEncodedName(), currentState);
      }
    } else {
      if (removeRegionInTransition(regionStateNode.getRegionInfo())) {
        LOG.debug("Removed {} from RIT list as reached to terminal state {}",
          regionStateNode.getRegionInfo().getEncodedName(), currentState);
      }
    }
  }

  private boolean isIsTableEnabled(TableName tableName) {
    if (tableStateManager != null) {
      return tableStateManager.isTableState(tableName, TableState.State.ENABLED,
        TableState.State.ENABLING);
    }
    // AssignmentManager calls setTableStateManager once hbase:meta is confirmed online, if it is
    // still null it means confirmation is still pending. One should not access TableStateManger
    // till the time.
    if (TableName.isMetaTableName(tableName)) {
      return true;
    } else {
      throw new RuntimeException(
        "TableStateManger can not be accessed before hbase:meta is confirmed on line");
    }
  }

  /**
   * Handles the deletion of a region by removing it from RIT tracking. This is called when a region
   * is permanently removed from the cluster, typically after a successful merge operation where the
   * parent regions are cleaned up. During table deletion, table should be already disabled and all
   * the region are already OFFLINE
   * @param regionInfo the region being deleted
   */
  public void handleRegionDelete(RegionInfo regionInfo) {
    removeRegionInTransition(regionInfo);
  }

  private boolean addRegionInTransition(final RegionStateNode regionStateNode) {
    return regionInTransition.putIfAbsent(regionStateNode.getRegionInfo(), regionStateNode) == null;
  }

  private boolean removeRegionInTransition(final RegionInfo regionInfo) {
    return regionInTransition.remove(regionInfo) != null;
  }

  public void stop() {
    regionInTransition.clear();
  }

  public boolean hasRegionsInTransition() {
    return !regionInTransition.isEmpty();
  }

  public List<RegionStateNode> getRegionsInTransition() {
    return new ArrayList<>(regionInTransition.values());
  }

  public void setTableStateManager(TableStateManager tableStateManager) {
    this.tableStateManager = tableStateManager;
  }
}
