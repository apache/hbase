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
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.TableStateManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RegionInTransitionTracker {
  private static final Logger LOG = LoggerFactory.getLogger(RegionInTransitionTracker.class);

  private final List<RegionState.State> DISABLE_TABLE_REGION_STATE =
    List.of(RegionState.State.OFFLINE, RegionState.State.CLOSED);

  private final List<RegionState.State> ENABLE_TABLE_REGION_STATE = List.of(RegionState.State.OPEN);

  private final ConcurrentSkipListMap<RegionInfo, RegionStateNode> regionInTransition =
    new ConcurrentSkipListMap<>(RegionInfo.COMPARATOR);

  private final TableStateManager tableStateManager;

  public RegionInTransitionTracker(TableStateManager tableStateManager) {
    this.tableStateManager = tableStateManager;
  }

  public boolean isRegionInTransition(final RegionInfo regionInfo) {
    return regionInTransition.containsKey(regionInfo);
  }

  public void regionCrashed(RegionStateNode regionStateNode) {
    if (addRegionInTransition(regionStateNode)) {
      LOG.debug("{} added to RIT list because hosting region server is crashed ",
        regionStateNode.getRegionInfo().getEncodedName());
    }
  }

  public void handleRegionStateNodeOperation(RegionStateNode regionStateNode) {
    // only consider default replica for availability
    if (regionStateNode.getRegionInfo().getReplicaId() != RegionInfo.DEFAULT_REPLICA_ID) {
      return;
    }

    RegionState.State currentState = regionStateNode.getState();
    boolean isTableEnabled = tableStateManager.isTableState(regionStateNode.getTable(),
      TableState.State.ENABLED, TableState.State.ENABLING);
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

}
