package org.apache.hadoop.hbase.master.assignment;

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

  private final List<RegionState.State> DISABLE_TABLE_REGION_STATE =
    List.of(RegionState.State.OFFLINE, RegionState.State.CLOSED);
  private final List<RegionState.State> ENABLE_TABLE_REGION_STATE =
    List.of(RegionState.State.OPEN, RegionState.State.SPLIT, RegionState.State.MERGED);

  private static final Logger LOG = LoggerFactory.getLogger(RegionInTransitionTracker.class);

  private final ConcurrentSkipListMap<RegionInfo, RegionStateNode> regionInTransition =
    new ConcurrentSkipListMap<>(RegionInfo.COMPARATOR);
  private final TableStateManager tableStateManager;

  public RegionInTransitionTracker(TableStateManager tableStateManager) {
    this.tableStateManager = tableStateManager;
  }

  public boolean isRegionInTransition(final RegionInfo regionInfo) {
    return regionInTransition.containsKey(regionInfo);
  }

  public void handleRegionStateNodeOperation(RegionStateNode regionStateNode) {
    RegionState.State currentState = regionStateNode.getState();

    if (!getExceptedRegionStates(regionStateNode).contains(currentState)) {
      addRegionInTransition(regionStateNode);
    } else {
      removeRegionInTransition(regionStateNode.getRegionInfo());
    }
  }

  public void handleRegionDelete(RegionStateNode regionStateNode) {
    removeRegionInTransition(regionStateNode.getRegionInfo());
  }

  private List<RegionState.State> getExceptedRegionStates(RegionStateNode regionStateNode) {
    if (tableStateManager.isTableState(regionStateNode.getTable(), TableState.State.ENABLED,
      TableState.State.ENABLING)) {
      return ENABLE_TABLE_REGION_STATE;
    } else {
      return DISABLE_TABLE_REGION_STATE;
    }
  }

  public void addRegionInTransition(final RegionStateNode regionStateNode) {
    if (regionInTransition.putIfAbsent(regionStateNode.getRegionInfo(), regionStateNode) == null) {
      LOG.info("Added to RIT list" + regionStateNode.getRegionInfo().getEncodedName());
    }
  }

  public void removeRegionInTransition(final RegionInfo regionInfo) {
    if (regionInTransition.remove(regionInfo) != null) {
      LOG.info("Removed from RIT list" + regionInfo);
    }
  }

  public void clear() {
    regionInTransition.clear();
  }

  public boolean hasRegionsInTransition() {
    return !regionInTransition.isEmpty();
  }

  public ConcurrentSkipListMap<RegionInfo, RegionStateNode> getRegionsInTransition() {
    return regionInTransition;
  }

}
