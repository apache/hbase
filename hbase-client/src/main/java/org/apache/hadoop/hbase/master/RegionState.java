/**
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

import java.util.Date;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

/**
 * State of a Region while undergoing transitions.
 * This class is immutable.
 */
@InterfaceAudience.Private
public class RegionState {

  @InterfaceAudience.Private
  @InterfaceStability.Evolving
  public enum State {
    OFFLINE,        // region is in an offline state
    PENDING_OPEN,   // same as OPENING, to be removed
    OPENING,        // server has begun to open but not yet done
    OPEN,           // server opened region and updated meta
    PENDING_CLOSE,  // same as CLOSING, to be removed
    CLOSING,        // server has begun to close but not yet done
    CLOSED,         // server closed region and updated meta
    SPLITTING,      // server started split of a region
    SPLIT,          // server completed split of a region
    FAILED_OPEN,    // failed to open, and won't retry any more
    FAILED_CLOSE,   // failed to close, and won't retry any more
    MERGING,        // server started merge a region
    MERGED,         // server completed merge a region
    SPLITTING_NEW,  // new region to be created when RS splits a parent
                    // region but hasn't be created yet, or master doesn't
                    // know it's already created
    MERGING_NEW;    // new region to be created when RS merges two
                    // daughter regions but hasn't be created yet, or
                    // master doesn't know it's already created

    /**
     * Convert to protobuf ClusterStatusProtos.RegionState.State
     */
    public ClusterStatusProtos.RegionState.State convert() {
      ClusterStatusProtos.RegionState.State rs;
      switch (this) {
      case OFFLINE:
        rs = ClusterStatusProtos.RegionState.State.OFFLINE;
        break;
      case PENDING_OPEN:
        rs = ClusterStatusProtos.RegionState.State.PENDING_OPEN;
        break;
      case OPENING:
        rs = ClusterStatusProtos.RegionState.State.OPENING;
        break;
      case OPEN:
        rs = ClusterStatusProtos.RegionState.State.OPEN;
        break;
      case PENDING_CLOSE:
        rs = ClusterStatusProtos.RegionState.State.PENDING_CLOSE;
        break;
      case CLOSING:
        rs = ClusterStatusProtos.RegionState.State.CLOSING;
        break;
      case CLOSED:
        rs = ClusterStatusProtos.RegionState.State.CLOSED;
        break;
      case SPLITTING:
        rs = ClusterStatusProtos.RegionState.State.SPLITTING;
        break;
      case SPLIT:
        rs = ClusterStatusProtos.RegionState.State.SPLIT;
        break;
      case FAILED_OPEN:
        rs = ClusterStatusProtos.RegionState.State.FAILED_OPEN;
        break;
      case FAILED_CLOSE:
        rs = ClusterStatusProtos.RegionState.State.FAILED_CLOSE;
        break;
      case MERGING:
        rs = ClusterStatusProtos.RegionState.State.MERGING;
        break;
      case MERGED:
        rs = ClusterStatusProtos.RegionState.State.MERGED;
        break;
      case SPLITTING_NEW:
        rs = ClusterStatusProtos.RegionState.State.SPLITTING_NEW;
        break;
      case MERGING_NEW:
        rs = ClusterStatusProtos.RegionState.State.MERGING_NEW;
        break;
      default:
        throw new IllegalStateException("");
      }
      return rs;
    }

    /**
     * Convert a protobuf HBaseProtos.RegionState.State to a RegionState.State
     *
     * @return the RegionState.State
     */
    public static State convert(ClusterStatusProtos.RegionState.State protoState) {
      State state;
      switch (protoState) {
      case OFFLINE:
        state = OFFLINE;
        break;
      case PENDING_OPEN:
        state = PENDING_OPEN;
        break;
      case OPENING:
        state = OPENING;
        break;
      case OPEN:
        state = OPEN;
        break;
      case PENDING_CLOSE:
        state = PENDING_CLOSE;
        break;
      case CLOSING:
        state = CLOSING;
        break;
      case CLOSED:
        state = CLOSED;
        break;
      case SPLITTING:
        state = SPLITTING;
        break;
      case SPLIT:
        state = SPLIT;
        break;
      case FAILED_OPEN:
        state = FAILED_OPEN;
        break;
      case FAILED_CLOSE:
        state = FAILED_CLOSE;
        break;
      case MERGING:
        state = MERGING;
        break;
      case MERGED:
        state = MERGED;
        break;
      case SPLITTING_NEW:
        state = SPLITTING_NEW;
        break;
      case MERGING_NEW:
        state = MERGING_NEW;
        break;
      default:
        throw new IllegalStateException("");
      }
      return state;
    }
  }

  private final long stamp;
  private final HRegionInfo hri;
  private final ServerName serverName;
  private final State state;

  public RegionState(HRegionInfo region, State state) {
    this(region, state, System.currentTimeMillis(), null);
  }

  public RegionState(HRegionInfo region,
      State state, ServerName serverName) {
    this(region, state, System.currentTimeMillis(), serverName);
  }

  public RegionState(HRegionInfo region,
      State state, long stamp, ServerName serverName) {
    this.hri = region;
    this.state = state;
    this.stamp = stamp;
    this.serverName = serverName;
  }

  public State getState() {
    return state;
  }

  public long getStamp() {
    return stamp;
  }

  public HRegionInfo getRegion() {
    return hri;
  }

  public ServerName getServerName() {
    return serverName;
  }

  /**
   * PENDING_CLOSE (to be removed) is the same as CLOSING
   */
  public boolean isClosing() {
    return state == State.PENDING_CLOSE || state == State.CLOSING;
  }

  public boolean isClosed() {
    return state == State.CLOSED;
  }

  /**
   * PENDING_OPEN (to be removed) is the same as OPENING
   */
  public boolean isOpening() {
    return state == State.PENDING_OPEN || state == State.OPENING;
  }

  public boolean isOpened() {
    return state == State.OPEN;
  }

  public boolean isOffline() {
    return state == State.OFFLINE;
  }

  public boolean isSplitting() {
    return state == State.SPLITTING;
  }

  public boolean isSplit() {
    return state == State.SPLIT;
  }

  public boolean isSplittingNew() {
    return state == State.SPLITTING_NEW;
  }

  public boolean isFailedOpen() {
    return state == State.FAILED_OPEN;
  }

  public boolean isFailedClose() {
    return state == State.FAILED_CLOSE;
  }

  public boolean isMerging() {
    return state == State.MERGING;
  }

  public boolean isMerged() {
    return state == State.MERGED;
  }

  public boolean isMergingNew() {
    return state == State.MERGING_NEW;
  }

  public boolean isOnServer(final ServerName sn) {
    return serverName != null && serverName.equals(sn);
  }

  public boolean isMergingOnServer(final ServerName sn) {
    return isOnServer(sn) && isMerging();
  }

  public boolean isMergingNewOnServer(final ServerName sn) {
    return isOnServer(sn) && isMergingNew();
  }

  public boolean isMergingNewOrOpenedOnServer(final ServerName sn) {
    return isOnServer(sn) && (isMergingNew() || isOpened());
  }

  public boolean isMergingNewOrOfflineOnServer(final ServerName sn) {
    return isOnServer(sn) && (isMergingNew() || isOffline());
  }

  public boolean isSplittingOnServer(final ServerName sn) {
    return isOnServer(sn) && isSplitting();
  }

  public boolean isSplittingNewOnServer(final ServerName sn) {
    return isOnServer(sn) && isSplittingNew();
  }

  public boolean isSplittingOrOpenedOnServer(final ServerName sn) {
    return isOnServer(sn) && (isSplitting() || isOpened());
  }

  public boolean isSplittingOrSplitOnServer(final ServerName sn) {
    return isOnServer(sn) && (isSplitting() || isSplit());
  }

  public boolean isClosingOrClosedOnServer(final ServerName sn) {
    return isOnServer(sn) && (isClosing() || isClosed());
  }

  public boolean isOpeningOrFailedOpenOnServer(final ServerName sn) {
    return isOnServer(sn) && (isOpening() || isFailedOpen());
  }

  public boolean isOpeningOrOpenedOnServer(final ServerName sn) {
    return isOnServer(sn) && (isOpening() || isOpened());
  }

  public boolean isOpenedOnServer(final ServerName sn) {
    return isOnServer(sn) && isOpened();
  }

  /**
   * Check if a region state can transition to offline
   */
  public boolean isReadyToOffline() {
    return isMerged() || isSplit() || isOffline()
      || isSplittingNew() || isMergingNew();
  }

  /**
   * Check if a region state can transition to online
   */
  public boolean isReadyToOnline() {
    return isOpened() || isSplittingNew() || isMergingNew();
  }

  /**
   * Check if a region state is one of offline states that
   * can't transition to pending_close/closing (unassign/offline)
   */
  public boolean isUnassignable() {
    return isUnassignable(state);
  }

  /**
   * Check if a region state is one of offline states that
   * can't transition to pending_close/closing (unassign/offline)
   */
  public static boolean isUnassignable(State state) {
    return state == State.MERGED || state == State.SPLIT || state == State.OFFLINE
      || state == State.SPLITTING_NEW || state == State.MERGING_NEW;
  }

  @Override
  public String toString() {
    return "{" + hri.getShortNameToLog()
      + " state=" + state
      + ", ts=" + stamp
      + ", server=" + serverName + "}";
  }

  /**
   * A slower (but more easy-to-read) stringification
   */
  public String toDescriptiveString() {
    long relTime = System.currentTimeMillis() - stamp;
    return hri.getRegionNameAsString()
      + " state=" + state
      + ", ts=" + new Date(stamp) + " (" + (relTime/1000) + "s ago)"
      + ", server=" + serverName;
  }

  /**
   * Convert a RegionState to an HBaseProtos.RegionState
   *
   * @return the converted HBaseProtos.RegionState
   */
  public ClusterStatusProtos.RegionState convert() {
    ClusterStatusProtos.RegionState.Builder regionState = ClusterStatusProtos.RegionState.newBuilder();
    regionState.setRegionInfo(HRegionInfo.convert(hri));
    regionState.setState(state.convert());
    regionState.setStamp(getStamp());
    return regionState.build();
  }

  /**
   * Convert a protobuf HBaseProtos.RegionState to a RegionState
   *
   * @return the RegionState
   */
  public static RegionState convert(ClusterStatusProtos.RegionState proto) {
    return new RegionState(HRegionInfo.convert(proto.getRegionInfo()),
      State.convert(proto.getState()), proto.getStamp(), null);
  }

  /**
   * Check if two states are the same, except timestamp
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    RegionState tmp = (RegionState)obj;
    return tmp.hri.equals(hri) && tmp.state == state
      && ((serverName != null && serverName.equals(tmp.serverName))
        || (tmp.serverName == null && serverName == null));
  }

  /**
   * Don't count timestamp in hash code calculation
   */
  @Override
  public int hashCode() {
    return (serverName != null ? serverName.hashCode() * 11 : 0)
      + hri.hashCode() + 5 * state.ordinal();
  }
}
