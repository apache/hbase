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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;

/**
 * State of a Region while undergoing transitions.
 * Region state cannot be modified except the stamp field.
 * So it is almost immutable.
 */
@InterfaceAudience.Private
public class RegionState {

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public enum State {
    OFFLINE,        // region is in an offline state
    PENDING_OPEN,   // sent rpc to server to open but has not begun
    OPENING,        // server has begun to open but not yet done
    OPEN,           // server opened region and updated meta
    PENDING_CLOSE,  // sent rpc to server to close but has not begun
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
    MERGING_NEW     // new region to be created when RS merges two
                    // daughter regions but hasn't be created yet, or
                    // master doesn't know it's already created
  }

  // Many threads can update the state at the stamp at the same time
  private final AtomicLong stamp;
  private HRegionInfo hri;

  private volatile ServerName serverName;
  private volatile State state;

  public RegionState() {
    this.stamp = new AtomicLong(System.currentTimeMillis());
  }

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
    this.stamp = new AtomicLong(stamp);
    this.serverName = serverName;
  }

  public void updateTimestampToNow() {
    setTimestamp(System.currentTimeMillis());
  }

  public State getState() {
    return state;
  }

  public long getStamp() {
    return stamp.get();
  }

  public HRegionInfo getRegion() {
    return hri;
  }

  public ServerName getServerName() {
    return serverName;
  }

  public boolean isClosing() {
    return state == State.CLOSING;
  }

  public boolean isClosed() {
    return state == State.CLOSED;
  }

  public boolean isPendingClose() {
    return state == State.PENDING_CLOSE;
  }

  public boolean isOpening() {
    return state == State.OPENING;
  }

  public boolean isOpened() {
    return state == State.OPEN;
  }

  public boolean isPendingOpen() {
    return state == State.PENDING_OPEN;
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

  public boolean isOpenOrMergingOnServer(final ServerName sn) {
    return isOnServer(sn) && (isOpened() || isMerging());
  }

  public boolean isOpenOrMergingNewOnServer(final ServerName sn) {
    return isOnServer(sn) && (isOpened() || isMergingNew());
  }

  public boolean isOpenOrSplittingOnServer(final ServerName sn) {
    return isOnServer(sn) && (isOpened() || isSplitting());
  }

  public boolean isOpenOrSplittingNewOnServer(final ServerName sn) {
    return isOnServer(sn) && (isOpened() || isSplittingNew());
  }

  public boolean isPendingOpenOrOpeningOnServer(final ServerName sn) {
    return isOnServer(sn) && isPendingOpenOrOpening();
  }

  // Failed open is also kind of pending open
  public boolean isPendingOpenOrOpening() {
    return isPendingOpen() || isOpening() || isFailedOpen();
  }

  public boolean isPendingCloseOrClosingOnServer(final ServerName sn) {
    return isOnServer(sn) && isPendingCloseOrClosing();
  }

  // Failed close is also kind of pending close
  public boolean isPendingCloseOrClosing() {
    return isPendingClose() || isClosing() || isFailedClose();
  }

  public boolean isOnServer(final ServerName sn) {
    return serverName != null && serverName.equals(sn);
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
    long lstamp = stamp.get();
    long relTime = System.currentTimeMillis() - lstamp;
    
    return hri.getRegionNameAsString()
      + " state=" + state
      + ", ts=" + new Date(lstamp) + " (" + (relTime/1000) + "s ago)"
      + ", server=" + serverName;
  }

  /**
   * Convert a RegionState to an HBaseProtos.RegionState
   *
   * @return the converted HBaseProtos.RegionState
   */
  public ClusterStatusProtos.RegionState convert() {
    ClusterStatusProtos.RegionState.Builder regionState = ClusterStatusProtos.RegionState.newBuilder();
    ClusterStatusProtos.RegionState.State rs;
    switch (this.state) {
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
    regionState.setRegionInfo(HRegionInfo.convert(hri));
    regionState.setState(rs);
    regionState.setStamp(getStamp());
    return regionState.build();
  }

  /**
   * Convert a protobuf HBaseProtos.RegionState to a RegionState
   *
   * @return the RegionState
   */
  public static RegionState convert(ClusterStatusProtos.RegionState proto) {
    RegionState.State state;
    switch (proto.getState()) {
    case OFFLINE:
      state = State.OFFLINE;
      break;
    case PENDING_OPEN:
      state = State.PENDING_OPEN;
      break;
    case OPENING:
      state = State.OPENING;
      break;
    case OPEN:
      state = State.OPEN;
      break;
    case PENDING_CLOSE:
      state = State.PENDING_CLOSE;
      break;
    case CLOSING:
      state = State.CLOSING;
      break;
    case CLOSED:
      state = State.CLOSED;
      break;
    case SPLITTING:
      state = State.SPLITTING;
      break;
    case SPLIT:
      state = State.SPLIT;
      break;
    case FAILED_OPEN:
      state = State.FAILED_OPEN;
      break;
    case FAILED_CLOSE:
      state = State.FAILED_CLOSE;
      break;
    case MERGING:
      state = State.MERGING;
      break;
    case MERGED:
      state = State.MERGED;
      break;
    case SPLITTING_NEW:
      state = State.SPLITTING_NEW;
      break;
    case MERGING_NEW:
      state = State.MERGING_NEW;
      break;
    default:
      throw new IllegalStateException("");
    }

    return new RegionState(HRegionInfo.convert(proto.getRegionInfo()),state,proto.getStamp(),null);
  }

  protected void setTimestamp(final long timestamp) {
    stamp.set(timestamp);
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
