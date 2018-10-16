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
package org.apache.hadoop.hbase.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ReplicationPeerImpl implements ReplicationPeer {

  private final Configuration conf;

  private final String id;

  private volatile ReplicationPeerConfig peerConfig;

  private volatile PeerState peerState;

  // The lower 16 bits are the current sync replication state, the higher 16 bits are the new sync
  // replication state. Embedded in one int so user can not get an inconsistency view of state and
  // new state.
  private volatile int syncReplicationStateBits;

  private static final int SHIFT = 16;

  private static final int AND_BITS = 0xFFFF;

  private final List<ReplicationPeerConfigListener> peerConfigListeners;

  /**
   * Constructor that takes all the objects required to communicate with the specified peer, except
   * for the region server addresses.
   * @param conf configuration object to this peer
   * @param id string representation of this peer's identifier
   * @param peerConfig configuration for the replication peer
   */
  public ReplicationPeerImpl(Configuration conf, String id, ReplicationPeerConfig peerConfig,
      boolean peerState, SyncReplicationState syncReplicationState,
      SyncReplicationState newSyncReplicationState) {
    this.conf = conf;
    this.id = id;
    this.peerState = peerState ? PeerState.ENABLED : PeerState.DISABLED;
    this.peerConfig = peerConfig;
    this.syncReplicationStateBits =
      syncReplicationState.value() | (newSyncReplicationState.value() << SHIFT);
    this.peerConfigListeners = new ArrayList<>();
  }

  public void setPeerState(boolean enabled) {
    this.peerState = enabled ? PeerState.ENABLED : PeerState.DISABLED;
  }

  public void setPeerConfig(ReplicationPeerConfig peerConfig) {
    this.peerConfig = peerConfig;
    peerConfigListeners.forEach(listener -> listener.peerConfigUpdated(peerConfig));
  }

  public void setNewSyncReplicationState(SyncReplicationState newState) {
    this.syncReplicationStateBits =
      (this.syncReplicationStateBits & AND_BITS) | (newState.value() << SHIFT);
  }

  public void transitSyncReplicationState() {
    this.syncReplicationStateBits =
      (this.syncReplicationStateBits >>> SHIFT) | (SyncReplicationState.NONE.value() << SHIFT);
  }

  /**
   * Get the identifier of this peer
   * @return string representation of the id (short)
   */
  @Override
  public String getId() {
    return id;
  }

  @Override
  public PeerState getPeerState() {
    return peerState;
  }

  private static SyncReplicationState getSyncReplicationState(int bits) {
    return SyncReplicationState.valueOf(bits & AND_BITS);
  }

  private static SyncReplicationState getNewSyncReplicationState(int bits) {
    return SyncReplicationState.valueOf(bits >>> SHIFT);
  }

  public Pair<SyncReplicationState, SyncReplicationState> getSyncReplicationStateAndNewState() {
    int bits = this.syncReplicationStateBits;
    return Pair.newPair(getSyncReplicationState(bits), getNewSyncReplicationState(bits));
  }

  public SyncReplicationState getNewSyncReplicationState() {
    return getNewSyncReplicationState(syncReplicationStateBits);
  }

  @Override
  public SyncReplicationState getSyncReplicationState() {
    return getSyncReplicationState(syncReplicationStateBits);
  }

  @Override
  public ReplicationPeerConfig getPeerConfig() {
    return peerConfig;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public Map<TableName, List<String>> getTableCFs() {
    return this.peerConfig.getTableCFsMap();
  }

  @Override
  public Set<String> getNamespaces() {
    return this.peerConfig.getNamespaces();
  }

  @Override
  public long getPeerBandwidth() {
    return this.peerConfig.getBandwidth();
  }

  @Override
  public void registerPeerConfigListener(ReplicationPeerConfigListener listener) {
    this.peerConfigListeners.add(listener);
  }
}
