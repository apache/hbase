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
package org.apache.hadoop.hbase.replication.regionserver;

import java.util.Optional;
import java.util.function.BiPredicate;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
class SyncReplicationPeerInfoProviderImpl implements SyncReplicationPeerInfoProvider {

  private final ReplicationPeers replicationPeers;

  private final SyncReplicationPeerMappingManager mapping;

  SyncReplicationPeerInfoProviderImpl(ReplicationPeers replicationPeers,
      SyncReplicationPeerMappingManager mapping) {
    this.replicationPeers = replicationPeers;
    this.mapping = mapping;
  }

  @Override
  public Optional<Pair<String, String>> getPeerIdAndRemoteWALDir(TableName table) {
    if (table == null) {
      return Optional.empty();
    }
    String peerId = mapping.getPeerId(table);
    if (peerId == null) {
      return Optional.empty();
    }
    ReplicationPeerImpl peer = replicationPeers.getPeer(peerId);
    if (peer == null) {
      return Optional.empty();
    }
    Pair<SyncReplicationState, SyncReplicationState> states =
        peer.getSyncReplicationStateAndNewState();
    if ((states.getFirst() == SyncReplicationState.ACTIVE &&
      states.getSecond() == SyncReplicationState.NONE) ||
      (states.getFirst() == SyncReplicationState.DOWNGRADE_ACTIVE &&
        states.getSecond() == SyncReplicationState.ACTIVE)) {
      return Optional.of(Pair.newPair(peerId, peer.getPeerConfig().getRemoteWALDir()));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public boolean checkState(TableName table,
      BiPredicate<SyncReplicationState, SyncReplicationState> checker) {
    String peerId = mapping.getPeerId(table);
    if (peerId == null) {
      return false;
    }
    ReplicationPeerImpl peer = replicationPeers.getPeer(peerId);
    if (peer == null) {
      return false;
    }
    Pair<SyncReplicationState, SyncReplicationState> states =
      peer.getSyncReplicationStateAndNewState();
    return checker.test(states.getFirst(), states.getSecond());
  }
}
