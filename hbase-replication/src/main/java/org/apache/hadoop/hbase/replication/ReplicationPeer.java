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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * ReplicationPeer manages enabled / disabled state for the peer.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public interface ReplicationPeer {

  /**
   * State of the peer, whether it is enabled or not
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
  enum PeerState {
    ENABLED,
    DISABLED
  }

  /**
   * Get the identifier of this peer
   * @return string representation of the id
   */
  String getId();

  /**
   * Returns the state of the peer by reading local cache.
   * @return the enabled state
   */
  PeerState getPeerState();

  /**
   * Returns the sync replication state of the peer by reading local cache.
   * <p>
   * If the peer is not a synchronous replication peer, a {@link SyncReplicationState#NONE} will be
   * returned.
   * @return the sync replication state
   */
  SyncReplicationState getSyncReplicationState();

  /**
   * Test whether the peer is enabled.
   * @return {@code true} if enabled, otherwise {@code false}.
   */
  default boolean isPeerEnabled() {
    return getPeerState() == PeerState.ENABLED;
  }

  /**
   * Get the peer config object
   * @return the ReplicationPeerConfig for this peer
   */
  ReplicationPeerConfig getPeerConfig();

  /**
   * Get the configuration object required to communicate with this peer
   * @return configuration object
   */
  Configuration getConfiguration();

  /**
   * Get replicable (table, cf-list) map of this peer
   * @return the replicable (table, cf-list) map
   */
  Map<TableName, List<String>> getTableCFs();

  /**
   * Get replicable namespace set of this peer
   * @return the replicable namespaces set
   */
  Set<String> getNamespaces();

  /**
   * Get the per node bandwidth upper limit for this peer
   * @return the bandwidth upper limit
   */
  long getPeerBandwidth();

  /**
   * Register a peer config listener to catch the peer config change event.
   * @param listener listener to catch the peer config change event.
   */
  void registerPeerConfigListener(ReplicationPeerConfigListener listener);

  /**
   * @deprecated Use {@link #registerPeerConfigListener(ReplicationPeerConfigListener)} instead.
   */
  @Deprecated
  default void trackPeerConfigChanges(ReplicationPeerConfigListener listener) {
    registerPeerConfigListener(listener);
  }
}