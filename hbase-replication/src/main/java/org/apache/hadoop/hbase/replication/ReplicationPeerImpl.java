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
package org.apache.hadoop.hbase.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ReplicationPeerImpl implements ReplicationPeer {

  private final Configuration conf;

  private final String id;

  private volatile ReplicationPeerConfig peerConfig;

  private volatile PeerState peerState;

  private final List<ReplicationPeerConfigListener> peerConfigListeners;

  /**
   * Constructor that takes all the objects required to communicate with the specified peer, except
   * for the region server addresses.
   * @param conf configuration object to this peer
   * @param id string representation of this peer's identifier
   * @param peerConfig configuration for the replication peer
   */
  public ReplicationPeerImpl(Configuration conf, String id, boolean peerState,
      ReplicationPeerConfig peerConfig) {
    this.conf = conf;
    this.id = id;
    setPeerState(peerState);
    this.peerConfig = peerConfig;
    this.peerConfigListeners = new ArrayList<>();
  }

  public void setPeerState(boolean enabled) {
    this.peerState = enabled ? PeerState.ENABLED : PeerState.DISABLED;
  }

  public void setPeerConfig(ReplicationPeerConfig peerConfig) {
    this.peerConfig = peerConfig;
    peerConfigListeners.forEach(listener -> listener.peerConfigUpdated(peerConfig));
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

  /**
   * Get the peer config object
   * @return the ReplicationPeerConfig for this peer
   */
  @Override
  public ReplicationPeerConfig getPeerConfig() {
    return peerConfig;
  }

  /**
   * Get the configuration object required to communicate with this peer
   * @return configuration object
   */
  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Get replicable (table, cf-list) map of this peer
   * @return the replicable (table, cf-list) map
   */
  @Override
  public Map<TableName, List<String>> getTableCFs() {
    return this.peerConfig.getTableCFsMap();
  }

  /**
   * Get replicable namespace set of this peer
   * @return the replicable namespaces set
   */
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
