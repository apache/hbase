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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;

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
   * Get the peer config object
   * @return the ReplicationPeerConfig for this peer
   */
  public ReplicationPeerConfig getPeerConfig();

  /**
   * Returns the state of the peer
   * @return the enabled state
   */
  PeerState getPeerState();

  /**
   * Get the configuration object required to communicate with this peer
   * @return configuration object
   */
  public Configuration getConfiguration();

  /**
   * Get replicable (table, cf-list) map of this peer
   * @return the replicable (table, cf-list) map
   */
  public Map<TableName, List<String>> getTableCFs();

  /**
   * Setup a callback for chanages to the replication peer config
   * @param listener Listener for config changes, usually a replication endpoint
   */
  void trackPeerConfigChanges(ReplicationPeerConfigListener listener);
}
