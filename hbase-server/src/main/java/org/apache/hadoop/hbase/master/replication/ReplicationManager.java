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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClientArguments;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Manages and performs all replication admin operations.
 * <p>
 * Used to add/remove a replication peer.
 */
@InterfaceAudience.Private
public class ReplicationManager {
  private final ReplicationQueuesClient replicationQueuesClient;
  private final ReplicationPeers replicationPeers;

  public ReplicationManager(Configuration conf, ZKWatcher zkw, Abortable abortable)
      throws IOException {
    try {
      this.replicationQueuesClient = ReplicationFactory
          .getReplicationQueuesClient(new ReplicationQueuesClientArguments(conf, abortable, zkw));
      this.replicationQueuesClient.init();
      this.replicationPeers = ReplicationFactory.getReplicationPeers(zkw, conf,
        this.replicationQueuesClient, abortable);
      this.replicationPeers.init();
    } catch (Exception e) {
      throw new IOException("Failed to construct ReplicationManager", e);
    }
  }

  public void addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws ReplicationException {
    checkPeerConfig(peerConfig);
    replicationPeers.registerPeer(peerId, peerConfig, enabled);
    replicationPeers.peerConnected(peerId);
  }

  public void removeReplicationPeer(String peerId) throws ReplicationException {
    replicationPeers.peerDisconnected(peerId);
    replicationPeers.unregisterPeer(peerId);
  }

  public void enableReplicationPeer(String peerId) throws ReplicationException {
    this.replicationPeers.enablePeer(peerId);
  }

  public void disableReplicationPeer(String peerId) throws ReplicationException {
    this.replicationPeers.disablePeer(peerId);
  }

  public ReplicationPeerConfig getPeerConfig(String peerId)
      throws ReplicationException, ReplicationPeerNotFoundException {
    ReplicationPeerConfig peerConfig = replicationPeers.getReplicationPeerConfig(peerId);
    if (peerConfig == null) {
      throw new ReplicationPeerNotFoundException(peerId);
    }
    return peerConfig;
  }

  public void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException, IOException {
    checkPeerConfig(peerConfig);
    this.replicationPeers.updatePeerConfig(peerId, peerConfig);
  }

  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern)
      throws ReplicationException {
    List<ReplicationPeerDescription> peers = new ArrayList<>();
    List<String> peerIds = replicationPeers.getAllPeerIds();
    for (String peerId : peerIds) {
      if (pattern == null || (pattern != null && pattern.matcher(peerId).matches())) {
        peers.add(new ReplicationPeerDescription(peerId,
            replicationPeers.getStatusOfPeerFromBackingStore(peerId),
            replicationPeers.getReplicationPeerConfig(peerId)));
      }
    }
    return peers;
  }

  /**
   * If replicate_all flag is true, it means all user tables will be replicated to peer cluster.
   * Then allow config exclude namespaces or exclude table-cfs which can't be replicated to
   * peer cluster.
   *
   * If replicate_all flag is false, it means all user tables can't be replicated to peer cluster.
   * Then allow to config namespaces or table-cfs which will be replicated to peer cluster.
   */
  private void checkPeerConfig(ReplicationPeerConfig peerConfig) {
    if (peerConfig.replicateAllUserTables()) {
      if ((peerConfig.getNamespaces() != null && !peerConfig.getNamespaces().isEmpty()) ||
        (peerConfig.getTableCFsMap() != null && !peerConfig.getTableCFsMap().isEmpty())) {
        throw new IllegalArgumentException("Need clean namespaces or table-cfs config firstly " +
          "when you want replicate all cluster");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getExcludeNamespaces(),
        peerConfig.getExcludeTableCFsMap());
    } else {
      if ((peerConfig.getExcludeNamespaces() != null
          && !peerConfig.getExcludeNamespaces().isEmpty())
          || (peerConfig.getExcludeTableCFsMap() != null
              && !peerConfig.getExcludeTableCFsMap().isEmpty())) {
        throw new IllegalArgumentException(
            "Need clean exclude-namespaces or exclude-table-cfs config firstly"
                + " when replicate_all flag is false");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getNamespaces(),
        peerConfig.getTableCFsMap());
    }
    checkConfiguredWALEntryFilters(peerConfig);
  }

  /**
   * Set a namespace in the peer config means that all tables in this namespace will be replicated
   * to the peer cluster.
   * <ol>
   * <li>If peer config already has a namespace, then not allow set any table of this namespace to
   * the peer config.</li>
   * <li>If peer config already has a table, then not allow set this table's namespace to the peer
   * config.</li>
   * </ol>
   * <p>
   * Set a exclude namespace in the peer config means that all tables in this namespace can't be
   * replicated to the peer cluster.
   * <ol>
   * <li>If peer config already has a exclude namespace, then not allow set any exclude table of
   * this namespace to the peer config.</li>
   * <li>If peer config already has a exclude table, then not allow set this table's namespace as a
   * exclude namespace.</li>
   * </ol>
   */
  private void checkNamespacesAndTableCfsConfigConflict(Set<String> namespaces,
      Map<TableName, ? extends Collection<String>> tableCfs) {
    if (namespaces == null || namespaces.isEmpty()) {
      return;
    }
    if (tableCfs == null || tableCfs.isEmpty()) {
      return;
    }
    for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {
      TableName table = entry.getKey();
      if (namespaces.contains(table.getNamespaceAsString())) {
        throw new IllegalArgumentException("Table-cfs " + table + " is conflict with namespaces "
            + table.getNamespaceAsString() + " in peer config");
      }
    }
  }

  private void checkConfiguredWALEntryFilters(ReplicationPeerConfig peerConfig) {
    String filterCSV = peerConfig.getConfiguration()
        .get(BaseReplicationEndpoint.REPLICATION_WALENTRYFILTER_CONFIG_KEY);
    if (filterCSV != null && !filterCSV.isEmpty()) {
      String[] filters = filterCSV.split(",");
      for (String filter : filters) {
        try {
          Class.forName(filter).newInstance();
        } catch (Exception e) {
          throw new IllegalArgumentException("Configured WALEntryFilter " + filter +
              " could not be created. Failing add/update " + "peer operation.", e);
        }
      }
    }
  }
}
