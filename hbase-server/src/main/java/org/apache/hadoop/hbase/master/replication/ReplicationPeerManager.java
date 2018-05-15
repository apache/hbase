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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * Manages and performs all replication admin operations.
 * <p>
 * Used to add/remove a replication peer.
 */
@InterfaceAudience.Private
public class ReplicationPeerManager {

  private final ReplicationPeerStorage peerStorage;

  private final ReplicationQueueStorage queueStorage;

  private final ConcurrentMap<String, ReplicationPeerDescription> peers;

  private final ImmutableMap<SyncReplicationState, EnumSet<SyncReplicationState>>
    allowedTransition = Maps.immutableEnumMap(ImmutableMap.of(SyncReplicationState.ACTIVE,
      EnumSet.of(SyncReplicationState.DOWNGRADE_ACTIVE, SyncReplicationState.STANDBY),
      SyncReplicationState.STANDBY, EnumSet.of(SyncReplicationState.DOWNGRADE_ACTIVE),
      SyncReplicationState.DOWNGRADE_ACTIVE,
      EnumSet.of(SyncReplicationState.STANDBY, SyncReplicationState.ACTIVE)));

  ReplicationPeerManager(ReplicationPeerStorage peerStorage, ReplicationQueueStorage queueStorage,
      ConcurrentMap<String, ReplicationPeerDescription> peers) {
    this.peerStorage = peerStorage;
    this.queueStorage = queueStorage;
    this.peers = peers;
  }

  private void checkQueuesDeleted(String peerId)
      throws ReplicationException, DoNotRetryIOException {
    for (ServerName replicator : queueStorage.getListOfReplicators()) {
      List<String> queueIds = queueStorage.getAllQueues(replicator);
      for (String queueId : queueIds) {
        ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
        if (queueInfo.getPeerId().equals(peerId)) {
          throw new DoNotRetryIOException("undeleted queue for peerId: " + peerId +
            ", replicator: " + replicator + ", queueId: " + queueId);
        }
      }
    }
    if (queueStorage.getAllPeersFromHFileRefsQueue().contains(peerId)) {
      throw new DoNotRetryIOException("Undeleted queue for peer " + peerId + " in hfile-refs");
    }
  }

  void preAddPeer(String peerId, ReplicationPeerConfig peerConfig)
      throws DoNotRetryIOException, ReplicationException {
    if (peerId.contains("-")) {
      throw new DoNotRetryIOException("Found invalid peer name: " + peerId);
    }
    checkPeerConfig(peerConfig);
    if (peers.containsKey(peerId)) {
      throw new DoNotRetryIOException("Replication peer " + peerId + " already exists");
    }
    // make sure that there is no queues with the same peer id. This may happen when we create a
    // peer with the same id with a old deleted peer. If the replication queues for the old peer
    // have not been cleaned up yet then we should not create the new peer, otherwise the old wal
    // file may also be replicated.
    checkQueuesDeleted(peerId);
  }

  private ReplicationPeerDescription checkPeerExists(String peerId) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc == null) {
      throw new DoNotRetryIOException("Replication peer " + peerId + " does not exist");
    }
    return desc;
  }

  private void checkPeerInDAStateIfSyncReplication(String peerId) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc != null && desc.getPeerConfig().isSyncReplication()
        && !SyncReplicationState.DOWNGRADE_ACTIVE.equals(desc.getSyncReplicationState())) {
      throw new DoNotRetryIOException("Couldn't remove synchronous replication peer with state="
          + desc.getSyncReplicationState()
          + ", Transit the synchronous replication state to be DOWNGRADE_ACTIVE firstly.");
    }
  }

  ReplicationPeerConfig preRemovePeer(String peerId) throws DoNotRetryIOException {
    ReplicationPeerDescription pd = checkPeerExists(peerId);
    checkPeerInDAStateIfSyncReplication(peerId);
    return pd.getPeerConfig();
  }

  void preEnablePeer(String peerId) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = checkPeerExists(peerId);
    if (desc.isEnabled()) {
      throw new DoNotRetryIOException("Replication peer " + peerId + " has already been enabled");
    }
  }

  void preDisablePeer(String peerId) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = checkPeerExists(peerId);
    if (!desc.isEnabled()) {
      throw new DoNotRetryIOException("Replication peer " + peerId + " has already been disabled");
    }
  }

  /**
   * Return the old peer description. Can never be null.
   */
  ReplicationPeerDescription preUpdatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws DoNotRetryIOException {
    checkPeerConfig(peerConfig);
    ReplicationPeerDescription desc = checkPeerExists(peerId);
    ReplicationPeerConfig oldPeerConfig = desc.getPeerConfig();
    if (!isStringEquals(peerConfig.getClusterKey(), oldPeerConfig.getClusterKey())) {
      throw new DoNotRetryIOException(
        "Changing the cluster key on an existing peer is not allowed. Existing key '" +
          oldPeerConfig.getClusterKey() + "' for peer " + peerId + " does not match new key '" +
          peerConfig.getClusterKey() + "'");
    }

    if (!isStringEquals(peerConfig.getReplicationEndpointImpl(),
      oldPeerConfig.getReplicationEndpointImpl())) {
      throw new DoNotRetryIOException("Changing the replication endpoint implementation class " +
        "on an existing peer is not allowed. Existing class '" +
        oldPeerConfig.getReplicationEndpointImpl() + "' for peer " + peerId +
        " does not match new class '" + peerConfig.getReplicationEndpointImpl() + "'");
    }

    if (!isStringEquals(peerConfig.getRemoteWALDir(), oldPeerConfig.getRemoteWALDir())) {
      throw new DoNotRetryIOException(
        "Changing the remote wal dir on an existing peer is not allowed. Existing remote wal " +
          "dir '" + oldPeerConfig.getRemoteWALDir() + "' for peer " + peerId +
          " does not match new remote wal dir '" + peerConfig.getRemoteWALDir() + "'");
    }

    if (oldPeerConfig.isSyncReplication()) {
      if (!ReplicationUtils.isNamespacesAndTableCFsEqual(oldPeerConfig, peerConfig)) {
        throw new DoNotRetryIOException(
          "Changing the replicated namespace/table config on a synchronous replication " +
            "peer(peerId: " + peerId + ") is not allowed.");
      }
    }
    return desc;
  }

  /**
   * @return the old desciption of the peer
   */
  ReplicationPeerDescription preTransitPeerSyncReplicationState(String peerId,
      SyncReplicationState state) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = checkPeerExists(peerId);
    SyncReplicationState fromState = desc.getSyncReplicationState();
    EnumSet<SyncReplicationState> allowedToStates = allowedTransition.get(fromState);
    if (allowedToStates == null || !allowedToStates.contains(state)) {
      throw new DoNotRetryIOException("Can not transit current cluster state from " + fromState +
        " to " + state + " for peer id=" + peerId);
    }
    return desc;
  }

  public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws ReplicationException {
    if (peers.containsKey(peerId)) {
      // this should be a retry, just return
      return;
    }
    ReplicationPeerConfig copiedPeerConfig = ReplicationPeerConfig.newBuilder(peerConfig).build();
    SyncReplicationState syncReplicationState =
      copiedPeerConfig.isSyncReplication() ? SyncReplicationState.DOWNGRADE_ACTIVE
        : SyncReplicationState.NONE;
    peerStorage.addPeer(peerId, copiedPeerConfig, enabled, syncReplicationState);
    peers.put(peerId,
      new ReplicationPeerDescription(peerId, enabled, copiedPeerConfig, syncReplicationState));
  }

  public void removePeer(String peerId) throws ReplicationException {
    if (!peers.containsKey(peerId)) {
      // this should be a retry, just return
      return;
    }
    peerStorage.removePeer(peerId);
    peers.remove(peerId);
  }

  private void setPeerState(String peerId, boolean enabled) throws ReplicationException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc.isEnabled() == enabled) {
      // this should be a retry, just return
      return;
    }
    peerStorage.setPeerState(peerId, enabled);
    peers.put(peerId, new ReplicationPeerDescription(peerId, enabled, desc.getPeerConfig(),
      desc.getSyncReplicationState()));
  }

  public void enablePeer(String peerId) throws ReplicationException {
    setPeerState(peerId, true);
  }

  public void disablePeer(String peerId) throws ReplicationException {
    setPeerState(peerId, false);
  }

  public void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException {
    // the checking rules are too complicated here so we give up checking whether this is a retry.
    ReplicationPeerDescription desc = peers.get(peerId);
    ReplicationPeerConfig oldPeerConfig = desc.getPeerConfig();
    ReplicationPeerConfigBuilder newPeerConfigBuilder =
      ReplicationPeerConfig.newBuilder(peerConfig);
    // we need to use the new conf to overwrite the old one.
    newPeerConfigBuilder.putAllConfiguration(oldPeerConfig.getConfiguration());
    newPeerConfigBuilder.putAllConfiguration(peerConfig.getConfiguration());
    newPeerConfigBuilder.putAllConfiguration(oldPeerConfig.getConfiguration());
    newPeerConfigBuilder.putAllConfiguration(peerConfig.getConfiguration());
    ReplicationPeerConfig newPeerConfig = newPeerConfigBuilder.build();
    peerStorage.updatePeerConfig(peerId, newPeerConfig);
    peers.put(peerId, new ReplicationPeerDescription(peerId, desc.isEnabled(), newPeerConfig,
      desc.getSyncReplicationState()));
  }

  public List<ReplicationPeerDescription> listPeers(Pattern pattern) {
    if (pattern == null) {
      return new ArrayList<>(peers.values());
    }
    return peers.values().stream().filter(r -> pattern.matcher(r.getPeerId()).matches())
      .collect(Collectors.toList());
  }

  public Optional<ReplicationPeerConfig> getPeerConfig(String peerId) {
    ReplicationPeerDescription desc = peers.get(peerId);
    return desc != null ? Optional.of(desc.getPeerConfig()) : Optional.empty();
  }

  void removeAllLastPushedSeqIds(String peerId) throws ReplicationException {
    queueStorage.removeLastSequenceIds(peerId);
  }

  public void setPeerNewSyncReplicationState(String peerId, SyncReplicationState state)
      throws ReplicationException {
    peerStorage.setPeerNewSyncReplicationState(peerId, state);
  }

  public void transitPeerSyncReplicationState(String peerId, SyncReplicationState newState)
      throws ReplicationException {
    if (peerStorage.getPeerNewSyncReplicationState(peerId) != SyncReplicationState.NONE) {
      // Only transit if this is not a retry
      peerStorage.transitPeerSyncReplicationState(peerId);
    }
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc.getSyncReplicationState() != newState) {
      // Only recreate the desc if this is not a retry
      peers.put(peerId,
        new ReplicationPeerDescription(peerId, desc.isEnabled(), desc.getPeerConfig(), newState));
    }
  }

  public void removeAllQueues(String peerId) throws ReplicationException {
    // Here we need two passes to address the problem of claimQueue. Maybe a claimQueue is still
    // on-going when the refresh peer config procedure is done, if a RS which has already been
    // scanned claims the queue of a RS which has not been scanned yet, we will miss that queue in
    // the scan here, and if the RS who has claimed the queue crashed before creating recovered
    // source, then the queue will leave there until the another RS detects the crash and helps
    // removing the queue.
    // A two pass scan can solve the problem. Anyway, the queue will not disappear during the
    // claiming, it will either under the old RS or under the new RS, and a queue can only be
    // claimed once after the refresh peer procedure done(as the next claim queue will just delete
    // it), so we can make sure that a two pass scan will finally find the queue and remove it,
    // unless it has already been removed by others.
    ReplicationUtils.removeAllQueues(queueStorage, peerId);
    ReplicationUtils.removeAllQueues(queueStorage, peerId);
  }

  public void removeAllQueuesAndHFileRefs(String peerId) throws ReplicationException {
    removeAllQueues(peerId);
    queueStorage.removePeerFromHFileRefs(peerId);
  }

  private void checkPeerConfig(ReplicationPeerConfig peerConfig) throws DoNotRetryIOException {
    checkClusterKey(peerConfig.getClusterKey());

    if (peerConfig.replicateAllUserTables()) {
      // If replicate_all flag is true, it means all user tables will be replicated to peer cluster.
      // Then allow config exclude namespaces or exclude table-cfs which can't be replicated to peer
      // cluster.
      if ((peerConfig.getNamespaces() != null && !peerConfig.getNamespaces().isEmpty()) ||
        (peerConfig.getTableCFsMap() != null && !peerConfig.getTableCFsMap().isEmpty())) {
        throw new DoNotRetryIOException("Need clean namespaces or table-cfs config firstly " +
          "when you want replicate all cluster");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getExcludeNamespaces(),
        peerConfig.getExcludeTableCFsMap());
    } else {
      // If replicate_all flag is false, it means all user tables can't be replicated to peer
      // cluster. Then allow to config namespaces or table-cfs which will be replicated to peer
      // cluster.
      if ((peerConfig.getExcludeNamespaces() != null &&
        !peerConfig.getExcludeNamespaces().isEmpty()) ||
        (peerConfig.getExcludeTableCFsMap() != null &&
          !peerConfig.getExcludeTableCFsMap().isEmpty())) {
        throw new DoNotRetryIOException(
          "Need clean exclude-namespaces or exclude-table-cfs config firstly" +
            " when replicate_all flag is false");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getNamespaces(),
        peerConfig.getTableCFsMap());
    }

    if (peerConfig.isSyncReplication()) {
      checkPeerConfigForSyncReplication(peerConfig);
    }

    checkConfiguredWALEntryFilters(peerConfig);
  }

  private void checkPeerConfigForSyncReplication(ReplicationPeerConfig peerConfig)
      throws DoNotRetryIOException {
    // This is used to reduce the difficulty for implementing the sync replication state transition
    // as we need to reopen all the related regions.
    // TODO: Add namespace, replicat_all flag back
    if (peerConfig.replicateAllUserTables()) {
      throw new DoNotRetryIOException(
        "Only support replicated table config for sync replication peer");
    }
    if (peerConfig.getNamespaces() != null && !peerConfig.getNamespaces().isEmpty()) {
      throw new DoNotRetryIOException(
        "Only support replicated table config for sync replication peer");
    }
    if (peerConfig.getTableCFsMap() == null || peerConfig.getTableCFsMap().isEmpty()) {
      throw new DoNotRetryIOException("Need config replicated tables for sync replication peer");
    }
    for (List<String> cfs : peerConfig.getTableCFsMap().values()) {
      if (cfs != null && !cfs.isEmpty()) {
        throw new DoNotRetryIOException(
          "Only support replicated table config for sync replication peer");
      }
    }
    Path remoteWALDir = new Path(peerConfig.getRemoteWALDir());
    if (!remoteWALDir.isAbsolute()) {
      throw new DoNotRetryIOException(
        "The remote WAL directory " + peerConfig.getRemoteWALDir() + " is not absolute");
    }
    URI remoteWALDirUri = remoteWALDir.toUri();
    if (remoteWALDirUri.getScheme() == null || remoteWALDirUri.getAuthority() == null) {
      throw new DoNotRetryIOException("The remote WAL directory " + peerConfig.getRemoteWALDir() +
        " is not qualified, you must provide scheme and authority");
    }
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
      Map<TableName, ? extends Collection<String>> tableCfs) throws DoNotRetryIOException {
    if (namespaces == null || namespaces.isEmpty()) {
      return;
    }
    if (tableCfs == null || tableCfs.isEmpty()) {
      return;
    }
    for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {
      TableName table = entry.getKey();
      if (namespaces.contains(table.getNamespaceAsString())) {
        throw new DoNotRetryIOException("Table-cfs " + table + " is conflict with namespaces " +
          table.getNamespaceAsString() + " in peer config");
      }
    }
  }

  private void checkConfiguredWALEntryFilters(ReplicationPeerConfig peerConfig)
      throws DoNotRetryIOException {
    String filterCSV = peerConfig.getConfiguration()
      .get(BaseReplicationEndpoint.REPLICATION_WALENTRYFILTER_CONFIG_KEY);
    if (filterCSV != null && !filterCSV.isEmpty()) {
      String[] filters = filterCSV.split(",");
      for (String filter : filters) {
        try {
          Class.forName(filter).getDeclaredConstructor().newInstance();
        } catch (Exception e) {
          throw new DoNotRetryIOException("Configured WALEntryFilter " + filter +
            " could not be created. Failing add/update peer operation.", e);
        }
      }
    }
  }

  private void checkClusterKey(String clusterKey) throws DoNotRetryIOException {
    try {
      ZKConfig.validateClusterKey(clusterKey);
    } catch (IOException e) {
      throw new DoNotRetryIOException("Invalid cluster key: " + clusterKey, e);
    }
  }

  public List<String> getSerialPeerIdsBelongsTo(TableName tableName) {
    return peers.values().stream().filter(p -> p.getPeerConfig().isSerial())
      .filter(p -> ReplicationUtils.contains(p.getPeerConfig(), tableName)).map(p -> p.getPeerId())
      .collect(Collectors.toList());
  }

  public ReplicationQueueStorage getQueueStorage() {
    return queueStorage;
  }

  public static ReplicationPeerManager create(ZKWatcher zk, Configuration conf)
      throws ReplicationException {
    ReplicationPeerStorage peerStorage =
      ReplicationStorageFactory.getReplicationPeerStorage(zk, conf);
    ConcurrentMap<String, ReplicationPeerDescription> peers = new ConcurrentHashMap<>();
    for (String peerId : peerStorage.listPeerIds()) {
      ReplicationPeerConfig peerConfig = peerStorage.getPeerConfig(peerId);
      boolean enabled = peerStorage.isPeerEnabled(peerId);
      SyncReplicationState state = peerStorage.getPeerSyncReplicationState(peerId);
      peers.put(peerId, new ReplicationPeerDescription(peerId, enabled, peerConfig, state));
    }
    return new ReplicationPeerManager(peerStorage,
      ReplicationStorageFactory.getReplicationQueueStorage(zk, conf), peers);
  }

  /**
   * For replication peer cluster key or endpoint class, null and empty string is same. So here
   * don't use {@link StringUtils#equals(CharSequence, CharSequence)} directly.
   */
  private boolean isStringEquals(String s1, String s2) {
    if (StringUtils.isBlank(s1)) {
      return StringUtils.isBlank(s2);
    }
    return s1.equals(s2);
  }
}
