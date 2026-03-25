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
package org.apache.hadoop.hbase.master.replication;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterMetrics;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionRegistryFactory;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationPeerStorage;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration.MigrationIterator;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration.ZkLastPushedSeqId;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorageForMigration.ZkReplicationQueueData;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;

/**
 * Manages and performs all replication admin operations.
 * <p>
 * Used to add/remove a replication peer.
 * <p>
 * Implement {@link ConfigurationObserver} mainly for recreating {@link ReplicationPeerStorage}, for
 * supporting migrating across different replication peer storages without restarting master.
 */
@InterfaceAudience.Private
public class ReplicationPeerManager implements ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationPeerManager.class);

  private volatile ReplicationPeerStorage peerStorage;

  private final ReplicationQueueStorage queueStorage;

  private final ConcurrentMap<String, ReplicationPeerDescription> peers;

  private final ImmutableMap<SyncReplicationState,
    EnumSet<SyncReplicationState>> allowedTransition =
      Maps.immutableEnumMap(ImmutableMap.of(SyncReplicationState.ACTIVE,
        EnumSet.of(SyncReplicationState.DOWNGRADE_ACTIVE, SyncReplicationState.STANDBY),
        SyncReplicationState.STANDBY, EnumSet.of(SyncReplicationState.DOWNGRADE_ACTIVE),
        SyncReplicationState.DOWNGRADE_ACTIVE,
        EnumSet.of(SyncReplicationState.STANDBY, SyncReplicationState.ACTIVE)));

  private final String clusterId;

  private volatile Configuration conf;

  // for dynamic recreating ReplicationPeerStorage.
  private final FileSystem fs;

  private final ZKWatcher zk;

  @FunctionalInterface
  interface ReplicationQueueStorageInitializer {

    void initialize() throws IOException;
  }

  private final ReplicationQueueStorageInitializer queueStorageInitializer;

  // we will mock this class in UT so leave the constructor as package private and not mark the
  // class as final, since mockito can not mock a final class
  ReplicationPeerManager(FileSystem fs, ZKWatcher zk, ReplicationPeerStorage peerStorage,
    ReplicationQueueStorage queueStorage, ConcurrentMap<String, ReplicationPeerDescription> peers,
    Configuration conf, String clusterId,
    ReplicationQueueStorageInitializer queueStorageInitializer) {
    this.fs = fs;
    this.zk = zk;
    this.peerStorage = peerStorage;
    this.queueStorage = queueStorage;
    this.peers = peers;
    this.conf = conf;
    this.clusterId = clusterId;
    this.queueStorageInitializer = queueStorageInitializer;
  }

  private void checkQueuesDeleted(String peerId)
    throws ReplicationException, DoNotRetryIOException {
    List<ReplicationQueueId> queueIds = queueStorage.listAllQueueIds(peerId);
    if (!queueIds.isEmpty()) {
      throw new DoNotRetryIOException("There are still " + queueIds.size()
        + " undeleted queue(s) for peerId: " + peerId + ", first is " + queueIds.get(0));
    }
    if (queueStorage.getAllPeersFromHFileRefsQueue().contains(peerId)) {
      throw new DoNotRetryIOException("Undeleted queue for peer " + peerId + " in hfile-refs");
    }
  }

  private void initializeQueueStorage() throws IOException {
    queueStorageInitializer.initialize();
  }

  void preAddPeer(String peerId, ReplicationPeerConfig peerConfig)
    throws ReplicationException, IOException {
    if (peerId.contains("-")) {
      throw new DoNotRetryIOException("Found invalid peer name: " + peerId);
    }
    checkPeerConfig(peerConfig);
    if (peerConfig.isSyncReplication()) {
      checkSyncReplicationPeerConfigConflict(peerConfig);
    }
    if (peers.containsKey(peerId)) {
      throw new DoNotRetryIOException("Replication peer " + peerId + " already exists");
    }

    // lazy create table
    initializeQueueStorage();
    // make sure that there is no queues with the same peer id. This may happen when we create a
    // peer with the same id with a old deleted peer. If the replication queues for the old peer
    // have not been cleaned up yet then we should not create the new peer, otherwise the old wal
    // file may also be replicated.
    checkQueuesDeleted(peerId);
  }

  private ReplicationPeerDescription checkPeerExists(String peerId) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc == null) {
      throw new ReplicationPeerNotFoundException(peerId);
    }
    return desc;
  }

  private void checkPeerInDAStateIfSyncReplication(String peerId) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (
      desc != null && desc.getPeerConfig().isSyncReplication()
        && !SyncReplicationState.DOWNGRADE_ACTIVE.equals(desc.getSyncReplicationState())
    ) {
      throw new DoNotRetryIOException(
        "Couldn't remove synchronous replication peer with state=" + desc.getSyncReplicationState()
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
        "Changing the cluster key on an existing peer is not allowed. Existing key '"
          + oldPeerConfig.getClusterKey() + "' for peer " + peerId + " does not match new key '"
          + peerConfig.getClusterKey() + "'");
    }

    if (
      !isStringEquals(peerConfig.getReplicationEndpointImpl(),
        oldPeerConfig.getReplicationEndpointImpl())
    ) {
      throw new DoNotRetryIOException("Changing the replication endpoint implementation class "
        + "on an existing peer is not allowed. Existing class '"
        + oldPeerConfig.getReplicationEndpointImpl() + "' for peer " + peerId
        + " does not match new class '" + peerConfig.getReplicationEndpointImpl() + "'");
    }

    if (!isStringEquals(peerConfig.getRemoteWALDir(), oldPeerConfig.getRemoteWALDir())) {
      throw new DoNotRetryIOException(
        "Changing the remote wal dir on an existing peer is not allowed. Existing remote wal "
          + "dir '" + oldPeerConfig.getRemoteWALDir() + "' for peer " + peerId
          + " does not match new remote wal dir '" + peerConfig.getRemoteWALDir() + "'");
    }

    if (oldPeerConfig.isSyncReplication()) {
      if (!ReplicationUtils.isNamespacesAndTableCFsEqual(oldPeerConfig, peerConfig)) {
        throw new DoNotRetryIOException(
          "Changing the replicated namespace/table config on a synchronous replication "
            + "peer(peerId: " + peerId + ") is not allowed.");
      }
    }
    return desc;
  }

  /** Returns the old desciption of the peer */
  ReplicationPeerDescription preTransitPeerSyncReplicationState(String peerId,
    SyncReplicationState state) throws DoNotRetryIOException {
    ReplicationPeerDescription desc = checkPeerExists(peerId);
    SyncReplicationState fromState = desc.getSyncReplicationState();
    EnumSet<SyncReplicationState> allowedToStates = allowedTransition.get(fromState);
    if (allowedToStates == null || !allowedToStates.contains(state)) {
      throw new DoNotRetryIOException("Can not transit current cluster state from " + fromState
        + " to " + state + " for peer id=" + peerId);
    }
    return desc;
  }

  public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
    throws ReplicationException {
    if (peers.containsKey(peerId)) {
      // this should be a retry, just return
      return;
    }
    peerConfig = ReplicationPeerConfigUtil.updateReplicationBasePeerConfigs(conf, peerConfig);
    ReplicationPeerConfig copiedPeerConfig = ReplicationPeerConfig.newBuilder(peerConfig).build();
    SyncReplicationState syncReplicationState = copiedPeerConfig.isSyncReplication()
      ? SyncReplicationState.DOWNGRADE_ACTIVE
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

  public boolean getPeerState(String peerId) throws ReplicationException {
    ReplicationPeerDescription desc = peers.get(peerId);
    if (desc != null) {
      return desc.isEnabled();
    } else {
      throw new ReplicationException("Replication Peer of " + peerId + " does not exist.");
    }
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
    queueStorage.removeAllQueues(peerId);
    queueStorage.removeAllQueues(peerId);
  }

  public void removeAllQueuesAndHFileRefs(String peerId) throws ReplicationException {
    removeAllQueues(peerId);
    queueStorage.removePeerFromHFileRefs(peerId);
  }

  private void checkClusterKey(String clusterKey, ReplicationEndpoint endpoint)
    throws DoNotRetryIOException {
    if (endpoint != null && !(endpoint instanceof HBaseReplicationEndpoint)) {
      return;
    }
    // Endpoints implementing HBaseReplicationEndpoint need to check cluster key
    URI connectionUri = ConnectionRegistryFactory.tryParseAsConnectionURI(clusterKey);
    try {
      if (connectionUri != null) {
        ConnectionRegistryFactory.validate(connectionUri);
      } else {
        ZKConfig.validateClusterKey(clusterKey);
      }
    } catch (IOException e) {
      throw new DoNotRetryIOException("Invalid cluster key: " + clusterKey, e);
    }
    if (endpoint != null && endpoint.canReplicateToSameCluster()) {
      return;
    }
    // make sure we do not replicate to same cluster
    String peerClusterId;
    try {
      if (connectionUri != null) {
        // fetch cluster id through standard admin API
        try (Connection conn = ConnectionFactory.createConnection(connectionUri, conf);
          Admin admin = conn.getAdmin()) {
          peerClusterId =
            admin.getClusterMetrics(EnumSet.of(ClusterMetrics.Option.CLUSTER_ID)).getClusterId();
        }
      } else {
        // Create the peer cluster config for get peer cluster id
        Configuration peerConf = HBaseConfiguration.createClusterConf(conf, clusterKey);
        try (ZKWatcher zkWatcher = new ZKWatcher(peerConf, this + "check-peer-cluster-id", null)) {
          peerClusterId = ZKClusterId.readClusterIdZNode(zkWatcher);
        }
      }
    } catch (IOException | KeeperException e) {
      // we just want to check whether we will replicate to the same cluster, so if we get an error
      // while getting the cluster id of the peer cluster, it means we are not connecting to
      // ourselves, as we are still alive. So here we just log the error and continue
      LOG.warn("Can't get peerClusterId for clusterKey=" + clusterKey, e);
      return;
    }
    // In rare case, zookeeper setting may be messed up. That leads to the incorrect
    // peerClusterId value, which is the same as the source clusterId
    if (clusterId.equals(peerClusterId)) {
      throw new DoNotRetryIOException("Invalid cluster key: " + clusterKey
        + ", should not replicate to itself for HBaseInterClusterReplicationEndpoint");
    }
  }

  private void checkPeerConfig(ReplicationPeerConfig peerConfig) throws DoNotRetryIOException {
    String replicationEndpointImpl = peerConfig.getReplicationEndpointImpl();
    ReplicationEndpoint endpoint = null;
    if (!StringUtils.isBlank(replicationEndpointImpl)) {
      try {
        // try creating a instance
        endpoint = Class.forName(replicationEndpointImpl).asSubclass(ReplicationEndpoint.class)
          .getDeclaredConstructor().newInstance();
      } catch (Throwable e) {
        throw new DoNotRetryIOException(
          "Can not instantiate configured replication endpoint class=" + replicationEndpointImpl,
          e);
      }
    }
    checkClusterKey(peerConfig.getClusterKey(), endpoint);

    if (peerConfig.replicateAllUserTables()) {
      // If replicate_all flag is true, it means all user tables will be replicated to peer cluster.
      // Then allow config exclude namespaces or exclude table-cfs which can't be replicated to peer
      // cluster.
      if (
        (peerConfig.getNamespaces() != null && !peerConfig.getNamespaces().isEmpty())
          || (peerConfig.getTableCFsMap() != null && !peerConfig.getTableCFsMap().isEmpty())
      ) {
        throw new DoNotRetryIOException("Need clean namespaces or table-cfs config firstly "
          + "when you want replicate all cluster");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getExcludeNamespaces(),
        peerConfig.getExcludeTableCFsMap());
    } else {
      // If replicate_all flag is false, it means all user tables can't be replicated to peer
      // cluster. Then allow to config namespaces or table-cfs which will be replicated to peer
      // cluster.
      if (
        (peerConfig.getExcludeNamespaces() != null && !peerConfig.getExcludeNamespaces().isEmpty())
          || (peerConfig.getExcludeTableCFsMap() != null
            && !peerConfig.getExcludeTableCFsMap().isEmpty())
      ) {
        throw new DoNotRetryIOException(
          "Need clean exclude-namespaces or exclude-table-cfs config firstly"
            + " when replicate_all flag is false");
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
      throw new DoNotRetryIOException("The remote WAL directory " + peerConfig.getRemoteWALDir()
        + " is not qualified, you must provide scheme and authority");
    }
  }

  private void checkSyncReplicationPeerConfigConflict(ReplicationPeerConfig peerConfig)
    throws DoNotRetryIOException {
    for (TableName tableName : peerConfig.getTableCFsMap().keySet()) {
      for (Map.Entry<String, ReplicationPeerDescription> entry : peers.entrySet()) {
        ReplicationPeerConfig rpc = entry.getValue().getPeerConfig();
        if (rpc.isSyncReplication() && rpc.getTableCFsMap().containsKey(tableName)) {
          throw new DoNotRetryIOException(
            "Table " + tableName + " has been replicated by peer " + entry.getKey());
        }
      }
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
        throw new DoNotRetryIOException("Table-cfs " + table + " is conflict with namespaces "
          + table.getNamespaceAsString() + " in peer config");
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
          throw new DoNotRetryIOException("Configured WALEntryFilter " + filter
            + " could not be created. Failing add/update peer operation.", e);
        }
      }
    }
  }

  public List<String> getSerialPeerIdsBelongsTo(TableName tableName) {
    return peers.values().stream().filter(p -> p.getPeerConfig().isSerial())
      .filter(p -> p.getPeerConfig().needToReplicate(tableName)).map(p -> p.getPeerId())
      .collect(Collectors.toList());
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public ReplicationPeerStorage getPeerStorage() {
    return peerStorage;
  }

  public ReplicationQueueStorage getQueueStorage() {
    return queueStorage;
  }

  private static Pair<ReplicationQueueStorage, ReplicationQueueStorageInitializer>
    createReplicationQueueStorage(MasterServices services) throws IOException {
    Configuration conf = services.getConfiguration();
    TableName replicationQueueTableName =
      TableName.valueOf(conf.get(ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME,
        ReplicationStorageFactory.REPLICATION_QUEUE_TABLE_NAME_DEFAULT.getNameAsString()));
    ReplicationQueueStorageInitializer initializer;
    if (services.getTableDescriptors().exists(replicationQueueTableName)) {
      // no need to create the table
      initializer = () -> {
      };
    } else {
      // lazy create the replication table.
      initializer = new ReplicationQueueStorageInitializer() {

        private volatile boolean created = false;

        @Override
        public void initialize() throws IOException {
          if (created) {
            return;
          }
          synchronized (this) {
            if (created) {
              return;
            }
            if (services.getTableDescriptors().exists(replicationQueueTableName)) {
              created = true;
              return;
            }
            long procId = services.createSystemTable(ReplicationStorageFactory
              .createReplicationQueueTableDescriptor(replicationQueueTableName));
            ProcedureExecutor<MasterProcedureEnv> procExec = services.getMasterProcedureExecutor();
            ProcedureSyncWait.waitFor(procExec.getEnvironment(), TimeUnit.MINUTES.toMillis(1),
              "Creating table " + replicationQueueTableName, () -> procExec.isFinished(procId));
          }
        }
      };
    }
    return Pair.newPair(ReplicationStorageFactory.getReplicationQueueStorage(
      services.getConnection(), conf, replicationQueueTableName), initializer);
  }

  public static ReplicationPeerManager create(MasterServices services, String clusterId)
    throws ReplicationException, IOException {
    Configuration conf = services.getConfiguration();
    FileSystem fs = services.getMasterFileSystem().getFileSystem();
    ZKWatcher zk = services.getZooKeeper();
    ReplicationPeerStorage peerStorage =
      ReplicationStorageFactory.getReplicationPeerStorage(fs, zk, conf);
    Pair<ReplicationQueueStorage, ReplicationQueueStorageInitializer> pair =
      createReplicationQueueStorage(services);
    ReplicationQueueStorage queueStorage = pair.getFirst();
    ConcurrentMap<String, ReplicationPeerDescription> peers = new ConcurrentHashMap<>();
    for (String peerId : peerStorage.listPeerIds()) {
      ReplicationPeerConfig peerConfig = peerStorage.getPeerConfig(peerId);
      if (
        ReplicationUtils.LEGACY_REGION_REPLICATION_ENDPOINT_NAME
          .equals(peerConfig.getReplicationEndpointImpl())
      ) {
        // If memstore region replication is enabled, there will be a special replication peer
        // usually called 'region_replica_replication'. We do not need to load it or migrate its
        // replication queue data since we do not rely on general replication framework for
        // region replication in 3.x now, please see HBASE-26233 for more details.
        // We can not delete it now since region server with old version still want to update
        // the replicated wal position to zk, if we delete the replication queue zk node, rs
        // will crash. See HBASE-29169 for more details.
        // In MigrateReplicationQueueFromZkToTableProcedure, finally we will call a deleteAllData on
        // the old replication queue storage, to make sure that we will delete the the queue data
        // for this peer and also the peer info in replication peer storage
        LOG.info("Found old region replica replication peer '{}', skip loading it", peerId);
        continue;
      }
      peerConfig = ReplicationPeerConfigUtil.updateReplicationBasePeerConfigs(conf, peerConfig);
      peerStorage.updatePeerConfig(peerId, peerConfig);
      boolean enabled = peerStorage.isPeerEnabled(peerId);
      SyncReplicationState state = peerStorage.getPeerSyncReplicationState(peerId);
      peers.put(peerId, new ReplicationPeerDescription(peerId, enabled, peerConfig, state));
    }
    return new ReplicationPeerManager(fs, zk, peerStorage, queueStorage, peers, conf, clusterId,
      pair.getSecond());
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

  @Override
  public void onConfigurationChange(Configuration conf) {
    this.conf = conf;
    this.peerStorage = ReplicationStorageFactory.getReplicationPeerStorage(fs, zk, conf);
  }

  private ReplicationQueueData convert(ZkReplicationQueueData zkData) {
    Map<String, ReplicationGroupOffset> groupOffsets = new HashMap<>();
    zkData.getWalOffsets().forEach((wal, offset) -> {
      String walGroup = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
      groupOffsets.compute(walGroup, (k, oldOffset) -> {
        if (oldOffset == null) {
          return new ReplicationGroupOffset(wal, offset);
        }
        // we should record the first wal's offset
        long oldWalTs = AbstractFSWALProvider.getTimestamp(oldOffset.getWal());
        long walTs = AbstractFSWALProvider.getTimestamp(wal);
        if (walTs < oldWalTs) {
          return new ReplicationGroupOffset(wal, offset);
        }
        return oldOffset;
      });
    });
    return new ReplicationQueueData(zkData.getQueueId(), ImmutableMap.copyOf(groupOffsets));
  }

  private void migrateQueues(ZKReplicationQueueStorageForMigration oldQueueStorage)
    throws Exception {
    MigrationIterator<Pair<ServerName, List<ZkReplicationQueueData>>> iter =
      oldQueueStorage.listAllQueues();
    for (;;) {
      Pair<ServerName, List<ZkReplicationQueueData>> pair = iter.next();
      if (pair == null) {
        return;
      }
      queueStorage.batchUpdateQueues(pair.getFirst(),
        pair.getSecond().stream().filter(data -> peers.containsKey(data.getQueueId().getPeerId()))
          .map(this::convert).collect(Collectors.toList()));
    }
  }

  private void migrateLastPushedSeqIds(ZKReplicationQueueStorageForMigration oldQueueStorage)
    throws Exception {
    MigrationIterator<List<ZkLastPushedSeqId>> iter = oldQueueStorage.listAllLastPushedSeqIds();
    for (;;) {
      List<ZkLastPushedSeqId> list = iter.next();
      if (list == null) {
        return;
      }
      queueStorage.batchUpdateLastSequenceIds(list.stream()
        .filter(data -> peers.containsKey(data.getPeerId())).collect(Collectors.toList()));
    }
  }

  private void migrateHFileRefs(ZKReplicationQueueStorageForMigration oldQueueStorage)
    throws Exception {
    MigrationIterator<Pair<String, List<String>>> iter = oldQueueStorage.listAllHFileRefs();
    for (;;) {
      Pair<String, List<String>> pair = iter.next();
      if (pair == null) {
        return;
      }
      if (peers.containsKey(pair.getFirst())) {
        queueStorage.batchUpdateHFileRefs(pair.getFirst(), pair.getSecond());
      }
    }
  }

  private interface ExceptionalRunnable {
    void run() throws Exception;
  }

  private CompletableFuture<?> runAsync(ExceptionalRunnable task, ExecutorService executor) {
    CompletableFuture<?> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        task.run();
        future.complete(null);
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  // this is for upgrading from 2.x to 3.x, in 3.x we will not load the 'region_replica_replication'
  // peer, but we still need to know whether we have it on the old storage
  boolean hasRegionReplicaReplicationPeer() throws ReplicationException {
    return peerStorage.listPeerIds().stream()
      .anyMatch(p -> p.equals(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_PEER));
  }

  /**
   * Submit the migration tasks to the given {@code executor}.
   */
  CompletableFuture<Void> migrateQueuesFromZk(ZKWatcher zookeeper, ExecutorService executor) {
    // the replication queue table creation is asynchronous and will be triggered by addPeer, so
    // here we need to manually initialize it since we will not call addPeer.
    try {
      initializeQueueStorage();
    } catch (IOException e) {
      return FutureUtils.failedFuture(e);
    }
    ZKReplicationQueueStorageForMigration oldStorage =
      new ZKReplicationQueueStorageForMigration(zookeeper, conf);
    return CompletableFuture.allOf(runAsync(() -> migrateQueues(oldStorage), executor),
      runAsync(() -> migrateLastPushedSeqIds(oldStorage), executor),
      runAsync(() -> migrateHFileRefs(oldStorage), executor));
  }

  void deleteLegacyRegionReplicaReplicationPeer() throws ReplicationException {
    for (String peerId : peerStorage.listPeerIds()) {
      ReplicationPeerConfig peerConfig = peerStorage.getPeerConfig(peerId);
      if (
        ReplicationUtils.LEGACY_REGION_REPLICATION_ENDPOINT_NAME
          .equals(peerConfig.getReplicationEndpointImpl())
      ) {
        LOG.info("Delete old region replica replication peer '{}'", peerId);
        peerStorage.removePeer(peerId);
      }
    }
  }
}
