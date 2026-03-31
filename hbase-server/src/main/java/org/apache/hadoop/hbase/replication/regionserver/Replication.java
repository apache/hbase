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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.regionserver.ReplicationSourceService;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gateway to Replication. Used by {@link org.apache.hadoop.hbase.regionserver.HRegionServer}.
 * <p>
 * Implement {@link PropagatingConfigurationObserver} mainly for registering
 * {@link ReplicationPeers}, so we can recreating the replication peer storage.
 */
@InterfaceAudience.Private
public class Replication implements ReplicationSourceService, PropagatingConfigurationObserver {
  private static final Logger LOG = LoggerFactory.getLogger(Replication.class);
  private boolean isReplicationForBulkLoadDataEnabled;
  private ReplicationSourceManager replicationManager;
  private ReplicationQueueStorage queueStorage;
  private ReplicationPeers replicationPeers;
  private volatile Configuration conf;
  private SyncReplicationPeerInfoProvider syncReplicationPeerInfoProvider;
  // Hosting server
  private Server server;
  private int statsPeriodInSecond;
  // ReplicationLoad to access replication metrics
  private ReplicationLoad replicationLoad;
  private MetricsReplicationGlobalSourceSource globalMetricsSource;

  private PeerProcedureHandler peerProcedureHandler;

  /**
   * Empty constructor
   */
  public Replication() {
  }

  @Override
  public void initialize(Server server, FileSystem fs, Path logDir, Path oldLogDir,
    WALFactory walFactory) throws IOException {
    this.server = server;
    this.conf = this.server.getConfiguration();
    this.isReplicationForBulkLoadDataEnabled =
      ReplicationUtils.isReplicationForBulkLoadDataEnabled(this.conf);
    if (this.isReplicationForBulkLoadDataEnabled) {
      if (
        conf.get(HConstants.REPLICATION_CLUSTER_ID) == null
          || conf.get(HConstants.REPLICATION_CLUSTER_ID).isEmpty()
      ) {
        throw new IllegalArgumentException(
          HConstants.REPLICATION_CLUSTER_ID + " cannot be null/empty when "
            + HConstants.REPLICATION_BULKLOAD_ENABLE_KEY + " is set to true.");
      }
    }

    try {
      this.queueStorage =
        ReplicationStorageFactory.getReplicationQueueStorage(server.getConnection(), conf);
      this.replicationPeers = ReplicationFactory.getReplicationPeers(server.getFileSystem(),
        server.getZooKeeper(), this.conf);
      this.replicationPeers.init();
    } catch (Exception e) {
      throw new IOException("Failed replication handler create", e);
    }
    UUID clusterId = null;
    try {
      clusterId = ZKClusterId.getUUIDForCluster(this.server.getZooKeeper());
    } catch (KeeperException ke) {
      throw new IOException("Could not read cluster id", ke);
    }
    SyncReplicationPeerMappingManager mapping = new SyncReplicationPeerMappingManager();
    this.globalMetricsSource = CompatibilitySingletonFactory
      .getInstance(MetricsReplicationSourceFactory.class).getGlobalSource();
    this.replicationManager = new ReplicationSourceManager(queueStorage, replicationPeers, conf,
      this.server, fs, logDir, oldLogDir, clusterId, walFactory, mapping, globalMetricsSource);
    this.statsPeriodInSecond = this.conf.getInt("replication.stats.thread.period.seconds", 5 * 60);
    this.replicationLoad = new ReplicationLoad();

    this.syncReplicationPeerInfoProvider =
      new SyncReplicationPeerInfoProviderImpl(replicationPeers, mapping);
    // Get the user-space WAL provider
    WALProvider walProvider = walFactory != null ? walFactory.getWALProvider() : null;
    if (walProvider != null) {
      walProvider
        .addWALActionsListener(new ReplicationSourceWALActionListener(conf, replicationManager));
      PeerActionListener peerActionListener = walProvider.getPeerActionListener();
      walProvider.setSyncReplicationPeerInfoProvider(syncReplicationPeerInfoProvider);
      // for sync replication state change, we need to reload the state twice, you can see the
      // code in PeerProcedureHandlerImpl, so here we need to go over the sync replication peers
      // to see if any of them are in the middle of the two refreshes, if so, we need to manually
      // repeat the action we have done in the first refresh, otherwise when the second refresh
      // comes we will be in trouble, such as NPE.
      replicationPeers.getAllPeerIds().stream().map(replicationPeers::getPeer)
        .filter(p -> p.getPeerConfig().isSyncReplication())
        .filter(p -> p.getNewSyncReplicationState() != SyncReplicationState.NONE)
        .forEach(p -> peerActionListener.peerSyncReplicationStateChange(p.getId(),
          p.getSyncReplicationState(), p.getNewSyncReplicationState(), 0));
      this.peerProcedureHandler =
        new PeerProcedureHandlerImpl(replicationManager, peerActionListener);
    } else {
      this.peerProcedureHandler =
        new PeerProcedureHandlerImpl(replicationManager, PeerActionListener.DUMMY);
    }
  }

  @Override
  public PeerProcedureHandler getPeerProcedureHandler() {
    return peerProcedureHandler;
  }

  /**
   * Stops replication service.
   */
  @Override
  public void stopReplicationService() {
    this.replicationManager.join();
  }

  /**
   * If replication is enabled and this cluster is a master, it starts
   */
  @Override
  public void startReplicationService() throws IOException {
    this.replicationManager.init();
    this.server.getChoreService().scheduleChore(new ReplicationStatisticsChore(
      "ReplicationSourceStatistics", server, (int) TimeUnit.SECONDS.toMillis(statsPeriodInSecond)));
    LOG.info("{} started", this.server.toString());
  }

  /**
   * Get the replication sources manager
   * @return the manager if replication is enabled, else returns false
   */
  public ReplicationSourceManager getReplicationManager() {
    return this.replicationManager;
  }

  void addHFileRefsToQueue(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
    throws IOException {
    try {
      this.replicationManager.addHFileRefs(tableName, family, pairs);
    } catch (IOException e) {
      LOG.error("Failed to add hfile references in the replication queue.", e);
      throw e;
    }
  }

  /**
   * Statistics task. Periodically prints the cache statistics to the log.
   */
  private final class ReplicationStatisticsChore extends ScheduledChore {

    ReplicationStatisticsChore(String name, Stoppable stopper, int period) {
      super(name, stopper, period);
    }

    @Override
    protected void chore() {
      printStats(replicationManager.getStats());
    }

    private void printStats(String stats) {
      if (!stats.isEmpty()) {
        LOG.info(stats);
      }
    }
  }

  @Override
  public ReplicationLoad refreshAndGetReplicationLoad() {
    if (this.replicationLoad == null) {
      return null;
    }
    // always build for latest data
    List<ReplicationSourceInterface> allSources = new ArrayList<>();
    allSources.addAll(this.replicationManager.getSources());
    allSources.addAll(this.replicationManager.getOldSources());
    this.replicationLoad.buildReplicationLoad(allSources, null);
    return this.replicationLoad;
  }

  @Override
  public SyncReplicationPeerInfoProvider getSyncReplicationPeerInfoProvider() {
    return syncReplicationPeerInfoProvider;
  }

  @Override
  public ReplicationPeers getReplicationPeers() {
    return replicationPeers;
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void registerChildren(ConfigurationManager manager) {
    manager.registerObserver(replicationPeers);
  }

  @Override
  public void deregisterChildren(ConfigurationManager manager) {
    manager.deregisterObserver(replicationPeers);
  }
}
