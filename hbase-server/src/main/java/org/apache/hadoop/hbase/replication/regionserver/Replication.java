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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.ReplicationSinkService;
import org.apache.hadoop.hbase.regionserver.ReplicationSourceService;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.SyncReplicationWALProvider;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.WALEntry;

/**
 * Gateway to Replication. Used by {@link org.apache.hadoop.hbase.regionserver.HRegionServer}.
 */
@InterfaceAudience.Private
public class Replication implements ReplicationSourceService, ReplicationSinkService {
  private static final Logger LOG =
      LoggerFactory.getLogger(Replication.class);
  private boolean isReplicationForBulkLoadDataEnabled;
  private ReplicationSourceManager replicationManager;
  private ReplicationQueueStorage queueStorage;
  private ReplicationPeers replicationPeers;
  private ReplicationTracker replicationTracker;
  private Configuration conf;
  private ReplicationSink replicationSink;
  private SyncReplicationPeerInfoProvider syncReplicationPeerInfoProvider;
  // Hosting server
  private Server server;
  /** Statistics thread schedule pool */
  private ScheduledExecutorService scheduleThreadPool;
  private int statsThreadPeriod;
  // ReplicationLoad to access replication metrics
  private ReplicationLoad replicationLoad;

  private PeerProcedureHandler peerProcedureHandler;

  /**
   * Empty constructor
   */
  public Replication() {
  }

  @Override
  public void initialize(Server server, FileSystem fs, Path logDir, Path oldLogDir,
      WALProvider walProvider) throws IOException {
    this.server = server;
    this.conf = this.server.getConfiguration();
    this.isReplicationForBulkLoadDataEnabled =
      ReplicationUtils.isReplicationForBulkLoadDataEnabled(this.conf);
    this.scheduleThreadPool = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder()
        .setNameFormat(server.getServerName().toShortString() + "Replication Statistics #%d")
        .setDaemon(true)
        .build());
    if (this.isReplicationForBulkLoadDataEnabled) {
      if (conf.get(HConstants.REPLICATION_CLUSTER_ID) == null
          || conf.get(HConstants.REPLICATION_CLUSTER_ID).isEmpty()) {
        throw new IllegalArgumentException(HConstants.REPLICATION_CLUSTER_ID
            + " cannot be null/empty when " + HConstants.REPLICATION_BULKLOAD_ENABLE_KEY
            + " is set to true.");
      }
    }

    try {
      this.queueStorage =
          ReplicationStorageFactory.getReplicationQueueStorage(server.getZooKeeper(), conf);
      this.replicationPeers =
          ReplicationFactory.getReplicationPeers(server.getZooKeeper(), this.conf);
      this.replicationPeers.init();
      this.replicationTracker =
          ReplicationFactory.getReplicationTracker(server.getZooKeeper(), this.server, this.server);
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
    this.replicationManager = new ReplicationSourceManager(queueStorage, replicationPeers,
        replicationTracker, conf, this.server, fs, logDir, oldLogDir, clusterId,
        walProvider != null ? walProvider.getWALFileLengthProvider() : p -> OptionalLong.empty(),
        mapping);
    this.syncReplicationPeerInfoProvider =
        new SyncReplicationPeerInfoProviderImpl(replicationPeers, mapping);
    PeerActionListener peerActionListener = PeerActionListener.DUMMY;
    if (walProvider != null) {
      walProvider
        .addWALActionsListener(new ReplicationSourceWALActionListener(conf, replicationManager));
      if (walProvider instanceof SyncReplicationWALProvider) {
        SyncReplicationWALProvider syncWALProvider = (SyncReplicationWALProvider) walProvider;
        peerActionListener = syncWALProvider;
        syncWALProvider.setPeerInfoProvider(syncReplicationPeerInfoProvider);
      }
    }
    this.statsThreadPeriod =
        this.conf.getInt("replication.stats.thread.period.seconds", 5 * 60);
    LOG.debug("Replication stats-in-log period={} seconds",  this.statsThreadPeriod);
    this.replicationLoad = new ReplicationLoad();

    this.peerProcedureHandler =
      new PeerProcedureHandlerImpl(replicationManager, peerActionListener);
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
    join();
  }

  /**
   * Join with the replication threads
   */
  public void join() {
    this.replicationManager.join();
    if (this.replicationSink != null) {
      this.replicationSink.stopReplicationSinkServices();
    }
    scheduleThreadPool.shutdown();
  }

  /**
   * Carry on the list of log entries down to the sink
   * @param entries list of entries to replicate
   * @param cells The data -- the cells -- that <code>entries</code> describes (the entries do not
   *          contain the Cells we are replicating; they are passed here on the side in this
   *          CellScanner).
   * @param replicationClusterId Id which will uniquely identify source cluster FS client
   *          configurations in the replication configuration directory
   * @param sourceBaseNamespaceDirPath Path that point to the source cluster base namespace
   *          directory required for replicating hfiles
   * @param sourceHFileArchiveDirPath Path that point to the source cluster hfile archive directory
   * @throws IOException
   */
  @Override
  public void replicateLogEntries(List<WALEntry> entries, CellScanner cells,
      String replicationClusterId, String sourceBaseNamespaceDirPath,
      String sourceHFileArchiveDirPath) throws IOException {
    this.replicationSink.replicateEntries(entries, cells, replicationClusterId,
      sourceBaseNamespaceDirPath, sourceHFileArchiveDirPath);
  }

  /**
   * If replication is enabled and this cluster is a master,
   * it starts
   * @throws IOException
   */
  @Override
  public void startReplicationService() throws IOException {
    this.replicationManager.init();
    this.replicationSink = new ReplicationSink(this.conf, this.server);
    this.scheduleThreadPool.scheduleAtFixedRate(
      new ReplicationStatisticsTask(this.replicationSink, this.replicationManager),
      statsThreadPeriod, statsThreadPeriod, TimeUnit.SECONDS);
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
  private final static class ReplicationStatisticsTask implements Runnable {

    private final ReplicationSink replicationSink;
    private final ReplicationSourceManager replicationManager;

    public ReplicationStatisticsTask(ReplicationSink replicationSink,
        ReplicationSourceManager replicationManager) {
      this.replicationManager = replicationManager;
      this.replicationSink = replicationSink;
    }

    @Override
    public void run() {
      printStats(this.replicationManager.getStats());
      printStats(this.replicationSink.getStats());
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
    buildReplicationLoad();
    return this.replicationLoad;
  }

  private void buildReplicationLoad() {
    List<MetricsSource> sourceMetricsList = new ArrayList<>();

    // get source
    List<ReplicationSourceInterface> sources = this.replicationManager.getSources();
    for (ReplicationSourceInterface source : sources) {
      sourceMetricsList.add(source.getSourceMetrics());
    }

    // get old source
    List<ReplicationSourceInterface> oldSources = this.replicationManager.getOldSources();
    for (ReplicationSourceInterface source : oldSources) {
      if (source instanceof ReplicationSource) {
        sourceMetricsList.add(source.getSourceMetrics());
      }
    }

    // get sink
    MetricsSink sinkMetrics = this.replicationSink.getSinkMetrics();
    this.replicationLoad.buildReplicationLoad(sourceMetricsList, sinkMetrics);
  }

  @Override
  public SyncReplicationPeerInfoProvider getSyncReplicationPeerInfoProvider() {
    return syncReplicationPeerInfoProvider;
  }

  @Override
  public ReplicationPeers getReplicationPeers() {
    return replicationPeers;
  }
}
