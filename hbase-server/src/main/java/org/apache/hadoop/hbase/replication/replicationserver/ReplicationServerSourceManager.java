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
package org.apache.hadoop.hbase.replication.replicationserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.HReplicationServer;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationSourceController;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationGlobalSourceSource;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.RecoveredReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceFactory;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.replication.regionserver.WALFileLengthProvider;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible to manage all the replication sources in ReplicationServer.
 */
@InterfaceAudience.Private
public class ReplicationServerSourceManager implements ReplicationSourceController {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationServerSourceManager.class);

  private final HReplicationServer replicationServer;

  // Total buffer size on this ReplicationServer for holding batched edits to be shipped.
  private final long totalBufferLimit;
  private AtomicLong totalBufferUsed = new AtomicLong();

  private final MetricsReplicationGlobalSourceSource globalMetrics;

  private final ConcurrentMap<ReplicationQueueInfo, ReplicationSourceInterface> sources =
    new ConcurrentHashMap<>();

  private final ZKReplicationQueueStorage zkQueueStorage;
  private final ReplicationPeers replicationPeers;

  private final Configuration conf;

  private final FileSystem fs;
  private final Path walRootDir;

  private final UUID clusterId;

  public ReplicationServerSourceManager(HReplicationServer replicationServer,
    ZKReplicationQueueStorage zkQueueStorage, ReplicationPeers replicationPeers, Configuration conf,
    FileSystem fs, Path walRootDir, UUID clusterId,
    MetricsReplicationGlobalSourceSource globalMetrics) {
    this.replicationServer = replicationServer;
    this.zkQueueStorage = zkQueueStorage;
    this.replicationPeers = replicationPeers;
    this.conf = conf;
    this.fs = fs;
    this.walRootDir = walRootDir;
    this.clusterId = clusterId;
    this.totalBufferLimit = this.conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
      HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    this.globalMetrics = globalMetrics;
  }

  /**
   * Returns the maximum size in bytes of edits held in memory which are pending replication
   * across all sources inside this RegionServer.
   */
  @Override
  public long getTotalBufferLimit() {
    return totalBufferLimit;
  }

  @Override
  public AtomicLong getTotalBufferUsed() {
    return totalBufferUsed;
  }

  @Override
  public MetricsReplicationGlobalSourceSource getGlobalMetrics() {
    return this.globalMetrics;
  }

  @Override
  public void finishRecoveredSource(RecoveredReplicationSource src) {
    this.sources.remove(src.getReplicationQueueInfo());
    deleteQueue(src.getReplicationQueueInfo());
    LOG.info("Finished recovering queue {} with the following stats: {}", src.getQueueId(),
      src.getStats());
  }

  /**
   * Get a list of all sources, including normal sources and recovered sources.
   * @return list of all sources
   */
  public List<ReplicationSourceInterface> getSources() {
    return new ArrayList<>(this.sources.values());
  }

  public void startReplicationSource(ServerName owner, String queueId) throws IOException,
    ReplicationException {
    ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(owner, queueId);
    String peerId = replicationQueueInfo.getPeerId();
    this.replicationPeers.addPeer(peerId);
    Path walDir = new Path(walRootDir, AbstractFSWALProvider.getWALDirectoryName(owner.toString()));

    ReplicationSourceInterface src = ReplicationSourceFactory.create(conf, queueId);

    ReplicationSourceInterface existSource = sources.putIfAbsent(replicationQueueInfo, src);
    if (existSource != null) {
      LOG.warn("Duplicate source exists for replication queue: owner={}, queueId={}", owner,
        queueId);
      return;
    }

    // init replication source
    src.init(conf, fs, walDir, this, zkQueueStorage, replicationPeers.getPeer(peerId),
      replicationServer, replicationQueueInfo, clusterId,
      createWALFileLengthProvider(replicationQueueInfo), new MetricsSource(replicationQueueInfo));

    List<String> waLsInQueue = zkQueueStorage.getWALsInQueue(owner, queueId);
    waLsInQueue.forEach(walName -> src.enqueueLog(new Path(walDir, walName)));

    if (!replicationQueueInfo.isQueueRecovered()) {
      ReplicationQueueListener queueListener =
        new ReplicationQueueListener(src, zkQueueStorage, walRootDir, replicationServer);
      queueListener.start(waLsInQueue);
    }

    src.startup();
    LOG.info("Start replication source for queue {} from region server {}", queueId, owner);
  }

  /**
   * Delete a complete queue of wals associated with a replication source
   * @param queueInfo the replication queue to delete
   */
  private void deleteQueue(ReplicationQueueInfo queueInfo) {
    abortWhenFail(() ->
      this.zkQueueStorage.removeQueue(queueInfo.getOwner(), queueInfo.getQueueId()));
  }

  private void abortWhenFail(ReplicationQueueOperation op) {
    try {
      op.exec();
    } catch (ReplicationException e) {
      replicationServer.abort("Failed to operate on replication queue", e);
    }
  }

  @FunctionalInterface
  private interface ReplicationQueueOperation {
    void exec() throws ReplicationException;
  }

  private WALFileLengthProvider createWALFileLengthProvider(ReplicationQueueInfo queueInfo) {
    if (queueInfo.isQueueRecovered()) {
      return p -> OptionalLong.empty();
    }
    return new RemoteWALFileLengthProvider(replicationServer.getAsyncClusterConnection(),
      queueInfo.getOwner());
  }
}
