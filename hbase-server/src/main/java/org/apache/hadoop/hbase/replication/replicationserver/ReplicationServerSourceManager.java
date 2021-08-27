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
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationSourceController;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationGlobalSourceSource;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationSourceFactory;
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
 * This class is responsible to manage all the replication sources on ReplicationServer.
 */
@InterfaceAudience.Private
public class ReplicationServerSourceManager implements ReplicationSourceController {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationServerSourceManager.class);

  private final Server server;

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
  // Path to the wal archive
  private final Path oldWalDir;

  private final UUID clusterId;

  public ReplicationServerSourceManager(Server server, FileSystem fs, Path walRootDir,
    Path oldWalDir, UUID clusterId) {
    this.server = server;
    this.conf = server.getConfiguration();
    this.fs = fs;
    this.walRootDir = walRootDir;
    this.oldWalDir = oldWalDir;
    this.clusterId = clusterId;
    this.zkQueueStorage = (ZKReplicationQueueStorage) ReplicationStorageFactory
      .getReplicationQueueStorage(server.getZooKeeper(), conf);
    this.replicationPeers =
      ReplicationFactory.getReplicationPeers(server.getZooKeeper(), conf);
    this.totalBufferLimit = this.conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
      HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    this.globalMetrics = CompatibilitySingletonFactory.getInstance(
      MetricsReplicationSourceFactory.class).getGlobalSource();
  }


  public void init() throws IOException, ReplicationException {
    this.replicationPeers.init();
  }

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

  public ReplicationSourceInterface getSource(ReplicationQueueInfo queueInfo) {
    return this.sources.get(queueInfo);
  }

  public ZKReplicationQueueStorage getZkQueueStorage() {
    return this.zkQueueStorage;
  }

  public void startReplicationSource(ServerName owner, String queueId) throws IOException,
    ReplicationException {
    ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(owner, queueId);
    String peerId = replicationQueueInfo.getPeerId();
    this.replicationPeers.addPeer(peerId);

    ReplicationSourceInterface src = ReplicationSourceFactory.create(conf, queueId);

    ReplicationSourceInterface existSource = sources.putIfAbsent(replicationQueueInfo, src);
    if (existSource != null) {
      LOG.warn("Duplicate source exists for replication queue: owner={}, queueId={}", owner,
        queueId);
      return;
    }

    Path walDir = replicationQueueInfo.isQueueRecovered() ? oldWalDir :
      new Path(walRootDir, AbstractFSWALProvider.getWALDirectoryName(owner.toString()));

    // init replication source
    src.init(conf, fs, walDir, this, zkQueueStorage, replicationPeers.getPeer(peerId),
      server, replicationQueueInfo, clusterId,
      createWALFileLengthProvider(replicationQueueInfo), new MetricsSource(replicationQueueInfo));

    List<String> waLsInQueue = zkQueueStorage.getWALsInQueue(owner, queueId);
    waLsInQueue.forEach(walName -> src.enqueueLog(new Path(walDir, walName)));

    src.startup();
    LOG.info("Start replication source for queue {} from region server {}", queueId, owner);
  }

  public void stopReplicationSource(ServerName owner, String queueId) throws IOException,
    ReplicationException {
    String terminateMessage = "Replication source was stopped by Master";
    ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(owner, queueId);
    ReplicationSourceInterface src = sources.remove(replicationQueueInfo);
    if (src != null) {
      // Just terminate the source, do not clean queue.
      src.terminate(terminateMessage);
    }
  }

  public void removePeer(String peerId) {
    String terminateMessage = "Replication stream was removed by a user";

    List<ReplicationQueueInfo> queuesToDelete = sources.keySet().stream()
      .filter(queueInfo -> queueInfo.getPeerId().equals(peerId)).collect(Collectors.toList());

    for (ReplicationQueueInfo queueInfo : queuesToDelete) {
      ReplicationSourceInterface source = sources.remove(queueInfo);
      source.terminate(terminateMessage);
      deleteQueue(queueInfo);
    }
    // Remove HFile Refs
    abortWhenFail(() -> this.zkQueueStorage.removePeerFromHFileRefs(peerId));
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
      server.abort("Failed to operate on replication queue", e);
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
    return new RemoteWALFileLengthProvider(server.getAsyncClusterConnection(),
      queueInfo.getOwner());
  }
}
