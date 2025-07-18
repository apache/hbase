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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationOffsetUtil;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.AbstractWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class is responsible to manage all the replication sources. There are two classes of
 * sources:
 * <ul>
 * <li>Normal sources are persistent and one per peer cluster</li>
 * <li>Old sources are recovered from a failed region server and our only goal is to finish
 * replicating the WAL queue it had</li>
 * </ul>
 * <p>
 * When a region server dies, this class uses a watcher to get notified and it tries to grab a lock
 * in order to transfer all the queues in a local old source.
 * <p>
 * Synchronization specification:
 * <ul>
 * <li>No need synchronized on {@link #sources}. {@link #sources} is a ConcurrentHashMap and there
 * is a Lock for peer id in {@link PeerProcedureHandlerImpl}. So there is no race for peer
 * operations.</li>
 * <li>Need synchronized on {@link #walsById}. There are four methods which modify it,
 * {@link #addPeer(String)}, {@link #removePeer(String)},
 * {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)} and
 * {@link #postLogRoll(Path)}. {@link #walsById} is a ConcurrentHashMap and there is a Lock for peer
 * id in {@link PeerProcedureHandlerImpl}. So there is no race between {@link #addPeer(String)} and
 * {@link #removePeer(String)}. {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)}
 * is called by {@link ReplicationSourceInterface}. So no race with {@link #addPeer(String)}.
 * {@link #removePeer(String)} will terminate the {@link ReplicationSourceInterface} firstly, then
 * remove the wals from {@link #walsById}. So no race with {@link #removePeer(String)}. The only
 * case need synchronized is {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)} and
 * {@link #postLogRoll(Path)}.</li>
 * <li>No need synchronized on {@link #walsByIdRecoveredQueues}. There are three methods which
 * modify it, {@link #removePeer(String)} ,
 * {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)} and
 * {@link #claimQueue(ReplicationQueueId)}.
 * {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)} is called by
 * {@link ReplicationSourceInterface}. {@link #removePeer(String)} will terminate the
 * {@link ReplicationSourceInterface} firstly, then remove the wals from
 * {@link #walsByIdRecoveredQueues}. And {@link #claimQueue(ReplicationQueueId)} will add the wals
 * to {@link #walsByIdRecoveredQueues} firstly, then start up a {@link ReplicationSourceInterface}.
 * So there is no race here. For {@link #claimQueue(ReplicationQueueId)} and
 * {@link #removePeer(String)}, there is already synchronized on {@link #oldsources}. So no need
 * synchronized on {@link #walsByIdRecoveredQueues}.</li>
 * <li>Need synchronized on {@link #latestPaths} to avoid the new open source miss new log.</li>
 * <li>Need synchronized on {@link #oldsources} to avoid adding recovered source for the
 * to-be-removed peer.</li>
 * </ul>
 */
@InterfaceAudience.Private
public class ReplicationSourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceManager.class);
  // all the sources that read this RS's logs and every peer only has one replication source
  private final ConcurrentMap<String, ReplicationSourceInterface> sources;
  // List of all the sources we got from died RSs
  private final List<ReplicationSourceInterface> oldsources;

  /**
   * Storage for queues that need persistance; e.g. Replication state so can be recovered after a
   * crash. queueStorage upkeep is spread about this class and passed to ReplicationSource instances
   * for these to do updates themselves. Not all ReplicationSource instances keep state.
   */
  private final ReplicationQueueStorage queueStorage;

  private final ReplicationPeers replicationPeers;
  // UUID for this cluster
  private final UUID clusterId;
  // All about stopping
  private final Server server;

  // All logs we are currently tracking
  // Index structure of the map is: queue_id->logPrefix/logGroup->logs
  private final ConcurrentMap<ReplicationQueueId, Map<String, NavigableSet<String>>> walsById;
  // Logs for recovered sources we are currently tracking
  // the map is: queue_id->logPrefix/logGroup->logs
  // for recovered source, the WAL files should already been moved to oldLogDir, and we have
  // different layout of old WAL files, for example, with server name sub directories or not, so
  // here we record the full path instead of just the name, so when refreshing we can enqueue the
  // WAL file again, without trying to guess the real path of the WAL files.
  private final ConcurrentMap<ReplicationQueueId,
    Map<String, NavigableSet<Path>>> walsByIdRecoveredQueues;

  private final SyncReplicationPeerMappingManager syncReplicationPeerMappingManager;

  private final Configuration conf;
  private final FileSystem fs;
  // The paths to the latest log of each wal group, for new coming peers
  private final Map<String, Path> latestPaths;
  // Path to the wals directories
  private final Path logDir;
  // Path to the wal archive
  private final Path oldLogDir;
  private final WALFactory walFactory;
  // The number of ms that we wait before moving znodes, HBASE-3596
  private final long sleepBeforeFailover;
  // Homemade executer service for replication
  private final ThreadPoolExecutor executor;

  private AtomicLong totalBufferUsed = new AtomicLong();

  // How long should we sleep for each retry when deleting remote wal files for sync replication
  // peer.
  private final long sleepForRetries;
  // Maximum number of retries before taking bold actions when deleting remote wal files for sync
  // replication peer.
  private final int maxRetriesMultiplier;
  // Total buffer size on this RegionServer for holding batched edits to be shipped.
  private final long totalBufferLimit;
  private final MetricsReplicationGlobalSourceSource globalMetrics;

  /**
   * Creates a replication manager and sets the watch on all the other registered region servers
   * @param queueStorage the interface for manipulating replication queues
   * @param conf         the configuration to use
   * @param server       the server for this region server
   * @param fs           the file system to use
   * @param logDir       the directory that contains all wal directories of live RSs
   * @param oldLogDir    the directory where old logs are archived
   */
  public ReplicationSourceManager(ReplicationQueueStorage queueStorage,
    ReplicationPeers replicationPeers, Configuration conf, Server server, FileSystem fs,
    Path logDir, Path oldLogDir, UUID clusterId, WALFactory walFactory,
    SyncReplicationPeerMappingManager syncReplicationPeerMappingManager,
    MetricsReplicationGlobalSourceSource globalMetrics) throws IOException {
    this.sources = new ConcurrentHashMap<>();
    this.queueStorage = queueStorage;
    this.replicationPeers = replicationPeers;
    this.server = server;
    this.walsById = new ConcurrentHashMap<>();
    this.walsByIdRecoveredQueues = new ConcurrentHashMap<>();
    this.oldsources = new ArrayList<>();
    this.conf = conf;
    this.fs = fs;
    this.logDir = logDir;
    this.oldLogDir = oldLogDir;
    // 30 seconds
    this.sleepBeforeFailover = conf.getLong("replication.sleep.before.failover", 30000);
    this.clusterId = clusterId;
    this.walFactory = walFactory;
    this.syncReplicationPeerMappingManager = syncReplicationPeerMappingManager;
    // It's preferable to failover 1 RS at a time, but with good zk servers
    // more could be processed at the same time.
    int nbWorkers = conf.getInt("replication.executor.workers", 1);
    // use a short 100ms sleep since this could be done inline with a RS startup
    // even if we fail, other region servers can take care of it
    this.executor = new ThreadPoolExecutor(nbWorkers, nbWorkers, 100, TimeUnit.MILLISECONDS,
      new LinkedBlockingQueue<>());
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat("ReplicationExecutor-%d");
    tfb.setDaemon(true);
    this.executor.setThreadFactory(tfb.build());
    this.latestPaths = new HashMap<>();
    this.sleepForRetries = this.conf.getLong("replication.source.sync.sleepforretries", 1000);
    this.maxRetriesMultiplier =
      this.conf.getInt("replication.source.sync.maxretriesmultiplier", 60);
    this.totalBufferLimit = conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
      HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    this.globalMetrics = globalMetrics;
  }

  /**
   * Adds a normal source per registered peer cluster.
   */
  void init() throws IOException {
    for (String id : this.replicationPeers.getAllPeerIds()) {
      addSource(id, true);
    }
  }

  /**
   * <ol>
   * <li>Add peer to replicationPeers</li>
   * <li>Add the normal source and related replication queue</li>
   * <li>Add HFile Refs</li>
   * </ol>
   * @param peerId the id of replication peer
   */
  public void addPeer(String peerId) throws IOException {
    boolean added = false;
    try {
      added = this.replicationPeers.addPeer(peerId);
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
    if (added) {
      addSource(peerId, false);
    }
  }

  /**
   * <ol>
   * <li>Remove peer for replicationPeers</li>
   * <li>Remove all the recovered sources for the specified id and related replication queues</li>
   * <li>Remove the normal source and related replication queue</li>
   * <li>Remove HFile Refs</li>
   * </ol>
   * @param peerId the id of the replication peer
   */
  public void removePeer(String peerId) {
    ReplicationPeer peer = replicationPeers.removePeer(peerId);
    String terminateMessage = "Replication stream was removed by a user";
    List<ReplicationSourceInterface> oldSourcesToDelete = new ArrayList<>();
    // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
    // see NodeFailoverWorker.run
    synchronized (this.oldsources) {
      // First close all the recovered sources for this peer
      for (ReplicationSourceInterface src : oldsources) {
        if (peerId.equals(src.getPeerId())) {
          oldSourcesToDelete.add(src);
        }
      }
      for (ReplicationSourceInterface src : oldSourcesToDelete) {
        src.terminate(terminateMessage);
        removeRecoveredSource(src);
      }
    }
    LOG.info("Number of deleted recovered sources for {}: {}", peerId, oldSourcesToDelete.size());
    // Now close the normal source for this peer
    ReplicationSourceInterface srcToRemove = this.sources.get(peerId);
    if (srcToRemove != null) {
      srcToRemove.terminate(terminateMessage);
      removeSource(srcToRemove);
    }
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    if (peerConfig.isSyncReplication()) {
      syncReplicationPeerMappingManager.remove(peerId, peerConfig);
    }
  }

  /**
   * @return a new 'classic' user-space replication source.
   * @param queueId the id of the replication queue to associate the ReplicationSource with.
   * @see #createCatalogReplicationSource(RegionInfo) for creating a ReplicationSource for meta.
   */
  private ReplicationSourceInterface createSource(ReplicationQueueData queueData,
    ReplicationPeer replicationPeer) throws IOException {
    ReplicationSourceInterface src = ReplicationSourceFactory.create(conf, queueData.getId());
    // Init the just created replication source. Pass the default walProvider's wal file length
    // provider. Presumption is we replicate user-space Tables only. For hbase:meta region replica
    // replication, see #createCatalogReplicationSource().
    WALFileLengthProvider walFileLengthProvider = this.walFactory.getWALProvider() != null
      ? this.walFactory.getWALProvider().getWALFileLengthProvider()
      : p -> OptionalLong.empty();
    src.init(conf, fs, this, queueStorage, replicationPeer, server, queueData, clusterId,
      walFileLengthProvider, new MetricsSource(queueData.getId().toString()));
    return src;
  }

  /**
   * Add a normal source for the given peer on this region server. Meanwhile, add new replication
   * queue to storage. For the newly added peer, we only need to enqueue the latest log of each wal
   * group and do replication.
   * <p/>
   * We add a {@code init} parameter to indicate whether this is part of the initialization process.
   * If so, we should skip adding the replication queues as this may introduce dead lock on region
   * server start up and hbase:replication table online.
   * @param peerId the id of the replication peer
   * @param init   whether this call is part of the initialization process
   * @return the source that was created
   */
  void addSource(String peerId, boolean init) throws IOException {
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    if (
      ReplicationUtils.LEGACY_REGION_REPLICATION_ENDPOINT_NAME
        .equals(peer.getPeerConfig().getReplicationEndpointImpl())
    ) {
      // we do not use this endpoint for region replication any more, see HBASE-26233
      LOG.info("Legacy region replication peer found, skip adding: {}", peer.getPeerConfig());
      return;
    }
    ReplicationQueueId queueId = new ReplicationQueueId(server.getServerName(), peerId);
    ReplicationSourceInterface src =
      createSource(new ReplicationQueueData(queueId, ImmutableMap.of()), peer);
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      this.sources.put(peerId, src);
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      this.walsById.put(queueId, walsByGroup);
      // Add the latest wal to that source's queue
      if (!latestPaths.isEmpty()) {
        for (Map.Entry<String, Path> walPrefixAndPath : latestPaths.entrySet()) {
          Path walPath = walPrefixAndPath.getValue();
          NavigableSet<String> wals = new TreeSet<>();
          wals.add(walPath.getName());
          walsByGroup.put(walPrefixAndPath.getKey(), wals);
          if (!init) {
            // Abort RS and throw exception to make add peer failed
            // Ideally we'd better use the current file size as offset so we can skip replicating
            // the data before adding replication peer, but the problem is that the file may not end
            // at a valid entry's ending, and the current WAL Reader implementation can not deal
            // with reading from the middle of a WAL entry. Can improve later.
            abortAndThrowIOExceptionWhenFail(
              () -> this.queueStorage.setOffset(queueId, walPrefixAndPath.getKey(),
                new ReplicationGroupOffset(walPath.getName(), 0), Collections.emptyMap()));
          }
          src.enqueueLog(walPath);
          LOG.trace("Enqueued {} to source {} during source creation.", walPath, src.getQueueId());
        }
      }
    }
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    if (peerConfig.isSyncReplication()) {
      syncReplicationPeerMappingManager.add(peer.getId(), peerConfig);
    }
    src.startup();
  }

  /**
   * <p>
   * This is used when we transit a sync replication peer to {@link SyncReplicationState#STANDBY}.
   * </p>
   * <p>
   * When transiting to {@link SyncReplicationState#STANDBY}, we can remove all the pending wal
   * files for a replication peer as we do not need to replicate them any more. And this is
   * necessary, otherwise when we transit back to {@link SyncReplicationState#DOWNGRADE_ACTIVE}
   * later, the stale data will be replicated again and cause inconsistency.
   * </p>
   * <p>
   * See HBASE-20426 for more details.
   * </p>
   * @param peerId the id of the sync replication peer
   */
  public void drainSources(String peerId) throws IOException, ReplicationException {
    String terminateMessage = "Sync replication peer " + peerId
      + " is transiting to STANDBY. Will close the previous replication source and open a new one";
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    assert peer.getPeerConfig().isSyncReplication();
    ReplicationQueueId queueId = new ReplicationQueueId(server.getServerName(), peerId);
    // TODO: use empty initial offsets for now, revisit when adding support for sync replication
    ReplicationSourceInterface src =
      createSource(new ReplicationQueueData(queueId, ImmutableMap.of()), peer);
    // synchronized here to avoid race with postLogRoll where we add new log to source and also
    // walsById.
    ReplicationSourceInterface toRemove;
    ReplicationQueueData queueData;
    synchronized (latestPaths) {
      // Here we make a copy of all the remaining wal files and then delete them from the
      // replication queue storage after releasing the lock. It is not safe to just remove the old
      // map from walsById since later we may fail to update the replication queue storage, and when
      // we retry next time, we can not know the wal files that needs to be set to the replication
      // queue storage
      ImmutableMap.Builder<String, ReplicationGroupOffset> builder = ImmutableMap.builder();
      synchronized (walsById) {
        walsById.get(queueId).forEach((group, wals) -> {
          if (!wals.isEmpty()) {
            builder.put(group, new ReplicationGroupOffset(wals.last(), -1));
          }
        });
      }
      queueData = new ReplicationQueueData(queueId, builder.build());
      src = createSource(queueData, peer);
      toRemove = sources.put(peerId, src);
      if (toRemove != null) {
        LOG.info("Terminate replication source for " + toRemove.getPeerId());
        toRemove.terminate(terminateMessage);
        toRemove.getSourceMetrics().clear();
      }
    }
    for (Map.Entry<String, ReplicationGroupOffset> entry : queueData.getOffsets().entrySet()) {
      queueStorage.setOffset(queueId, entry.getKey(), entry.getValue(), Collections.emptyMap());
    }
    LOG.info("Startup replication source for " + src.getPeerId());
    src.startup();
    synchronized (walsById) {
      Map<String, NavigableSet<String>> wals = walsById.get(queueId);
      queueData.getOffsets().forEach((group, offset) -> {
        NavigableSet<String> walsByGroup = wals.get(group);
        if (walsByGroup != null) {
          walsByGroup.headSet(offset.getWal(), true).clear();
        }
      });
    }
    // synchronized on oldsources to avoid race with NodeFailoverWorker. Since NodeFailoverWorker is
    // a background task, we will delete the file from replication queue storage under the lock to
    // simplify the logic.
    synchronized (this.oldsources) {
      for (Iterator<ReplicationSourceInterface> iter = oldsources.iterator(); iter.hasNext();) {
        ReplicationSourceInterface oldSource = iter.next();
        if (oldSource.getPeerId().equals(peerId)) {
          ReplicationQueueId oldSourceQueueId = oldSource.getQueueId();
          oldSource.terminate(terminateMessage);
          oldSource.getSourceMetrics().clear();
          queueStorage.removeQueue(oldSourceQueueId);
          walsByIdRecoveredQueues.remove(oldSourceQueueId);
          iter.remove();
        }
      }
    }
  }

  private ReplicationSourceInterface createRefreshedSource(ReplicationQueueId queueId,
    ReplicationPeer peer) throws IOException, ReplicationException {
    Map<String, ReplicationGroupOffset> offsets = queueStorage.getOffsets(queueId);
    return createSource(new ReplicationQueueData(queueId, ImmutableMap.copyOf(offsets)), peer);
  }

  /**
   * Close the previous replication sources of this peer id and open new sources to trigger the new
   * replication state changes or new replication config changes. Here we don't need to change
   * replication queue storage and only to enqueue all logs to the new replication source
   * @param peerId the id of the replication peer
   */
  public void refreshSources(String peerId) throws ReplicationException, IOException {
    String terminateMessage = "Peer " + peerId
      + " state or config changed. Will close the previous replication source and open a new one";
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    ReplicationQueueId queueId = new ReplicationQueueId(server.getServerName(), peerId);
    ReplicationSourceInterface src;
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      ReplicationSourceInterface toRemove = this.sources.remove(peerId);
      if (toRemove != null) {
        LOG.info("Terminate replication source for " + toRemove.getPeerId());
        // Do not clear metrics
        toRemove.terminate(terminateMessage, null, false);
      }
      src = createRefreshedSource(queueId, peer);
      this.sources.put(peerId, src);
      for (NavigableSet<String> walsByGroup : walsById.get(queueId).values()) {
        walsByGroup.forEach(wal -> src.enqueueLog(new Path(this.logDir, wal)));
      }
    }
    LOG.info("Startup replication source for " + src.getPeerId());
    src.startup();

    List<ReplicationSourceInterface> toStartup = new ArrayList<>();
    // synchronized on oldsources to avoid race with NodeFailoverWorker
    synchronized (this.oldsources) {
      List<ReplicationQueueId> oldSourceQueueIds = new ArrayList<>();
      for (Iterator<ReplicationSourceInterface> iter = this.oldsources.iterator(); iter
        .hasNext();) {
        ReplicationSourceInterface oldSource = iter.next();
        if (oldSource.getPeerId().equals(peerId)) {
          oldSourceQueueIds.add(oldSource.getQueueId());
          oldSource.terminate(terminateMessage);
          iter.remove();
        }
      }
      for (ReplicationQueueId oldSourceQueueId : oldSourceQueueIds) {
        ReplicationSourceInterface recoveredReplicationSource =
          createRefreshedSource(oldSourceQueueId, peer);
        this.oldsources.add(recoveredReplicationSource);
        for (NavigableSet<Path> walsByGroup : walsByIdRecoveredQueues.get(oldSourceQueueId)
          .values()) {
          walsByGroup.forEach(wal -> recoveredReplicationSource.enqueueLog(wal));
        }
        toStartup.add(recoveredReplicationSource);
      }
    }
    for (ReplicationSourceInterface replicationSource : toStartup) {
      replicationSource.startup();
    }
  }

  /**
   * Clear the metrics and related replication queue of the specified old source
   * @param src source to clear
   */
  private boolean removeRecoveredSource(ReplicationSourceInterface src) {
    if (!this.oldsources.remove(src)) {
      return false;
    }
    LOG.info("Done with the recovered queue {}", src.getQueueId());
    // Delete queue from storage and memory
    deleteQueue(src.getQueueId());
    this.walsByIdRecoveredQueues.remove(src.getQueueId());
    return true;
  }

  void finishRecoveredSource(ReplicationSourceInterface src) {
    synchronized (oldsources) {
      if (!removeRecoveredSource(src)) {
        return;
      }
    }
    LOG.info("Finished recovering queue {} with the following stats: {}", src.getQueueId(),
      src.getStats());
  }

  /**
   * Clear the metrics and related replication queue of the specified old source
   * @param src source to clear
   */
  void removeSource(ReplicationSourceInterface src) {
    LOG.info("Done with the queue " + src.getQueueId());
    this.sources.remove(src.getPeerId());
    // Delete queue from storage and memory
    deleteQueue(src.getQueueId());
    this.walsById.remove(src.getQueueId());

  }

  /**
   * Delete a complete queue of wals associated with a replication source
   * @param queueId the id of replication queue to delete
   */
  private void deleteQueue(ReplicationQueueId queueId) {
    abortWhenFail(() -> this.queueStorage.removeQueue(queueId));
  }

  @FunctionalInterface
  private interface ReplicationQueueOperation {
    void exec() throws ReplicationException;
  }

  /**
   * Refresh replication source will terminate the old source first, then the source thread will be
   * interrupted. Need to handle it instead of abort the region server.
   */
  private void interruptOrAbortWhenFail(ReplicationQueueOperation op) {
    try {
      op.exec();
    } catch (ReplicationException e) {
      if (
        e.getCause() != null && e.getCause() instanceof KeeperException.SystemErrorException
          && e.getCause().getCause() != null
          && e.getCause().getCause() instanceof InterruptedException
      ) {
        // ReplicationRuntimeException(a RuntimeException) is thrown out here. The reason is
        // that thread is interrupted deep down in the stack, it should pass the following
        // processing logic and propagate to the most top layer which can handle this exception
        // properly. In this specific case, the top layer is ReplicationSourceShipper#run().
        throw new ReplicationRuntimeException(
          "Thread is interrupted, the replication source may be terminated",
          e.getCause().getCause());
      }
      server.abort("Failed to operate on replication queue", e);
    }
  }

  private void abortWhenFail(ReplicationQueueOperation op) {
    try {
      op.exec();
    } catch (ReplicationException e) {
      server.abort("Failed to operate on replication queue", e);
    }
  }

  private void throwIOExceptionWhenFail(ReplicationQueueOperation op) throws IOException {
    try {
      op.exec();
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
  }

  private void abortAndThrowIOExceptionWhenFail(ReplicationQueueOperation op) throws IOException {
    try {
      op.exec();
    } catch (ReplicationException e) {
      server.abort("Failed to operate on replication queue", e);
      throw new IOException(e);
    }
  }

  /**
   * This method will log the current position to storage. And also clean old logs from the
   * replication queue.
   * @param source     the replication source
   * @param entryBatch the wal entry batch we just shipped
   */
  public void logPositionAndCleanOldLogs(ReplicationSourceInterface source,
    WALEntryBatch entryBatch) {
    String walName = entryBatch.getLastWalPath().getName();
    String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(walName);
    // if end of file, we just set the offset to -1 so we know that this file has already been fully
    // replicated, otherwise we need to compare the file length
    ReplicationGroupOffset offset = new ReplicationGroupOffset(walName,
      entryBatch.isEndOfFile() ? -1 : entryBatch.getLastWalPosition());
    interruptOrAbortWhenFail(() -> this.queueStorage.setOffset(source.getQueueId(), walPrefix,
      offset, entryBatch.getLastSeqIds()));
    cleanOldLogs(walName, entryBatch.isEndOfFile(), source);
  }

  void postAppend(final long size, final long time, final WALKey logkey, final WALEdit logEdit) {
    try {
      long walReplAppendSize = 0;
      List<ReplicationSourceInterface> replicationSources = getSources();
      for (ReplicationSourceInterface replicationSourceI : replicationSources) {
        if (replicationSourceI instanceof ReplicationSource) {
          MetricsSource source = replicationSourceI.getSourceMetrics();
          ReplicationSource replicationSource = (ReplicationSource) replicationSourceI;
          WALEntryFilter filter = replicationSource.getWalEntryFilter();
          WAL.Entry filtered = filter.filter(new WAL.Entry((WALKeyImpl) logkey, logEdit));
          if (filtered != null && filtered.getEdit() != null && !filtered.getEdit().isEmpty()) {
            walReplAppendSize = ReplicationSourceWALReader.getEntrySizeIncludeBulkLoad(filtered);
            // update the replication metrics source for table at the run time
            TableName tableName = logkey.getTableName();
            source.incrTableWalAppendBytes(tableName.getNameAsString(), walReplAppendSize);
          }
        }
      }
    } catch (Throwable t) {
      LOG.warn("ReplicationSourceManager.postAppend meet throwable: ", t);
      LOG.debug("ReplicationSourceManager.postAppend meet throwable, logkey is: " + logkey
        + ",logEdit is: " + logEdit);
    }
  }

  /**
   * Cleans a log file and all older logs from replication queue. Called when we are sure that a log
   * file is closed and has no more entries.
   * @param log       Path to the log
   * @param inclusive whether we should also remove the given log file
   * @param source    the replication source
   */
  void cleanOldLogs(String log, boolean inclusive, ReplicationSourceInterface source) {
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(log);
    if (source.isRecovered()) {
      NavigableSet<Path> wals = walsByIdRecoveredQueues.get(source.getQueueId()).get(logPrefix);
      if (wals != null) {
        // here we just want to compare the timestamp, so it is OK to just create a fake WAL path
        NavigableSet<String> walsToRemove = wals.headSet(new Path(oldLogDir, log), inclusive)
          .stream().map(Path::getName).collect(Collectors.toCollection(TreeSet::new));
        if (walsToRemove.isEmpty()) {
          return;
        }
        cleanOldLogs(walsToRemove, source);
        walsToRemove.clear();
      }
    } else {
      NavigableSet<String> wals;
      NavigableSet<String> walsToRemove;
      // synchronized on walsById to avoid race with postLogRoll
      synchronized (this.walsById) {
        wals = walsById.get(source.getQueueId()).get(logPrefix);
        if (wals == null) {
          return;
        }
        walsToRemove = wals.headSet(log, inclusive);
        if (walsToRemove.isEmpty()) {
          return;
        }
        walsToRemove = new TreeSet<>(walsToRemove);
      }
      // cleanOldLogs may spend some time, especially for sync replication where we may want to
      // remove remote wals as the remote cluster may have already been down, so we do it outside
      // the lock to avoid block preLogRoll
      cleanOldLogs(walsToRemove, source);
      // now let's remove the files in the set
      synchronized (this.walsById) {
        wals.removeAll(walsToRemove);
      }
    }
  }

  private void removeRemoteWALs(String peerId, String remoteWALDir, Collection<String> wals)
    throws IOException {
    Path remoteWALDirForPeer = ReplicationUtils.getPeerRemoteWALDir(remoteWALDir, peerId);
    FileSystem fs = ReplicationUtils.getRemoteWALFileSystem(conf, remoteWALDir);
    for (String wal : wals) {
      Path walFile = new Path(remoteWALDirForPeer, wal);
      try {
        if (!fs.delete(walFile, false) && fs.exists(walFile)) {
          throw new IOException("Can not delete " + walFile);
        }
      } catch (FileNotFoundException e) {
        // Just ignore since this means the file has already been deleted.
        // The javadoc of the FileSystem.delete methods does not specify the behavior of deleting an
        // inexistent file, so here we deal with both, i.e, check the return value of the
        // FileSystem.delete, and also catch FNFE.
        LOG.debug("The remote wal {} has already been deleted?", walFile, e);
      }
    }
  }

  private void cleanOldLogs(NavigableSet<String> wals, ReplicationSourceInterface source) {
    LOG.debug("Removing {} logs in the list: {}", wals.size(), wals);
    // The intention here is that, we want to delete the remote wal files ASAP as it may effect the
    // failover time if you want to transit the remote cluster from S to A. And the infinite retry
    // is not a problem, as if we can not contact with the remote HDFS cluster, then usually we can
    // not contact with the HBase cluster either, so the replication will be blocked either.
    if (source.isSyncReplication()) {
      String peerId = source.getPeerId();
      String remoteWALDir = source.getPeer().getPeerConfig().getRemoteWALDir();
      // Filter out the wals need to be removed from the remote directory. Its name should be the
      // special format, and also, the peer id in its name should match the peer id for the
      // replication source.
      List<String> remoteWals =
        wals.stream().filter(w -> AbstractWALProvider.getSyncReplicationPeerIdFromWALName(w)
          .map(peerId::equals).orElse(false)).collect(Collectors.toList());
      LOG.debug("Removing {} logs from remote dir {} in the list: {}", remoteWals.size(),
        remoteWALDir, remoteWals);
      if (!remoteWals.isEmpty()) {
        for (int sleepMultiplier = 0;;) {
          try {
            removeRemoteWALs(peerId, remoteWALDir, remoteWals);
            break;
          } catch (IOException e) {
            LOG.warn("Failed to delete remote wals from remote dir {} for peer {}", remoteWALDir,
              peerId);
          }
          if (!source.isSourceActive()) {
            // skip the following operations
            return;
          }
          if (
            ReplicationUtils.sleepForRetries("Failed to delete remote wals", sleepForRetries,
              sleepMultiplier, maxRetriesMultiplier)
          ) {
            sleepMultiplier++;
          }
        }
      }
    }
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
  public void postLogRoll(Path newLog) throws IOException {
    String logName = newLog.getName();
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(logName);
    // synchronized on latestPaths to avoid the new open source miss the new log
    synchronized (this.latestPaths) {
      // synchronized on walsById to avoid race with cleanOldLogs
      synchronized (this.walsById) {
        // Update walsById map
        for (Map.Entry<ReplicationQueueId, Map<String, NavigableSet<String>>> entry : this.walsById
          .entrySet()) {
          ReplicationQueueId queueId = entry.getKey();
          String peerId = queueId.getPeerId();
          Map<String, NavigableSet<String>> walsByPrefix = entry.getValue();
          boolean existingPrefix = false;
          for (Map.Entry<String, NavigableSet<String>> walsEntry : walsByPrefix.entrySet()) {
            SortedSet<String> wals = walsEntry.getValue();
            if (this.sources.isEmpty()) {
              // If there's no slaves, don't need to keep the old wals since
              // we only consider the last one when a new slave comes in
              wals.clear();
            }
            if (logPrefix.equals(walsEntry.getKey())) {
              wals.add(logName);
              existingPrefix = true;
            }
          }
          if (!existingPrefix) {
            // The new log belongs to a new group, add it into this peer
            LOG.debug("Start tracking logs for wal group {} for peer {}", logPrefix, peerId);
            NavigableSet<String> wals = new TreeSet<>();
            wals.add(logName);
            walsByPrefix.put(logPrefix, wals);
          }
        }
      }

      // Add to latestPaths
      latestPaths.put(logPrefix, newLog);
    }
    // This only updates the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.enqueueLog(newLog);
      LOG.trace("Enqueued {} to source {} while performing postLogRoll operation.", newLog,
        source.getQueueId());
    }
  }

  /**
   * Check whether we should replicate the given {@code wal}.
   * @param wal the file name of the wal
   * @return {@code true} means we should replicate the given {@code wal}, otherwise {@code false}.
   */
  private boolean shouldReplicate(ReplicationGroupOffset offset, String wal) {
    // skip replicating meta wals
    if (AbstractFSWALProvider.isMetaFile(wal)) {
      return false;
    }
    return ReplicationOffsetUtil.shouldReplicate(offset, wal);
  }

  void claimQueue(ReplicationQueueId queueId) {
    claimQueue(queueId, false);
  }

  // sorted from oldest to newest
  private PriorityQueue<Path> getWALFilesToReplicate(ServerName sourceRS, boolean syncUp,
    Map<String, ReplicationGroupOffset> offsets) throws IOException {
    List<Path> walFiles = AbstractFSWALProvider.getArchivedWALFiles(conf, sourceRS,
      URLEncoder.encode(sourceRS.toString(), StandardCharsets.UTF_8.name()));
    if (syncUp) {
      // we also need to list WALs directory for ReplicationSyncUp
      walFiles.addAll(AbstractFSWALProvider.getWALFiles(conf, sourceRS));
    }
    PriorityQueue<Path> walFilesPQ =
      new PriorityQueue<>(AbstractFSWALProvider.TIMESTAMP_COMPARATOR);
    // sort the wal files and also filter out replicated files
    for (Path file : walFiles) {
      String walGroupId = AbstractFSWALProvider.getWALPrefixFromWALName(file.getName());
      ReplicationGroupOffset groupOffset = offsets.get(walGroupId);
      if (shouldReplicate(groupOffset, file.getName())) {
        walFilesPQ.add(file);
      } else {
        LOG.debug("Skip enqueuing log {} because it is before the start offset {}", file.getName(),
          groupOffset);
      }
    }
    return walFilesPQ;
  }

  private void addRecoveredSource(ReplicationSourceInterface src, ReplicationPeerImpl oldPeer,
    ReplicationQueueId claimedQueueId, PriorityQueue<Path> walFiles) {
    ReplicationPeerImpl peer = replicationPeers.getPeer(src.getPeerId());
    if (peer == null || peer != oldPeer) {
      src.terminate("Recovered queue doesn't belong to any current peer");
      deleteQueue(claimedQueueId);
      return;
    }
    // Do not setup recovered queue if a sync replication peer is in STANDBY state, or is
    // transiting to STANDBY state. The only exception is we are in STANDBY state and
    // transiting to DA, under this state we will replay the remote WAL and they need to be
    // replicated back.
    if (peer.getPeerConfig().isSyncReplication()) {
      Pair<SyncReplicationState, SyncReplicationState> stateAndNewState =
        peer.getSyncReplicationStateAndNewState();
      if (
        (stateAndNewState.getFirst().equals(SyncReplicationState.STANDBY)
          && stateAndNewState.getSecond().equals(SyncReplicationState.NONE))
          || stateAndNewState.getSecond().equals(SyncReplicationState.STANDBY)
      ) {
        src.terminate("Sync replication peer is in STANDBY state");
        deleteQueue(claimedQueueId);
        return;
      }
    }
    // track sources in walsByIdRecoveredQueues
    Map<String, NavigableSet<Path>> walsByGroup = new HashMap<>();
    walsByIdRecoveredQueues.put(claimedQueueId, walsByGroup);
    for (Path wal : walFiles) {
      String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal.getName());
      NavigableSet<Path> wals = walsByGroup.get(walPrefix);
      if (wals == null) {
        wals = new TreeSet<>(AbstractFSWALProvider.TIMESTAMP_COMPARATOR);
        walsByGroup.put(walPrefix, wals);
      }
      wals.add(wal);
    }
    oldsources.add(src);
    LOG.info("Added source for recovered queue {}, number of wals to replicate: {}", claimedQueueId,
      walFiles.size());
    for (Path wal : walFiles) {
      LOG.debug("Enqueueing log {} from recovered queue for source: {}", wal, claimedQueueId);
      src.enqueueLog(wal);
    }
    src.startup();
  }

  /**
   * Claim a replication queue.
   * <p/>
   * We add a flag to indicate whether we are called by ReplicationSyncUp. For normal claiming queue
   * operation, we are the last step of a SCP, so we can assume that all the WAL files are under
   * oldWALs directory. But for ReplicationSyncUp, we may want to claim the replication queue for a
   * region server which has not been processed by SCP yet, so we still need to look at its WALs
   * directory.
   * @param queueId the replication queue id we want to claim
   * @param syncUp  whether we are called by ReplicationSyncUp
   */
  void claimQueue(ReplicationQueueId queueId, boolean syncUp) {
    // Wait a bit before transferring the queues, we may be shutting down.
    // This sleep may not be enough in some cases.
    try {
      Thread.sleep(sleepBeforeFailover
        + (long) (ThreadLocalRandom.current().nextFloat() * sleepBeforeFailover));
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting before transferring a queue.");
      Thread.currentThread().interrupt();
    }
    // We try to lock that rs' queue directory
    if (server.isStopped()) {
      LOG.info("Not transferring queue since we are shutting down");
      return;
    }
    // After claim the queues from dead region server, we will skip to start the
    // RecoveredReplicationSource if the peer has been removed. but there's possible that remove a
    // peer with peerId = 2 and add a peer with peerId = 2 again during failover. So we need to get
    // a copy of the replication peer first to decide whether we should start the
    // RecoveredReplicationSource. If the latest peer is not the old peer, we should also skip to
    // start the RecoveredReplicationSource, Otherwise the rs will abort (See HBASE-20475).
    String peerId = queueId.getPeerId();
    ReplicationPeerImpl oldPeer = replicationPeers.getPeer(peerId);
    if (oldPeer == null) {
      LOG.info("Not transferring queue since the replication peer {} for queue {} does not exist",
        peerId, queueId);
      return;
    }
    Map<String, ReplicationGroupOffset> offsets;
    try {
      offsets = queueStorage.claimQueue(queueId, server.getServerName());
    } catch (ReplicationException e) {
      LOG.error("ReplicationException: cannot claim dead region ({})'s replication queue",
        queueId.getServerName(), e);
      server.abort("Failed to claim queue from dead regionserver.", e);
      return;
    }
    if (offsets.isEmpty()) {
      // someone else claimed the queue
      return;
    }
    ServerName sourceRS = queueId.getServerWALsBelongTo();
    ReplicationQueueId claimedQueueId = queueId.claim(server.getServerName());
    ReplicationPeerImpl peer = replicationPeers.getPeer(peerId);
    if (peer == null || peer != oldPeer) {
      LOG.warn("Skipping failover for peer {} of node {}, peer is null", peerId, sourceRS);
      deleteQueue(claimedQueueId);
      return;
    }
    ReplicationSourceInterface src;
    try {
      src =
        createSource(new ReplicationQueueData(claimedQueueId, ImmutableMap.copyOf(offsets)), peer);
    } catch (IOException e) {
      LOG.error("Can not create replication source for peer {} and queue {}", peerId,
        claimedQueueId, e);
      server.abort("Failed to create replication source after claiming queue.", e);
      return;
    }
    PriorityQueue<Path> walFiles;
    try {
      walFiles = getWALFilesToReplicate(sourceRS, syncUp, offsets);
    } catch (IOException e) {
      LOG.error("Can not list wal files for peer {} and queue {}", peerId, queueId, e);
      server.abort("Can not list wal files after claiming queue.", e);
      return;
    }
    // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
    synchronized (oldsources) {
      addRecoveredSource(src, oldPeer, claimedQueueId, walFiles);
    }
  }

  /**
   * Terminate the replication on this region server
   */
  public void join() {
    this.executor.shutdown();
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.terminate("Region server is closing");
    }
    synchronized (oldsources) {
      for (ReplicationSourceInterface source : this.oldsources) {
        source.terminate("Region server is closing");
      }
    }
  }

  /**
   * Get a copy of the wals of the normal sources on this rs
   * @return a sorted set of wal names
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public Map<ReplicationQueueId, Map<String, NavigableSet<String>>> getWALs() {
    return Collections.unmodifiableMap(walsById);
  }

  /**
   * Get a list of all the normal sources of this rs
   * @return list of all normal sources
   */
  public List<ReplicationSourceInterface> getSources() {
    return new ArrayList<>(this.sources.values());
  }

  /**
   * Get a list of all the recovered sources of this rs
   * @return list of all recovered sources
   */
  public List<ReplicationSourceInterface> getOldSources() {
    return this.oldsources;
  }

  /**
   * Get the normal source for a given peer
   * @return the normal source for the give peer if it exists, otherwise null.
   */
  public ReplicationSourceInterface getSource(String peerId) {
    return this.sources.get(peerId);
  }

  int getSizeOfLatestPath() {
    synchronized (latestPaths) {
      return latestPaths.size();
    }
  }

  Set<Path> getLastestPath() {
    synchronized (latestPaths) {
      return Sets.newHashSet(latestPaths.values());
    }
  }

  public long getTotalBufferUsed() {
    return totalBufferUsed.get();
  }

  /**
   * Returns the maximum size in bytes of edits held in memory which are pending replication across
   * all sources inside this RegionServer.
   */
  public long getTotalBufferLimit() {
    return totalBufferLimit;
  }

  /**
   * Get the directory where wals are archived
   * @return the directory where wals are archived
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /**
   * Get the directory where wals are stored by their RSs
   * @return the directory where wals are stored by their RSs
   */
  public Path getLogDir() {
    return this.logDir;
  }

  /**
   * Get the handle on the local file system
   * @return Handle on the local file system
   */
  public FileSystem getFs() {
    return this.fs;
  }

  /**
   * Get the ReplicationPeers used by this ReplicationSourceManager
   * @return the ReplicationPeers used by this ReplicationSourceManager
   */
  public ReplicationPeers getReplicationPeers() {
    return this.replicationPeers;
  }

  /**
   * Get a string representation of all the sources' metrics
   */
  public String getStats() {
    StringBuilder stats = new StringBuilder();
    // Print stats that apply across all Replication Sources
    stats.append("Global stats: ");
    stats.append("WAL Edits Buffer Used=").append(getTotalBufferUsed()).append("B, Limit=")
      .append(getTotalBufferLimit()).append("B\n");
    for (ReplicationSourceInterface source : this.sources.values()) {
      stats.append("Normal source for cluster " + source.getPeerId() + ": ");
      stats.append(source.getStats() + "\n");
    }
    for (ReplicationSourceInterface oldSource : oldsources) {
      stats.append("Recovered source for cluster/machine(s) " + oldSource.getPeerId() + ": ");
      stats.append(oldSource.getStats() + "\n");
    }
    return stats.toString();
  }

  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
    throws IOException {
    for (ReplicationSourceInterface source : this.sources.values()) {
      throwIOExceptionWhenFail(() -> source.addHFileRefs(tableName, family, pairs));
    }
  }

  public void cleanUpHFileRefs(String peerId, List<String> files) {
    interruptOrAbortWhenFail(() -> this.queueStorage.removeHFileRefs(peerId, files));
  }

  int activeFailoverTaskCount() {
    return executor.getActiveCount();
  }

  MetricsReplicationGlobalSourceSource getGlobalMetrics() {
    return this.globalMetrics;
  }

  ReplicationQueueStorage getQueueStorage() {
    return queueStorage;
  }

  /**
   * Acquire the buffer quota for {@link Entry} which is added to {@link WALEntryBatch}.
   * @param entry the wal entry which is added to {@link WALEntryBatch} and should acquire buffer
   *              quota.
   * @return true if we should clear buffer and push all
   */
  boolean acquireWALEntryBufferQuota(WALEntryBatch walEntryBatch, Entry entry) {
    long entrySize = walEntryBatch.incrementUsedBufferSize(entry);
    return this.acquireBufferQuota(entrySize);
  }

  /**
   * To release the buffer quota of {@link WALEntryBatch} which acquired by
   * {@link ReplicationSourceManager#acquireWALEntryBufferQuota}.
   * @return the released buffer quota size.
   */
  long releaseWALEntryBatchBufferQuota(WALEntryBatch walEntryBatch) {
    long usedBufferSize = walEntryBatch.getUsedBufferSize();
    if (usedBufferSize > 0) {
      this.releaseBufferQuota(usedBufferSize);
    }
    return usedBufferSize;
  }

  /**
   * Add the size to {@link ReplicationSourceManager#totalBufferUsed} and check if it exceeds
   * {@link ReplicationSourceManager#totalBufferLimit}.
   * @return true if {@link ReplicationSourceManager#totalBufferUsed} exceeds
   *         {@link ReplicationSourceManager#totalBufferLimit},we should stop increase buffer and
   *         ship all.
   */
  boolean acquireBufferQuota(long size) {
    if (size < 0) {
      throw new IllegalArgumentException("size should not less than 0");
    }
    long newBufferUsed = addTotalBufferUsed(size);
    return newBufferUsed >= totalBufferLimit;
  }

  /**
   * To release the buffer quota which acquired by
   * {@link ReplicationSourceManager#acquireBufferQuota}.
   */
  void releaseBufferQuota(long size) {
    if (size < 0) {
      throw new IllegalArgumentException("size should not less than 0");
    }
    addTotalBufferUsed(-size);
  }

  private long addTotalBufferUsed(long size) {
    if (size == 0) {
      return totalBufferUsed.get();
    }
    long newBufferUsed = totalBufferUsed.addAndGet(size);
    // Record the new buffer usage
    this.globalMetrics.setWALReaderEditsBufferBytes(newBufferUsed);
    return newBufferUsed;
  }

  /**
   * Check if {@link ReplicationSourceManager#totalBufferUsed} exceeds
   * {@link ReplicationSourceManager#totalBufferLimit} for peer.
   * @return true if {@link ReplicationSourceManager#totalBufferUsed} not more than
   *         {@link ReplicationSourceManager#totalBufferLimit}.
   */
  boolean checkBufferQuota(String peerId) {
    // try not to go over total quota
    if (totalBufferUsed.get() > totalBufferLimit) {
      LOG.warn("peer={}, can't read more edits from WAL as buffer usage {}B exceeds limit {}B",
        peerId, totalBufferUsed.get(), totalBufferLimit);
      return false;
    }
    return true;
  }
}
