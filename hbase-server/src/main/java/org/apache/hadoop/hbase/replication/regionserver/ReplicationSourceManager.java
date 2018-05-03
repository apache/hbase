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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationListener;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.SyncReplicationWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
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
 * {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)} and {@link #preLogRoll(Path)}.
 * {@link #walsById} is a ConcurrentHashMap and there is a Lock for peer id in
 * {@link PeerProcedureHandlerImpl}. So there is no race between {@link #addPeer(String)} and
 * {@link #removePeer(String)}. {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)}
 * is called by {@link ReplicationSourceInterface}. So no race with {@link #addPeer(String)}.
 * {@link #removePeer(String)} will terminate the {@link ReplicationSourceInterface} firstly, then
 * remove the wals from {@link #walsById}. So no race with {@link #removePeer(String)}. The only
 * case need synchronized is {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)} and
 * {@link #preLogRoll(Path)}.</li>
 * <li>No need synchronized on {@link #walsByIdRecoveredQueues}. There are three methods which
 * modify it, {@link #removePeer(String)} ,
 * {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)} and
 * {@link ReplicationSourceManager.NodeFailoverWorker#run()}.
 * {@link #cleanOldLogs(String, boolean, ReplicationSourceInterface)} is called by
 * {@link ReplicationSourceInterface}. {@link #removePeer(String)} will terminate the
 * {@link ReplicationSourceInterface} firstly, then remove the wals from
 * {@link #walsByIdRecoveredQueues}. And {@link ReplicationSourceManager.NodeFailoverWorker#run()}
 * will add the wals to {@link #walsByIdRecoveredQueues} firstly, then start up a
 * {@link ReplicationSourceInterface}. So there is no race here. For
 * {@link ReplicationSourceManager.NodeFailoverWorker#run()} and {@link #removePeer(String)}, there
 * is already synchronized on {@link #oldsources}. So no need synchronized on
 * {@link #walsByIdRecoveredQueues}.</li>
 * <li>Need synchronized on {@link #latestPaths} to avoid the new open source miss new log.</li>
 * <li>Need synchronized on {@link #oldsources} to avoid adding recovered source for the
 * to-be-removed peer.</li>
 * </ul>
 */
@InterfaceAudience.Private
public class ReplicationSourceManager implements ReplicationListener {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSourceManager.class);
  // all the sources that read this RS's logs and every peer only has one replication source
  private final ConcurrentMap<String, ReplicationSourceInterface> sources;
  // List of all the sources we got from died RSs
  private final List<ReplicationSourceInterface> oldsources;
  private final ReplicationQueueStorage queueStorage;
  private final ReplicationTracker replicationTracker;
  private final ReplicationPeers replicationPeers;
  // UUID for this cluster
  private final UUID clusterId;
  // All about stopping
  private final Server server;

  // All logs we are currently tracking
  // Index structure of the map is: queue_id->logPrefix/logGroup->logs
  // For normal replication source, the peer id is same with the queue id
  private final ConcurrentMap<String, Map<String, NavigableSet<String>>> walsById;
  // Logs for recovered sources we are currently tracking
  // the map is: queue_id->logPrefix/logGroup->logs
  // For recovered source, the queue id's format is peer_id-servername-*
  private final ConcurrentMap<String, Map<String, NavigableSet<String>>> walsByIdRecoveredQueues;

  private final SyncReplicationPeerMappingManager syncReplicationPeerMappingManager;

  private final Configuration conf;
  private final FileSystem fs;
  // The paths to the latest log of each wal group, for new coming peers
  private final Set<Path> latestPaths;
  // Path to the wals directories
  private final Path logDir;
  // Path to the wal archive
  private final Path oldLogDir;
  private final WALFileLengthProvider walFileLengthProvider;
  // The number of ms that we wait before moving znodes, HBASE-3596
  private final long sleepBeforeFailover;
  // Homemade executer service for replication
  private final ThreadPoolExecutor executor;

  private final boolean replicationForBulkLoadDataEnabled;

  private AtomicLong totalBufferUsed = new AtomicLong();

  // How long should we sleep for each retry when deleting remote wal files for sync replication
  // peer.
  private final long sleepForRetries;
  // Maximum number of retries before taking bold actions when deleting remote wal files for sync
  // replication peer.
  private final int maxRetriesMultiplier;

  /**
   * Creates a replication manager and sets the watch on all the other registered region servers
   * @param queueStorage the interface for manipulating replication queues
   * @param replicationPeers
   * @param replicationTracker
   * @param conf the configuration to use
   * @param server the server for this region server
   * @param fs the file system to use
   * @param logDir the directory that contains all wal directories of live RSs
   * @param oldLogDir the directory where old logs are archived
   * @param clusterId
   */
  public ReplicationSourceManager(ReplicationQueueStorage queueStorage,
      ReplicationPeers replicationPeers, ReplicationTracker replicationTracker, Configuration conf,
      Server server, FileSystem fs, Path logDir, Path oldLogDir, UUID clusterId,
      WALFileLengthProvider walFileLengthProvider,
      SyncReplicationPeerMappingManager syncReplicationPeerMappingManager) throws IOException {
    this.sources = new ConcurrentHashMap<>();
    this.queueStorage = queueStorage;
    this.replicationPeers = replicationPeers;
    this.replicationTracker = replicationTracker;
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
    this.walFileLengthProvider = walFileLengthProvider;
    this.syncReplicationPeerMappingManager = syncReplicationPeerMappingManager;
    this.replicationTracker.registerListener(this);
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
    this.latestPaths = new HashSet<Path>();
    this.replicationForBulkLoadDataEnabled = conf.getBoolean(
      HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    this.sleepForRetries = this.conf.getLong("replication.source.sync.sleepforretries", 1000);
    this.maxRetriesMultiplier =
      this.conf.getInt("replication.source.sync.maxretriesmultiplier", 60);
  }

  /**
   * Adds a normal source per registered peer cluster and tries to process all old region server wal
   * queues
   * <p>
   * The returned future is for adoptAbandonedQueues task.
   */
  Future<?> init() throws IOException {
    for (String id : this.replicationPeers.getAllPeerIds()) {
      addSource(id);
      if (replicationForBulkLoadDataEnabled) {
        // Check if peer exists in hfile-refs queue, if not add it. This can happen in the case
        // when a peer was added before replication for bulk loaded data was enabled.
        throwIOExceptionWhenFail(() -> this.queueStorage.addPeerToHFileRefs(id));
      }
    }
    return this.executor.submit(this::adoptAbandonedQueues);
  }

  private void adoptAbandonedQueues() {
    List<ServerName> currentReplicators = null;
    try {
      currentReplicators = queueStorage.getListOfReplicators();
    } catch (ReplicationException e) {
      server.abort("Failed to get all replicators", e);
      return;
    }
    if (currentReplicators == null || currentReplicators.isEmpty()) {
      return;
    }
    List<ServerName> otherRegionServers = replicationTracker.getListOfRegionServers().stream()
        .map(ServerName::valueOf).collect(Collectors.toList());
    LOG.info(
      "Current list of replicators: " + currentReplicators + " other RSs: " + otherRegionServers);

    // Look if there's anything to process after a restart
    for (ServerName rs : currentReplicators) {
      if (!otherRegionServers.contains(rs)) {
        transferQueues(rs);
      }
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
      addSource(peerId);
      if (replicationForBulkLoadDataEnabled) {
        throwIOExceptionWhenFail(() -> this.queueStorage.addPeerToHFileRefs(peerId));
      }
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
    LOG.info(
      "Number of deleted recovered sources for " + peerId + ": " + oldSourcesToDelete.size());
    // Now close the normal source for this peer
    ReplicationSourceInterface srcToRemove = this.sources.get(peerId);
    if (srcToRemove != null) {
      srcToRemove.terminate(terminateMessage);
      removeSource(srcToRemove);
    } else {
      // This only happened in unit test TestReplicationSourceManager#testPeerRemovalCleanup
      // Delete queue from storage and memory and queue id is same with peer id for normal
      // source
      deleteQueue(peerId);
      this.walsById.remove(peerId);
    }
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    if (peerConfig.isSyncReplication()) {
      syncReplicationPeerMappingManager.remove(peerId, peerConfig);
    }
    // Remove HFile Refs
    abortWhenFail(() -> this.queueStorage.removePeerFromHFileRefs(peerId));
  }

  /**
   * Factory method to create a replication source
   * @param queueId the id of the replication queue
   * @return the created source
   */
  private ReplicationSourceInterface createSource(String queueId, ReplicationPeer replicationPeer)
      throws IOException {
    ReplicationSourceInterface src = ReplicationSourceFactory.create(conf, queueId);

    MetricsSource metrics = new MetricsSource(queueId);
    // init replication source
    src.init(conf, fs, this, queueStorage, replicationPeer, server, queueId, clusterId,
      walFileLengthProvider, metrics);
    return src;
  }

  /**
   * Add a normal source for the given peer on this region server. Meanwhile, add new replication
   * queue to storage. For the newly added peer, we only need to enqueue the latest log of each wal
   * group and do replication
   * @param peerId the id of the replication peer
   * @return the source that was created
   */
  @VisibleForTesting
  ReplicationSourceInterface addSource(String peerId) throws IOException {
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    ReplicationSourceInterface src = createSource(peerId, peer);
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      this.sources.put(peerId, src);
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      this.walsById.put(peerId, walsByGroup);
      // Add the latest wal to that source's queue
      if (this.latestPaths.size() > 0) {
        for (Path logPath : latestPaths) {
          String name = logPath.getName();
          String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(name);
          NavigableSet<String> logs = new TreeSet<>();
          logs.add(name);
          walsByGroup.put(walPrefix, logs);
          // Abort RS and throw exception to make add peer failed
          abortAndThrowIOExceptionWhenFail(
            () -> this.queueStorage.addWAL(server.getServerName(), peerId, name));
          src.enqueueLog(logPath);
        }
      }
    }
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    if (peerConfig.isSyncReplication()) {
      syncReplicationPeerMappingManager.add(peer.getId(), peerConfig);
    }
    src.startup();
    return src;
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
    String terminateMessage = "Sync replication peer " + peerId +
      " is transiting to STANDBY. Will close the previous replication source and open a new one";
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    assert peer.getPeerConfig().isSyncReplication();
    ReplicationSourceInterface src = createSource(peerId, peer);
    // synchronized here to avoid race with preLogRoll where we add new log to source and also
    // walsById.
    ReplicationSourceInterface toRemove;
    Map<String, NavigableSet<String>> wals = new HashMap<>();
    synchronized (latestPaths) {
      toRemove = sources.put(peerId, src);
      if (toRemove != null) {
        LOG.info("Terminate replication source for " + toRemove.getPeerId());
        toRemove.terminate(terminateMessage);
        toRemove.getSourceMetrics().clear();
      }
      // Here we make a copy of all the remaining wal files and then delete them from the
      // replication queue storage after releasing the lock. It is not safe to just remove the old
      // map from walsById since later we may fail to delete them from the replication queue
      // storage, and when we retry next time, we can not know the wal files that need to be deleted
      // from the replication queue storage.
      walsById.get(peerId).forEach((k, v) -> wals.put(k, new TreeSet<>(v)));
    }
    LOG.info("Startup replication source for " + src.getPeerId());
    src.startup();
    for (NavigableSet<String> walsByGroup : wals.values()) {
      for (String wal : walsByGroup) {
        queueStorage.removeWAL(server.getServerName(), peerId, wal);
      }
    }
    synchronized (walsById) {
      Map<String, NavigableSet<String>> oldWals = walsById.get(peerId);
      wals.forEach((k, v) -> {
        NavigableSet<String> walsByGroup = oldWals.get(k);
        if (walsByGroup != null) {
          walsByGroup.removeAll(v);
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
          String queueId = oldSource.getQueueId();
          oldSource.terminate(terminateMessage);
          oldSource.getSourceMetrics().clear();
          queueStorage.removeQueue(server.getServerName(), queueId);
          walsByIdRecoveredQueues.remove(queueId);
          iter.remove();
        }
      }
    }
  }

  /**
   * Close the previous replication sources of this peer id and open new sources to trigger the new
   * replication state changes or new replication config changes. Here we don't need to change
   * replication queue storage and only to enqueue all logs to the new replication source
   * @param peerId the id of the replication peer
   */
  public void refreshSources(String peerId) throws IOException {
    String terminateMessage = "Peer " + peerId +
      " state or config changed. Will close the previous replication source and open a new one";
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    ReplicationSourceInterface src = createSource(peerId, peer);
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      ReplicationSourceInterface toRemove = this.sources.put(peerId, src);
      if (toRemove != null) {
        LOG.info("Terminate replication source for " + toRemove.getPeerId());
        toRemove.terminate(terminateMessage);
      }
      for (NavigableSet<String> walsByGroup : walsById.get(peerId).values()) {
        walsByGroup.forEach(wal -> src.enqueueLog(new Path(this.logDir, wal)));
      }
    }
    LOG.info("Startup replication source for " + src.getPeerId());
    src.startup();

    List<ReplicationSourceInterface> toStartup = new ArrayList<>();
    // synchronized on oldsources to avoid race with NodeFailoverWorker
    synchronized (this.oldsources) {
      List<String> previousQueueIds = new ArrayList<>();
      for (ReplicationSourceInterface oldSource : this.oldsources) {
        if (oldSource.getPeerId().equals(peerId)) {
          previousQueueIds.add(oldSource.getQueueId());
          oldSource.terminate(terminateMessage);
          this.oldsources.remove(oldSource);
        }
      }
      for (String queueId : previousQueueIds) {
        ReplicationSourceInterface replicationSource = createSource(queueId, peer);
        this.oldsources.add(replicationSource);
        for (SortedSet<String> walsByGroup : walsByIdRecoveredQueues.get(queueId).values()) {
          walsByGroup.forEach(wal -> src.enqueueLog(new Path(wal)));
        }
        toStartup.add(replicationSource);
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
  private void deleteQueue(String queueId) {
    abortWhenFail(() -> this.queueStorage.removeQueue(server.getServerName(), queueId));
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
      if (e.getCause() != null && e.getCause() instanceof KeeperException.SystemErrorException
          && e.getCause().getCause() != null && e.getCause()
          .getCause() instanceof InterruptedException) {
        throw new RuntimeException(
            "Thread is interrupted, the replication source may be terminated");
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
   * @param source the replication source
   * @param entryBatch the wal entry batch we just shipped
   */
  public void logPositionAndCleanOldLogs(ReplicationSourceInterface source,
      WALEntryBatch entryBatch) {
    String fileName = entryBatch.getLastWalPath().getName();
    interruptOrAbortWhenFail(() -> this.queueStorage.setWALPosition(server.getServerName(),
      source.getQueueId(), fileName, entryBatch.getLastWalPosition(), entryBatch.getLastSeqIds()));
    cleanOldLogs(fileName, entryBatch.isEndOfFile(), source);
  }

  /**
   * Cleans a log file and all older logs from replication queue. Called when we are sure that a log
   * file is closed and has no more entries.
   * @param log Path to the log
   * @param inclusive whether we should also remove the given log file
   * @param source the replication source
   */
  @VisibleForTesting
  void cleanOldLogs(String log, boolean inclusive, ReplicationSourceInterface source) {
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(log);
    if (source.isRecovered()) {
      NavigableSet<String> wals = walsByIdRecoveredQueues.get(source.getQueueId()).get(logPrefix);
      if (wals != null) {
        NavigableSet<String> walsToRemove = wals.headSet(log, inclusive);
        if (walsToRemove.isEmpty()) {
          return;
        }
        cleanOldLogs(walsToRemove, source);
        walsToRemove.clear();
      }
    } else {
      NavigableSet<String> wals;
      NavigableSet<String> walsToRemove;
      // synchronized on walsById to avoid race with preLogRoll
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
    Path remoteWALDirForPeer = ReplicationUtils.getRemoteWALDirForPeer(remoteWALDir, peerId);
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
      List<String> remoteWals = wals.stream().filter(w -> SyncReplicationWALProvider
        .getSyncReplicationPeerIdFromWALName(w).map(peerId::equals).orElse(false))
        .collect(Collectors.toList());
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
          if (ReplicationUtils.sleepForRetries("Failed to delete remote wals", sleepForRetries,
            sleepMultiplier, maxRetriesMultiplier)) {
            sleepMultiplier++;
          }
        }
      }
    }
    String queueId = source.getQueueId();
    for (String wal : wals) {
      interruptOrAbortWhenFail(
        () -> this.queueStorage.removeWAL(server.getServerName(), queueId, wal));
    }
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
  @VisibleForTesting
  public void preLogRoll(Path newLog) throws IOException {
    String logName = newLog.getName();
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(logName);
    // synchronized on latestPaths to avoid the new open source miss the new log
    synchronized (this.latestPaths) {
      // Add log to queue storage
      for (ReplicationSourceInterface source : this.sources.values()) {
        // If record log to queue storage failed, abort RS and throw exception to make log roll
        // failed
        abortAndThrowIOExceptionWhenFail(
          () -> this.queueStorage.addWAL(server.getServerName(), source.getQueueId(), logName));
      }

      // synchronized on walsById to avoid race with cleanOldLogs
      synchronized (this.walsById) {
        // Update walsById map
        for (Map.Entry<String, Map<String, NavigableSet<String>>> entry : this.walsById
          .entrySet()) {
          String peerId = entry.getKey();
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
      Iterator<Path> iterator = latestPaths.iterator();
      while (iterator.hasNext()) {
        Path path = iterator.next();
        if (path.getName().contains(logPrefix)) {
          iterator.remove();
          break;
        }
      }
      this.latestPaths.add(newLog);
    }
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
  @VisibleForTesting
  public void postLogRoll(Path newLog) throws IOException {
    // This only updates the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.enqueueLog(newLog);
    }
  }

  @Override
  public void regionServerRemoved(String regionserver) {
    transferQueues(ServerName.valueOf(regionserver));
  }

  /**
   * Transfer all the queues of the specified to this region server. First it tries to grab a lock
   * and if it works it will move the old queues and finally will delete the old queues.
   * <p>
   * It creates one old source for any type of source of the old rs.
   */
  private void transferQueues(ServerName deadRS) {
    if (server.getServerName().equals(deadRS)) {
      // it's just us, give up
      return;
    }
    NodeFailoverWorker transfer = new NodeFailoverWorker(deadRS);
    try {
      this.executor.execute(transfer);
    } catch (RejectedExecutionException ex) {
      LOG.info("Cancelling the transfer of " + deadRS + " because of " + ex.getMessage());
    }
  }

  /**
   * Class responsible to setup new ReplicationSources to take care of the queues from dead region
   * servers.
   */
  class NodeFailoverWorker extends Thread {

    private final ServerName deadRS;
    // After claim the queues from dead region server, the NodeFailoverWorker will skip to start
    // the RecoveredReplicationSource if the peer has been removed. but there's possible that
    // remove a peer with peerId = 2 and add a peer with peerId = 2 again during the
    // NodeFailoverWorker. So we need a deep copied <peerId, peer> map to decide whether we
    // should start the RecoveredReplicationSource. If the latest peer is not the old peer when
    // NodeFailoverWorker begin, we should skip to start the RecoveredReplicationSource, Otherwise
    // the rs will abort (See HBASE-20475).
    private final Map<String, ReplicationPeerImpl> peersSnapshot;

    @VisibleForTesting
    public NodeFailoverWorker(ServerName deadRS) {
      super("Failover-for-" + deadRS);
      this.deadRS = deadRS;
      peersSnapshot = new HashMap<>(replicationPeers.getPeerCache());
    }

    private boolean isOldPeer(String peerId, ReplicationPeerImpl newPeerRef) {
      ReplicationPeerImpl oldPeerRef = peersSnapshot.get(peerId);
      return oldPeerRef != null && oldPeerRef == newPeerRef;
    }

    @Override
    public void run() {
      // Wait a bit before transferring the queues, we may be shutting down.
      // This sleep may not be enough in some cases.
      try {
        Thread.sleep(sleepBeforeFailover +
          (long) (ThreadLocalRandom.current().nextFloat() * sleepBeforeFailover));
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting before transferring a queue.");
        Thread.currentThread().interrupt();
      }
      // We try to lock that rs' queue directory
      if (server.isStopped()) {
        LOG.info("Not transferring queue since we are shutting down");
        return;
      }
      Map<String, Set<String>> newQueues = new HashMap<>();
      try {
        List<String> queues = queueStorage.getAllQueues(deadRS);
        while (!queues.isEmpty()) {
          Pair<String, SortedSet<String>> peer = queueStorage.claimQueue(deadRS,
            queues.get(ThreadLocalRandom.current().nextInt(queues.size())), server.getServerName());
          long sleep = sleepBeforeFailover / 2;
          if (!peer.getSecond().isEmpty()) {
            newQueues.put(peer.getFirst(), peer.getSecond());
            sleep = sleepBeforeFailover;
          }
          try {
            Thread.sleep(sleep);
          } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting before transferring a queue.");
            Thread.currentThread().interrupt();
          }
          queues = queueStorage.getAllQueues(deadRS);
        }
        if (queues.isEmpty()) {
          queueStorage.removeReplicatorIfQueueIsEmpty(deadRS);
        }
      } catch (ReplicationException e) {
        server.abort("Failed to claim queue from dead regionserver", e);
        return;
      }
      // Copying over the failed queue is completed.
      if (newQueues.isEmpty()) {
        // We either didn't get the lock or the failed region server didn't have any outstanding
        // WALs to replicate, so we are done.
        return;
      }

      for (Map.Entry<String, Set<String>> entry : newQueues.entrySet()) {
        String queueId = entry.getKey();
        Set<String> walsSet = entry.getValue();
        try {
          // there is not an actual peer defined corresponding to peerId for the failover.
          ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(queueId);
          String actualPeerId = replicationQueueInfo.getPeerId();

          ReplicationPeerImpl peer = replicationPeers.getPeer(actualPeerId);
          if (peer == null || !isOldPeer(actualPeerId, peer)) {
            LOG.warn("Skipping failover for peer {} of node {}, peer is null", actualPeerId,
              deadRS);
            abortWhenFail(() -> queueStorage.removeQueue(server.getServerName(), queueId));
            continue;
          }
          if (server instanceof ReplicationSyncUp.DummyServer
              && peer.getPeerState().equals(PeerState.DISABLED)) {
            LOG.warn("Peer {} is disabled. ReplicationSyncUp tool will skip "
                + "replicating data to this peer.",
              actualPeerId);
            continue;
          }

          ReplicationSourceInterface src = createSource(queueId, peer);
          // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
          synchronized (oldsources) {
            peer = replicationPeers.getPeer(src.getPeerId());
            if (peer == null || !isOldPeer(src.getPeerId(), peer)) {
              src.terminate("Recovered queue doesn't belong to any current peer");
              deleteQueue(queueId);
              continue;
            }
            // Do not setup recovered queue if a sync replication peer is in STANDBY state, or is
            // transiting to STANDBY state. The only exception is we are in STANDBY state and
            // transiting to DA, under this state we will replay the remote WAL and they need to be
            // replicated back.
            if (peer.getPeerConfig().isSyncReplication()) {
              Pair<SyncReplicationState, SyncReplicationState> stateAndNewState =
                peer.getSyncReplicationStateAndNewState();
              if ((stateAndNewState.getFirst().equals(SyncReplicationState.STANDBY) &&
                stateAndNewState.getSecond().equals(SyncReplicationState.NONE)) ||
                stateAndNewState.getSecond().equals(SyncReplicationState.STANDBY)) {
                src.terminate("Sync replication peer is in STANDBY state");
                deleteQueue(queueId);
                continue;
              }
            }
            // track sources in walsByIdRecoveredQueues
            Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
            walsByIdRecoveredQueues.put(queueId, walsByGroup);
            for (String wal : walsSet) {
              String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
              NavigableSet<String> wals = walsByGroup.get(walPrefix);
              if (wals == null) {
                wals = new TreeSet<>();
                walsByGroup.put(walPrefix, wals);
              }
              wals.add(wal);
            }
            oldsources.add(src);
            for (String wal : walsSet) {
              src.enqueueLog(new Path(oldLogDir, wal));
            }
            src.startup();
          }
        } catch (IOException e) {
          // TODO manage it
          LOG.error("Failed creating a source", e);
        }
      }
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
  }

  /**
   * Get a copy of the wals of the normal sources on this rs
   * @return a sorted set of wal names
   */
  @VisibleForTesting
  public Map<String, Map<String, NavigableSet<String>>> getWALs() {
    return Collections.unmodifiableMap(walsById);
  }

  /**
   * Get a copy of the wals of the recovered sources on this rs
   * @return a sorted set of wal names
   */
  @VisibleForTesting
  Map<String, Map<String, NavigableSet<String>>> getWalsByIdRecoveredQueues() {
    return Collections.unmodifiableMap(walsByIdRecoveredQueues);
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
  @VisibleForTesting
  public ReplicationSourceInterface getSource(String peerId) {
    return this.sources.get(peerId);
  }

  @VisibleForTesting
  List<String> getAllQueues() throws IOException {
    List<String> allQueues = Collections.emptyList();
    try {
      allQueues = queueStorage.getAllQueues(server.getServerName());
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
    return allQueues;
  }

  @VisibleForTesting
  int getSizeOfLatestPath() {
    synchronized (latestPaths) {
      return latestPaths.size();
    }
  }

  @VisibleForTesting
  public AtomicLong getTotalBufferUsed() {
    return totalBufferUsed;
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
}
