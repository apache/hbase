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
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
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
import org.apache.hadoop.hbase.replication.ReplicationSourceController;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.replication.SyncReplicationState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
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
 * <li>Need synchronized on {@link #latestPaths} to avoid the new open source miss new log.</li>
 * <li>Need synchronized on {@link #oldsources} to avoid adding recovered source for the
 * to-be-removed peer.</li>
 * </ul>
 */
@InterfaceAudience.Private
public class ReplicationSourceManager implements ReplicationListener, ReplicationSourceController {
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

  private final SyncReplicationPeerMappingManager syncReplicationPeerMappingManager;

  private final Configuration conf;
  private final FileSystem fs;
  // The paths to the latest log of each wal group, for new coming peers
  private final Map<String, Path> latestPaths;
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

  // Total buffer size on this RegionServer for holding batched edits to be shipped.
  private final long totalBufferLimit;
  private final MetricsReplicationGlobalSourceSource globalMetrics;

  private final Map<String, MetricsSource> sourceMetrics = new HashMap<>();

  /**
   * When enable replication offload, will not create replication source and only write WAL to
   * replication queue storage. The replication source will be started by ReplicationServer.
   */
  private final boolean replicationOffload;

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
      SyncReplicationPeerMappingManager syncReplicationPeerMappingManager,
      MetricsReplicationGlobalSourceSource globalMetrics) throws IOException {
    this.sources = new ConcurrentHashMap<>();
    this.queueStorage = queueStorage;
    this.replicationPeers = replicationPeers;
    this.replicationTracker = replicationTracker;
    this.server = server;
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
    this.executor = new ThreadPoolExecutor(nbWorkers, nbWorkers, 100,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat("ReplicationExecutor-%d");
    tfb.setDaemon(true);
    this.executor.setThreadFactory(tfb.build());
    this.latestPaths = new HashMap<>();
    this.replicationForBulkLoadDataEnabled = conf.getBoolean(
      HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    this.totalBufferLimit = conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
        HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    this.globalMetrics = globalMetrics;
    this.replicationOffload = conf.getBoolean(HConstants.REPLICATION_OFFLOAD_ENABLE_KEY,
      HConstants.REPLICATION_OFFLOAD_ENABLE_DEFAULT);
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

  @VisibleForTesting
  @Override
  public AtomicLong getTotalBufferUsed() {
    return totalBufferUsed;
  }

  @Override
  public long getTotalBufferLimit() {
    return totalBufferLimit;
  }

  @Override
  public void finishRecoveredSource(RecoveredReplicationSource src) {
    synchronized (oldsources) {
      if (!removeRecoveredSource(src)) {
        return;
      }
    }
    LOG.info("Finished recovering queue {} with the following stats: {}", src.getQueueId(),
      src.getStats());
  }

  @Override
  public MetricsReplicationGlobalSourceSource getGlobalMetrics() {
    return this.globalMetrics;
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
    return true;
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
    sourceMetrics.put(queueId, metrics);
    // init replication source
    src.init(conf, fs, logDir, this, queueStorage, replicationPeer, server, queueId, clusterId,
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
  void addSource(String peerId) throws IOException {
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    ReplicationSourceInterface src = createSource(peerId, peer);
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      this.sources.put(peerId, src);
      // Add the latest wal to that source's queue
      if (!latestPaths.isEmpty()) {
        for (Map.Entry<String, Path> walPrefixAndPath : latestPaths.entrySet()) {
          Path walPath = walPrefixAndPath.getValue();
          // Abort RS and throw exception to make add peer failed
          abortAndThrowIOExceptionWhenFail(
            () -> this.queueStorage.addWAL(server.getServerName(), peerId, walPath.getName()));
          src.enqueueLog(walPath);
          LOG.trace("Enqueued {} to source {} during source creation.", walPath, src.getQueueId());
        }
      }
    }
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    if (peerConfig.isSyncReplication()) {
      syncReplicationPeerMappingManager.add(peer.getId(), peerConfig);
    }
    if (!replicationOffload) {
      src.startup();
    }
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
  void drainSources(String peerId) throws IOException, ReplicationException {
    if (replicationOffload) {
      throw new ReplicationException(
        "Should not add use sync replication when replication offload enabled");
    }
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
      this.queueStorage.getWALsInQueue(this.server.getServerName(), peerId).forEach(wal -> {
        String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
        wals.computeIfAbsent(walPrefix, p -> new TreeSet<>()).add(wal);
      });
    }
    LOG.info("Startup replication source for " + src.getPeerId());
    src.startup();
    for (NavigableSet<String> walsByGroup : wals.values()) {
      for (String wal : walsByGroup) {
        queueStorage.removeWAL(server.getServerName(), peerId, wal);
      }
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
  void refreshSources(String peerId) throws ReplicationException, IOException {
    String terminateMessage = "Peer " + peerId +
      " state or config changed. Will close the previous replication source and open a new one";
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    ReplicationSourceInterface src = createSource(peerId, peer);
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      ReplicationSourceInterface toRemove = this.sources.put(peerId, src);
      if (toRemove != null) {
        LOG.info("Terminate replication source for " + toRemove.getPeerId());
        // Do not clear metrics
        toRemove.terminate(terminateMessage, null, false);
      }
      this.queueStorage.getWALsInQueue(this.server.getServerName(), peerId)
        .forEach(wal -> src.enqueueLog(new Path(this.logDir, wal)));
    }
    LOG.info("Startup replication source for " + src.getPeerId());
    if (!replicationOffload) {
      src.startup();
    }

    List<ReplicationSourceInterface> toStartup = new ArrayList<>();
    // synchronized on oldsources to avoid race with NodeFailoverWorker
    synchronized (this.oldsources) {
      List<String> previousQueueIds = new ArrayList<>();
      for (Iterator<ReplicationSourceInterface> iter = this.oldsources.iterator(); iter
          .hasNext();) {
        ReplicationSourceInterface oldSource = iter.next();
        if (oldSource.getPeerId().equals(peerId)) {
          previousQueueIds.add(oldSource.getQueueId());
          oldSource.terminate(terminateMessage);
          iter.remove();
        }
      }
      for (String queueId : previousQueueIds) {
        ReplicationSourceInterface recoveredReplicationSource = createSource(queueId, peer);
        this.oldsources.add(recoveredReplicationSource);
        this.queueStorage.getWALsInQueue(this.server.getServerName(), queueId)
          .forEach(wal -> recoveredReplicationSource.enqueueLog(new Path(wal)));
        toStartup.add(recoveredReplicationSource);
      }
    }
    if (!replicationOffload) {
      for (ReplicationSourceInterface replicationSource : toStartup) {
        replicationSource.startup();
      }
    }
  }

  /**
   * Clear the metrics and related replication queue of the specified old source
   * @param src source to clear
   */
  private void removeSource(ReplicationSourceInterface src) {
    this.sources.remove(src.getPeerId());
    // Delete queue from storage and memory
    deleteQueue(src.getQueueId());
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

  @InterfaceAudience.Private
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
      // Add to latestPaths
      latestPaths.put(logPrefix, newLog);
    }
  }

  @InterfaceAudience.Private
  public void postLogRoll(Path newLog) {
    // This only updates the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.enqueueLog(newLog);
      LOG.trace("Enqueued {} to source {} while performing postLogRoll operation.",
          newLog, source.getQueueId());
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
      CompatibilitySingletonFactory.getInstance(MetricsReplicationSourceFactory.class)
          .getGlobalSource().incrFailedRecoveryQueue();
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
        LOG.error(String.format("ReplicationException: cannot claim dead region (%s)'s " +
            "replication queue. Znode : (%s)" +
            " Possible solution: check if znode size exceeds jute.maxBuffer value. " +
            " If so, increase it for both client and server side." + e),  deadRS,
            queueStorage.getRsNode(deadRS));
        server.abort("Failed to claim queue from dead regionserver.", e);
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
            oldsources.add(src);
            LOG.trace("Added source for recovered queue: " + src.getQueueId());
            for (String wal : walsSet) {
              LOG.trace("Enqueueing log from recovered queue for source: " + src.getQueueId());
              src.enqueueLog(new Path(oldLogDir, wal));
            }
            if (!replicationOffload) {
              src.startup();
            }
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
  public Map<String, Map<String, NavigableSet<String>>> getWALs()
    throws ReplicationException {
    Map<String, Map<String, NavigableSet<String>>> walsById = new HashMap<>();
    for (ReplicationSourceInterface source : sources.values()) {
      String queueId = source.getQueueId();
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      walsById.put(queueId, walsByGroup);
      for (String wal : this.queueStorage.getWALsInQueue(this.server.getServerName(), queueId)) {
        String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
        walsByGroup.computeIfAbsent(walPrefix, p -> new TreeSet<>()).add(wal);
      }
    }
    return Collections.unmodifiableMap(walsById);
  }

  /**
   * Get a copy of the wals of the recovered sources on this rs
   * @return a sorted set of wal names
   */
  @VisibleForTesting
  Map<String, Map<String, NavigableSet<String>>> getWalsByIdRecoveredQueues()
    throws ReplicationException {
    Map<String, Map<String, NavigableSet<String>>> walsByIdRecoveredQueues = new HashMap<>();
    for (ReplicationSourceInterface source : oldsources) {
      String queueId = source.getQueueId();
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      walsByIdRecoveredQueues.put(queueId, walsByGroup);
      for (String wal : this.queueStorage.getWALsInQueue(this.server.getServerName(), queueId)) {
        String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
        walsByGroup.computeIfAbsent(walPrefix, p -> new TreeSet<>()).add(wal);
      }
    }
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
  Set<Path> getLastestPath() {
    synchronized (latestPaths) {
      return Sets.newHashSet(latestPaths.values());
    }
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
    stats.append("WAL Edits Buffer Used=").append(getTotalBufferUsed().get()).append("B, Limit=")
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
      throwIOExceptionWhenFail(() -> addHFileRefs(source.getPeerId(), tableName, family, pairs));
    }
  }

  /**
   * Add hfile names to the queue to be replicated.
   * @param peerId the replication peer id
   * @param tableName Name of the table these files belongs to
   * @param family Name of the family these files belong to
   * @param pairs list of pairs of { HFile location in staging dir, HFile path in region dir which
   *          will be added in the queue for replication}
   * @throws ReplicationException If failed to add hfile references
   */
  private void addHFileRefs(String peerId, TableName tableName, byte[] family,
    List<Pair<Path, Path>> pairs) throws ReplicationException {
    // Only the normal replication source update here, its peerId is equals to queueId.
    MetricsSource metrics = sourceMetrics.get(peerId);
    ReplicationPeer replicationPeer = replicationPeers.getPeer(peerId);
    Set<String> namespaces = replicationPeer.getNamespaces();
    Map<TableName, List<String>> tableCFMap = replicationPeer.getTableCFs();
    if (tableCFMap != null) { // All peers with TableCFs
      List<String> tableCfs = tableCFMap.get(tableName);
      if (tableCFMap.containsKey(tableName)
        && (tableCfs == null || tableCfs.contains(Bytes.toString(family)))) {
        this.queueStorage.addHFileRefs(peerId, pairs);
        metrics.incrSizeOfHFileRefsQueue(pairs.size());
      } else {
        LOG.debug("HFiles will not be replicated belonging to the table {} family {} to peer id {}",
          tableName, Bytes.toString(family), peerId);
      }
    } else if (namespaces != null) { // Only for set NAMESPACES peers
      if (namespaces.contains(tableName.getNamespaceAsString())) {
        this.queueStorage.addHFileRefs(peerId, pairs);
        metrics.incrSizeOfHFileRefsQueue(pairs.size());
      } else {
        LOG.debug("HFiles will not be replicated belonging to the table {} family {} to peer id {}",
          tableName, Bytes.toString(family), peerId);
      }
    } else {
      // user has explicitly not defined any table cfs for replication, means replicate all the
      // data
      this.queueStorage.addHFileRefs(peerId, pairs);
      metrics.incrSizeOfHFileRefsQueue(pairs.size());
    }
  }

  int activeFailoverTaskCount() {
    return executor.getActiveCount();
  }

  @InterfaceAudience.Private
  Server getServer() {
    return this.server;
  }

  @InterfaceAudience.Private
  ReplicationQueueStorage getQueueStorage() {
    return this.queueStorage;
  }
}
