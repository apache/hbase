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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.OptionalLong;
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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeer.PeerState;
import org.apache.hadoop.hbase.replication.ReplicationPeerImpl;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * {@link #cleanOldLogs(NavigableSet, String, boolean, String)} and {@link #preLogRoll(Path)}.
 * {@link #walsById} is a ConcurrentHashMap and there is a Lock for peer id in
 * {@link PeerProcedureHandlerImpl}. So there is no race between {@link #addPeer(String)} and
 * {@link #removePeer(String)}. {@link #cleanOldLogs(NavigableSet, String, boolean, String)} is
 * called by {@link ReplicationSourceInterface}. So no race with {@link #addPeer(String)}.
 * {@link #removePeer(String)} will terminate the {@link ReplicationSourceInterface} firstly, then
 * remove the wals from {@link #walsById}. So no race with {@link #removePeer(String)}. The only
 * case need synchronized is {@link #cleanOldLogs(NavigableSet, String, boolean, String)} and
 * {@link #preLogRoll(Path)}.</li>
 * <li>No need synchronized on {@link #walsByIdRecoveredQueues}. There are three methods which
 * modify it, {@link #removePeer(String)},
 * {@link #cleanOldLogs(NavigableSet, String, boolean, String)} and
 * {@link ReplicationSourceManager#claimQueue(ServerName, String)}.
 * {@link #cleanOldLogs(NavigableSet, String, boolean, String)} is called by
 * {@link ReplicationSourceInterface}. {@link #removePeer(String)} will terminate the
 * {@link ReplicationSourceInterface} firstly, then remove the wals from
 * {@link #walsByIdRecoveredQueues}. And
 * {@link ReplicationSourceManager#claimQueue(ServerName, String)} will add the wals to
 * {@link #walsByIdRecoveredQueues} firstly, then start up a {@link ReplicationSourceInterface}. So
 * there is no race here. For {@link ReplicationSourceManager#claimQueue(ServerName, String)} and
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
  // For normal replication source, the peer id is same with the queue id
  private final ConcurrentMap<String, Map<String, NavigableSet<String>>> walsById;
  // Logs for recovered sources we are currently tracking
  // the map is: queue_id->logPrefix/logGroup->logs
  // For recovered source, the queue id's format is peer_id-servername-*
  private final ConcurrentMap<String, Map<String, NavigableSet<String>>> walsByIdRecoveredQueues;

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

  private final boolean replicationForBulkLoadDataEnabled;

  private AtomicLong totalBufferUsed = new AtomicLong();
  // Total buffer size on this RegionServer for holding batched edits to be shipped.
  private final long totalBufferLimit;
  private final MetricsReplicationGlobalSourceSource globalMetrics;

  /**
   * A special ReplicationSource for hbase:meta Region Read Replicas. Usually this reference remains
   * empty. If an hbase:meta Region is opened on this server, we will create an instance of a
   * hbase:meta CatalogReplicationSource and it will live the life of the Server thereafter; i.e. we
   * will not shut it down even if the hbase:meta moves away from this server (in case it later gets
   * moved back). We synchronize on this instance testing for presence and if absent, while creating
   * so only created and started once.
   */
  AtomicReference<ReplicationSourceInterface> catalogReplicationSource = new AtomicReference<>();

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
    MetricsReplicationGlobalSourceSource globalMetrics) throws IOException {
    // CopyOnWriteArrayList is thread-safe.
    // Generally, reading is more than modifying.
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
    this.sleepBeforeFailover = conf.getLong("replication.sleep.before.failover", 30000); // 30
                                                                                         // seconds
    this.clusterId = clusterId;
    this.walFactory = walFactory;
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
    replicationForBulkLoadDataEnabled = conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
      HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    this.totalBufferLimit = conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
      HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    this.globalMetrics = globalMetrics;
  }

  /**
   * Adds a normal source per registered peer cluster.
   */
  void init() throws IOException {
    for (String id : this.replicationPeers.getAllPeerIds()) {
      addSource(id);
      if (replicationForBulkLoadDataEnabled) {
        // Check if peer exists in hfile-refs queue, if not add it. This can happen in the case
        // when a peer was added before replication for bulk loaded data was enabled.
        throwIOExceptionWhenFail(() -> this.queueStorage.addPeerToHFileRefs(id));
      }
    }
  }

  /**
   * 1. Add peer to replicationPeers 2. Add the normal source and related replication queue 3. Add
   * HFile Refs
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
   * 1. Remove peer for replicationPeers 2. Remove all the recovered sources for the specified id
   * and related replication queues 3. Remove the normal source and related replication queue 4.
   * Remove HFile Refs
   * @param peerId the id of the replication peer
   */
  public void removePeer(String peerId) {
    replicationPeers.removePeer(peerId);
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
    LOG
      .info("Number of deleted recovered sources for " + peerId + ": " + oldSourcesToDelete.size());
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

    // Remove HFile Refs
    abortWhenFail(() -> this.queueStorage.removePeerFromHFileRefs(peerId));
  }

  /**
   * @return a new 'classic' user-space replication source.
   * @param queueId the id of the replication queue to associate the ReplicationSource with.
   * @see #createCatalogReplicationSource(RegionInfo) for creating a ReplicationSource for meta.
   */
  private ReplicationSourceInterface createSource(String queueId, ReplicationPeer replicationPeer)
    throws IOException {
    ReplicationSourceInterface src = ReplicationSourceFactory.create(conf, queueId);
    // Init the just created replication source. Pass the default walProvider's wal file length
    // provider. Presumption is we replicate user-space Tables only. For hbase:meta region replica
    // replication, see #createCatalogReplicationSource().
    WALFileLengthProvider walFileLengthProvider = this.walFactory.getWALProvider() != null
      ? this.walFactory.getWALProvider().getWALFileLengthProvider()
      : p -> OptionalLong.empty();

    // Create merged configuration with peer overrides as higher priority and
    // global config as lower priority
    Configuration mergedConf = conf;
    if (!replicationPeer.getPeerConfig().getConfiguration().isEmpty()) {
      CompoundConfiguration compound = new CompoundConfiguration();
      compound.add(conf);
      compound.addStringMap(replicationPeer.getPeerConfig().getConfiguration());
      mergedConf = compound;
    }

    src.init(mergedConf, fs, this, queueStorage, replicationPeer, server, queueId, clusterId,
      walFileLengthProvider, new MetricsSource(queueId));
    return src;
  }

  /**
   * Add a normal source for the given peer on this region server. Meanwhile, add new replication
   * queue to storage. For the newly added peer, we only need to enqueue the latest log of each wal
   * group and do replication
   * @param peerId the id of the replication peer
   * @return the source that was created
   */
  ReplicationSourceInterface addSource(String peerId) throws IOException {
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    ReplicationSourceInterface src = createSource(peerId, peer);
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      this.sources.put(peerId, src);
      Map<String, NavigableSet<String>> walsByGroup = new HashMap<>();
      this.walsById.put(peerId, walsByGroup);
      // Add the latest wal to that source's queue
      if (!latestPaths.isEmpty()) {
        for (Map.Entry<String, Path> walPrefixAndPath : latestPaths.entrySet()) {
          Path walPath = walPrefixAndPath.getValue();
          NavigableSet<String> wals = new TreeSet<>();
          wals.add(walPath.getName());
          walsByGroup.put(walPrefixAndPath.getKey(), wals);
          // Abort RS and throw exception to make add peer failed
          abortAndThrowIOExceptionWhenFail(
            () -> this.queueStorage.addWAL(server.getServerName(), peerId, walPath.getName()));
          src.enqueueLog(walPath);
          LOG.trace("Enqueued {} to source {} during source creation.", walPath, src.getQueueId());
        }
      }
    }
    src.startup();
    return src;
  }

  /**
   * Close the previous replication sources of this peer id and open new sources to trigger the new
   * replication state changes or new replication config changes. Here we don't need to change
   * replication queue storage and only to enqueue all logs to the new replication source
   * @param peerId the id of the replication peer
   */
  public void refreshSources(String peerId) throws IOException {
    String terminateMessage = "Peer " + peerId
      + " state or config changed. Will close the previous replication source and open a new one";
    ReplicationPeer peer = replicationPeers.getPeer(peerId);
    ReplicationSourceInterface src;
    // synchronized on latestPaths to avoid missing the new log
    synchronized (this.latestPaths) {
      ReplicationSourceInterface toRemove = this.sources.remove(peerId);
      if (toRemove != null) {
        LOG.info("Terminate replication source for " + toRemove.getPeerId());
        // Do not clear metrics
        toRemove.terminate(terminateMessage, null, false);
      }
      src = createSource(peerId, peer);
      this.sources.put(peerId, src);
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
        for (SortedSet<String> walsByGroup : walsByIdRecoveredQueues.get(queueId).values()) {
          walsByGroup.forEach(wal -> recoveredReplicationSource.enqueueLog(new Path(wal)));
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
  void removeRecoveredSource(ReplicationSourceInterface src) {
    LOG.info("Done with the recovered queue " + src.getQueueId());
    this.oldsources.remove(src);
    // Delete queue from storage and memory
    deleteQueue(src.getQueueId());
    this.walsByIdRecoveredQueues.remove(src.getQueueId());
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
   * @param entryBatch the wal entry batch we just shipped
   */
  public void logPositionAndCleanOldLogs(ReplicationSourceInterface source,
    WALEntryBatch entryBatch) {
    String fileName = entryBatch.getLastWalPath().getName();
    String queueId = source.getQueueId();
    interruptOrAbortWhenFail(() -> this.queueStorage.setWALPosition(server.getServerName(), queueId,
      fileName, entryBatch.getLastWalPosition(), entryBatch.getLastSeqIds()));
    cleanOldLogs(fileName, entryBatch.isEndOfFile(), queueId, source.isRecovered());
  }

  /**
   * Cleans a log file and all older logs from replication queue. Called when we are sure that a log
   * file is closed and has no more entries.
   * @param log            Path to the log
   * @param inclusive      whether we should also remove the given log file
   * @param queueId        id of the replication queue
   * @param queueRecovered Whether this is a recovered queue
   */
  void cleanOldLogs(String log, boolean inclusive, String queueId, boolean queueRecovered) {
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(log);
    if (queueRecovered) {
      NavigableSet<String> wals = walsByIdRecoveredQueues.get(queueId).get(logPrefix);
      if (wals != null) {
        cleanOldLogs(wals, log, inclusive, queueId);
      }
    } else {
      // synchronized on walsById to avoid race with preLogRoll
      synchronized (this.walsById) {
        NavigableSet<String> wals = walsById.get(queueId).get(logPrefix);
        if (wals != null) {
          cleanOldLogs(wals, log, inclusive, queueId);
        }
      }
    }
  }

  private void cleanOldLogs(NavigableSet<String> wals, String key, boolean inclusive, String id) {
    NavigableSet<String> walSet = wals.headSet(key, inclusive);
    if (walSet.isEmpty()) {
      return;
    }
    LOG.debug("Removing {} logs in the list: {}", walSet.size(), walSet);
    for (String wal : walSet) {
      interruptOrAbortWhenFail(() -> this.queueStorage.removeWAL(server.getServerName(), id, wal));
    }
    walSet.clear();
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
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
      latestPaths.put(logPrefix, newLog);
    }
  }

  // public because of we call it in TestReplicationEmptyWALRecovery
  public void postLogRoll(Path newLog) throws IOException {
    // This only updates the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources.values()) {
      source.enqueueLog(newLog);
      LOG.trace("Enqueued {} to source {} while performing postLogRoll operation.", newLog,
        source.getQueueId());
    }
  }

  void claimQueue(ServerName deadRS, String queue) {
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
    // After claim the queues from dead region server, wewill skip to start the
    // RecoveredReplicationSource if the peer has been removed. but there's possible that remove a
    // peer with peerId = 2 and add a peer with peerId = 2 again during failover. So we need to get
    // a copy of the replication peer first to decide whether we should start the
    // RecoveredReplicationSource. If the latest peer is not the old peer, we should also skip to
    // start the RecoveredReplicationSource, Otherwise the rs will abort (See HBASE-20475).
    String peerId = new ReplicationQueueInfo(queue).getPeerId();
    ReplicationPeerImpl oldPeer = replicationPeers.getPeer(peerId);
    if (oldPeer == null) {
      LOG.info("Not transferring queue since the replication peer {} for queue {} does not exist",
        peerId, queue);
      return;
    }
    Pair<String, SortedSet<String>> claimedQueue;
    try {
      claimedQueue = queueStorage.claimQueue(deadRS, queue, server.getServerName());
    } catch (ReplicationException e) {
      LOG.error(
        "ReplicationException: cannot claim dead region ({})'s " + "replication queue. Znode : ({})"
          + " Possible solution: check if znode size exceeds jute.maxBuffer value. "
          + " If so, increase it for both client and server side.",
        deadRS, queueStorage.getRsNode(deadRS), e);
      server.abort("Failed to claim queue from dead regionserver.", e);
      return;
    }
    if (claimedQueue.getSecond().isEmpty()) {
      return;
    }
    String queueId = claimedQueue.getFirst();
    Set<String> walsSet = claimedQueue.getSecond();
    ReplicationPeerImpl peer = replicationPeers.getPeer(peerId);
    if (peer == null || peer != oldPeer) {
      LOG.warn("Skipping failover for peer {} of node {}, peer is null", peerId, deadRS);
      abortWhenFail(() -> queueStorage.removeQueue(server.getServerName(), queueId));
      return;
    }
    if (
      server instanceof ReplicationSyncUp.DummyServer
        && peer.getPeerState().equals(PeerState.DISABLED)
    ) {
      LOG.warn(
        "Peer {} is disabled. ReplicationSyncUp tool will skip " + "replicating data to this peer.",
        peerId);
      return;
    }

    ReplicationSourceInterface src;
    try {
      src = createSource(queueId, peer);
    } catch (IOException e) {
      LOG.error("Can not create replication source for peer {} and queue {}", peerId, queueId, e);
      server.abort("Failed to create replication source after claiming queue.", e);
      return;
    }
    // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
    synchronized (oldsources) {
      peer = replicationPeers.getPeer(src.getPeerId());
      if (peer == null || peer != oldPeer) {
        src.terminate("Recovered queue doesn't belong to any current peer");
        deleteQueue(queueId);
        return;
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
      LOG.info("Added source for recovered queue {}", src.getQueueId());
      for (String wal : walsSet) {
        LOG.trace("Enqueueing log from recovered queue for source: " + src.getQueueId());
        src.enqueueLog(new Path(oldLogDir, wal));
      }
      src.startup();
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
  public Map<String, Map<String, NavigableSet<String>>> getWALs() {
    return Collections.unmodifiableMap(walsById);
  }

  /**
   * Get a copy of the wals of the recovered sources on this rs
   * @return a sorted set of wal names
   */
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
  public ReplicationSourceInterface getSource(String peerId) {
    return this.sources.get(peerId);
  }

  List<String> getAllQueues() throws IOException {
    List<String> allQueues = Collections.emptyList();
    try {
      allQueues = queueStorage.getAllQueues(server.getServerName());
    } catch (ReplicationException e) {
      throw new IOException(e);
    }
    return allQueues;
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

  /**
   * Add an hbase:meta Catalog replication source. Called on open of an hbase:meta Region. Create it
   * once only. If exists already, use the existing one.
   * @see #removeCatalogReplicationSource(RegionInfo)
   * @see #addSource(String) This is specialization on the addSource method.
   */
  public ReplicationSourceInterface addCatalogReplicationSource(RegionInfo regionInfo)
    throws IOException {
    // Poor-man's putIfAbsent
    synchronized (this.catalogReplicationSource) {
      ReplicationSourceInterface rs = this.catalogReplicationSource.get();
      return rs != null
        ? rs
        : this.catalogReplicationSource.getAndSet(createCatalogReplicationSource(regionInfo));
    }
  }

  /**
   * Remove the hbase:meta Catalog replication source. Called when we close hbase:meta.
   * @see #addCatalogReplicationSource(RegionInfo regionInfo)
   */
  public void removeCatalogReplicationSource(RegionInfo regionInfo) {
    // Nothing to do. Leave any CatalogReplicationSource in place in case an hbase:meta Region
    // comes back to this server.
  }

  /**
   * Create, initialize, and start the Catalog ReplicationSource. Presumes called one-time only
   * (caller must ensure one-time only call). This ReplicationSource is NOT created via
   * {@link ReplicationSourceFactory}.
   * @see #addSource(String) This is a specialization of the addSource call.
   * @see #catalogReplicationSource for a note on this ReplicationSource's lifecycle (and more on
   *      why the special handling).
   */
  private ReplicationSourceInterface createCatalogReplicationSource(RegionInfo regionInfo)
    throws IOException {
    // Instantiate meta walProvider. Instantiated here or over in the #warmupRegion call made by the
    // Master on a 'move' operation. Need to do extra work if we did NOT instantiate the provider.
    WALProvider walProvider = this.walFactory.getMetaWALProvider();
    boolean instantiate = walProvider == null;
    if (instantiate) {
      walProvider = this.walFactory.getMetaProvider();
    }
    // Here we do a specialization on what {@link ReplicationSourceFactory} does. There is no need
    // for persisting offset into WALs up in zookeeper (via ReplicationQueueInfo) as the catalog
    // read replicas feature that makes use of the source does a reset on a crash of the WAL
    // source process. See "4.1 Skip maintaining zookeeper replication queue (offsets/WALs)" in the
    // design doc attached to HBASE-18070 'Enable memstore replication for meta replica' for detail.
    CatalogReplicationSourcePeer peer =
      new CatalogReplicationSourcePeer(this.conf, this.clusterId.toString());
    final ReplicationSourceInterface crs = new CatalogReplicationSource();
    crs.init(conf, fs, this, new NoopReplicationQueueStorage(), peer, server, peer.getId(),
      clusterId, walProvider.getWALFileLengthProvider(), new MetricsSource(peer.getId()));
    // Add listener on the provider so we can pick up the WAL to replicate on roll.
    WALActionsListener listener = new WALActionsListener() {
      @Override
      public void postLogRoll(Path oldPath, Path newPath) throws IOException {
        crs.enqueueLog(newPath);
      }
    };
    walProvider.addWALActionsListener(listener);
    if (!instantiate) {
      // If we did not instantiate provider, need to add our listener on already-created WAL
      // instance too (listeners are passed by provider to WAL instance on creation but if provider
      // created already, our listener add above is missed). And add the current WAL file to the
      // Replication Source so it can start replicating it.
      WAL wal = walProvider.getWAL(regionInfo);
      wal.registerWALActionsListener(listener);
      crs.enqueueLog(((AbstractFSWAL) wal).getCurrentFileName());
    }
    return crs.startup();
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
