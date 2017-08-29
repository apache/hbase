/*
 *
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

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationListener;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;

/**
 * This class is responsible to manage all the replication
 * sources. There are two classes of sources:
 * <ul>
 * <li> Normal sources are persistent and one per peer cluster</li>
 * <li> Old sources are recovered from a failed region server and our
 * only goal is to finish replicating the WAL queue it had up in ZK</li>
 * </ul>
 *
 * When a region server dies, this class uses a watcher to get notified and it
 * tries to grab a lock in order to transfer all the queues in a local
 * old source.
 *
 * This class implements the ReplicationListener interface so that it can track changes in
 * replication state.
 */
@InterfaceAudience.Private
public class ReplicationSourceManager implements ReplicationListener {
  private static final Log LOG =
      LogFactory.getLog(ReplicationSourceManager.class);
  // List of all the sources that read this RS's logs
  private final List<ReplicationSourceInterface> sources;
  // List of all the sources we got from died RSs
  private final List<ReplicationSourceInterface> oldsources;
  private final ReplicationQueues replicationQueues;
  private final ReplicationTracker replicationTracker;
  private final ReplicationPeers replicationPeers;
  // UUID for this cluster
  private final UUID clusterId;
  // All about stopping
  private final Server server;
  // All logs we are currently tracking
  // Index structure of the map is: peer_id->logPrefix/logGroup->logs
  private final Map<String, Map<String, SortedSet<String>>> walsById;
  // Logs for recovered sources we are currently tracking
  private final Map<String, Map<String, SortedSet<String>>> walsByIdRecoveredQueues;
  private final Configuration conf;
  private final FileSystem fs;
  // The paths to the latest log of each wal group, for new coming peers
  private Set<Path> latestPaths;
  // Path to the wals directories
  private final Path logDir;
  // Path to the wal archive
  private final Path oldLogDir;
  // The number of ms that we wait before moving znodes, HBASE-3596
  private final long sleepBeforeFailover;
  // Homemade executer service for replication
  private final ThreadPoolExecutor executor;

  private final Random rand;
  private final boolean replicationForBulkLoadDataEnabled;

  private Connection connection;
  private long replicationWaitTime;

  private AtomicLong totalBufferUsed = new AtomicLong();

  /**
   * Creates a replication manager and sets the watch on all the other registered region servers
   * @param replicationQueues the interface for manipulating replication queues
   * @param replicationPeers
   * @param replicationTracker
   * @param conf the configuration to use
   * @param server the server for this region server
   * @param fs the file system to use
   * @param logDir the directory that contains all wal directories of live RSs
   * @param oldLogDir the directory where old logs are archived
   * @param clusterId
   */
  public ReplicationSourceManager(final ReplicationQueues replicationQueues,
      final ReplicationPeers replicationPeers, final ReplicationTracker replicationTracker,
      final Configuration conf, final Server server, final FileSystem fs, final Path logDir,
      final Path oldLogDir, final UUID clusterId) throws IOException {
    //CopyOnWriteArrayList is thread-safe.
    //Generally, reading is more than modifying.
    this.sources = new CopyOnWriteArrayList<>();
    this.replicationQueues = replicationQueues;
    this.replicationPeers = replicationPeers;
    this.replicationTracker = replicationTracker;
    this.server = server;
    this.walsById = new HashMap<>();
    this.walsByIdRecoveredQueues = new ConcurrentHashMap<>();
    this.oldsources = new CopyOnWriteArrayList<>();
    this.conf = conf;
    this.fs = fs;
    this.logDir = logDir;
    this.oldLogDir = oldLogDir;
    this.sleepBeforeFailover =
        conf.getLong("replication.sleep.before.failover", 30000); // 30 seconds
    this.clusterId = clusterId;
    this.replicationTracker.registerListener(this);
    this.replicationPeers.getAllPeerIds();
    // It's preferable to failover 1 RS at a time, but with good zk servers
    // more could be processed at the same time.
    int nbWorkers = conf.getInt("replication.executor.workers", 1);
    // use a short 100ms sleep since this could be done inline with a RS startup
    // even if we fail, other region servers can take care of it
    this.executor = new ThreadPoolExecutor(nbWorkers, nbWorkers,
        100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat("ReplicationExecutor-%d");
    tfb.setDaemon(true);
    this.executor.setThreadFactory(tfb.build());
    this.rand = new Random();
    this.latestPaths = Collections.synchronizedSet(new HashSet<Path>());
    replicationForBulkLoadDataEnabled =
        conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
          HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    this.replicationWaitTime = conf.getLong(HConstants.REPLICATION_SERIALLY_WAITING_KEY,
          HConstants.REPLICATION_SERIALLY_WAITING_DEFAULT);
    connection = ConnectionFactory.createConnection(conf);
  }

  /**
   * Provide the id of the peer and a log key and this method will figure which
   * wal it belongs to and will log, for this region server, the current
   * position. It will also clean old logs from the queue.
   * @param log Path to the log currently being replicated from
   * replication status in zookeeper. It will also delete older entries.
   * @param id id of the peer cluster
   * @param position current location in the log
   * @param queueRecovered indicates if this queue comes from another region server
   * @param holdLogInZK if true then the log is retained in ZK
   */
  public void logPositionAndCleanOldLogs(Path log, String id, long position,
      boolean queueRecovered, boolean holdLogInZK) {
    String fileName = log.getName();
    this.replicationQueues.setLogPosition(id, fileName, position);
    if (holdLogInZK) {
     return;
    }
    cleanOldLogs(fileName, id, queueRecovered);
  }

  /**
   * Cleans a log file and all older files from ZK. Called when we are sure that a
   * log file is closed and has no more entries.
   * @param key Path to the log
   * @param id id of the peer cluster
   * @param queueRecovered Whether this is a recovered queue
   */
  public void cleanOldLogs(String key, String id, boolean queueRecovered) {
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(key);
    if (queueRecovered) {
      SortedSet<String> wals = walsByIdRecoveredQueues.get(id).get(logPrefix);
      if (wals != null && !wals.first().equals(key)) {
        cleanOldLogs(wals, key, id);
      }
    } else {
      synchronized (this.walsById) {
        SortedSet<String> wals = walsById.get(id).get(logPrefix);
        if (wals != null && !wals.first().equals(key)) {
          cleanOldLogs(wals, key, id);
        }
      }
    }
 }

  private void cleanOldLogs(SortedSet<String> wals, String key, String id) {
    SortedSet<String> walSet = wals.headSet(key);
    LOG.debug("Removing " + walSet.size() + " logs in the list: " + walSet);
    for (String wal : walSet) {
      this.replicationQueues.removeLog(id, wal);
    }
    walSet.clear();
  }

  /**
   * Adds a normal source per registered peer cluster and tries to process all
   * old region server wal queues
   */
  protected void init() throws IOException, ReplicationException {
    for (String id : this.replicationPeers.getConnectedPeerIds()) {
      addSource(id);
      if (replicationForBulkLoadDataEnabled) {
        // Check if peer exists in hfile-refs queue, if not add it. This can happen in the case
        // when a peer was added before replication for bulk loaded data was enabled.
        this.replicationQueues.addPeerToHFileRefs(id);
      }
    }
    AdoptAbandonedQueuesWorker adoptionWorker = new AdoptAbandonedQueuesWorker();
    try {
      this.executor.execute(adoptionWorker);
    } catch (RejectedExecutionException ex) {
      LOG.info("Cancelling the adoption of abandoned queues because of " + ex.getMessage());
    }
  }

  /**
   * Add sources for the given peer cluster on this region server. For the newly added peer, we only
   * need to enqueue the latest log of each wal group and do replication
   * @param id the id of the peer cluster
   * @return the source that was created
   * @throws IOException
   */
  protected ReplicationSourceInterface addSource(String id) throws IOException,
      ReplicationException {
    ReplicationPeerConfig peerConfig = replicationPeers.getReplicationPeerConfig(id);
    ReplicationPeer peer = replicationPeers.getConnectedPeer(id);
    ReplicationSourceInterface src =
        getReplicationSource(this.conf, this.fs, this, this.replicationQueues,
          this.replicationPeers, server, id, this.clusterId, peerConfig, peer);
    synchronized (this.walsById) {
      this.sources.add(src);
      Map<String, SortedSet<String>> walsByGroup = new HashMap<>();
      this.walsById.put(id, walsByGroup);
      // Add the latest wal to that source's queue
      synchronized (latestPaths) {
        if (this.latestPaths.size() > 0) {
          for (Path logPath : latestPaths) {
            String name = logPath.getName();
            String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(name);
            SortedSet<String> logs = new TreeSet<>();
            logs.add(name);
            walsByGroup.put(walPrefix, logs);
            try {
              this.replicationQueues.addLog(id, name);
            } catch (ReplicationException e) {
              String message =
                  "Cannot add log to queue when creating a new source, queueId=" + id
                      + ", filename=" + name;
              server.stop(message);
              throw e;
            }
            src.enqueueLog(logPath);
          }
        }
      }
    }
    src.startup();
    return src;
  }

  /**
   * Delete a complete queue of wals associated with a peer cluster
   * @param peerId Id of the peer cluster queue of wals to delete
   */
  public void deleteSource(String peerId, boolean closeConnection) {
    this.replicationQueues.removeQueue(peerId);
    if (closeConnection) {
      this.replicationPeers.peerDisconnected(peerId);
    }
  }

  /**
   * Terminate the replication on this region server
   */
  public void join() {
    this.executor.shutdown();
    for (ReplicationSourceInterface source : this.sources) {
      source.terminate("Region server is closing");
    }
  }

  /**
   * Get a copy of the wals of the first source on this rs
   * @return a sorted set of wal names
   */
  protected Map<String, Map<String, SortedSet<String>>> getWALs() {
    return Collections.unmodifiableMap(walsById);
  }

  /**
   * Get a copy of the wals of the recovered sources on this rs
   * @return a sorted set of wal names
   */
  protected Map<String, Map<String, SortedSet<String>>> getWalsByIdRecoveredQueues() {
    return Collections.unmodifiableMap(walsByIdRecoveredQueues);
  }

  /**
   * Get a list of all the normal sources of this rs
   * @return lis of all sources
   */
  public List<ReplicationSourceInterface> getSources() {
    return this.sources;
  }

  /**
   * Get a list of all the old sources of this rs
   * @return list of all old sources
   */
  public List<ReplicationSourceInterface> getOldSources() {
    return this.oldsources;
  }

  /**
   * Get the normal source for a given peer
   * @param peerId
   * @return the normal source for the give peer if it exists, otherwise null.
   */
  public ReplicationSourceInterface getSource(String peerId) {
    for (ReplicationSourceInterface source: getSources()) {
      if (source.getPeerId().equals(peerId)) {
        return source;
      }
    }
    return null;
  }

  @VisibleForTesting
  List<String> getAllQueues() {
    return replicationQueues.getAllQueues();
  }

  void preLogRoll(Path newLog) throws IOException {
    recordLog(newLog);
    String logName = newLog.getName();
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(logName);
    synchronized (latestPaths) {
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

  /**
   * Check and enqueue the given log to the correct source. If there's still no source for the
   * group to which the given log belongs, create one
   * @param logPath the log path to check and enqueue
   * @throws IOException
   */
  private void recordLog(Path logPath) throws IOException {
    String logName = logPath.getName();
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(logName);
    // update replication queues on ZK
    // synchronize on replicationPeers to avoid adding source for the to-be-removed peer
    synchronized (replicationPeers) {
      for (String id : replicationPeers.getConnectedPeerIds()) {
        try {
          this.replicationQueues.addLog(id, logName);
        } catch (ReplicationException e) {
          throw new IOException("Cannot add log to replication queue"
              + " when creating a new source, queueId=" + id + ", filename=" + logName, e);
        }
      }
    }
    // update walsById map
    synchronized (walsById) {
      for (Map.Entry<String, Map<String, SortedSet<String>>> entry : this.walsById.entrySet()) {
        String peerId = entry.getKey();
        Map<String, SortedSet<String>> walsByPrefix = entry.getValue();
        boolean existingPrefix = false;
        for (Map.Entry<String, SortedSet<String>> walsEntry : walsByPrefix.entrySet()) {
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
          LOG.debug("Start tracking logs for wal group " + logPrefix + " for peer " + peerId);
          SortedSet<String> wals = new TreeSet<>();
          wals.add(logName);
          walsByPrefix.put(logPrefix, wals);
        }
      }
    }
  }

  void postLogRoll(Path newLog) throws IOException {
    // This only updates the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources) {
      source.enqueueLog(newLog);
    }
  }

  @VisibleForTesting
  public AtomicLong getTotalBufferUsed() {
    return totalBufferUsed;
  }

  /**
   * Factory method to create a replication source
   * @param conf the configuration to use
   * @param fs the file system to use
   * @param manager the manager to use
   * @param server the server object for this region server
   * @param peerId the id of the peer cluster
   * @return the created source
   * @throws IOException
   */
  protected ReplicationSourceInterface getReplicationSource(final Configuration conf,
      final FileSystem fs, final ReplicationSourceManager manager,
      final ReplicationQueues replicationQueues, final ReplicationPeers replicationPeers,
      final Server server, final String peerId, final UUID clusterId,
      final ReplicationPeerConfig peerConfig, final ReplicationPeer replicationPeer)
      throws IOException {
    RegionServerCoprocessorHost rsServerHost = null;
    TableDescriptors tableDescriptors = null;
    if (server instanceof HRegionServer) {
      rsServerHost = ((HRegionServer) server).getRegionServerCoprocessorHost();
      tableDescriptors = ((HRegionServer) server).getTableDescriptors();
    }

    ReplicationSourceInterface src = ReplicationSourceFactory.create(conf, peerId);

    ReplicationEndpoint replicationEndpoint = null;
    try {
      String replicationEndpointImpl = peerConfig.getReplicationEndpointImpl();
      if (replicationEndpointImpl == null) {
        // Default to HBase inter-cluster replication endpoint
        replicationEndpointImpl = HBaseInterClusterReplicationEndpoint.class.getName();
      }
      @SuppressWarnings("rawtypes")
      Class c = Class.forName(replicationEndpointImpl);
      replicationEndpoint = (ReplicationEndpoint) c.newInstance();
      if(rsServerHost != null) {
        ReplicationEndpoint newReplicationEndPoint = rsServerHost
            .postCreateReplicationEndPoint(replicationEndpoint);
        if(newReplicationEndPoint != null) {
          // Override the newly created endpoint from the hook with configured end point
          replicationEndpoint = newReplicationEndPoint;
        }
      }
    } catch (Exception e) {
      LOG.warn("Passed replication endpoint implementation throws errors"
          + " while initializing ReplicationSource for peer: " + peerId, e);
      throw new IOException(e);
    }

    MetricsSource metrics = new MetricsSource(peerId);
    // init replication source
    src.init(conf, fs, manager, replicationQueues, replicationPeers, server, peerId,
      clusterId, replicationEndpoint, metrics);

    // init replication endpoint
    replicationEndpoint.init(new ReplicationEndpoint.Context(replicationPeer.getConfiguration(),
      fs, peerId, clusterId, replicationPeer, metrics, tableDescriptors, server));

    return src;
  }

  /**
   * Transfer all the queues of the specified to this region server.
   * First it tries to grab a lock and if it works it will move the
   * znodes and finally will delete the old znodes.
   *
   * It creates one old source for any type of source of the old rs.
   * @param rsZnode
   */
  private void transferQueues(String rsZnode) {
    NodeFailoverWorker transfer =
        new NodeFailoverWorker(rsZnode, this.replicationQueues, this.replicationPeers,
            this.clusterId);
    try {
      this.executor.execute(transfer);
    } catch (RejectedExecutionException ex) {
      LOG.info("Cancelling the transfer of " + rsZnode + " because of " + ex.getMessage());
    }
  }

  /**
   * Clear the references to the specified old source
   * @param src source to clear
   */
  public void closeRecoveredQueue(ReplicationSourceInterface src) {
    LOG.info("Done with the recovered queue " + src.getPeerClusterZnode());
    if (src instanceof ReplicationSource) {
      ((ReplicationSource) src).getSourceMetrics().clear();
    }
    this.oldsources.remove(src);
    deleteSource(src.getPeerClusterZnode(), false);
    this.walsByIdRecoveredQueues.remove(src.getPeerClusterZnode());
  }

  /**
   * Clear the references to the specified old source
   * @param src source to clear
   */
  public void closeQueue(ReplicationSourceInterface src) {
    LOG.info("Done with the queue " + src.getPeerClusterZnode());
    src.getSourceMetrics().clear();
    this.sources.remove(src);
    deleteSource(src.getPeerClusterZnode(), true);
    this.walsById.remove(src.getPeerClusterZnode());
  }

  /**
   * Thie method first deletes all the recovered sources for the specified
   * id, then deletes the normal source (deleting all related data in ZK).
   * @param id The id of the peer cluster
   */
  public void removePeer(String id) {
    LOG.info("Closing the following queue " + id + ", currently have "
        + sources.size() + " and another "
        + oldsources.size() + " that were recovered");
    String terminateMessage = "Replication stream was removed by a user";
    List<ReplicationSourceInterface> oldSourcesToDelete = new ArrayList<>();
    // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
    // see NodeFailoverWorker.run
    synchronized (oldsources) {
      // First close all the recovered sources for this peer
      for (ReplicationSourceInterface src : oldsources) {
        if (id.equals(src.getPeerId())) {
          oldSourcesToDelete.add(src);
        }
      }
      for (ReplicationSourceInterface src : oldSourcesToDelete) {
        src.terminate(terminateMessage);
        closeRecoveredQueue(src);
      }
    }
    LOG.info("Number of deleted recovered sources for " + id + ": "
        + oldSourcesToDelete.size());
    // Now look for the one on this cluster
    List<ReplicationSourceInterface> srcToRemove = new ArrayList<>();
    // synchronize on replicationPeers to avoid adding source for the to-be-removed peer
    synchronized (this.replicationPeers) {
      for (ReplicationSourceInterface src : this.sources) {
        if (id.equals(src.getPeerId())) {
          srcToRemove.add(src);
        }
      }
      if (srcToRemove.isEmpty()) {
        LOG.error("The peer we wanted to remove is missing a ReplicationSourceInterface. " +
            "This could mean that ReplicationSourceInterface initialization failed for this peer " +
            "and that replication on this peer may not be caught up. peerId=" + id);
      }
      for (ReplicationSourceInterface toRemove : srcToRemove) {
        toRemove.terminate(terminateMessage);
        closeQueue(toRemove);
      }
      deleteSource(id, true);
    }
  }

  @Override
  public void regionServerRemoved(String regionserver) {
    transferQueues(regionserver);
  }

  @Override
  public void peerRemoved(String peerId) {
    removePeer(peerId);
    this.replicationQueues.removePeerFromHFileRefs(peerId);
  }

  @Override
  public void peerListChanged(List<String> peerIds) {
    for (String id : peerIds) {
      try {
        boolean added = this.replicationPeers.peerConnected(id);
        if (added) {
          addSource(id);
          if (replicationForBulkLoadDataEnabled) {
            this.replicationQueues.addPeerToHFileRefs(id);
          }
        }
      } catch (Exception e) {
        LOG.error("Error while adding a new peer", e);
      }
    }
  }

  /**
   * Class responsible to setup new ReplicationSources to take care of the
   * queues from dead region servers.
   */
  class NodeFailoverWorker extends Thread {

    private String rsZnode;
    private final ReplicationQueues rq;
    private final ReplicationPeers rp;
    private final UUID clusterId;

    /**
     * @param rsZnode
     */
    public NodeFailoverWorker(String rsZnode) {
      this(rsZnode, replicationQueues, replicationPeers, ReplicationSourceManager.this.clusterId);
    }

    public NodeFailoverWorker(String rsZnode, final ReplicationQueues replicationQueues,
        final ReplicationPeers replicationPeers, final UUID clusterId) {
      super("Failover-for-"+rsZnode);
      this.rsZnode = rsZnode;
      this.rq = replicationQueues;
      this.rp = replicationPeers;
      this.clusterId = clusterId;
    }

    @Override
    public void run() {
      if (this.rq.isThisOurRegionServer(rsZnode)) {
        return;
      }
      // Wait a bit before transferring the queues, we may be shutting down.
      // This sleep may not be enough in some cases.
      try {
        Thread.sleep(sleepBeforeFailover + (long) (rand.nextFloat() * sleepBeforeFailover));
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
      List<String> peers = rq.getUnClaimedQueueIds(rsZnode);
      while (peers != null && !peers.isEmpty()) {
        Pair<String, SortedSet<String>> peer = this.rq.claimQueue(rsZnode,
            peers.get(rand.nextInt(peers.size())));
        long sleep = sleepBeforeFailover/2;
        if (peer != null) {
          newQueues.put(peer.getFirst(), peer.getSecond());
          sleep = sleepBeforeFailover;
        }
        try {
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting before transferring a queue.");
          Thread.currentThread().interrupt();
        }
        peers = rq.getUnClaimedQueueIds(rsZnode);
      }
      if (peers != null) {
        rq.removeReplicatorIfQueueIsEmpty(rsZnode);
      }
      // Copying over the failed queue is completed.
      if (newQueues.isEmpty()) {
        // We either didn't get the lock or the failed region server didn't have any outstanding
        // WALs to replicate, so we are done.
        return;
      }

      for (Map.Entry<String, Set<String>> entry : newQueues.entrySet()) {
        String peerId = entry.getKey();
        Set<String> walsSet = entry.getValue();
        try {
          // there is not an actual peer defined corresponding to peerId for the failover.
          ReplicationQueueInfo replicationQueueInfo = new ReplicationQueueInfo(peerId);
          String actualPeerId = replicationQueueInfo.getPeerId();
          ReplicationPeer peer = replicationPeers.getConnectedPeer(actualPeerId);
          ReplicationPeerConfig peerConfig = null;
          try {
            peerConfig = replicationPeers.getReplicationPeerConfig(actualPeerId);
          } catch (ReplicationException ex) {
            LOG.warn("Received exception while getting replication peer config, skipping replay"
                + ex);
          }
          if (peer == null || peerConfig == null) {
            LOG.warn("Skipping failover for peer:" + actualPeerId + " of node" + rsZnode);
            replicationQueues.removeQueue(peerId);
            continue;
          }
          // track sources in walsByIdRecoveredQueues
          Map<String, SortedSet<String>> walsByGroup = new HashMap<>();
          walsByIdRecoveredQueues.put(peerId, walsByGroup);
          for (String wal : walsSet) {
            String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
            SortedSet<String> wals = walsByGroup.get(walPrefix);
            if (wals == null) {
              wals = new TreeSet<>();
              walsByGroup.put(walPrefix, wals);
            }
            wals.add(wal);
          }

          // enqueue sources
          ReplicationSourceInterface src =
              getReplicationSource(conf, fs, ReplicationSourceManager.this, this.rq, this.rp,
                server, peerId, this.clusterId, peerConfig, peer);
          // synchronized on oldsources to avoid adding recovered source for the to-be-removed peer
          // see removePeer
          synchronized (oldsources) {
            if (!this.rp.getConnectedPeerIds().contains(src.getPeerId())) {
              src.terminate("Recovered queue doesn't belong to any current peer");
              closeRecoveredQueue(src);
              continue;
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

  class AdoptAbandonedQueuesWorker extends Thread{

    public AdoptAbandonedQueuesWorker() {}

    @Override
    public void run() {
      List<String> currentReplicators = replicationQueues.getListOfReplicators();
      if (currentReplicators == null || currentReplicators.isEmpty()) {
        return;
      }
      List<String> otherRegionServers = replicationTracker.getListOfRegionServers();
      LOG.info("Current list of replicators: " + currentReplicators + " other RSs: "
        + otherRegionServers);

      // Look if there's anything to process after a restart
      for (String rs : currentReplicators) {
        if (!otherRegionServers.contains(rs)) {
          transferQueues(rs);
        }
      }
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

  public Connection getConnection() {
    return this.connection;
  }

  /**
   * Get the ReplicationPeers used by this ReplicationSourceManager
   * @return the ReplicationPeers used by this ReplicationSourceManager
   */
  public ReplicationPeers getReplicationPeers() {return this.replicationPeers;}

  /**
   * Get a string representation of all the sources' metrics
   */
  public String getStats() {
    StringBuffer stats = new StringBuffer();
    for (ReplicationSourceInterface source : sources) {
      stats.append("Normal source for cluster " + source.getPeerId() + ": ");
      stats.append(source.getStats() + "\n");
    }
    for (ReplicationSourceInterface oldSource : oldsources) {
      stats.append("Recovered source for cluster/machine(s) " + oldSource.getPeerId()+": ");
      stats.append(oldSource.getStats()+ "\n");
    }
    return stats.toString();
  }

  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
      throws ReplicationException {
    for (ReplicationSourceInterface source : this.sources) {
      source.addHFileRefs(tableName, family, pairs);
    }
  }

  public void cleanUpHFileRefs(String peerId, List<String> files) {
    this.replicationQueues.removeHFileRefs(peerId, files);
  }

  /**
   * Whether an entry can be pushed to the peer or not right now.
   * If we enable serial replication, we can not push the entry until all entries in its region
   * whose sequence numbers are smaller than this entry have been pushed.
   * For each ReplicationSource, we need only check the first entry in each region, as long as it
   * can be pushed, we can push all in this ReplicationSource.
   * This method will be blocked until we can push.
   * @return the first barrier of entry's region, or -1 if there is no barrier. It is used to
   *         prevent saving positions in the region of no barrier.
   */
  void waitUntilCanBePushed(byte[] encodedName, long seq, String peerId)
      throws IOException, InterruptedException {

    /**
     * There are barriers for this region and position for this peer. N barriers form N intervals,
     * (b1,b2) (b2,b3) ... (bn,max). Generally, there is no logs whose seq id is not greater than
     * the first barrier and the last interval is start from the last barrier.
     *
     * There are several conditions that we can push now, otherwise we should block:
     * 1) "Serial replication" is not enabled, we can push all logs just like before. This case
     *    should not call this method.
     * 2) There is no barriers for this region, or the seq id is smaller than the first barrier.
     *    It is mainly because we alter REPLICATION_SCOPE = 2. We can not guarantee the
     *    order of logs that is written before altering.
     * 3) This entry is in the first interval of barriers. We can push them because it is the
     *    start of a region. But if the region is created by region split, we should check
     *    if the parent regions are fully pushed.
     * 4) If the entry's seq id and the position are in same section, or the pos is the last
     *    number of previous section. Because when open a region we put a barrier the number
     *    is the last log's id + 1.
     * 5) Log's seq is smaller than pos in meta, we are retrying. It may happen when a RS crashes
     *    after save replication meta and before save zk offset.
     */
    List<Long> barriers = MetaTableAccessor.getReplicationBarriers(connection, encodedName);
    if (barriers.isEmpty() || seq <= barriers.get(0)) {
      // Case 2
      return;
    }
    int interval = Collections.binarySearch(barriers, seq);
    if (interval < 0) {
      interval = -interval - 1;// get the insert position if negative
    }
    if (interval == 1) {
      // Case 3
      // Check if there are parent regions
      String parentValue = MetaTableAccessor.getSerialReplicationParentRegion(connection,
          encodedName);
      if (parentValue == null) {
        // This region has no parent or the parent's log entries are fully pushed.
        return;
      }
      while (true) {
        boolean allParentDone = true;
        String[] parentRegions = parentValue.split(",");
        for (String parent : parentRegions) {
          byte[] region = Bytes.toBytes(parent);
          long pos = MetaTableAccessor.getReplicationPositionForOnePeer(connection, region, peerId);
          List<Long> parentBarriers = MetaTableAccessor.getReplicationBarriers(connection, region);
          if (parentBarriers.size() > 0
              && parentBarriers.get(parentBarriers.size() - 1) - 1 > pos) {
            allParentDone = false;
            // For a closed region, we will write a close event marker to WAL whose sequence id is
            // larger than final barrier but still smaller than next region's openSeqNum.
            // So if the pos is larger than last barrier, we can say we have read the event marker
            // which means the parent region has been fully pushed.
            LOG.info(Bytes.toString(encodedName) + " can not start pushing because parent region's"
                + " log has not been fully pushed: parent=" + Bytes.toString(region) + " pos=" + pos
                + " barriers=" + Arrays.toString(barriers.toArray()));
            break;
          }
        }
        if (allParentDone) {
          return;
        } else {
          Thread.sleep(replicationWaitTime);
        }
      }

    }

    while (true) {
      long pos = MetaTableAccessor.getReplicationPositionForOnePeer(connection, encodedName, peerId);
      if (seq <= pos) {
        // Case 5
      }
      if (pos >= 0) {
        // Case 4
        int posInterval = Collections.binarySearch(barriers, pos);
        if (posInterval < 0) {
          posInterval = -posInterval - 1;// get the insert position if negative
        }
        if (posInterval == interval || pos == barriers.get(interval - 1) - 1) {
          return;
        }
      }

      LOG.info(Bytes.toString(encodedName) + " can not start pushing to peer " + peerId
          + " because previous log has not been pushed: sequence=" + seq + " pos=" + pos
          + " barriers=" + Arrays.toString(barriers.toArray()));
      Thread.sleep(replicationWaitTime);
    }
  }

}
