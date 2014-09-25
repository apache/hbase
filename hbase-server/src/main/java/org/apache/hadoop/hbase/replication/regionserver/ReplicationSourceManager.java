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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationListener;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.ReplicationTracker;
import org.apache.zookeeper.KeeperException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This class is responsible to manage all the replication
 * sources. There are two classes of sources:
 * <li> Normal sources are persistent and one per peer cluster</li>
 * <li> Old sources are recovered from a failed region server and our
 * only goal is to finish replicating the HLog queue it had up in ZK</li>
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
  private final Stoppable stopper;
  // All logs we are currently tracking
  private final Map<String, SortedSet<String>> hlogsById;
  // Logs for recovered sources we are currently tracking
  private final Map<String, SortedSet<String>> hlogsByIdRecoveredQueues;
  private final Configuration conf;
  private final FileSystem fs;
  // The path to the latest log we saw, for new coming sources
  private Path latestPath;
  // Path to the hlogs directories
  private final Path logDir;
  // Path to the hlog archive
  private final Path oldLogDir;
  // The number of ms that we wait before moving znodes, HBASE-3596
  private final long sleepBeforeFailover;
  // Homemade executer service for replication
  private final ThreadPoolExecutor executor;

  private final Random rand;


  /**
   * Creates a replication manager and sets the watch on all the other registered region servers
   * @param replicationQueues the interface for manipulating replication queues
   * @param replicationPeers
   * @param replicationTracker
   * @param conf the configuration to use
   * @param stopper the stopper object for this region server
   * @param fs the file system to use
   * @param logDir the directory that contains all hlog directories of live RSs
   * @param oldLogDir the directory where old logs are archived
   * @param clusterId
   */
  public ReplicationSourceManager(final ReplicationQueues replicationQueues,
      final ReplicationPeers replicationPeers, final ReplicationTracker replicationTracker,
      final Configuration conf, final Stoppable stopper, final FileSystem fs, final Path logDir,
      final Path oldLogDir, final UUID clusterId) {
    //CopyOnWriteArrayList is thread-safe.
    //Generally, reading is more than modifying. 
    this.sources = new CopyOnWriteArrayList<ReplicationSourceInterface>();
    this.replicationQueues = replicationQueues;
    this.replicationPeers = replicationPeers;
    this.replicationTracker = replicationTracker;
    this.stopper = stopper;
    this.hlogsById = new HashMap<String, SortedSet<String>>();
    this.hlogsByIdRecoveredQueues = new ConcurrentHashMap<String, SortedSet<String>>();
    this.oldsources = new CopyOnWriteArrayList<ReplicationSourceInterface>();
    this.conf = conf;
    this.fs = fs;
    this.logDir = logDir;
    this.oldLogDir = oldLogDir;
    this.sleepBeforeFailover = conf.getLong("replication.sleep.before.failover", 2000);
    this.clusterId = clusterId;
    this.replicationTracker.registerListener(this);
    this.replicationPeers.getAllPeerIds();
    // It's preferable to failover 1 RS at a time, but with good zk servers
    // more could be processed at the same time.
    int nbWorkers = conf.getInt("replication.executor.workers", 1);
    // use a short 100ms sleep since this could be done inline with a RS startup
    // even if we fail, other region servers can take care of it
    this.executor = new ThreadPoolExecutor(nbWorkers, nbWorkers,
        100, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setNameFormat("ReplicationExecutor-%d");
    this.executor.setThreadFactory(tfb.build());
    this.rand = new Random();
  }

  /**
   * Provide the id of the peer and a log key and this method will figure which
   * hlog it belongs to and will log, for this region server, the current
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
    if (queueRecovered) {
      SortedSet<String> hlogs = hlogsByIdRecoveredQueues.get(id);
      if (hlogs != null && !hlogs.first().equals(key)) {
        cleanOldLogs(hlogs, key, id);
      }
    } else {
      synchronized (this.hlogsById) {
        SortedSet<String> hlogs = hlogsById.get(id);
        if (!hlogs.first().equals(key)) {
          cleanOldLogs(hlogs, key, id);
        }
      }
    }
 }
  
  private void cleanOldLogs(SortedSet<String> hlogs, String key, String id) {
    SortedSet<String> hlogSet = hlogs.headSet(key);
    LOG.debug("Removing " + hlogSet.size() + " logs in the list: " + hlogSet);
    for (String hlog : hlogSet) {
      this.replicationQueues.removeLog(id, hlog);
    }
    hlogSet.clear();
  }

  /**
   * Adds a normal source per registered peer cluster and tries to process all
   * old region server hlog queues
   */
  protected void init() throws IOException, ReplicationException {
    for (String id : this.replicationPeers.getConnectedPeers()) {
      addSource(id);
    }
    List<String> currentReplicators = this.replicationQueues.getListOfReplicators();
    if (currentReplicators == null || currentReplicators.size() == 0) {
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

  /**
   * Add a new normal source to this region server
   * @param id the id of the peer cluster
   * @return the source that was created
   * @throws IOException
   */
  protected ReplicationSourceInterface addSource(String id) throws IOException,
      ReplicationException {
    ReplicationSourceInterface src =
        getReplicationSource(this.conf, this.fs, this, this.replicationQueues,
          this.replicationPeers, stopper, id, this.clusterId);
    synchronized (this.hlogsById) {
      this.sources.add(src);
      this.hlogsById.put(id, new TreeSet<String>());
      // Add the latest hlog to that source's queue
      if (this.latestPath != null) {
        String name = this.latestPath.getName();
        this.hlogsById.get(id).add(name);
        try {
          this.replicationQueues.addLog(src.getPeerClusterZnode(), name);
        } catch (ReplicationException e) {
          String message =
              "Cannot add log to queue when creating a new source, queueId="
                  + src.getPeerClusterZnode() + ", filename=" + name;
          stopper.stop(message);
          throw e;
        }
        src.enqueueLog(this.latestPath);
      }
    }
    src.startup();
    return src;
  }

  /**
   * Delete a complete queue of hlogs associated with a peer cluster
   * @param peerId Id of the peer cluster queue of hlogs to delete
   */
  public void deleteSource(String peerId, boolean closeConnection) {
    this.replicationQueues.removeQueue(peerId);
    if (closeConnection) {
      this.replicationPeers.disconnectFromPeer(peerId);
    }
  }

  /**
   * Terminate the replication on this region server
   */
  public void join() {
    this.executor.shutdown();
    if (this.sources.size() == 0) {
      this.replicationQueues.removeAllQueues();
    }
    for (ReplicationSourceInterface source : this.sources) {
      source.terminate("Region server is closing");
    }
  }

  /**
   * Get a copy of the hlogs of the first source on this rs
   * @return a sorted set of hlog names
   */
  protected Map<String, SortedSet<String>> getHLogs() {
    return Collections.unmodifiableMap(hlogsById);
  }
  
  /**
   * Get a copy of the hlogs of the recovered sources on this rs
   * @return a sorted set of hlog names
   */
  protected Map<String, SortedSet<String>> getHlogsByIdRecoveredQueues() {
    return Collections.unmodifiableMap(hlogsByIdRecoveredQueues);
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

  void preLogRoll(Path newLog) throws IOException {
    synchronized (this.hlogsById) {
      String name = newLog.getName();
      for (ReplicationSourceInterface source : this.sources) {
        try {
          this.replicationQueues.addLog(source.getPeerClusterZnode(), name);
        } catch (ReplicationException e) {
          throw new IOException("Cannot add log to replication queue with id="
              + source.getPeerClusterZnode() + ", filename=" + name, e);
        }
      }
      for (SortedSet<String> hlogs : this.hlogsById.values()) {
        if (this.sources.isEmpty()) {
          // If there's no slaves, don't need to keep the old hlogs since
          // we only consider the last one when a new slave comes in
          hlogs.clear();
        }
        hlogs.add(name);
      }
    }

    this.latestPath = newLog;
  }

  void postLogRoll(Path newLog) throws IOException {
    // This only updates the sources we own, not the recovered ones
    for (ReplicationSourceInterface source : this.sources) {
      source.enqueueLog(newLog);
    }
  }

  /**
   * Factory method to create a replication source
   * @param conf the configuration to use
   * @param fs the file system to use
   * @param manager the manager to use
   * @param stopper the stopper object for this region server
   * @param peerId the id of the peer cluster
   * @return the created source
   * @throws IOException
   */
  protected ReplicationSourceInterface getReplicationSource(final Configuration conf,
      final FileSystem fs, final ReplicationSourceManager manager,
      final ReplicationQueues replicationQueues, final ReplicationPeers replicationPeers,
      final Stoppable stopper, final String peerId, final UUID clusterId) throws IOException {
    ReplicationSourceInterface src;
    try {
      @SuppressWarnings("rawtypes")
      Class c = Class.forName(conf.get("replication.replicationsource.implementation",
          ReplicationSource.class.getCanonicalName()));
      src = (ReplicationSourceInterface) c.newInstance();
    } catch (Exception e) {
      LOG.warn("Passed replication source implementation throws errors, " +
          "defaulting to ReplicationSource", e);
      src = new ReplicationSource();

    }
    src.init(conf, fs, manager, replicationQueues, replicationPeers, stopper, peerId, clusterId);
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
    this.oldsources.remove(src);
    deleteSource(src.getPeerClusterZnode(), false);
    this.hlogsByIdRecoveredQueues.remove(src.getPeerClusterZnode());
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
    ReplicationSourceInterface srcToRemove = null;
    List<ReplicationSourceInterface> oldSourcesToDelete =
        new ArrayList<ReplicationSourceInterface>();
    // First close all the recovered sources for this peer
    for (ReplicationSourceInterface src : oldsources) {
      if (id.equals(src.getPeerClusterId())) {
        oldSourcesToDelete.add(src);
      }
    }
    for (ReplicationSourceInterface src : oldSourcesToDelete) {
      src.terminate(terminateMessage);
      closeRecoveredQueue((src));
    }
    LOG.info("Number of deleted recovered sources for " + id + ": "
        + oldSourcesToDelete.size());
    // Now look for the one on this cluster
    for (ReplicationSourceInterface src : this.sources) {
      if (id.equals(src.getPeerClusterId())) {
        srcToRemove = src;
        break;
      }
    }
    if (srcToRemove == null) {
      LOG.error("The queue we wanted to close is missing " + id);
      return;
    }
    srcToRemove.terminate(terminateMessage);
    this.sources.remove(srcToRemove);
    deleteSource(id, true);
  }

  @Override
  public void regionServerRemoved(String regionserver) {
    transferQueues(regionserver);
  }

  @Override
  public void peerRemoved(String peerId) {
    removePeer(peerId);
  }

  @Override
  public void peerListChanged(List<String> peerIds) {
    for (String id : peerIds) {
      try {
        boolean added = this.replicationPeers.connectToPeer(id);
        if (added) {
          addSource(id);
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
     *
     * @param rsZnode
     */
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
      if (this.rq.isThisOurZnode(rsZnode)) {
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
      if (stopper.isStopped()) {
        LOG.info("Not transferring queue since we are shutting down");
        return;
      }
      SortedMap<String, SortedSet<String>> newQueues = null;

      newQueues = this.rq.claimQueues(rsZnode);

      // Copying over the failed queue is completed.
      if (newQueues.isEmpty()) {
        // We either didn't get the lock or the failed region server didn't have any outstanding
        // HLogs to replicate, so we are done.
        return;
      }

      for (Map.Entry<String, SortedSet<String>> entry : newQueues.entrySet()) {
        String peerId = entry.getKey();
        try {
          ReplicationSourceInterface src =
              getReplicationSource(conf, fs, ReplicationSourceManager.this, this.rq, this.rp,
                stopper, peerId, this.clusterId);
          if (!this.rp.getConnectedPeers().contains((src.getPeerClusterId()))) {
            src.terminate("Recovered queue doesn't belong to any current peer");
            break;
          }
          oldsources.add(src);
          SortedSet<String> hlogsSet = entry.getValue();
          for (String hlog : hlogsSet) {
            src.enqueueLog(new Path(oldLogDir, hlog));
          }
          src.startup();
          hlogsByIdRecoveredQueues.put(peerId, hlogsSet);
        } catch (IOException e) {
          // TODO manage it
          LOG.error("Failed creating a source", e);
        }
      }
    }
  }

  /**
   * Get the directory where hlogs are archived
   * @return the directory where hlogs are archived
   */
  public Path getOldLogDir() {
    return this.oldLogDir;
  }

  /**
   * Get the directory where hlogs are stored by their RSs
   * @return the directory where hlogs are stored by their RSs
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
   * Get a string representation of all the sources' metrics
   */
  public String getStats() {
    StringBuffer stats = new StringBuffer();
    for (ReplicationSourceInterface source : sources) {
      stats.append("Normal source for cluster " + source.getPeerClusterId() + ": ");
      stats.append(source.getStats() + "\n");
    }
    for (ReplicationSourceInterface oldSource : oldsources) {
      stats.append("Recovered source for cluster/machine(s) " + oldSource.getPeerClusterId() + ": ");
      stats.append(oldSource.getStats()+ "\n");
    }
    return stats.toString();
  }
}
