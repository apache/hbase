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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.replication.ClusterMarkingEntryFilter;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.SystemTableWALEntryFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;


/**
 * Class that handles the source of a replication stream.
 * Currently does not handle more than 1 slave
 * For each slave cluster it selects a random number of peers
 * using a replication ratio. For example, if replication ration = 0.1
 * and slave cluster has 100 region servers, 10 will be selected.
 * <p>
 * A stream is considered down when we cannot contact a region server on the
 * peer cluster for more than 55 seconds by default.
 * </p>
 *
 */
@InterfaceAudience.Private
public class ReplicationSource extends Thread implements ReplicationSourceInterface {

  private static final Log LOG = LogFactory.getLog(ReplicationSource.class);
  // Queues of logs to process, entry in format of walGroupId->queue,
  // each presents a queue for one wal group
  private Map<String, PriorityBlockingQueue<Path>> queues = new HashMap<>();
  // per group queue size, keep no more than this number of logs in each wal group
  protected int queueSizePerGroup;
  protected ReplicationQueues replicationQueues;
  private ReplicationPeers replicationPeers;

  protected Configuration conf;
  protected ReplicationQueueInfo replicationQueueInfo;
  // id of the peer cluster this source replicates to
  private String peerId;

  // The manager of all sources to which we ping back our progress
  protected ReplicationSourceManager manager;
  // Should we stop everything?
  protected Stoppable stopper;
  // How long should we sleep for each retry
  private long sleepForRetries;
  protected FileSystem fs;
  // id of this cluster
  private UUID clusterId;
  // id of the other cluster
  private UUID peerClusterId;
  // total number of edits we replicated
  private AtomicLong totalReplicatedEdits = new AtomicLong(0);
  // The znode we currently play with
  protected String peerClusterZnode;
  // Maximum number of retries before taking bold actions
  private int maxRetriesMultiplier;
  // Indicates if this particular source is running
  private volatile boolean sourceRunning = false;
  // Metrics for this source
  private MetricsSource metrics;
  //WARN threshold for the number of queued logs, defaults to 2
  private int logQueueWarnThreshold;
  // ReplicationEndpoint which will handle the actual replication
  private ReplicationEndpoint replicationEndpoint;
  // A filter (or a chain of filters) for the WAL entries.
  private WALEntryFilter walEntryFilter;
  // throttler
  private ReplicationThrottler throttler;
  private long defaultBandwidth;
  private long currentBandwidth;
  protected final ConcurrentHashMap<String, ReplicationSourceShipperThread> workerThreads =
      new ConcurrentHashMap<>();

  private AtomicLong totalBufferUsed;

  /**
   * Instantiation method used by region servers
   *
   * @param conf configuration to use
   * @param fs file system to use
   * @param manager replication manager to ping to
   * @param stopper     the atomic boolean to use to stop the regionserver
   * @param peerClusterZnode the name of our znode
   * @param clusterId unique UUID for the cluster
   * @param replicationEndpoint the replication endpoint implementation
   * @param metrics metrics for replication source
   * @throws IOException
   */
  @Override
  public void init(final Configuration conf, final FileSystem fs,
      final ReplicationSourceManager manager, final ReplicationQueues replicationQueues,
      final ReplicationPeers replicationPeers, final Stoppable stopper,
      final String peerClusterZnode, final UUID clusterId, ReplicationEndpoint replicationEndpoint,
      final MetricsSource metrics)
          throws IOException {
    this.stopper = stopper;
    this.conf = HBaseConfiguration.create(conf);
    decorateConf();
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);    // 1 second
    this.maxRetriesMultiplier =
        this.conf.getInt("replication.source.maxretriesmultiplier", 300); // 5 minutes @ 1 sec per
    this.queueSizePerGroup = this.conf.getInt("hbase.regionserver.maxlogs", 32);
    this.replicationQueues = replicationQueues;
    this.replicationPeers = replicationPeers;
    this.manager = manager;
    this.fs = fs;
    this.metrics = metrics;
    this.clusterId = clusterId;

    this.peerClusterZnode = peerClusterZnode;
    this.replicationQueueInfo = new ReplicationQueueInfo(peerClusterZnode);
    // ReplicationQueueInfo parses the peerId out of the znode for us
    this.peerId = this.replicationQueueInfo.getPeerId();
    this.logQueueWarnThreshold = this.conf.getInt("replication.source.log.queue.warn", 2);
    this.replicationEndpoint = replicationEndpoint;

    defaultBandwidth = this.conf.getLong("replication.source.per.peer.node.bandwidth", 0);
    currentBandwidth = getCurrentBandwidth();
    this.throttler = new ReplicationThrottler((double) currentBandwidth / 10.0);
    this.totalBufferUsed = manager.getTotalBufferUsed();
    LOG.info("peerClusterZnode=" + peerClusterZnode + ", ReplicationSource : " + peerId
        + ", currentBandwidth=" + this.currentBandwidth);
  }

  private void decorateConf() {
    String replicationCodec = this.conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);
    if (StringUtils.isNotEmpty(replicationCodec)) {
      this.conf.set(HConstants.RPC_CODEC_CONF_KEY, replicationCodec);
    }
  }

  @Override
  public void enqueueLog(Path log) {
    String logPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(log.getName());
    PriorityBlockingQueue<Path> queue = queues.get(logPrefix);
    if (queue == null) {
      queue = new PriorityBlockingQueue<>(queueSizePerGroup, new LogsComparator());
      queues.put(logPrefix, queue);
      if (this.sourceRunning) {
        // new wal group observed after source startup, start a new worker thread to track it
        // notice: it's possible that log enqueued when this.running is set but worker thread
        // still not launched, so it's necessary to check workerThreads before start the worker
        tryStartNewShipperThread(logPrefix, queue);
      }
    }
    queue.put(log);
    this.metrics.incrSizeOfLogQueue();
    // This will log a warning for each new log that gets created above the warn threshold
    int queueSize = queue.size();
    if (queueSize > this.logQueueWarnThreshold) {
      LOG.warn("WAL group " + logPrefix + " queue size: " + queueSize
          + " exceeds value of replication.source.log.queue.warn: " + logQueueWarnThreshold);
    }
  }

  @Override
  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
      throws ReplicationException {
    String peerId = peerClusterZnode;
    if (peerId.contains("-")) {
      // peerClusterZnode will be in the form peerId + "-" + rsZNode.
      // A peerId will not have "-" in its name, see HBASE-11394
      peerId = peerClusterZnode.split("-")[0];
    }
    Map<TableName, List<String>> tableCFMap = replicationPeers.getConnectedPeer(peerId).getTableCFs();
    if (tableCFMap != null) {
      List<String> tableCfs = tableCFMap.get(tableName);
      if (tableCFMap.containsKey(tableName)
          && (tableCfs == null || tableCfs.contains(Bytes.toString(family)))) {
        this.replicationQueues.addHFileRefs(peerId, pairs);
        metrics.incrSizeOfHFileRefsQueue(pairs.size());
      } else {
        LOG.debug("HFiles will not be replicated belonging to the table " + tableName + " family "
            + Bytes.toString(family) + " to peer id " + peerId);
      }
    } else {
      // user has explicitly not defined any table cfs for replication, means replicate all the
      // data
      this.replicationQueues.addHFileRefs(peerId, pairs);
      metrics.incrSizeOfHFileRefsQueue(pairs.size());
    }
  }

  @Override
  public void run() {
    // mark we are running now
    this.sourceRunning = true;
    try {
      // start the endpoint, connect to the cluster
      Service.State state = replicationEndpoint.start().get();
      if (state != Service.State.RUNNING) {
        LOG.warn("ReplicationEndpoint was not started. Exiting");
        uninitialize();
        return;
      }
    } catch (Exception ex) {
      LOG.warn("Error starting ReplicationEndpoint, exiting", ex);
      throw new RuntimeException(ex);
    }

    // get the WALEntryFilter from ReplicationEndpoint and add it to default filters
    ArrayList<WALEntryFilter> filters = Lists.newArrayList(
      (WALEntryFilter)new SystemTableWALEntryFilter());
    WALEntryFilter filterFromEndpoint = this.replicationEndpoint.getWALEntryfilter();
    if (filterFromEndpoint != null) {
      filters.add(filterFromEndpoint);
    }
    this.walEntryFilter = new ChainWALEntryFilter(filters);

    int sleepMultiplier = 1;
    // delay this until we are in an asynchronous thread
    while (this.isSourceActive() && this.peerClusterId == null) {
      this.peerClusterId = replicationEndpoint.getPeerUUID();
      if (this.isSourceActive() && this.peerClusterId == null) {
        if (sleepForRetries("Cannot contact the peer's zk ensemble", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }

    // In rare case, zookeeper setting may be messed up. That leads to the incorrect
    // peerClusterId value, which is the same as the source clusterId
    if (clusterId.equals(peerClusterId) && !replicationEndpoint.canReplicateToSameCluster()) {
      this.terminate("ClusterId " + clusterId + " is replicating to itself: peerClusterId "
          + peerClusterId + " which is not allowed by ReplicationEndpoint:"
          + replicationEndpoint.getClass().getName(), null, false);
      this.manager.closeQueue(this);
      return;
    }
    LOG.info("Replicating " + clusterId + " -> " + peerClusterId);
    // start workers
    for (Map.Entry<String, PriorityBlockingQueue<Path>> entry : queues.entrySet()) {
      String walGroupId = entry.getKey();
      PriorityBlockingQueue<Path> queue = entry.getValue();
      tryStartNewShipperThread(walGroupId, queue);
    }
  }

  protected void tryStartNewShipperThread(String walGroupId, PriorityBlockingQueue<Path> queue) {
    final ReplicationSourceShipperThread worker = new ReplicationSourceShipperThread(conf,
        walGroupId, queue, this);
    ReplicationSourceShipperThread extant = workerThreads.putIfAbsent(walGroupId, worker);
    if (extant != null) {
      LOG.debug("Someone has beat us to start a worker thread for wal group " + walGroupId);
    } else {
      LOG.debug("Starting up worker for wal group " + walGroupId);
      worker.startup(getUncaughtExceptionHandler());
      worker.setWALReader(startNewWALReaderThread(worker.getName(), walGroupId, queue,
        worker.getStartPosition()));
      workerThreads.put(walGroupId, worker);
    }
  }

  protected ReplicationSourceWALReaderThread startNewWALReaderThread(String threadName,
      String walGroupId, PriorityBlockingQueue<Path> queue, long startPosition) {
    ArrayList<WALEntryFilter> filters = Lists.newArrayList(walEntryFilter,
      new ClusterMarkingEntryFilter(clusterId, peerClusterId, replicationEndpoint));
    ChainWALEntryFilter readerFilter = new ChainWALEntryFilter(filters);
    ReplicationSourceWALReaderThread walReader = new ReplicationSourceWALReaderThread(manager,
        replicationQueueInfo, queue, startPosition, fs, conf, readerFilter, metrics);
    Threads.setDaemonThreadRunning(walReader, threadName
        + ".replicationSource.replicationWALReaderThread." + walGroupId + "," + peerClusterZnode,
      getUncaughtExceptionHandler());
    return walReader;
  }

  public Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
    return new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(final Thread t, final Throwable e) {
        RSRpcServices.exitIfOOME(e);
        LOG.error("Unexpected exception in " + t.getName() + " currentPath=" + getCurrentPath(), e);
        stopper.stop("Unexpected exception in " + t.getName());
      }
    };
  }

  @Override
  public ReplicationEndpoint getReplicationEndpoint() {
    return this.replicationEndpoint;
  }

  @Override
  public ReplicationSourceManager getSourceManager() {
    return this.manager;
  }

  @Override
  public void tryThrottle(int batchSize) throws InterruptedException {
    checkBandwidthChangeAndResetThrottler();
    if (throttler.isEnabled()) {
      long sleepTicks = throttler.getNextSleepInterval(batchSize);
      if (sleepTicks > 0) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("To sleep " + sleepTicks + "ms for throttling control");
        }
        Thread.sleep(sleepTicks);
        // reset throttler's cycle start tick when sleep for throttling occurs
        throttler.resetStartTick();
      }
    }
  }

  private void checkBandwidthChangeAndResetThrottler() {
    long peerBandwidth = getCurrentBandwidth();
    if (peerBandwidth != currentBandwidth) {
      currentBandwidth = peerBandwidth;
      throttler.setBandwidth((double) currentBandwidth / 10.0);
      LOG.info("ReplicationSource : " + peerId
          + " bandwidth throttling changed, currentBandWidth=" + currentBandwidth);
    }
  }

  private long getCurrentBandwidth() {
    ReplicationPeer replicationPeer = this.replicationPeers.getConnectedPeer(peerId);
    long peerBandwidth = replicationPeer != null ? replicationPeer.getPeerBandwidth() : 0;
    // user can set peer bandwidth to 0 to use default bandwidth
    return peerBandwidth != 0 ? peerBandwidth : defaultBandwidth;
  }

  private void uninitialize() {
    LOG.debug("Source exiting " + this.peerId);
    metrics.clear();
    if (replicationEndpoint.state() == Service.State.STARTING
        || replicationEndpoint.state() == Service.State.RUNNING) {
      replicationEndpoint.stopAndWait();
    }
  }

  /**
   * Do the sleeping logic
   * @param msg Why we sleep
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  protected boolean sleepForRetries(String msg, int sleepMultiplier) {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace(msg + ", sleeping " + sleepForRetries + " times " + sleepMultiplier);
      }
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping between retries");
      Thread.currentThread().interrupt();
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  /**
   * check whether the peer is enabled or not
   *
   * @return true if the peer is enabled, otherwise false
   */
  @Override
  public boolean isPeerEnabled() {
    return this.replicationPeers.getStatusOfPeer(this.peerId);
  }

  @Override
  public void startup() {
    String n = Thread.currentThread().getName();
    Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(final Thread t, final Throwable e) {
        LOG.error("Unexpected exception in ReplicationSource", e);
      }
    };
    Threads
        .setDaemonThreadRunning(this, n + ".replicationSource," + this.peerClusterZnode, handler);
  }

  @Override
  public void terminate(String reason) {
    terminate(reason, null);
  }

  @Override
  public void terminate(String reason, Exception cause) {
    terminate(reason, cause, true);
  }

  public void terminate(String reason, Exception cause, boolean join) {
    if (cause == null) {
      LOG.info("Closing source "
          + this.peerClusterZnode + " because: " + reason);

    } else {
      LOG.error("Closing source " + this.peerClusterZnode
          + " because an error occurred: " + reason, cause);
    }
    this.sourceRunning = false;
    Collection<ReplicationSourceShipperThread> workers = workerThreads.values();
    for (ReplicationSourceShipperThread worker : workers) {
      worker.stopWorker();
      worker.entryReader.interrupt();
      worker.interrupt();
    }
    ListenableFuture<Service.State> future = null;
    if (this.replicationEndpoint != null) {
      future = this.replicationEndpoint.stop();
    }
    if (join) {
      for (ReplicationSourceShipperThread worker : workers) {
        Threads.shutdown(worker, this.sleepForRetries);
        LOG.info("ReplicationSourceWorker " + worker.getName() + " terminated");
      }
      if (future != null) {
        try {
          future.get(sleepForRetries * maxRetriesMultiplier, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          LOG.warn("Got exception while waiting for endpoint to shutdown for replication source :"
              + this.peerClusterZnode,
            e);
        }
      }
    }
  }

  @Override
  public String getPeerClusterZnode() {
    return this.peerClusterZnode;
  }

  @Override
  public String getPeerId() {
    return this.peerId;
  }

  @Override
  public Path getCurrentPath() {
    // only for testing
    for (ReplicationSourceShipperThread worker : workerThreads.values()) {
      if (worker.getCurrentPath() != null) return worker.getCurrentPath();
    }
    return null;
  }

  @Override
  public boolean isSourceActive() {
    return !this.stopper.isStopped() && this.sourceRunning;
  }

  /**
   * Comparator used to compare logs together based on their start time
   */
  public static class LogsComparator implements Comparator<Path> {

    @Override
    public int compare(Path o1, Path o2) {
      return Long.valueOf(getTS(o1)).compareTo(getTS(o2));
    }

    /**
     * Split a path to get the start time
     * For example: 10.20.20.171%3A60020.1277499063250
     * @param p path to split
     * @return start time
     */
    private static long getTS(Path p) {
      int tsIndex = p.getName().lastIndexOf('.') + 1;
      return Long.parseLong(p.getName().substring(tsIndex));
    }
  }

  @Override
  public String getStats() {
    StringBuilder sb = new StringBuilder();
    sb.append("Total replicated edits: ").append(totalReplicatedEdits)
        .append(", current progress: \n");
    for (Map.Entry<String, ReplicationSourceShipperThread> entry : workerThreads.entrySet()) {
      String walGroupId = entry.getKey();
      ReplicationSourceShipperThread worker = entry.getValue();
      long position = worker.getCurrentPosition();
      Path currentPath = worker.getCurrentPath();
      sb.append("walGroup [").append(walGroupId).append("]: ");
      if (currentPath != null) {
        sb.append("currently replicating from: ").append(currentPath).append(" at position: ")
            .append(position).append("\n");
      } else {
        sb.append("no replication ongoing, waiting for new log");
      }
    }
    return sb.toString();
  }

  @Override
  public MetricsSource getSourceMetrics() {
    return this.metrics;
  }

  @Override
  public void postShipEdits(List<Entry> entries, int batchSize) {
    if (throttler.isEnabled()) {
      throttler.addPushSize(batchSize);
    }
    totalReplicatedEdits.addAndGet(entries.size());
    totalBufferUsed.addAndGet(-batchSize);
  }
}
