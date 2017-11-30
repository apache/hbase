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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.BulkLoadDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.StoreDescriptor;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
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
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceWALReaderThread.WALEntryBatch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
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
  private Map<String, PriorityBlockingQueue<Path>> queues =
      new HashMap<String, PriorityBlockingQueue<Path>>();
  // per group queue size, keep no more than this number of logs in each wal group
  private int queueSizePerGroup;
  private ReplicationQueues replicationQueues;
  private ReplicationPeers replicationPeers;

  private Configuration conf;
  private ReplicationQueueInfo replicationQueueInfo;
  // id of the peer cluster this source replicates to
  private String peerId;
  // The manager of all sources to which we ping back our progress
  private ReplicationSourceManager manager;
  // Should we stop everything?
  private Stoppable stopper;
  // How long should we sleep for each retry
  private long sleepForRetries;
  private FileSystem fs;
  // id of this cluster
  private UUID clusterId;
  // id of the other cluster
  private UUID peerClusterId;
  // total number of edits we replicated
  private AtomicLong totalReplicatedEdits = new AtomicLong(0);
  // total number of edits we replicated
  private AtomicLong totalReplicatedOperations = new AtomicLong(0);
  // The znode we currently play with
  private String peerClusterZnode;
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
  private ConcurrentHashMap<String, ReplicationSourceShipperThread> workerThreads =
      new ConcurrentHashMap<String, ReplicationSourceShipperThread>();

  // Hold the state of a replication worker thread
  public enum WorkerState {
    RUNNING,
    STOPPED,
    FINISHED  // The worker is done processing a recovered queue
  }

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
    String logPrefix = DefaultWALProvider.getWALPrefixFromWALName(log.getName());
    PriorityBlockingQueue<Path> queue = queues.get(logPrefix);
    if (queue == null) {
      queue = new PriorityBlockingQueue<Path>(queueSizePerGroup, new LogsComparator());
      queues.put(logPrefix, queue);
      if (this.sourceRunning) {
        // new wal group observed after source startup, start a new worker thread to track it
        // notice: it's possible that log enqueued when this.running is set but worker thread
        // still not launched, so it's necessary to check workerThreads before start the worker
        final ReplicationSourceShipperThread worker =
            new ReplicationSourceShipperThread(logPrefix, queue, replicationQueueInfo, this);
        ReplicationSourceShipperThread extant = workerThreads.putIfAbsent(logPrefix, worker);
        if (extant != null) {
          LOG.debug("Someone has beat us to start a worker thread for wal group " + logPrefix);
        } else {
          LOG.debug("Starting up worker for wal group " + logPrefix);
          worker.startup();
        }
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
    Map<TableName, List<String>> tableCFMap = replicationPeers.getPeer(peerId).getTableCFs();
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

  private void uninitialize() {
    LOG.debug("Source exiting " + this.peerId);
    metrics.clear();
    if (replicationEndpoint.state() == Service.State.STARTING
        || replicationEndpoint.state() == Service.State.RUNNING) {
      replicationEndpoint.stopAndWait();
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
      final ReplicationSourceShipperThread worker =
          new ReplicationSourceShipperThread(walGroupId, queue, replicationQueueInfo, this);
      ReplicationSourceShipperThread extant = workerThreads.putIfAbsent(walGroupId, worker);
      if (extant != null) {
        LOG.debug("Someone has beat us to start a worker thread for wal group " + walGroupId);
      } else {
        LOG.debug("Starting up worker for wal group " + walGroupId);
        worker.startup();
      }
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
  protected boolean isPeerEnabled() {
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
      worker.setWorkerState(WorkerState.STOPPED);
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
  public String getPeerClusterId() {
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

  private boolean isSourceActive() {
    return !this.stopper.isStopped() && this.sourceRunning;
  }

  /**
   * Comparator used to compare logs together based on their start time
   */
  public static class LogsComparator implements Comparator<Path> {

    @Override
    public int compare(Path o1, Path o2) {
      return Long.compare(getTS(o1), getTS(o2));
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

  private long getCurrentBandwidth() {
    ReplicationPeer replicationPeer = this.replicationPeers.getPeer(peerId);
    long peerBandwidth = replicationPeer != null ? replicationPeer.getPeerBandwidth() : 0;
    // user can set peer bandwidth to 0 to use default bandwidth
    return peerBandwidth != 0 ? peerBandwidth : defaultBandwidth;
  }

  // This thread reads entries from a queue and ships them.
  // Entries are placed onto the queue by ReplicationSourceWALReaderThread
  public class ReplicationSourceShipperThread extends Thread {
    ReplicationSourceInterface source;
    String walGroupId;
    PriorityBlockingQueue<Path> queue;
    ReplicationQueueInfo replicationQueueInfo;
    // Last position in the log that we sent to ZooKeeper
    private long lastLoggedPosition = -1;
    // Path of the current log
    private volatile Path currentPath;
    // Current state of the worker thread
    private WorkerState state;
    ReplicationSourceWALReaderThread entryReader;

    public ReplicationSourceShipperThread(String walGroupId,
        PriorityBlockingQueue<Path> queue, ReplicationQueueInfo replicationQueueInfo,
        ReplicationSourceInterface source) {
      this.walGroupId = walGroupId;
      this.queue = queue;
      this.replicationQueueInfo = replicationQueueInfo;
      this.source = source;
    }

    @Override
    public void run() {
      setWorkerState(WorkerState.RUNNING);
      // Loop until we close down
      while (isWorkerActive()) {
        int sleepMultiplier = 1;
        // Sleep until replication is enabled again
        if (!isPeerEnabled()) {
          if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
            sleepMultiplier++;
          }
          continue;
        }
        while (entryReader == null) {
          if (sleepForRetries("Replication WAL entry reader thread not initialized",
            sleepMultiplier)) {
            sleepMultiplier++;
          }
          if (sleepMultiplier == maxRetriesMultiplier) {
            LOG.warn("Replication WAL entry reader thread not initialized");
          }
        }

        try {
          WALEntryBatch entryBatch = entryReader.take();
          shipEdits(entryBatch);
          if (replicationQueueInfo.isQueueRecovered() && entryBatch.getWalEntries().isEmpty()
              && entryBatch.getLastSeqIds().isEmpty()) {
            LOG.debug("Finished recovering queue for group " + walGroupId + " of peer "
                + peerClusterZnode);
            metrics.incrCompletedRecoveryQueue();
            setWorkerState(WorkerState.FINISHED);
            continue;
          }
        } catch (InterruptedException e) {
          LOG.trace("Interrupted while waiting for next replication entry batch", e);
          Thread.currentThread().interrupt();
        }
      }

      if (replicationQueueInfo.isQueueRecovered() && getWorkerState() == WorkerState.FINISHED) {
        // use synchronize to make sure one last thread will clean the queue
        synchronized (this) {
          Threads.sleep(100);// wait a short while for other worker thread to fully exit
          boolean allOtherTaskDone = true;
          for (ReplicationSourceShipperThread worker : workerThreads.values()) {
            if (!worker.equals(this) && worker.getWorkerState() != WorkerState.FINISHED) {
              allOtherTaskDone = false;
              break;
            }
          }
          if (allOtherTaskDone) {
            manager.closeRecoveredQueue(this.source);
            LOG.info("Finished recovering queue " + peerClusterZnode
                + " with the following stats: " + getStats());
          }
        }
      }
      // If the worker exits run loop without finishing it's task, mark it as stopped.
      if (state != WorkerState.FINISHED) {
        setWorkerState(WorkerState.STOPPED);
      }
    }

    private void cleanUpHFileRefs(WALEdit edit) throws IOException {
      String peerId = peerClusterZnode;
      if (peerId.contains("-")) {
        // peerClusterZnode will be in the form peerId + "-" + rsZNode.
        // A peerId will not have "-" in its name, see HBASE-11394
        peerId = peerClusterZnode.split("-")[0];
      }
      List<Cell> cells = edit.getCells();
      int totalCells = cells.size();
      for (int i = 0; i < totalCells; i++) {
        Cell cell = cells.get(i);
        if (CellUtil.matchingQualifier(cell, WALEdit.BULK_LOAD)) {
          BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cell);
          List<StoreDescriptor> stores = bld.getStoresList();
          int totalStores = stores.size();
          for (int j = 0; j < totalStores; j++) {
            List<String> storeFileList = stores.get(j).getStoreFileList();
            manager.cleanUpHFileRefs(peerId, storeFileList);
            metrics.decrSizeOfHFileRefsQueue(storeFileList.size());
          }
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

    /**
     * Do the shipping logic
     */
    protected void shipEdits(WALEntryBatch entryBatch) {
      List<Entry> entries = entryBatch.getWalEntries();
      long lastReadPosition = entryBatch.getLastWalPosition();
      currentPath = entryBatch.getLastWalPath();
      int sleepMultiplier = 0;
      if (entries.isEmpty()) {
        if (lastLoggedPosition != lastReadPosition) {
          updateLogPosition(lastReadPosition);
          // if there was nothing to ship and it's not an error
          // set "ageOfLastShippedOp" to <now> to indicate that we're current
          metrics.setAgeOfLastShippedOp(EnvironmentEdgeManager.currentTime(), walGroupId);
        }
        return;
      }
      int currentSize = (int) entryBatch.getHeapSize();
      while (isWorkerActive()) {
        try {
          checkBandwidthChangeAndResetThrottler();
          if (throttler.isEnabled()) {
            long sleepTicks = throttler.getNextSleepInterval(currentSize);
            if (sleepTicks > 0) {
              try {
                if (LOG.isTraceEnabled()) {
                  LOG.trace("To sleep " + sleepTicks + "ms for throttling control");
                }
                Thread.sleep(sleepTicks);
              } catch (InterruptedException e) {
                LOG.debug("Interrupted while sleeping for throttling control");
                Thread.currentThread().interrupt();
                // current thread might be interrupted to terminate
                // directly go back to while() for confirm this
                continue;
              }
              // reset throttler's cycle start tick when sleep for throttling occurs
              throttler.resetStartTick();
            }
          }
          // create replicateContext here, so the entries can be GC'd upon return from this call
          // stack
          ReplicationEndpoint.ReplicateContext replicateContext =
              new ReplicationEndpoint.ReplicateContext();
          replicateContext.setEntries(entries).setSize(currentSize);
          replicateContext.setWalGroupId(walGroupId);

          long startTimeNs = System.nanoTime();
          // send the edits to the endpoint. Will block until the edits are shipped and acknowledged
          boolean replicated = replicationEndpoint.replicate(replicateContext);
          long endTimeNs = System.nanoTime();

          if (!replicated) {
            continue;
          } else {
            sleepMultiplier = Math.max(sleepMultiplier - 1, 0);
          }

          if (this.lastLoggedPosition != lastReadPosition) {
            //Clean up hfile references
            int size = entries.size();
            for (int i = 0; i < size; i++) {
              cleanUpHFileRefs(entries.get(i).getEdit());
            }
            //Log and clean up WAL logs
            updateLogPosition(lastReadPosition);
          }
          if (throttler.isEnabled()) {
            throttler.addPushSize(currentSize);
          }
          totalReplicatedEdits.addAndGet(entries.size());
          totalReplicatedOperations.addAndGet(entryBatch.getNbOperations());
          // FIXME check relationship between wal group and overall
          metrics.shipBatch(entryBatch.getNbOperations(), currentSize, entryBatch.getNbHFiles());
          metrics.setAgeOfLastShippedOp(entries.get(entries.size() - 1).getKey().getWriteTime(),
            walGroupId);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Replicated " + totalReplicatedEdits + " entries in total, or "
                + totalReplicatedOperations + " operations in "
                + ((endTimeNs - startTimeNs) / 1000000) + " ms");
          }
          break;
        } catch (Exception ex) {
          LOG.warn(replicationEndpoint.getClass().getName() + " threw unknown exception:"
              + org.apache.hadoop.util.StringUtils.stringifyException(ex));
          if (sleepForRetries("ReplicationEndpoint threw exception", sleepMultiplier)) {
            sleepMultiplier++;
          }
        }
      }
    }

    private void updateLogPosition(long lastReadPosition) {
      manager.logPositionAndCleanOldLogs(currentPath, peerClusterZnode, lastReadPosition,
        this.replicationQueueInfo.isQueueRecovered(), false);
      lastLoggedPosition = lastReadPosition;
    }

    public void startup() {
      String n = Thread.currentThread().getName();
      Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
          RSRpcServices.exitIfOOME(e);
          LOG.error("Unexpected exception in ReplicationSourceWorkerThread," + " currentPath="
              + getCurrentPath(), e);
          stopper.stop("Unexpected exception in ReplicationSourceWorkerThread");
        }
      };
      Threads.setDaemonThreadRunning(this, n + ".replicationSource." + walGroupId + ","
          + peerClusterZnode, handler);
      workerThreads.put(walGroupId, this);

      long startPosition = 0;

      if (this.replicationQueueInfo.isQueueRecovered()) {
        startPosition = getRecoveredQueueStartPos(startPosition);
        int numRetries = 0;
        while (numRetries <= maxRetriesMultiplier) {
          try {
            locateRecoveredPaths();
            break;
          } catch (IOException e) {
            LOG.error("Error while locating recovered queue paths, attempt #" + numRetries);
            numRetries++;
          }
        }
      }

      startWALReaderThread(n, handler, startPosition);
    }

    // If this is a recovered queue, the queue is already full and the first log
    // normally has a position (unless the RS failed between 2 logs)
    private long getRecoveredQueueStartPos(long startPosition) {
      try {
        startPosition =
            (replicationQueues.getLogPosition(peerClusterZnode, this.queue.peek().getName()));
        if (LOG.isTraceEnabled()) {
          LOG.trace("Recovered queue started with log " + this.queue.peek() + " at position "
              + startPosition);
        }
      } catch (ReplicationException e) {
        terminate("Couldn't get the position of this recovered queue " + peerClusterZnode, e);
      }
      return startPosition;
    }

    // start a background thread to read and batch entries
    private void startWALReaderThread(String threadName, Thread.UncaughtExceptionHandler handler,
        long startPosition) {
      ArrayList<WALEntryFilter> filters = Lists.newArrayList(walEntryFilter,
        new ClusterMarkingEntryFilter(clusterId, peerClusterId, replicationEndpoint));
      ChainWALEntryFilter readerFilter = new ChainWALEntryFilter(filters);
      entryReader = new ReplicationSourceWALReaderThread(manager, replicationQueueInfo, queue,
          startPosition, fs, conf, readerFilter, metrics);
      Threads.setDaemonThreadRunning(entryReader, threadName
          + ".replicationSource.replicationWALReaderThread." + walGroupId + "," + peerClusterZnode,
        handler);
    }

    // Loops through the recovered queue and tries to find the location of each log
    // this is necessary because the logs may have moved before recovery was initiated
    private void locateRecoveredPaths() throws IOException {
      boolean hasPathChanged = false;
      PriorityBlockingQueue<Path> newPaths =
          new PriorityBlockingQueue<Path>(queueSizePerGroup, new LogsComparator());
      pathsLoop: for (Path path : queue) {
        if (fs.exists(path)) { // still in same location, don't need to do anything
          newPaths.add(path);
          continue;
        }
        // Path changed - try to find the right path.
        hasPathChanged = true;
        if (stopper instanceof ReplicationSyncUp.DummyServer) {
          // In the case of disaster/recovery, HMaster may be shutdown/crashed before flush data
          // from .logs to .oldlogs. Loop into .logs folders and check whether a match exists
          Path newPath = getReplSyncUpPath(path);
          newPaths.add(newPath);
          continue;
        } else {
          // See if Path exists in the dead RS folder (there could be a chain of failures
          // to look at)
          List<String> deadRegionServers = this.replicationQueueInfo.getDeadRegionServers();
          LOG.info("NB dead servers : " + deadRegionServers.size());
          final Path walDir = FSUtils.getWALRootDir(conf);
          for (String curDeadServerName : deadRegionServers) {
            final Path deadRsDirectory =
                new Path(walDir, DefaultWALProvider.getWALDirectoryName(curDeadServerName));
            Path[] locs = new Path[] { new Path(deadRsDirectory, path.getName()), new Path(
                deadRsDirectory.suffix(DefaultWALProvider.SPLITTING_EXT), path.getName()) };
            for (Path possibleLogLocation : locs) {
              LOG.info("Possible location " + possibleLogLocation.toUri().toString());
              if (manager.getFs().exists(possibleLogLocation)) {
                // We found the right new location
                LOG.info("Log " + path + " still exists at " + possibleLogLocation);
                newPaths.add(possibleLogLocation);
                continue pathsLoop;
              }
            }
          }
          // didn't find a new location
          LOG.error(
            String.format("WAL Path %s doesn't exist and couldn't find its new location", path));
          newPaths.add(path);
        }
      }

      if (hasPathChanged) {
        if (newPaths.size() != queue.size()) { // this shouldn't happen
          LOG.error("Recovery queue size is incorrect");
          throw new IOException("Recovery queue size error");
        }
        // put the correct locations in the queue
        // since this is a recovered queue with no new incoming logs,
        // there shouldn't be any concurrency issues
        queue.clear();
        for (Path path : newPaths) {
          queue.add(path);
        }
      }
    }

    // N.B. the ReplicationSyncUp tool sets the manager.getWALDir to the root of the wal
    // area rather than to the wal area for a particular region server.
    private Path getReplSyncUpPath(Path path) throws IOException {
      FileStatus[] rss = fs.listStatus(manager.getLogDir());
      for (FileStatus rs : rss) {
        Path p = rs.getPath();
        FileStatus[] logs = fs.listStatus(p);
        for (FileStatus log : logs) {
          p = new Path(p, log.getPath().getName());
          if (p.getName().equals(path.getName())) {
            LOG.info("Log " + p.getName() + " found at " + p);
            return p;
          }
        }
      }
      LOG.error("Didn't find path for: " + path.getName());
      return path;
    }

    public Path getCurrentPath() {
      return this.entryReader.getCurrentPath();
    }

    public long getCurrentPosition() {
      return this.lastLoggedPosition;
    }

    private boolean isWorkerActive() {
      return !stopper.isStopped() && state == WorkerState.RUNNING && !isInterrupted();
    }

    private void terminate(String reason, Exception cause) {
      if (cause == null) {
        LOG.info("Closing worker for wal group " + this.walGroupId + " because: " + reason);

      } else {
        LOG.error("Closing worker for wal group " + this.walGroupId
            + " because an error occurred: " + reason, cause);
      }
      entryReader.interrupt();
      Threads.shutdown(entryReader, sleepForRetries);
      setWorkerState(WorkerState.STOPPED);
      this.interrupt();
      Threads.shutdown(this, sleepForRetries);
      LOG.info("ReplicationSourceWorker " + this.getName() + " terminated");
    }

    /**
     * Set the worker state
     * @param state
     */
    public void setWorkerState(WorkerState state) {
      this.state = state;
      if (entryReader != null) {
        entryReader.setReaderRunning(state == WorkerState.RUNNING);
      }
    }

    /**
     * Get the current state of this worker.
     * @return WorkerState
     */
    public WorkerState getWorkerState() {
      return state;
    }
  }
}
