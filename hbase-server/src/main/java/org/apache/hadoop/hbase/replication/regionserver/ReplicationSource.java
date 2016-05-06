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

import java.io.EOFException;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.SystemTableWALEntryFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.LeaseNotRecoveredException;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALKey;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

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
public class ReplicationSource extends Thread
    implements ReplicationSourceInterface {

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
  // Max size in bytes of entriesArray
  private long replicationQueueSizeCapacity;
  // Max number of entries in entriesArray
  private int replicationQueueNbCapacity;
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
  private ConcurrentHashMap<String, ReplicationSourceWorkerThread> workerThreads =
      new ConcurrentHashMap<String, ReplicationSourceWorkerThread>();

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
    this.replicationQueueSizeCapacity =
        this.conf.getLong("replication.source.size.capacity", 1024*1024*64);
    this.replicationQueueNbCapacity =
        this.conf.getInt("replication.source.nb.capacity", 25000);
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);    // 1 second
    this.maxRetriesMultiplier =
        this.conf.getInt("replication.source.maxretriesmultiplier", 300); // 5 minutes @ 1 sec per
    this.queueSizePerGroup = this.conf.getInt("hbase.regionserver.maxlogs", 32);
    long bandwidth = this.conf.getLong("replication.source.per.peer.node.bandwidth", 0);
    this.throttler = new ReplicationThrottler((double)bandwidth/10.0);
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
        final ReplicationSourceWorkerThread worker =
            new ReplicationSourceWorkerThread(logPrefix, queue, replicationQueueInfo, this);
        ReplicationSourceWorkerThread extant = workerThreads.putIfAbsent(logPrefix, worker);
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
  public void addHFileRefs(TableName tableName, byte[] family, List<String> files)
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
        this.replicationQueues.addHFileRefs(peerId, files);
        metrics.incrSizeOfHFileRefsQueue(files.size());
      } else {
        LOG.debug("HFiles will not be replicated belonging to the table " + tableName + " family "
            + Bytes.toString(family) + " to peer id " + peerId);
      }
    } else {
      // user has explicitly not defined any table cfs for replication, means replicate all the
      // data
      this.replicationQueues.addHFileRefs(peerId, files);
      metrics.incrSizeOfHFileRefsQueue(files.size());
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
    }
    LOG.info("Replicating " + clusterId + " -> " + peerClusterId);
    // start workers
    for (Map.Entry<String, PriorityBlockingQueue<Path>> entry : queues.entrySet()) {
      String walGroupId = entry.getKey();
      PriorityBlockingQueue<Path> queue = entry.getValue();
      final ReplicationSourceWorkerThread worker =
          new ReplicationSourceWorkerThread(walGroupId, queue, replicationQueueInfo, this);
      ReplicationSourceWorkerThread extant = workerThreads.putIfAbsent(walGroupId, worker);
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
    Collection<ReplicationSourceWorkerThread> workers = workerThreads.values();
    for (ReplicationSourceWorkerThread worker : workers) {
      worker.setWorkerRunning(false);
      worker.interrupt();
    }
    ListenableFuture<Service.State> future = null;
    if (this.replicationEndpoint != null) {
      future = this.replicationEndpoint.stop();
    }
    if (join) {
      for (ReplicationSourceWorkerThread worker : workers) {
        Threads.shutdown(worker, this.sleepForRetries);
        LOG.info("ReplicationSourceWorker " + worker.getName() + " terminated");
      }
      if (future != null) {
        try {
          future.get();
        } catch (Exception e) {
          LOG.warn("Got exception:" + e);
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
    for (ReplicationSourceWorkerThread worker : workerThreads.values()) {
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
    for (Map.Entry<String, ReplicationSourceWorkerThread> entry : workerThreads.entrySet()) {
      String walGroupId = entry.getKey();
      ReplicationSourceWorkerThread worker = entry.getValue();
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

  /**
   * Get Replication Source Metrics
   * @return sourceMetrics
   */
  public MetricsSource getSourceMetrics() {
    return this.metrics;
  }

  public class ReplicationSourceWorkerThread extends Thread {
    private ReplicationSource source;
    private String walGroupId;
    private PriorityBlockingQueue<Path> queue;
    private ReplicationQueueInfo replicationQueueInfo;
    // Our reader for the current log. open/close handled by repLogReader
    private WAL.Reader reader;
    // Last position in the log that we sent to ZooKeeper
    private long lastLoggedPosition = -1;
    // Path of the current log
    private volatile Path currentPath;
    // Handle on the log reader helper
    private ReplicationWALReaderManager repLogReader;
    // Current number of operations (Put/Delete) that we need to replicate
    private int currentNbOperations = 0;
    // Current size of data we need to replicate
    private int currentSize = 0;
    // Indicates whether this particular worker is running
    private boolean workerRunning = true;
    // Current number of hfiles that we need to replicate
    private long currentNbHFiles = 0;

    public ReplicationSourceWorkerThread(String walGroupId, PriorityBlockingQueue<Path> queue,
        ReplicationQueueInfo replicationQueueInfo, ReplicationSource source) {
      this.walGroupId = walGroupId;
      this.queue = queue;
      this.replicationQueueInfo = replicationQueueInfo;
      this.repLogReader = new ReplicationWALReaderManager(fs, conf);
      this.source = source;
    }

    @Override
    public void run() {
      // If this is recovered, the queue is already full and the first log
      // normally has a position (unless the RS failed between 2 logs)
      if (this.replicationQueueInfo.isQueueRecovered()) {
        try {
          this.repLogReader.setPosition(replicationQueues.getLogPosition(peerClusterZnode,
            this.queue.peek().getName()));
          if (LOG.isTraceEnabled()) {
            LOG.trace("Recovered queue started with log " + this.queue.peek() + " at position "
                + this.repLogReader.getPosition());
          }
        } catch (ReplicationException e) {
          terminate("Couldn't get the position of this recovered queue " + peerClusterZnode, e);
        }
      }
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
        Path oldPath = getCurrentPath(); //note that in the current scenario,
                                         //oldPath will be null when a log roll
                                         //happens.
        // Get a new path
        boolean hasCurrentPath = getNextPath();
        if (getCurrentPath() != null && oldPath == null) {
          sleepMultiplier = 1; //reset the sleepMultiplier on a path change
        }
        if (!hasCurrentPath) {
          if (sleepForRetries("No log to process", sleepMultiplier)) {
            sleepMultiplier++;
          }
          continue;
        }
        boolean currentWALisBeingWrittenTo = false;
        //For WAL files we own (rather than recovered), take a snapshot of whether the
        //current WAL file (this.currentPath) is in use (for writing) NOW!
        //Since the new WAL paths are enqueued only after the prev WAL file
        //is 'closed', presence of an element in the queue means that
        //the previous WAL file was closed, else the file is in use (currentPath)
        //We take the snapshot now so that we are protected against races
        //where a new file gets enqueued while the current file is being processed
        //(and where we just finished reading the current file).
        if (!this.replicationQueueInfo.isQueueRecovered() && queue.size() == 0) {
          currentWALisBeingWrittenTo = true;
        }
        // Open a reader on it
        if (!openReader(sleepMultiplier)) {
          // Reset the sleep multiplier, else it'd be reused for the next file
          sleepMultiplier = 1;
          continue;
        }

        // If we got a null reader but didn't continue, then sleep and continue
        if (this.reader == null) {
          if (sleepForRetries("Unable to open a reader", sleepMultiplier)) {
            sleepMultiplier++;
          }
          continue;
        }

        boolean gotIOE = false;
        currentNbOperations = 0;
        currentNbHFiles = 0;
        List<WAL.Entry> entries = new ArrayList<WAL.Entry>(1);
        currentSize = 0;
        try {
          if (readAllEntriesToReplicateOrNextFile(currentWALisBeingWrittenTo, entries)) {
            continue;
          }
        } catch (IOException ioe) {
          LOG.warn(peerClusterZnode + " Got: ", ioe);
          gotIOE = true;
          if (ioe.getCause() instanceof EOFException) {

            boolean considerDumping = false;
            if (this.replicationQueueInfo.isQueueRecovered()) {
              try {
                FileStatus stat = fs.getFileStatus(this.currentPath);
                if (stat.getLen() == 0) {
                  LOG.warn(peerClusterZnode + " Got EOF and the file was empty");
                }
                considerDumping = true;
              } catch (IOException e) {
                LOG.warn(peerClusterZnode + " Got while getting file size: ", e);
              }
            }

            if (considerDumping &&
                sleepMultiplier == maxRetriesMultiplier &&
                processEndOfFile()) {
              continue;
            }
          }
        } finally {
          try {
            this.reader = null;
            this.repLogReader.closeReader();
          } catch (IOException e) {
            gotIOE = true;
            LOG.warn("Unable to finalize the tailing of a file", e);
          }
        }

        // If we didn't get anything to replicate, or if we hit a IOE,
        // wait a bit and retry.
        // But if we need to stop, don't bother sleeping
        if (isWorkerActive() && (gotIOE || entries.isEmpty())) {
          if (this.lastLoggedPosition != this.repLogReader.getPosition()) {
            manager.logPositionAndCleanOldLogs(this.currentPath,
                peerClusterZnode, this.repLogReader.getPosition(),
                this.replicationQueueInfo.isQueueRecovered(), currentWALisBeingWrittenTo);
            this.lastLoggedPosition = this.repLogReader.getPosition();
          }
          // Reset the sleep multiplier if nothing has actually gone wrong
          if (!gotIOE) {
            sleepMultiplier = 1;
            // if there was nothing to ship and it's not an error
            // set "ageOfLastShippedOp" to <now> to indicate that we're current
            metrics.setAgeOfLastShippedOp(EnvironmentEdgeManager.currentTime(), walGroupId);
          }
          if (sleepForRetries("Nothing to replicate", sleepMultiplier)) {
            sleepMultiplier++;
          }
          continue;
        }
        sleepMultiplier = 1;
        shipEdits(currentWALisBeingWrittenTo, entries);
      }
      if (replicationQueueInfo.isQueueRecovered()) {
        // use synchronize to make sure one last thread will clean the queue
        synchronized (workerThreads) {
          Threads.sleep(100);// wait a short while for other worker thread to fully exit
          boolean allOtherTaskDone = true;
          for (ReplicationSourceWorkerThread worker : workerThreads.values()) {
            if (!worker.equals(this) && worker.isAlive()) {
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
    }

    /**
     * Read all the entries from the current log files and retain those that need to be replicated.
     * Else, process the end of the current file.
     * @param currentWALisBeingWrittenTo is the current WAL being written to
     * @param entries resulting entries to be replicated
     * @return true if we got nothing and went to the next file, false if we got entries
     * @throws IOException
     */
    protected boolean readAllEntriesToReplicateOrNextFile(boolean currentWALisBeingWrittenTo,
        List<WAL.Entry> entries) throws IOException {
      long seenEntries = 0;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Seeking in " + this.currentPath + " at position "
            + this.repLogReader.getPosition());
      }
      this.repLogReader.seek();
      long positionBeforeRead = this.repLogReader.getPosition();
      WAL.Entry entry = this.repLogReader.readNextAndSetPosition();
      while (entry != null) {
        metrics.incrLogEditsRead();
        seenEntries++;

        // don't replicate if the log entries have already been consumed by the cluster
        if (replicationEndpoint.canReplicateToSameCluster()
            || !entry.getKey().getClusterIds().contains(peerClusterId)) {
          // Remove all KVs that should not be replicated
          entry = walEntryFilter.filter(entry);
          WALEdit edit = null;
          WALKey logKey = null;
          if (entry != null) {
            edit = entry.getEdit();
            logKey = entry.getKey();
          }

          if (edit != null && edit.size() != 0) {
            // Mark that the current cluster has the change
            logKey.addClusterId(clusterId);
            currentNbOperations += countDistinctRowKeys(edit);
            entries.add(entry);
            currentSize += entry.getEdit().heapSize();
            currentSize += calculateTotalSizeOfStoreFiles(edit);
          } else {
            metrics.incrLogEditsFiltered();
          }
        }
        // Stop if too many entries or too big
        // FIXME check the relationship between single wal group and overall
        if (currentSize >= replicationQueueSizeCapacity
            || entries.size() >= replicationQueueNbCapacity) {
          break;
        }
        try {
          entry = this.repLogReader.readNextAndSetPosition();
        } catch (IOException ie) {
          LOG.debug("Break on IOE: " + ie.getMessage());
          break;
        }
      }
      metrics.incrLogReadInBytes(this.repLogReader.getPosition() - positionBeforeRead);
      if (currentWALisBeingWrittenTo) {
        return false;
      }
      // If we didn't get anything and the queue has an object, it means we
      // hit the end of the file for sure
      return seenEntries == 0 && processEndOfFile();
    }

    /**
     * Calculate the total size of all the store files
     * @param edit edit to count row keys from
     * @return the total size of the store files
     */
    private int calculateTotalSizeOfStoreFiles(WALEdit edit) {
      List<Cell> cells = edit.getCells();
      int totalStoreFilesSize = 0;

      int totalCells = edit.size();
      for (int i = 0; i < totalCells; i++) {
        if (CellUtil.matchingQualifier(cells.get(i), WALEdit.BULK_LOAD)) {
          try {
            BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cells.get(i));
            List<StoreDescriptor> stores = bld.getStoresList();
            int totalStores = stores.size();
            for (int j = 0; j < totalStores; j++) {
              totalStoreFilesSize += stores.get(j).getStoreFileSize();
            }
          } catch (IOException e) {
            LOG.error("Failed to deserialize bulk load entry from wal edit. "
                + "Size of HFiles part of cell will not be considered in replication "
                + "request size calculation.", e);
          }
        }
      }
      return totalStoreFilesSize;
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

    /**
     * Poll for the next path
     * @return true if a path was obtained, false if not
     */
    protected boolean getNextPath() {
      try {
        if (this.currentPath == null) {
          this.currentPath = queue.poll(sleepForRetries, TimeUnit.MILLISECONDS);
          metrics.decrSizeOfLogQueue();
          if (this.currentPath != null) {
            // For recovered queue: must use peerClusterZnode since peerId is a parsed value
            manager.cleanOldLogs(this.currentPath.getName(), peerClusterZnode,
              this.replicationQueueInfo.isQueueRecovered());
            if (LOG.isTraceEnabled()) {
              LOG.trace("New log: " + this.currentPath);
            }
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while reading edits", e);
      }
      return this.currentPath != null;
    }

    /**
     * Open a reader on the current path
     *
     * @param sleepMultiplier by how many times the default sleeping time is augmented
     * @return true if we should continue with that file, false if we are over with it
     */
    protected boolean openReader(int sleepMultiplier) {
      try {
        try {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Opening log " + this.currentPath);
          }
          this.reader = repLogReader.openReader(this.currentPath);
        } catch (FileNotFoundException fnfe) {
          if (this.replicationQueueInfo.isQueueRecovered()) {
            // We didn't find the log in the archive directory, look if it still
            // exists in the dead RS folder (there could be a chain of failures
            // to look at)
            List<String> deadRegionServers = this.replicationQueueInfo.getDeadRegionServers();
            LOG.info("NB dead servers : " + deadRegionServers.size());
            final Path rootDir = FSUtils.getRootDir(conf);
            for (String curDeadServerName : deadRegionServers) {
              final Path deadRsDirectory = new Path(rootDir,
                  DefaultWALProvider.getWALDirectoryName(curDeadServerName));
              Path[] locs = new Path[] {
                  new Path(deadRsDirectory, currentPath.getName()),
                  new Path(deadRsDirectory.suffix(DefaultWALProvider.SPLITTING_EXT),
                                            currentPath.getName()),
              };
              for (Path possibleLogLocation : locs) {
                LOG.info("Possible location " + possibleLogLocation.toUri().toString());
                if (manager.getFs().exists(possibleLogLocation)) {
                  // We found the right new location
                  LOG.info("Log " + this.currentPath + " still exists at " +
                      possibleLogLocation);
                  // Breaking here will make us sleep since reader is null
                  // TODO why don't we need to set currentPath and call openReader here?
                  return true;
                }
              }
            }
            // In the case of disaster/recovery, HMaster may be shutdown/crashed before flush data
            // from .logs to .oldlogs. Loop into .logs folders and check whether a match exists
            if (stopper instanceof ReplicationSyncUp.DummyServer) {
              // N.B. the ReplicationSyncUp tool sets the manager.getLogDir to the root of the wal
              //      area rather than to the wal area for a particular region server.
              FileStatus[] rss = fs.listStatus(manager.getLogDir());
              for (FileStatus rs : rss) {
                Path p = rs.getPath();
                FileStatus[] logs = fs.listStatus(p);
                for (FileStatus log : logs) {
                  p = new Path(p, log.getPath().getName());
                  if (p.getName().equals(currentPath.getName())) {
                    currentPath = p;
                    LOG.info("Log " + currentPath.getName() + " found at " + currentPath);
                    // Open the log at the new location
                    this.openReader(sleepMultiplier);
                    return true;
                  }
                }
              }
            }

            // TODO What happens if the log was missing from every single location?
            // Although we need to check a couple of times as the log could have
            // been moved by the master between the checks
            // It can also happen if a recovered queue wasn't properly cleaned,
            // such that the znode pointing to a log exists but the log was
            // deleted a long time ago.
            // For the moment, we'll throw the IO and processEndOfFile
            throw new IOException("File from recovered queue is " +
                "nowhere to be found", fnfe);
          } else {
            // If the log was archived, continue reading from there
            Path archivedLogLocation =
                new Path(manager.getOldLogDir(), currentPath.getName());
            if (manager.getFs().exists(archivedLogLocation)) {
              currentPath = archivedLogLocation;
              LOG.info("Log " + this.currentPath + " was moved to " +
                  archivedLogLocation);
              // Open the log at the new location
              this.openReader(sleepMultiplier);

            }
            // TODO What happens the log is missing in both places?
          }
        }
      } catch (LeaseNotRecoveredException lnre) {
        // HBASE-15019 the WAL was not closed due to some hiccup.
        LOG.warn(peerClusterZnode + " Try to recover the WAL lease " + currentPath, lnre);
        recoverLease(conf, currentPath);
        this.reader = null;
      } catch (IOException ioe) {
        if (ioe instanceof EOFException && isCurrentLogEmpty()) return true;
        LOG.warn(peerClusterZnode + " Got: ", ioe);
        this.reader = null;
        if (ioe.getCause() instanceof NullPointerException) {
          // Workaround for race condition in HDFS-4380
          // which throws a NPE if we open a file before any data node has the most recent block
          // Just sleep and retry. Will require re-reading compressed WALs for compressionContext.
          LOG.warn("Got NPE opening reader, will retry.");
        } else if (sleepMultiplier >= maxRetriesMultiplier) {
          // TODO Need a better way to determine if a file is really gone but
          // TODO without scanning all logs dir
          LOG.warn("Waited too long for this file, considering dumping");
          return !processEndOfFile();
        }
      }
      return true;
    }

    private void recoverLease(final Configuration conf, final Path path) {
      try {
        final FileSystem dfs = FSUtils.getCurrentFileSystem(conf);
        FSUtils fsUtils = FSUtils.getInstance(dfs, conf);
        fsUtils.recoverFileLease(dfs, path, conf, new CancelableProgressable() {
          @Override
          public boolean progress() {
            LOG.debug("recover WAL lease: " + path);
            return isWorkerActive();
          }
        });
      } catch (IOException e) {
        LOG.warn("unable to recover lease for WAL: " + path, e);
      }
    }

    /*
     * Checks whether the current log file is empty, and it is not a recovered queue. This is to
     * handle scenario when in an idle cluster, there is no entry in the current log and we keep on
     * trying to read the log file and get EOFException. In case of a recovered queue the last log
     * file may be empty, and we don't want to retry that.
     */
    private boolean isCurrentLogEmpty() {
      return (this.repLogReader.getPosition() == 0 &&
          !this.replicationQueueInfo.isQueueRecovered() && queue.size() == 0);
    }

    /**
     * Count the number of different row keys in the given edit because of mini-batching. We assume
     * that there's at least one Cell in the WALEdit.
     * @param edit edit to count row keys from
     * @return number of different row keys
     */
    private int countDistinctRowKeys(WALEdit edit) {
      List<Cell> cells = edit.getCells();
      int distinctRowKeys = 1;
      int totalHFileEntries = 0;
      Cell lastCell = cells.get(0);
      
      int totalCells = edit.size();
      for (int i = 0; i < totalCells; i++) {
        // Count HFiles to be replicated
        if (CellUtil.matchingQualifier(cells.get(i), WALEdit.BULK_LOAD)) {
          try {
            BulkLoadDescriptor bld = WALEdit.getBulkLoadDescriptor(cells.get(i));
            List<StoreDescriptor> stores = bld.getStoresList();
            int totalStores = stores.size();
            for (int j = 0; j < totalStores; j++) {
              totalHFileEntries += stores.get(j).getStoreFileList().size();
            }
          } catch (IOException e) {
            LOG.error("Failed to deserialize bulk load entry from wal edit. "
                + "Then its hfiles count will not be added into metric.");
          }
        }

        if (!CellUtil.matchingRow(cells.get(i), lastCell)) {
          distinctRowKeys++;
        }
        lastCell = cells.get(i);
      }
      currentNbHFiles += totalHFileEntries;
      return distinctRowKeys + totalHFileEntries;
    }

    /**
     * Do the shipping logic
     * @param currentWALisBeingWrittenTo was the current WAL being (seemingly)
     * written to when this method was called
     */
    protected void shipEdits(boolean currentWALisBeingWrittenTo, List<WAL.Entry> entries) {
      int sleepMultiplier = 0;
      if (entries.isEmpty()) {
        LOG.warn("Was given 0 edits to ship");
        return;
      }
      while (isWorkerActive()) {
        try {
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

          if (this.lastLoggedPosition != this.repLogReader.getPosition()) {
            //Clean up hfile references
            int size = entries.size();
            for (int i = 0; i < size; i++) {
              cleanUpHFileRefs(entries.get(i).getEdit());
            }
            //Log and clean up WAL logs
            manager.logPositionAndCleanOldLogs(this.currentPath, peerClusterZnode,
              this.repLogReader.getPosition(), this.replicationQueueInfo.isQueueRecovered(),
              currentWALisBeingWrittenTo);
            this.lastLoggedPosition = this.repLogReader.getPosition();
          }
          if (throttler.isEnabled()) {
            throttler.addPushSize(currentSize);
          }
          totalReplicatedEdits.addAndGet(entries.size());
          totalReplicatedOperations.addAndGet(currentNbOperations);
          // FIXME check relationship between wal group and overall
          metrics.shipBatch(currentNbOperations, currentSize / 1024, currentNbHFiles);
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

    /**
     * If the queue isn't empty, switch to the next one Else if this is a recovered queue, it means
     * we're done! Else we'll just continue to try reading the log file
     * @return true if we're done with the current file, false if we should continue trying to read
     *         from it
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DE_MIGHT_IGNORE",
        justification = "Yeah, this is how it works")
    protected boolean processEndOfFile() {
      if (this.queue.size() != 0) {
        if (LOG.isTraceEnabled()) {
          String filesize = "N/A";
          try {
            FileStatus stat = fs.getFileStatus(this.currentPath);
            filesize = stat.getLen() + "";
          } catch (IOException ex) {
          }
          LOG.trace("Reached the end of log " + this.currentPath + ", stats: " + getStats()
              + ", and the length of the file is " + filesize);
        }
        this.currentPath = null;
        this.repLogReader.finishCurrentFile();
        this.reader = null;
        return true;
      } else if (this.replicationQueueInfo.isQueueRecovered()) {
        LOG.debug("Finished recovering queue for group " + walGroupId + " of peer "
            + peerClusterZnode);
        workerRunning = false;
        return true;
      }
      return false;
    }

    public void startup() {
      String n = Thread.currentThread().getName();
      Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(final Thread t, final Throwable e) {
          LOG.error("Unexpected exception in ReplicationSourceWorkerThread," + " currentPath="
              + getCurrentPath(), e);
        }
      };
      Threads.setDaemonThreadRunning(this, n + ".replicationSource." + walGroupId + ","
          + peerClusterZnode, handler);
      workerThreads.put(walGroupId, this);
    }

    public Path getCurrentPath() {
      return this.currentPath;
    }

    public long getCurrentPosition() {
      return this.repLogReader.getPosition();
    }

    private boolean isWorkerActive() {
      return !stopper.isStopped() && workerRunning && !isInterrupted();
    }

    private void terminate(String reason, Exception cause) {
      if (cause == null) {
        LOG.info("Closing worker for wal group " + this.walGroupId + " because: " + reason);

      } else {
        LOG.error("Closing worker for wal group " + this.walGroupId
            + " because an error occurred: " + reason, cause);
      }
      this.interrupt();
      Threads.shutdown(this, sleepForRetries);
      LOG.info("ReplicationSourceWorker " + this.getName() + " terminated");
    }

    public void setWorkerRunning(boolean workerRunning) {
      this.workerRunning = workerRunning;
    }
  }
}
