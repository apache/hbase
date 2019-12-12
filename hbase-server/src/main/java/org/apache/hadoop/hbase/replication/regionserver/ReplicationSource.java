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

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.getArchivedLogPath;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.replication.ClusterMarkingEntryFilter;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.SystemTableWALEntryFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceShipper.WorkerState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.FutureCallback;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Futures;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ListenableFuture;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

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
 */
@InterfaceAudience.Private
public class ReplicationSource implements ReplicationSourceInterface {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSource.class);
  // Queues of logs to process, entry in format of walGroupId->queue,
  // each presents a queue for one wal group
  private Map<String, PriorityBlockingQueue<Path>> queues = new HashMap<>();
  // per group queue size, keep no more than this number of logs in each wal group
  protected int queueSizePerGroup;
  protected ReplicationQueueStorage queueStorage;
  protected ReplicationPeer replicationPeer;

  protected Configuration conf;
  protected ReplicationQueueInfo replicationQueueInfo;

  // The manager of all sources to which we ping back our progress
  protected ReplicationSourceManager manager;
  // Should we stop everything?
  protected Server server;
  // How long should we sleep for each retry
  private long sleepForRetries;
  protected FileSystem fs;
  // id of this cluster
  private UUID clusterId;
  // total number of edits we replicated
  private AtomicLong totalReplicatedEdits = new AtomicLong(0);
  // The znode we currently play with
  protected String queueId;
  // Maximum number of retries before taking bold actions
  private int maxRetriesMultiplier;
  // Indicates if this particular source is running
  private volatile boolean sourceRunning = false;
  // Metrics for this source
  private MetricsSource metrics;
  // WARN threshold for the number of queued logs, defaults to 2
  private int logQueueWarnThreshold;
  // ReplicationEndpoint which will handle the actual replication
  private volatile ReplicationEndpoint replicationEndpoint;
  // A filter (or a chain of filters) for the WAL entries.
  protected volatile WALEntryFilter walEntryFilter;
  // throttler
  private ReplicationThrottler throttler;
  private long defaultBandwidth;
  private long currentBandwidth;
  private WALFileLengthProvider walFileLengthProvider;

  @VisibleForTesting
  protected final Map<String, ReplicationSourceShipper> workerThreads =
      new ConcurrentHashMap<>();

  private final ListeningExecutorService executorService =
      MoreExecutors.listeningDecorator(Executors.newCachedThreadPool(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("replicationSource-%d")
            .setUncaughtExceptionHandler(this::uncaughtException).build()));

  private AtomicLong totalBufferUsed;

  public static final String WAIT_ON_ENDPOINT_SECONDS =
    "hbase.replication.wait.on.endpoint.seconds";
  public static final int DEFAULT_WAIT_ON_ENDPOINT_SECONDS = 30;
  private int waitOnEndpointSeconds = -1;

  private Thread initThread;

  /**
   * Instantiation method used by region servers
   * @param conf configuration to use
   * @param fs file system to use
   * @param manager replication manager to ping to
   * @param server the server for this region server
   * @param queueId the id of our replication queue
   * @param clusterId unique UUID for the cluster
   * @param metrics metrics for replication source
   */
  @Override
  public void init(Configuration conf, FileSystem fs, ReplicationSourceManager manager,
      ReplicationQueueStorage queueStorage, ReplicationPeer replicationPeer, Server server,
      String queueId, UUID clusterId, WALFileLengthProvider walFileLengthProvider,
      MetricsSource metrics) throws IOException {
    this.server = server;
    this.conf = HBaseConfiguration.create(conf);
    this.waitOnEndpointSeconds =
      this.conf.getInt(WAIT_ON_ENDPOINT_SECONDS, DEFAULT_WAIT_ON_ENDPOINT_SECONDS);
    decorateConf();
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);    // 1 second
    this.maxRetriesMultiplier =
        this.conf.getInt("replication.source.maxretriesmultiplier", 300); // 5 minutes @ 1 sec per
    this.queueSizePerGroup = this.conf.getInt("hbase.regionserver.maxlogs", 32);
    this.queueStorage = queueStorage;
    this.replicationPeer = replicationPeer;
    this.manager = manager;
    this.fs = fs;
    this.metrics = metrics;
    this.clusterId = clusterId;

    this.queueId = queueId;
    this.replicationQueueInfo = new ReplicationQueueInfo(queueId);
    this.logQueueWarnThreshold = this.conf.getInt("replication.source.log.queue.warn", 2);

    defaultBandwidth = this.conf.getLong("replication.source.per.peer.node.bandwidth", 0);
    currentBandwidth = getCurrentBandwidth();
    this.throttler = new ReplicationThrottler((double) currentBandwidth / 10.0);
    this.totalBufferUsed = manager.getTotalBufferUsed();
    this.walFileLengthProvider = walFileLengthProvider;
    LOG.info("queueId={}, ReplicationSource: {}, currentBandwidth={}", queueId,
      replicationPeer.getId(), this.currentBandwidth);
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
      // make sure that we do not use an empty queue when setting up a ReplicationSource, otherwise
      // the shipper may quit immediately
      queue.put(log);
      queues.put(logPrefix, queue);
      if (this.isSourceActive() && this.walEntryFilter != null) {
        // new wal group observed after source startup, start a new worker thread to track it
        // notice: it's possible that log enqueued when this.running is set but worker thread
        // still not launched, so it's necessary to check workerThreads before start the worker
        tryStartNewShipper(logPrefix, queue);
      }
    } else {
      queue.put(log);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("{} Added log file {} to queue of source {}.", logPeerId(), logPrefix,
        this.replicationQueueInfo.getQueueId());
    }
    this.metrics.incrSizeOfLogQueue();
    // This will log a warning for each new log that gets created above the warn threshold
    int queueSize = queue.size();
    if (queueSize > this.logQueueWarnThreshold) {
      LOG.warn("{} WAL group {} queue size: {} exceeds value of "
        + "replication.source.log.queue.warn: {}", logPeerId(),
        logPrefix, queueSize, logQueueWarnThreshold);
    }
  }

  @Override
  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
      throws ReplicationException {
    String peerId = replicationPeer.getId();
    Map<TableName, List<String>> tableCFMap = replicationPeer.getTableCFs();
    if (tableCFMap != null) {
      List<String> tableCfs = tableCFMap.get(tableName);
      if (tableCFMap.containsKey(tableName)
          && (tableCfs == null || tableCfs.contains(Bytes.toString(family)))) {
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

  private ReplicationEndpoint createReplicationEndpoint()
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
    RegionServerCoprocessorHost rsServerHost = null;
    if (server instanceof HRegionServer) {
      rsServerHost = ((HRegionServer) server).getRegionServerCoprocessorHost();
    }
    String replicationEndpointImpl = replicationPeer.getPeerConfig().getReplicationEndpointImpl();

    ReplicationEndpoint replicationEndpoint;
    if (replicationEndpointImpl == null) {
      // Default to HBase inter-cluster replication endpoint; skip reflection
      replicationEndpoint = new HBaseInterClusterReplicationEndpoint();
    } else {
      try {
        replicationEndpoint = Class.forName(replicationEndpointImpl)
            .asSubclass(ReplicationEndpoint.class)
            .getDeclaredConstructor()
            .newInstance();
      } catch (NoSuchMethodException | InvocationTargetException e) {
        throw new IllegalArgumentException(e);
      }
    }
    if (rsServerHost != null) {
      ReplicationEndpoint newReplicationEndPoint =
        rsServerHost.postCreateReplicationEndPoint(replicationEndpoint);
      if (newReplicationEndPoint != null) {
        // Override the newly created endpoint from the hook with configured end point
        replicationEndpoint = newReplicationEndPoint;
      }
    }
    return replicationEndpoint;
  }

  private void initAndStartReplicationEndpoint(ReplicationEndpoint replicationEndpoint)
      throws IOException, TimeoutException {
    TableDescriptors tableDescriptors = null;
    if (server instanceof HRegionServer) {
      tableDescriptors = ((HRegionServer) server).getTableDescriptors();
    }
    replicationEndpoint
      .init(new ReplicationEndpoint.Context(server, conf, replicationPeer.getConfiguration(), fs,
        replicationPeer.getId(), clusterId, replicationPeer, metrics, tableDescriptors, server));
    replicationEndpoint.start();
    replicationEndpoint.awaitRunning(waitOnEndpointSeconds, TimeUnit.SECONDS);
  }

  private void initializeWALEntryFilter(UUID peerClusterId) {
    // get the WALEntryFilter from ReplicationEndpoint and add it to default filters
    ArrayList<WALEntryFilter> filters =
      Lists.<WALEntryFilter> newArrayList(new SystemTableWALEntryFilter());
    WALEntryFilter filterFromEndpoint = this.replicationEndpoint.getWALEntryfilter();
    if (filterFromEndpoint != null) {
      filters.add(filterFromEndpoint);
    }
    filters.add(new ClusterMarkingEntryFilter(clusterId, peerClusterId, replicationEndpoint));
    this.walEntryFilter = new ChainWALEntryFilter(filters);
  }

  /**
   * Synchronized method so that only one item is inserted at a time. Should be the only place that
   * insertions are performed on the workerThreads data structure.
   *
   * @param walGroupId The WAL group ID
   * @param queue The queue of paths to process
   */
  private synchronized void tryStartNewShipper(final String walGroupId,
      final PriorityBlockingQueue<Path> queue) {

    if (workerThreads.containsKey(walGroupId)) {
      return;
    }

    LOG.debug("{} Register worker for wallGroupID {}", logPeerId(), walGroupId);
    ReplicationSourceShipper worker = createNewShipper(walGroupId, queue);

    ReplicationSourceWALReader walReader =
        createNewWALReader(walGroupId, queue, worker.getStartPosition());

    // Worker will stop walReader when it stops
    worker.setWALReader(walReader);

    workerThreads.put(walGroupId, worker);

    // Kick of the WAL reader first to get a head start
    executorService.submit(walReader);

    // Add hook to unregister worker when it completes
    // Be mindful. Listener may run in this thread or in the worker thread, just depends on if the
    // worker finishes quickly, before the listener is even applied. If it is run in this thread, be
    // careful not to dead lock on something.
    ListenableFuture<ReplicationSourceShipper.WorkerState> f = executorService.submit(worker);
    Futures.addCallback(f, new FutureCallback<ReplicationSourceShipper.WorkerState>() {
      public void onSuccess(ReplicationSourceShipper.WorkerState state) {
        LOG.debug("ReplicationSourceShipper finished: [state={}]", state);
        if (state == WorkerState.FINISHED) {
          LOG.debug("Unregister worker node for wallGroupID: {}", walGroupId);
          workerThreads.remove(walGroupId);
          tryFinish();
        }
      }

      public void onFailure(Throwable thrown) {
        LOG.warn("Thread failed", thrown);
      }
    }, MoreExecutors.directExecutor());
  }

  protected void tryFinish() {
  }

  @Override
  public Map<String, ReplicationStatus> getWalGroupStatus() {
    Map<String, ReplicationStatus> sourceReplicationStatus = new TreeMap<>();
    long ageOfLastShippedOp, replicationDelay, fileSize;
    for (Map.Entry<String, ReplicationSourceShipper> walGroupShipper : workerThreads.entrySet()) {
      String walGroupId = walGroupShipper.getKey();
      ReplicationSourceShipper shipper = walGroupShipper.getValue();
      ageOfLastShippedOp = metrics.getAgeofLastShippedOp(walGroupId);
      int queueSize = queues.get(walGroupId).size();
      replicationDelay = metrics.getReplicationDelay();
      Path currentPath = shipper.getCurrentPath();
      fileSize = -1;
      if (currentPath != null) {
        try {
          fileSize = getFileSize(currentPath);
        } catch (IOException e) {
          LOG.warn("Ignore the exception as the file size of HLog only affects the web ui", e);
        }
      } else {
        currentPath = new Path("NO_LOGS_IN_QUEUE");
        LOG.warn("{} No replication ongoing, waiting for new log", logPeerId());
      }
      ReplicationStatus.ReplicationStatusBuilder statusBuilder = ReplicationStatus.newBuilder();
      statusBuilder.withPeerId(this.getPeerId())
          .withQueueSize(queueSize)
          .withWalGroup(walGroupId)
          .withCurrentPath(currentPath)
          .withCurrentPosition(shipper.getCurrentPosition())
          .withFileSize(fileSize)
          .withAgeOfLastShippedOp(ageOfLastShippedOp)
          .withReplicationDelay(replicationDelay);
      sourceReplicationStatus.put(this.getPeerId() + "=>" + walGroupId, statusBuilder.build());
    }
    return sourceReplicationStatus;
  }

  private long getFileSize(Path currentPath) throws IOException {
    long fileSize;
    try {
      fileSize = fs.getContentSummary(currentPath).getLength();
    } catch (FileNotFoundException e) {
      currentPath = getArchivedLogPath(currentPath, conf);
      fileSize = fs.getContentSummary(currentPath).getLength();
    }
    return fileSize;
  }

  protected ReplicationSourceShipper createNewShipper(String walGroupId,
      PriorityBlockingQueue<Path> queue) {
    return new ReplicationSourceShipper(conf, walGroupId, queue, this);
  }

  private ReplicationSourceWALReader createNewWALReader(String walGroupId,
      PriorityBlockingQueue<Path> queue, long startPosition) {
    return replicationPeer.getPeerConfig().isSerial()
      ? new SerialReplicationSourceWALReader(fs, conf, queue, startPosition, walEntryFilter, this)
      : new ReplicationSourceWALReader(fs, conf, queue, startPosition, walEntryFilter, this);
  }

  protected final void uncaughtException(Thread t, Throwable e) {
    RSRpcServices.exitIfOOME(e);
    LOG.error("Unexpected exception in {} currentPath={}",
      t.getName(), getCurrentPath(), e);
    server.abort("Unexpected exception in " + t.getName(), e);
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
          LOG.trace("{} To sleep {}ms for throttling control", logPeerId(), sleepTicks);
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
      LOG.info("ReplicationSource : {} bandwidth throttling changed, currentBandWidth={}",
        replicationPeer.getId(), currentBandwidth);
    }
  }

  private long getCurrentBandwidth() {
    long peerBandwidth = replicationPeer.getPeerBandwidth();
    // user can set peer bandwidth to 0 to use default bandwidth
    return peerBandwidth != 0 ? peerBandwidth : defaultBandwidth;
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
        LOG.trace("{} {}, sleeping {} times {}",
          logPeerId(), msg, sleepForRetries, sleepMultiplier);
      }
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      LOG.debug("{} Interrupted while sleeping between retries", logPeerId());
      Thread.currentThread().interrupt();
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  private void initialize() {
    int sleepMultiplier = 1;
    while (this.isSourceActive()) {
      ReplicationEndpoint replicationEndpoint;
      try {
        replicationEndpoint = createReplicationEndpoint();
      } catch (Exception e) {
        LOG.warn("{} error creating ReplicationEndpoint, retry", logPeerId(), e);
        if (sleepForRetries("Error creating ReplicationEndpoint", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }

      try {
        initAndStartReplicationEndpoint(replicationEndpoint);
        this.replicationEndpoint = replicationEndpoint;
        break;
      } catch (Exception e) {
        LOG.warn("{} Error starting ReplicationEndpoint, retry", logPeerId(), e);
        replicationEndpoint.stop();
        if (sleepForRetries("Error starting ReplicationEndpoint", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }

    if (!this.isSourceActive()) {
      return;
    }

    sleepMultiplier = 1;
    UUID peerClusterId;
    // delay this until we are in an asynchronous thread
    for (;;) {
      peerClusterId = replicationEndpoint.getPeerUUID();
      if (this.isSourceActive() && peerClusterId == null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("{} Could not connect to Peer ZK. Sleeping for {} millis", logPeerId(),
            (this.sleepForRetries * sleepMultiplier));
        }
        if (sleepForRetries("Cannot contact the peer's zk ensemble", sleepMultiplier)) {
          sleepMultiplier++;
        }
      } else {
        break;
      }
    }

    if(!this.isSourceActive()) {
      return;
    }

    // In rare case, zookeeper setting may be messed up. That leads to the incorrect
    // peerClusterId value, which is the same as the source clusterId
    if (clusterId.equals(peerClusterId) && !replicationEndpoint.canReplicateToSameCluster()) {
      this.terminate("ClusterId " + clusterId + " is replicating to itself: peerClusterId "
          + peerClusterId + " which is not allowed by ReplicationEndpoint:"
          + replicationEndpoint.getClass().getName(), null, false);
      this.manager.removeSource(this);
      return;
    }
    LOG.info("{} Source: {}, is now replicating from cluster: {}; to peer cluster: {};",
      logPeerId(), this.replicationQueueInfo.getQueueId(), clusterId, peerClusterId);

    initializeWALEntryFilter(peerClusterId);

    // start workers
    for (Map.Entry<String, PriorityBlockingQueue<Path>> entry : queues.entrySet()) {
      String walGroupId = entry.getKey();
      PriorityBlockingQueue<Path> queue = entry.getValue();
      tryStartNewShipper(walGroupId, queue);
    }
  }

  @Override
  public void startup() {
    // mark we are running now
    this.sourceRunning = true;
    initThread = new Thread(this::initialize);
    Threads.setDaemonThreadRunning(initThread,
      Thread.currentThread().getName() + ".replicationSource," + this.queueId,
      this::uncaughtException);
  }

  @Override
  public void terminate(String reason) {
    terminate(reason, null);
  }

  @Override
  public void terminate(String reason, Exception cause) {
    terminate(reason, cause, true);
  }

  @Override
  public void terminate(String reason, Exception cause, boolean clearMetrics) {
    if (cause == null) {
      LOG.info("{} Closing source {} because: {}", logPeerId(), this.queueId, reason);
    } else {
      LOG.error("{} Closing source {} because an error occurred: {}",
        logPeerId(), this.queueId, reason, cause);
    }
    this.sourceRunning = false;
    if (initThread != null && Thread.currentThread() != initThread) {
      // This usually won't happen but anyway, let's wait until the initialization thread exits.
      // And notice that we may call terminate directly from the initThread so here we need to
      // avoid join on ourselves.
      initThread.interrupt();
      Threads.shutdown(initThread, this.sleepForRetries);
    }

    final boolean shutdownSuccess = MoreExecutors.shutdownAndAwaitTermination(this.executorService,
      10L, TimeUnit.SECONDS);
    if (!shutdownSuccess) {
      LOG.info("{} Unable to shutdown thread pool completely", logPeerId());
    }

    if (this.replicationEndpoint != null) {
      this.replicationEndpoint.stop();
      try {
        this.replicationEndpoint.awaitTerminated(sleepForRetries * maxRetriesMultiplier,
          TimeUnit.MILLISECONDS);
      } catch (TimeoutException te) {
        LOG.warn("{} Got exception while waiting for endpoint to shutdown "
            + "for replication source : {}",
          logPeerId(), this.queueId, te);
      }
    }
    if (clearMetrics) {
      this.metrics.clear();
    }
  }

  @Override
  public String getQueueId() {
    return this.queueId;
  }

  @Override
  public Path getCurrentPath() {
    // only for testing
    for (ReplicationSourceShipper worker : workerThreads.values()) {
      if (worker.getCurrentPath() != null) {
        return worker.getCurrentPath();
      }
    }
    return null;
  }

  @Override
  public boolean isSourceActive() {
    return !this.server.isStopped() && this.sourceRunning;
  }

  public UUID getPeerClusterUUID(){
    return this.clusterId;
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
     * <p>
     * Split a path to get the start time
     * </p>
     * <p>
     * For example: 10.20.20.171%3A60020.1277499063250
     * </p>
     * @param p path to split
     * @return start time
     */
    private static long getTS(Path p) {
      return AbstractFSWALProvider.getWALStartTimeFromWALName(p.getName());
    }
  }

  public ReplicationQueueInfo getReplicationQueueInfo() {
    return replicationQueueInfo;
  }

  public boolean isWorkerRunning() {
    return !this.workerThreads.isEmpty();
  }

  @Override
  public String getStats() {
    StringBuilder sb = new StringBuilder();
    sb.append("Total replicated edits: ").append(totalReplicatedEdits)
        .append(", current progress: \n");
    for (Map.Entry<String, ReplicationSourceShipper> entry : workerThreads.entrySet()) {
      String walGroupId = entry.getKey();
      ReplicationSourceShipper worker = entry.getValue();
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
  //offsets totalBufferUsed by deducting shipped batchSize.
  public void postShipEdits(List<Entry> entries, int batchSize) {
    if (throttler.isEnabled()) {
      throttler.addPushSize(batchSize);
    }
    totalReplicatedEdits.addAndGet(entries.size());
    totalBufferUsed.addAndGet(-batchSize);
  }

  @Override
  public WALFileLengthProvider getWALFileLengthProvider() {
    return walFileLengthProvider;
  }

  @Override
  public ServerName getServerWALsBelongTo() {
    return server.getServerName();
  }

  @Override
  public ReplicationPeer getPeer() {
    return replicationPeer;
  }

  Server getServer() {
    return server;
  }

  ReplicationQueueStorage getQueueStorage() {
    return queueStorage;
  }

  private String logPeerId(){
    return "[Source for peer " + this.getPeer().getId() + "]:";
  }
}
