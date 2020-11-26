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

import static org.apache.hadoop.hbase.wal.AbstractFSWALProvider.getArchivedLogPath;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Class that handles the source of a replication stream.
 * Currently does not handle more than 1 slave cluster.
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

  private boolean abortOnError;
  //This is needed for the startup loop to identify when there's already
  //an initialization happening (but not finished yet),
  //so that it doesn't try submit another initialize thread.
  //NOTE: this should only be set to false at the end of initialize method, prior to return.
  private AtomicBoolean startupOngoing = new AtomicBoolean(false);
  //Flag that signalizes uncaught error happening while starting up the source
  // and a retry should be attempted
  private AtomicBoolean retryStartup = new AtomicBoolean(false);

  /**
   * A filter (or a chain of filters) for WAL entries; filters out edits.
   */
  protected volatile WALEntryFilter walEntryFilter;

  // throttler
  private ReplicationThrottler throttler;
  private long defaultBandwidth;
  private long currentBandwidth;
  private WALFileLengthProvider walFileLengthProvider;
  protected final ConcurrentHashMap<String, ReplicationSourceShipper> workerThreads =
      new ConcurrentHashMap<>();

  private AtomicLong totalBufferUsed;

  public static final String WAIT_ON_ENDPOINT_SECONDS =
    "hbase.replication.wait.on.endpoint.seconds";
  public static final int DEFAULT_WAIT_ON_ENDPOINT_SECONDS = 30;
  private int waitOnEndpointSeconds = -1;

  private Thread initThread;

  /**
   * WALs to replicate.
   * Predicate that returns 'true' for WALs to replicate and false for WALs to skip.
   */
  private final Predicate<Path> filterInWALs;

  /**
   * Base WALEntry filters for this class. Unmodifiable. Set on construction.
   * Filters *out* edits we do not want replicated, passed on to replication endpoints.
   * This is the basic set. Down in #initializeWALEntryFilter this set is added to the end of
   * the WALEntry filter chain. These are put after those that we pick up from the configured
   * endpoints and other machinations to create the final {@link #walEntryFilter}.
   * @see WALEntryFilter
   */
  private final List<WALEntryFilter> baseFilterOutWALEntries;

  ReplicationSource() {
    // Default, filters *in* all WALs but meta WALs & filters *out* all WALEntries of System Tables.
    this(p -> !AbstractFSWALProvider.isMetaFile(p),
      Lists.newArrayList(new SystemTableWALEntryFilter()));
  }

  /**
   * @param replicateWAL Pass a filter to run against WAL Path; filter *in* WALs to Replicate;
   *   i.e. return 'true' if you want to replicate the content of the WAL.
   * @param baseFilterOutWALEntries Base set of filters you want applied always; filters *out*
   *   WALEntries so they never make it out of this ReplicationSource.
   */
  ReplicationSource(Predicate<Path> replicateWAL, List<WALEntryFilter> baseFilterOutWALEntries) {
    this.filterInWALs = replicateWAL;
    this.baseFilterOutWALEntries = Collections.unmodifiableList(baseFilterOutWALEntries);
  }

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

    // A defaultBandwidth of '0' means no bandwidth; i.e. no throttling.
    defaultBandwidth = this.conf.getLong("replication.source.per.peer.node.bandwidth", 0);
    currentBandwidth = getCurrentBandwidth();
    this.throttler = new ReplicationThrottler((double) currentBandwidth / 10.0);
    this.totalBufferUsed = manager.getTotalBufferUsed();
    this.walFileLengthProvider = walFileLengthProvider;

    this.abortOnError = this.conf.getBoolean("replication.source.regionserver.abort",
      true);

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
  public void enqueueLog(Path wal) {
    if (!this.filterInWALs.test(wal)) {
      LOG.trace("NOT replicating {}", wal);
      return;
    }
    // Use WAL prefix as the WALGroupId for this peer.
    String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal.getName());
    PriorityBlockingQueue<Path> queue = queues.get(walPrefix);
    if (queue == null) {
      queue = new PriorityBlockingQueue<>(queueSizePerGroup,
        new AbstractFSWALProvider.WALStartTimeComparator());
      // make sure that we do not use an empty queue when setting up a ReplicationSource, otherwise
      // the shipper may quit immediately
      queue.put(wal);
      queues.put(walPrefix, queue);
      if (this.isSourceActive() && this.walEntryFilter != null) {
        // new wal group observed after source startup, start a new worker thread to track it
        // notice: it's possible that wal enqueued when this.running is set but worker thread
        // still not launched, so it's necessary to check workerThreads before start the worker
        tryStartNewShipper(walPrefix, queue);
      }
    } else {
      queue.put(wal);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("{} Added wal {} to queue of source {}.", logPeerId(), walPrefix,
        this.replicationQueueInfo.getQueueId());
    }
    this.metrics.incrSizeOfLogQueue();
    // This will wal a warning for each new wal that gets created above the warn threshold
    int queueSize = queue.size();
    if (queueSize > this.logQueueWarnThreshold) {
      LOG.warn("{} WAL group {} queue size: {} exceeds value of " +
          "replication.source.log.queue.warn {}", logPeerId(), walPrefix, queueSize,
        logQueueWarnThreshold);
    }
  }

  @Override
  public void addHFileRefs(TableName tableName, byte[] family, List<Pair<Path, Path>> pairs)
      throws ReplicationException {
    String peerId = replicationPeer.getId();
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
    List<WALEntryFilter> filters = new ArrayList<>(this.baseFilterOutWALEntries);
    WALEntryFilter filterFromEndpoint = this.replicationEndpoint.getWALEntryfilter();
    if (filterFromEndpoint != null) {
      filters.add(filterFromEndpoint);
    }
    filters.add(new ClusterMarkingEntryFilter(clusterId, peerClusterId, replicationEndpoint));
    this.walEntryFilter = new ChainWALEntryFilter(filters);
  }

  private void tryStartNewShipper(String walGroupId, PriorityBlockingQueue<Path> queue) {
    workerThreads.compute(walGroupId, (key, value) -> {
      if (value != null) {
        LOG.debug("{} preempted start of shipping worker walGroupId={}", logPeerId(), walGroupId);
        return value;
      } else {
        LOG.debug("{} starting shipping worker for walGroupId={}", logPeerId(), walGroupId);
        ReplicationSourceShipper worker = createNewShipper(walGroupId, queue);
        ReplicationSourceWALReader walReader =
            createNewWALReader(walGroupId, queue, worker.getStartPosition());
        Threads.setDaemonThreadRunning(
            walReader, Thread.currentThread().getName()
            + ".replicationSource.wal-reader." + walGroupId + "," + queueId,
          (t,e) -> this.uncaughtException(t, e, this.manager, this.getPeerId()));
        worker.setWALReader(walReader);
        worker.startup((t,e) -> this.uncaughtException(t, e, this.manager, this.getPeerId()));
        return worker;
      }
    });
  }

  @Override
  public Map<String, ReplicationStatus> getWalGroupStatus() {
    Map<String, ReplicationStatus> sourceReplicationStatus = new TreeMap<>();
    long ageOfLastShippedOp, replicationDelay, fileSize;
    for (Map.Entry<String, ReplicationSourceShipper> walGroupShipper : workerThreads.entrySet()) {
      String walGroupId = walGroupShipper.getKey();
      ReplicationSourceShipper shipper = walGroupShipper.getValue();
      ageOfLastShippedOp = metrics.getAgeOfLastShippedOp(walGroupId);
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

  /**
   * Call after {@link #initializeWALEntryFilter(UUID)} else it will be null.
   * @return WAL Entry Filter Chain to use on WAL files filtering *out* WALEntry edits.
   */
  WALEntryFilter getWalEntryFilter() {
    return walEntryFilter;
  }

  protected final void uncaughtException(Thread t, Throwable e,
      ReplicationSourceManager manager, String peerId) {
    RSRpcServices.exitIfOOME(e);
    LOG.error("Unexpected exception in {} currentPath={}",
      t.getName(), getCurrentPath(), e);
    if(abortOnError){
      server.abort("Unexpected exception in " + t.getName(), e);
    }
    if(manager != null){
      while (true) {
        try {
          LOG.info("Refreshing replication sources now due to previous error on thread: {}",
            t.getName());
          manager.refreshSources(peerId);
          break;
        } catch (IOException e1) {
          LOG.error("Replication sources refresh failed.", e1);
          sleepForRetries("Sleeping before try refreshing sources again",
            maxRetriesMultiplier);
        }
      }
    }
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
    // User can set peer bandwidth to 0 to use default bandwidth.
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
      if(LOG.isDebugEnabled()) {
        LOG.debug("{} Interrupted while sleeping between retries", logPeerId());
      }
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
        } else {
          retryStartup.set(!this.abortOnError);
          this.startupOngoing.set(false);
          throw new RuntimeException("Exhausted retries to start replication endpoint.");
        }
      }
    }

    if (!this.isSourceActive()) {
      retryStartup.set(!this.abortOnError);
      this.startupOngoing.set(false);
      throw new IllegalStateException("Source should be active.");
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
      retryStartup.set(!this.abortOnError);
      this.startupOngoing.set(false);
      throw new IllegalStateException("Source should be active.");
    }
    LOG.info("{} queueId={} (queues={}) is replicating from cluster={} to cluster={}",
      logPeerId(), this.replicationQueueInfo.getQueueId(), this.queues.size(), clusterId,
      peerClusterId);
    initializeWALEntryFilter(peerClusterId);
    // Start workers
    for (Map.Entry<String, PriorityBlockingQueue<Path>> entry : queues.entrySet()) {
      String walGroupId = entry.getKey();
      PriorityBlockingQueue<Path> queue = entry.getValue();
      tryStartNewShipper(walGroupId, queue);
    }
    this.startupOngoing.set(false);
  }

  @Override
  public ReplicationSourceInterface startup() {
    if (this.sourceRunning) {
      return this;
    }
    this.sourceRunning = true;
    startupOngoing.set(true);
    initThread = new Thread(this::initialize);
    Threads.setDaemonThreadRunning(initThread,
      Thread.currentThread().getName() + ".replicationSource," + this.queueId,
      (t,e) -> {
        //if first initialization attempt failed, and abortOnError is false, we will
        //keep looping in this thread until initialize eventually succeeds,
        //while the server main startup one can go on with its work.
        sourceRunning = false;
        uncaughtException(t, e, null, null);
        retryStartup.set(!this.abortOnError);
        do {
          if(retryStartup.get()) {
            this.sourceRunning = true;
            startupOngoing.set(true);
            retryStartup.set(false);
            try {
              initialize();
            } catch(Throwable error){
              sourceRunning = false;
              uncaughtException(t, error, null, null);
              retryStartup.set(!this.abortOnError);
            }
          }
        } while ((this.startupOngoing.get() || this.retryStartup.get()) && !this.abortOnError);
      });
    return this;
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
    terminate(reason, cause, clearMetrics, true);
  }

  public void terminate(String reason, Exception cause, boolean clearMetrics,
      boolean join) {
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
    Collection<ReplicationSourceShipper> workers = workerThreads.values();
    for (ReplicationSourceShipper worker : workers) {
      worker.stopWorker();
      if(worker.entryReader != null) {
        worker.entryReader.setReaderRunning(false);
      }
    }

    if (this.replicationEndpoint != null) {
      this.replicationEndpoint.stop();
    }
    for (ReplicationSourceShipper worker : workers) {
      if (worker.isAlive() || worker.entryReader.isAlive()) {
        try {
          // Wait worker to stop
          Thread.sleep(this.sleepForRetries);
        } catch (InterruptedException e) {
          LOG.info("{} Interrupted while waiting {} to stop", logPeerId(), worker.getName());
          Thread.currentThread().interrupt();
        }
        // If worker still is alive after waiting, interrupt it
        if (worker.isAlive()) {
          worker.interrupt();
        }
        // If entry reader is alive after waiting, interrupt it
        if (worker.entryReader.isAlive()) {
          worker.entryReader.interrupt();
        }
      }
    }

    if (join) {
      for (ReplicationSourceShipper worker : workers) {
        Threads.shutdown(worker, this.sleepForRetries);
        LOG.info("{} ReplicationSourceWorker {} terminated", logPeerId(), worker.getName());
      }
      if (this.replicationEndpoint != null) {
        try {
          this.replicationEndpoint.awaitTerminated(sleepForRetries * maxRetriesMultiplier,
            TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
          LOG.warn("{} Got exception while waiting for endpoint to shutdown "
            + "for replication source : {}", logPeerId(), this.queueId, te);
        }
      }
    }
    if (clearMetrics) {
      // Can be null in test context.
      if (this.metrics != null) {
        this.metrics.clear();
      }
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

  public ReplicationQueueInfo getReplicationQueueInfo() {
    return replicationQueueInfo;
  }

  public boolean isWorkerRunning(){
    for(ReplicationSourceShipper worker : this.workerThreads.values()){
      if(worker.isActive()){
        return worker.isActive();
      }
    }
    return false;
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
    long newBufferUsed = totalBufferUsed.addAndGet(-batchSize);
    // Record the new buffer usage
    this.manager.getGlobalMetrics().setWALReaderEditsBufferBytes(newBufferUsed);
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

  @Override
  public ReplicationQueueStorage getReplicationQueueStorage() {
    return queueStorage;
  }

  void removeWorker(ReplicationSourceShipper worker) {
    workerThreads.remove(worker.walGroupId, worker);
  }

  private String logPeerId(){
    return "peerId=" + this.getPeerId() + ",";
  }
}
