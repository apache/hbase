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
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSinkManager.SinkPeer;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * A {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint}
 * implementation for replicating to another HBase cluster.
 * For the slave cluster it selects a random number of peers
 * using a replication ratio. For example, if replication ration = 0.1
 * and slave cluster has 100 region servers, 10 will be selected.
 * <p>
 * A stream is considered down when we cannot contact a region server on the
 * peer cluster for more than 55 seconds by default.
 * </p>
 */
@InterfaceAudience.Private
public class HBaseInterClusterReplicationEndpoint extends HBaseReplicationEndpoint {
  private static final Logger LOG =
      LoggerFactory.getLogger(HBaseInterClusterReplicationEndpoint.class);

  private static final long DEFAULT_MAX_TERMINATION_WAIT_MULTIPLIER = 2;

  private AsyncClusterConnection conn;
  private Configuration conf;
  // How long should we sleep for each retry
  private long sleepForRetries;
  // Maximum number of retries before taking bold actions
  private int maxRetriesMultiplier;
  // Socket timeouts require even bolder actions since we don't want to DDOS
  private int socketTimeoutMultiplier;
  // Amount of time for shutdown to wait for all tasks to complete
  private long maxTerminationWait;
  // Size limit for replication RPCs, in bytes
  private int replicationRpcLimit;
  //Metrics for this source
  private MetricsSource metrics;
  // Handles connecting to peer region servers
  private ReplicationSinkManager replicationSinkMgr;
  private boolean peersSelected = false;
  private String replicationClusterId = "";
  private ThreadPoolExecutor exec;
  private int maxThreads;
  private Path baseNamespaceDir;
  private Path hfileArchiveDir;
  private boolean replicationBulkLoadDataEnabled;
  private Abortable abortable;
  private boolean dropOnDeletedTables;
  private boolean isSerial = false;

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    this.conf = HBaseConfiguration.create(ctx.getConfiguration());
    decorateConf();
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    this.socketTimeoutMultiplier = this.conf.getInt("replication.source.socketTimeoutMultiplier",
        maxRetriesMultiplier);
    // A Replicator job is bound by the RPC timeout. We will wait this long for all Replicator
    // tasks to terminate when doStop() is called.
    long maxTerminationWaitMultiplier = this.conf.getLong(
        "replication.source.maxterminationmultiplier",
        DEFAULT_MAX_TERMINATION_WAIT_MULTIPLIER);
    this.maxTerminationWait = maxTerminationWaitMultiplier *
        this.conf.getLong(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    // TODO: This connection is replication specific or we should make it particular to
    // replication and make replication specific settings such as compression or codec to use
    // passing Cells.
    this.conn =
      ClusterConnectionFactory.createAsyncClusterConnection(conf, null, User.getCurrent());
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);
    this.metrics = context.getMetrics();
    // ReplicationQueueInfo parses the peerId out of the znode for us
    this.replicationSinkMgr = new ReplicationSinkManager(conn, this, this.conf);
    // per sink thread pool
    this.maxThreads = this.conf.getInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
      HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT);
    this.exec = new ThreadPoolExecutor(maxThreads, maxThreads, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    this.exec.allowCoreThreadTimeOut(true);
    this.abortable = ctx.getAbortable();
    // Set the size limit for replication RPCs to 95% of the max request size.
    // We could do with less slop if we have an accurate estimate of encoded size. Being
    // conservative for now.
    this.replicationRpcLimit = (int)(0.95 * conf.getLong(RpcServer.MAX_REQUEST_SIZE,
      RpcServer.DEFAULT_MAX_REQUEST_SIZE));
    this.dropOnDeletedTables =
        this.conf.getBoolean(HConstants.REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);

    this.replicationBulkLoadDataEnabled =
        conf.getBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY,
          HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    if (this.replicationBulkLoadDataEnabled) {
      replicationClusterId = this.conf.get(HConstants.REPLICATION_CLUSTER_ID);
    }
    // Construct base namespace directory and hfile archive directory path
    Path rootDir = FSUtils.getRootDir(conf);
    Path baseNSDir = new Path(HConstants.BASE_NAMESPACE_DIR);
    baseNamespaceDir = new Path(rootDir, baseNSDir);
    hfileArchiveDir = new Path(rootDir, new Path(HConstants.HFILE_ARCHIVE_DIRECTORY, baseNSDir));
    isSerial = context.getPeerConfig().isSerial();
  }

  private void decorateConf() {
    String replicationCodec = this.conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);
    if (StringUtils.isNotEmpty(replicationCodec)) {
      this.conf.set(HConstants.RPC_CODEC_CONF_KEY, replicationCodec);
    }
  }

  private void connectToPeers() {
    getRegionServers();

    int sleepMultiplier = 1;

    // Connect to peer cluster first, unless we have to stop
    while (this.isRunning() && replicationSinkMgr.getNumSinks() == 0) {
      replicationSinkMgr.chooseSinks();
      if (this.isRunning() && replicationSinkMgr.getNumSinks() == 0) {
        if (sleepForRetries("Waiting for peers", sleepMultiplier)) {
          sleepMultiplier++;
        }
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
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  private int getEstimatedEntrySize(Entry e) {
    long size = e.getKey().estimatedSerializedSizeOf() + e.getEdit().estimatedSerializedSizeOf();
    return (int) size;
  }

  private List<List<Entry>> createParallelBatches(final List<Entry> entries) {
    int numSinks = Math.max(replicationSinkMgr.getNumSinks(), 1);
    int n = Math.min(Math.min(this.maxThreads, entries.size() / 100 + 1), numSinks);
    List<List<Entry>> entryLists =
        Stream.generate(ArrayList<Entry>::new).limit(n).collect(Collectors.toList());
    int[] sizes = new int[n];
    for (Entry e : entries) {
      int index = Math.abs(Bytes.hashCode(e.getKey().getEncodedRegionName()) % n);
      int entrySize = getEstimatedEntrySize(e);
      // If this batch has at least one entry and is over sized, move it to the tail of list and
      // initialize the entryLists[index] to be a empty list.
      if (sizes[index] > 0 && sizes[index] + entrySize > replicationRpcLimit) {
        entryLists.add(entryLists.get(index));
        entryLists.set(index, new ArrayList<>());
        sizes[index] = 0;
      }
      entryLists.get(index).add(e);
      sizes[index] += entrySize;
    }
    return entryLists;
  }

  private List<List<Entry>> createSerialBatches(final List<Entry> entries) {
    Map<byte[], List<Entry>> regionEntries = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Entry e : entries) {
      regionEntries.computeIfAbsent(e.getKey().getEncodedRegionName(), key -> new ArrayList<>())
          .add(e);
    }
    return new ArrayList<>(regionEntries.values());
  }

  /**
   * Divide the entries into multiple batches, so that we can replicate each batch in a thread pool
   * concurrently. Note that, for serial replication, we need to make sure that entries from the
   * same region to be replicated serially, so entries from the same region consist of a batch, and
   * we will divide a batch into several batches by replicationRpcLimit in method
   * serialReplicateRegionEntries()
   */
  private List<List<Entry>> createBatches(final List<Entry> entries) {
    if (isSerial) {
      return createSerialBatches(entries);
    } else {
      return createParallelBatches(entries);
    }
  }

  private TableName parseTable(String msg) {
    // ... TableNotFoundException: '<table>'/n...
    Pattern p = Pattern.compile("TableNotFoundException: '([\\S]*)'");
    Matcher m = p.matcher(msg);
    if (m.find()) {
      String table = m.group(1);
      try {
        // double check that table is a valid table name
        TableName.valueOf(TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(table)));
        return TableName.valueOf(table);
      } catch (IllegalArgumentException ignore) {
      }
    }
    return null;
  }

  // Filter a set of batches by TableName
  private List<List<Entry>> filterBatches(final List<List<Entry>> oldEntryList, TableName table) {
    return oldEntryList
        .stream().map(entries -> entries.stream()
            .filter(e -> !e.getKey().getTableName().equals(table)).collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  private void reconnectToPeerCluster() {
    AsyncClusterConnection connection = null;
    try {
      connection =
        ClusterConnectionFactory.createAsyncClusterConnection(conf, null, User.getCurrent());
    } catch (IOException ioe) {
      LOG.warn("Failed to create connection for peer cluster", ioe);
    }
    if (connection != null) {
      this.conn = connection;
    }
  }

  private long parallelReplicate(CompletionService<Integer> pool, ReplicateContext replicateContext,
      List<List<Entry>> batches) throws IOException {
    int futures = 0;
    for (int i = 0; i < batches.size(); i++) {
      List<Entry> entries = batches.get(i);
      if (!entries.isEmpty()) {
        LOG.trace("Submitting {} entries of total size {}", entries.size(),
          replicateContext.getSize());
        // RuntimeExceptions encountered here bubble up and are handled in ReplicationSource
        pool.submit(createReplicator(entries, i));
        futures++;
      }
    }

    IOException iox = null;
    long lastWriteTime = 0;
    for (int i = 0; i < futures; i++) {
      try {
        // wait for all futures, remove successful parts
        // (only the remaining parts will be retried)
        Future<Integer> f = pool.take();
        int index = f.get();
        List<Entry> batch = batches.get(index);
        batches.set(index, Collections.emptyList()); // remove successful batch
        // Find the most recent write time in the batch
        long writeTime = batch.get(batch.size() - 1).getKey().getWriteTime();
        if (writeTime > lastWriteTime) {
          lastWriteTime = writeTime;
        }
      } catch (InterruptedException ie) {
        iox = new IOException(ie);
      } catch (ExecutionException ee) {
        // cause must be an IOException
        iox = (IOException) ee.getCause();
      }
    }
    if (iox != null) {
      // if we had any exceptions, try again
      throw iox;
    }
    return lastWriteTime;
  }

  /**
   * Do the shipping logic
   */
  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    CompletionService<Integer> pool = new ExecutorCompletionService<>(this.exec);
    int sleepMultiplier = 1;

    if (!peersSelected && this.isRunning()) {
      connectToPeers();
      peersSelected = true;
    }

    int numSinks = replicationSinkMgr.getNumSinks();
    if (numSinks == 0) {
      LOG.warn("No replication sinks found, returning without replicating. The source should " +
          "retry with the same set of edits.");
      return false;
    }

    List<List<Entry>> batches = createBatches(replicateContext.getEntries());
    while (this.isRunning() && !exec.isShutdown()) {
      if (!isPeerEnabled()) {
        if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }
      if (this.conn == null) {
        reconnectToPeerCluster();
      }
      try {
        // replicate the batches to sink side.
        parallelReplicate(pool, replicateContext, batches);
        return true;
      } catch (IOException ioe) {
        if (ioe instanceof RemoteException) {
          ioe = ((RemoteException) ioe).unwrapRemoteException();
          LOG.warn("Can't replicate because of an error on the remote cluster: ", ioe);
          if (ioe instanceof TableNotFoundException) {
            if (dropOnDeletedTables) {
              // this is a bit fragile, but cannot change how TNFE is serialized
              // at least check whether the table name is legal
              TableName table = parseTable(ioe.getMessage());
              if (table != null) {
                try (Connection localConn =
                    ConnectionFactory.createConnection(ctx.getLocalConfiguration())) {
                  if (!localConn.getAdmin().tableExists(table)) {
                    // Would potentially be better to retry in one of the outer loops
                    // and add a table filter there; but that would break the encapsulation,
                    // so we're doing the filtering here.
                    LOG.info("Missing table detected at sink, local table also does not exist, " +
                        "filtering edits for '" + table + "'");
                    batches = filterBatches(batches, table);
                    continue;
                  }
                } catch (IOException iox) {
                  LOG.warn("Exception checking for local table: ", iox);
                }
              }
            }
            // fall through and sleep below
          } else {
            LOG.warn("Peer encountered RemoteException, rechecking all sinks: ", ioe);
            replicationSinkMgr.chooseSinks();
          }
        } else {
          if (ioe instanceof SocketTimeoutException) {
            // This exception means we waited for more than 60s and nothing
            // happened, the cluster is alive and calling it right away
            // even for a test just makes things worse.
            sleepForRetries("Encountered a SocketTimeoutException. Since the " +
                  "call to the remote cluster timed out, which is usually " +
                  "caused by a machine failure or a massive slowdown",
              this.socketTimeoutMultiplier);
          } else if (ioe instanceof ConnectException || ioe instanceof UnknownHostException) {
            LOG.warn("Peer is unavailable, rechecking all sinks: ", ioe);
            replicationSinkMgr.chooseSinks();
          } else {
            LOG.warn("Can't replicate because of a local or network error: ", ioe);
          }
        }
        if (sleepForRetries("Since we are unable to replicate", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
    return false; // in case we exited before replicating
  }

  protected boolean isPeerEnabled() {
    return ctx.getReplicationPeer().isPeerEnabled();
  }

  @Override
  protected void doStop() {
    disconnect(); // don't call super.doStop()
    if (this.conn != null) {
      try {
        this.conn.close();
        this.conn = null;
      } catch (IOException e) {
        LOG.warn("Failed to close the connection");
      }
    }
    // Allow currently running replication tasks to finish
    exec.shutdown();
    try {
      exec.awaitTermination(maxTerminationWait, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
    }
    // Abort if the tasks did not terminate in time
    if (!exec.isTerminated()) {
      String errMsg = "HBaseInterClusterReplicationEndpoint termination failed. The " +
          "ThreadPoolExecutor failed to finish all tasks within " + maxTerminationWait + "ms. " +
          "Aborting to prevent Replication from deadlocking. See HBASE-16081.";
      abortable.abort(errMsg, new IOException(errMsg));
    }
    notifyStopped();
  }

  @VisibleForTesting
  protected int replicateEntries(List<Entry> entries, int batchIndex) throws IOException {
    SinkPeer sinkPeer = null;
    try {
      int entriesHashCode = System.identityHashCode(entries);
      if (LOG.isTraceEnabled()) {
        long size = entries.stream().mapToLong(this::getEstimatedEntrySize).sum();
        LOG.trace("Replicating batch {} of {} entries with total size {} bytes to {}",
          entriesHashCode, entries.size(), size, replicationClusterId);
      }
      sinkPeer = replicationSinkMgr.getReplicationSink();
      AsyncRegionServerAdmin rsAdmin = sinkPeer.getRegionServer();
      try {
        ReplicationProtbufUtil.replicateWALEntry(rsAdmin,
          entries.toArray(new Entry[entries.size()]), replicationClusterId, baseNamespaceDir,
          hfileArchiveDir);
        LOG.trace("Completed replicating batch {}", entriesHashCode);
      } catch (IOException e) {
        LOG.trace("Failed replicating batch {}", entriesHashCode, e);
        throw e;
      }
      replicationSinkMgr.reportSinkSuccess(sinkPeer);
    } catch (IOException ioe) {
      if (sinkPeer != null) {
        replicationSinkMgr.reportBadSink(sinkPeer);
      }
      throw ioe;
    }
    return batchIndex;
  }

  private int serialReplicateRegionEntries(List<Entry> entries, int batchIndex)
      throws IOException {
    int batchSize = 0, index = 0;
    List<Entry> batch = new ArrayList<>();
    for (Entry entry : entries) {
      int entrySize = getEstimatedEntrySize(entry);
      if (batchSize > 0 && batchSize + entrySize > replicationRpcLimit) {
        replicateEntries(batch, index++);
        batch.clear();
        batchSize = 0;
      }
      batch.add(entry);
      batchSize += entrySize;
    }
    if (batchSize > 0) {
      replicateEntries(batch, index);
    }
    return batchIndex;
  }

  @VisibleForTesting
  protected Callable<Integer> createReplicator(List<Entry> entries, int batchIndex) {
    return isSerial ? () -> serialReplicateRegionEntries(entries, batchIndex)
        : () -> replicateEntries(entries, batchIndex);
  }
}
