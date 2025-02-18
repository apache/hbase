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
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.ReplicationProtobufUtil;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.wal.WALUtil;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationResult;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterators;
import org.apache.hbase.thirdparty.com.google.common.collect.PeekingIterator;

/**
 * A {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} implementation for replicating
 * to another HBase cluster. For the slave cluster it selects a random number of peers using a
 * replication ratio. For example, if replication ration = 0.1 and slave cluster has 100 region
 * servers, 10 will be selected.
 * <p>
 * A stream is considered down when we cannot contact a region server on the peer cluster for more
 * than 55 seconds by default.
 * </p>
 */
@InterfaceAudience.Private
public class HBaseInterClusterReplicationEndpoint extends HBaseReplicationEndpoint {
  private static final Logger LOG =
    LoggerFactory.getLogger(HBaseInterClusterReplicationEndpoint.class);

  /** Drop edits for tables that been deleted from the replication source and target */
  public static final String REPLICATION_DROP_ON_DELETED_TABLE_KEY =
    "hbase.replication.drop.on.deleted.table";
  /** Drop edits for CFs that been deleted from the replication source and target */
  public static final String REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY =
    "hbase.replication.drop.on.deleted.columnfamily";

  // How long should we sleep for each retry
  private long sleepForRetries;
  // Maximum number of retries before taking bold actions
  private int maxRetriesMultiplier;
  // Socket timeouts require even bolder actions since we don't want to DDOS
  private int socketTimeoutMultiplier;
  // Size limit for replication RPCs, in bytes
  private int replicationRpcLimit;
  // Metrics for this source
  private MetricsSource metrics;
  private boolean peersSelected = false;
  private String replicationClusterId = "";
  private int maxThreads;
  private Path baseNamespaceDir;
  private Path hfileArchiveDir;
  private boolean replicationBulkLoadDataEnabled;
  private boolean dropOnDeletedTables;
  private boolean dropOnDeletedColumnFamilies;
  private boolean isSerial = false;
  // Initialising as 0 to guarantee at least one logging message
  private long lastSinkFetchTime = 0;

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    decorateConf();
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 300);
    this.socketTimeoutMultiplier =
      this.conf.getInt("replication.source.socketTimeoutMultiplier", maxRetriesMultiplier);
    this.sleepForRetries = this.conf.getLong("replication.source.sleepforretries", 1000);
    this.metrics = context.getMetrics();
    // per sink thread pool
    this.maxThreads = this.conf.getInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
      HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT);
    // Set the size limit for replication RPCs to 95% of the max request size.
    // We could do with less slop if we have an accurate estimate of encoded size. Being
    // conservative for now.
    this.replicationRpcLimit =
      (int) (0.95 * conf.getLong(RpcServer.MAX_REQUEST_SIZE, RpcServer.DEFAULT_MAX_REQUEST_SIZE));
    this.dropOnDeletedTables = this.conf.getBoolean(REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);
    this.dropOnDeletedColumnFamilies =
      this.conf.getBoolean(REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY, false);

    this.replicationBulkLoadDataEnabled = conf.getBoolean(
      HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, HConstants.REPLICATION_BULKLOAD_ENABLE_DEFAULT);
    if (this.replicationBulkLoadDataEnabled) {
      replicationClusterId = this.conf.get(HConstants.REPLICATION_CLUSTER_ID);
    }
    // Construct base namespace directory and hfile archive directory path
    Path rootDir = CommonFSUtils.getRootDir(conf);
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
    int sleepMultiplier = 1;
    // Connect to peer cluster first, unless we have to stop
    while (this.isRunning() && getNumSinks() == 0) {
      chooseSinks();
      if (this.isRunning() && getNumSinks() == 0) {
        if (sleepForRetries("Waiting for peers", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
  }

  /**
   * Do the sleeping logic
   * @param msg             Why we sleep
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  private boolean sleepForRetries(String msg, int sleepMultiplier) {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("{} {}, sleeping {} times {}", logPeerId(), msg, sleepForRetries,
          sleepMultiplier);
      }
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} {} Interrupted while sleeping between retries", msg, logPeerId());
      }
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  private int getEstimatedEntrySize(Entry e) {
    long size = e.getKey().estimatedSerializedSizeOf() + e.getEdit().estimatedSerializedSizeOf();
    return (int) size;
  }

  private List<List<Entry>> createParallelBatches(final List<Entry> entries) {
    int numSinks = Math.max(getNumSinks(), 1);
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

  /**
   * Check if there's an {@link TableNotFoundException} in the caused by stacktrace.
   */
  public static boolean isTableNotFoundException(Throwable io) {
    if (io instanceof RemoteException) {
      io = ((RemoteException) io).unwrapRemoteException();
    }
    if (io != null && io.getMessage().contains("TableNotFoundException")) {
      return true;
    }
    for (; io != null; io = io.getCause()) {
      if (io instanceof TableNotFoundException) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if there's an {@link NoSuchColumnFamilyException} in the caused by stacktrace.
   */
  public static boolean isNoSuchColumnFamilyException(Throwable io) {
    if (io instanceof RemoteException) {
      io = ((RemoteException) io).unwrapRemoteException();
    }
    if (io != null && io.getMessage().contains("NoSuchColumnFamilyException")) {
      return true;
    }
    for (; io != null; io = io.getCause()) {
      if (io instanceof NoSuchColumnFamilyException) {
        return true;
      }
    }
    return false;
  }

  List<List<Entry>> filterNotExistTableEdits(final List<List<Entry>> oldEntryList) {
    List<List<Entry>> entryList = new ArrayList<>();
    Map<TableName, Boolean> existMap = new HashMap<>();
    try (Connection localConn = ConnectionFactory.createConnection(ctx.getLocalConfiguration());
      Admin localAdmin = localConn.getAdmin()) {
      for (List<Entry> oldEntries : oldEntryList) {
        List<Entry> entries = new ArrayList<>();
        for (Entry e : oldEntries) {
          TableName tableName = e.getKey().getTableName();
          boolean exist = true;
          if (existMap.containsKey(tableName)) {
            exist = existMap.get(tableName);
          } else {
            try {
              exist = localAdmin.tableExists(tableName);
              existMap.put(tableName, exist);
            } catch (IOException iox) {
              LOG.warn("Exception checking for local table " + tableName, iox);
              // we can't drop edits without full assurance, so we assume table exists.
              exist = true;
            }
          }
          if (exist) {
            entries.add(e);
          } else {
            // Would potentially be better to retry in one of the outer loops
            // and add a table filter there; but that would break the encapsulation,
            // so we're doing the filtering here.
            LOG.warn("Missing table detected at sink, local table also does not exist, "
              + "filtering edits for table '{}'", tableName);
          }
        }
        if (!entries.isEmpty()) {
          entryList.add(entries);
        }
      }
    } catch (IOException iox) {
      LOG.warn("Exception when creating connection to check local table", iox);
      return oldEntryList;
    }
    return entryList;
  }

  List<List<Entry>> filterNotExistColumnFamilyEdits(final List<List<Entry>> oldEntryList) {
    List<List<Entry>> entryList = new ArrayList<>();
    Map<TableName, Set<String>> existColumnFamilyMap = new HashMap<>();
    try (Connection localConn = ConnectionFactory.createConnection(ctx.getLocalConfiguration());
      Admin localAdmin = localConn.getAdmin()) {
      for (List<Entry> oldEntries : oldEntryList) {
        List<Entry> entries = new ArrayList<>();
        for (Entry e : oldEntries) {
          TableName tableName = e.getKey().getTableName();
          if (!existColumnFamilyMap.containsKey(tableName)) {
            try {
              Set<String> cfs = localAdmin.getDescriptor(tableName).getColumnFamilyNames().stream()
                .map(Bytes::toString).collect(Collectors.toSet());
              existColumnFamilyMap.put(tableName, cfs);
            } catch (Exception ex) {
              LOG.warn("Exception getting cf names for local table {}", tableName, ex);
              // if catch any exception, we are not sure about table's description,
              // so replicate raw entry
              entries.add(e);
              continue;
            }
          }

          Set<String> existColumnFamilies = existColumnFamilyMap.get(tableName);
          Set<String> missingCFs = new HashSet<>();
          WALEdit walEdit = new WALEdit();
          walEdit.getCells().addAll(e.getEdit().getCells());
          WALUtil.filterCells(walEdit, cell -> {
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            if (existColumnFamilies.contains(cf)) {
              return cell;
            } else {
              missingCFs.add(cf);
              return null;
            }
          });
          if (!walEdit.isEmpty()) {
            Entry newEntry = new Entry(e.getKey(), walEdit);
            entries.add(newEntry);
          }

          if (!missingCFs.isEmpty()) {
            // Would potentially be better to retry in one of the outer loops
            // and add a table filter there; but that would break the encapsulation,
            // so we're doing the filtering here.
            LOG.warn(
              "Missing column family detected at sink, local column family also does not exist,"
                + " filtering edits for table '{}',column family '{}'",
              tableName, missingCFs);
          }
        }
        if (!entries.isEmpty()) {
          entryList.add(entries);
        }
      }
    } catch (IOException iox) {
      LOG.warn("Exception when creating connection to check local table", iox);
      return oldEntryList;
    }
    return entryList;
  }

  private long parallelReplicate(ReplicateContext replicateContext, List<List<Entry>> batches)
    throws IOException {
    List<CompletableFuture<Integer>> futures =
      new ArrayList<CompletableFuture<Integer>>(batches.size());
    for (int i = 0; i < batches.size(); i++) {
      List<Entry> entries = batches.get(i);
      if (entries.isEmpty()) {
        continue;
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("{} Submitting {} entries of total size {}", logPeerId(), entries.size(),
          replicateContext.getSize());
      }
      // RuntimeExceptions encountered here bubble up and are handled in ReplicationSource
      futures.add(asyncReplicate(entries, i, replicateContext.getTimeout()));
    }

    IOException iox = null;
    long lastWriteTime = 0;

    for (CompletableFuture<Integer> f : futures) {
      try {
        // wait for all futures, remove successful parts
        // (only the remaining parts will be retried)
        int index = FutureUtils.get(f);
        List<Entry> batch = batches.get(index);
        batches.set(index, Collections.emptyList()); // remove successful batch
        // Find the most recent write time in the batch
        long writeTime = batch.get(batch.size() - 1).getKey().getWriteTime();
        if (writeTime > lastWriteTime) {
          lastWriteTime = writeTime;
        }
      } catch (IOException e) {
        iox = e;
      } catch (RuntimeException e) {
        iox = new IOException(e);
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
  public ReplicationResult replicate(ReplicateContext replicateContext) {
    int sleepMultiplier = 1;
    int initialTimeout = replicateContext.getTimeout();

    if (!peersSelected && this.isRunning()) {
      connectToPeers();
      peersSelected = true;
    }

    int numSinks = getNumSinks();
    if (numSinks == 0) {
      if (
        (EnvironmentEdgeManager.currentTime() - lastSinkFetchTime) >= (maxRetriesMultiplier * 1000)
      ) {
        LOG.warn("No replication sinks found, returning without replicating. "
          + "The source should retry with the same set of edits. Not logging this again for "
          + "the next {} seconds.", maxRetriesMultiplier);
        lastSinkFetchTime = EnvironmentEdgeManager.currentTime();
      }
      sleepForRetries("No sinks available at peer", sleepMultiplier);
      return ReplicationResult.FAILED;
    }

    List<List<Entry>> batches = createBatches(replicateContext.getEntries());
    while (this.isRunning()) {
      if (!isPeerEnabled()) {
        if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }
      try {
        // replicate the batches to sink side.
        parallelReplicate(replicateContext, batches);
        return ReplicationResult.COMMITTED;
      } catch (IOException ioe) {
        if (ioe instanceof RemoteException) {
          if (dropOnDeletedTables && isTableNotFoundException(ioe)) {
            // Only filter the edits to replicate and don't change the entries in replicateContext
            // as the upper layer rely on it.
            batches = filterNotExistTableEdits(batches);
            if (batches.isEmpty()) {
              LOG.warn("After filter not exist table's edits, 0 edits to replicate, just return");
              return ReplicationResult.COMMITTED;
            }
          } else if (dropOnDeletedColumnFamilies && isNoSuchColumnFamilyException(ioe)) {
            batches = filterNotExistColumnFamilyEdits(batches);
            if (batches.isEmpty()) {
              LOG.warn("After filter not exist column family's edits, 0 edits to replicate, "
                + "just return");
              return ReplicationResult.COMMITTED;
            }
          } else {
            LOG.warn("{} Peer encountered RemoteException, rechecking all sinks: ", logPeerId(),
              ioe);
            chooseSinks();
          }
        } else {
          if (ioe instanceof SocketTimeoutException) {
            // This exception means we waited for more than 60s and nothing
            // happened, the cluster is alive and calling it right away
            // even for a test just makes things worse.
            sleepForRetries(
              "Encountered a SocketTimeoutException. Since the "
                + "call to the remote cluster timed out, which is usually "
                + "caused by a machine failure or a massive slowdown",
              this.socketTimeoutMultiplier);
          } else if (ioe instanceof ConnectException || ioe instanceof UnknownHostException) {
            LOG.warn("{} Peer is unavailable, rechecking all sinks: ", logPeerId(), ioe);
            chooseSinks();
          } else if (ioe instanceof CallTimeoutException) {
            replicateContext
              .setTimeout(ReplicationUtils.getAdaptiveTimeout(initialTimeout, sleepMultiplier));
          } else {
            LOG.warn("{} Can't replicate because of a local or network error: ", logPeerId(), ioe);
          }
        }
        if (sleepForRetries("Since we are unable to replicate", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
    return ReplicationResult.FAILED; // in case we exited before replicating
  }

  protected boolean isPeerEnabled() {
    return ctx.getReplicationPeer().isPeerEnabled();
  }

  protected CompletableFuture<Integer> replicateEntries(List<Entry> entries, int batchIndex,
    int timeout) {
    int entriesHashCode = System.identityHashCode(entries);
    if (LOG.isTraceEnabled()) {
      long size = entries.stream().mapToLong(this::getEstimatedEntrySize).sum();
      LOG.trace("{} Replicating batch {} of {} entries with total size {} bytes to {}", logPeerId(),
        entriesHashCode, entries.size(), size, replicationClusterId);
    }
    SinkPeer sinkPeer = null;
    final CompletableFuture<Integer> resultCompletableFuture = new CompletableFuture<Integer>();
    try {
      sinkPeer = getReplicationSink();
    } catch (IOException e) {
      this.onReplicateWALEntryException(entriesHashCode, e, sinkPeer);
      resultCompletableFuture.completeExceptionally(e);
      return resultCompletableFuture;
    }
    assert sinkPeer != null;
    AsyncRegionServerAdmin rsAdmin = sinkPeer.getRegionServer();
    final SinkPeer sinkPeerToUse = sinkPeer;
    FutureUtils.addListener(
      ReplicationProtobufUtil.replicateWALEntry(rsAdmin, entries.toArray(new Entry[entries.size()]),
        replicationClusterId, baseNamespaceDir, hfileArchiveDir, timeout),
      (response, exception) -> {
        if (exception != null) {
          onReplicateWALEntryException(entriesHashCode, exception, sinkPeerToUse);
          resultCompletableFuture.completeExceptionally(exception);
          return;
        }
        reportSinkSuccess(sinkPeerToUse);
        resultCompletableFuture.complete(batchIndex);
      });
    return resultCompletableFuture;
  }

  private void onReplicateWALEntryException(int entriesHashCode, Throwable exception,
    final SinkPeer sinkPeer) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("{} Failed replicating batch {}", logPeerId(), entriesHashCode, exception);
    }
    if (exception instanceof IOException) {
      if (sinkPeer != null) {
        reportBadSink(sinkPeer);
      }
    }
  }

  /**
   * Here for {@link HBaseInterClusterReplicationEndpoint#isSerialis} is true, we iterator over the
   * WAL {@link Entry} list, once we reached a batch limit, we send it out, and in the callback, we
   * send the next batch, until we send all entries out.
   */
  private CompletableFuture<Integer> serialReplicateRegionEntries(
    PeekingIterator<Entry> walEntryPeekingIterator, int batchIndex, int timeout) {
    if (!walEntryPeekingIterator.hasNext()) {
      return CompletableFuture.completedFuture(batchIndex);
    }
    int batchSize = 0;
    List<Entry> batch = new ArrayList<>();
    while (walEntryPeekingIterator.hasNext()) {
      Entry entry = walEntryPeekingIterator.peek();
      int entrySize = getEstimatedEntrySize(entry);
      if (batchSize > 0 && batchSize + entrySize > replicationRpcLimit) {
        break;
      }
      walEntryPeekingIterator.next();
      batch.add(entry);
      batchSize += entrySize;
    }

    if (batchSize <= 0) {
      return CompletableFuture.completedFuture(batchIndex);
    }
    final CompletableFuture<Integer> resultCompletableFuture = new CompletableFuture<Integer>();
    FutureUtils.addListener(replicateEntries(batch, batchIndex, timeout), (response, exception) -> {
      if (exception != null) {
        resultCompletableFuture.completeExceptionally(exception);
        return;
      }
      if (!walEntryPeekingIterator.hasNext()) {
        resultCompletableFuture.complete(batchIndex);
        return;
      }
      FutureUtils.addListener(
        serialReplicateRegionEntries(walEntryPeekingIterator, batchIndex, timeout),
        (currentResponse, currentException) -> {
          if (currentException != null) {
            resultCompletableFuture.completeExceptionally(currentException);
            return;
          }
          resultCompletableFuture.complete(batchIndex);
        });
    });
    return resultCompletableFuture;
  }

  /**
   * Replicate entries to peer cluster by async API.
   */
  protected CompletableFuture<Integer> asyncReplicate(List<Entry> entries, int batchIndex,
    int timeout) {
    return isSerial
      ? serialReplicateRegionEntries(Iterators.peekingIterator(entries.iterator()), batchIndex,
        timeout)
      : replicateEntries(entries, batchIndex, timeout);
  }

  private String logPeerId() {
    return "[Source for peer " + this.ctx.getPeerId() + "]:";
  }
}
