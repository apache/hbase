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
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionAdminServiceCallable;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.RetryingCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALSplitter.EntryBuffers;
import org.apache.hadoop.hbase.wal.WALSplitter.OutputSink;
import org.apache.hadoop.hbase.wal.WALSplitter.PipelineController;
import org.apache.hadoop.hbase.wal.WALSplitter.RegionEntryBuffer;
import org.apache.hadoop.hbase.wal.WALSplitter.SinkWriter;
import org.apache.hadoop.hbase.replication.BaseWALEntryFilter;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.util.StringUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.protobuf.ServiceException;

/**
 * A {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} endpoint
 * which receives the WAL edits from the WAL, and sends the edits to replicas
 * of regions.
 */
@InterfaceAudience.Private
public class RegionReplicaReplicationEndpoint extends HBaseReplicationEndpoint {

  private static final Log LOG = LogFactory.getLog(RegionReplicaReplicationEndpoint.class);

  // Can be configured differently than hbase.client.retries.number
  private static String CLIENT_RETRIES_NUMBER
    = "hbase.region.replica.replication.client.retries.number";

  private Configuration conf;
  private ClusterConnection connection;
  private TableDescriptors tableDescriptors;

  // Reuse WALSplitter constructs as a WAL pipe
  private PipelineController controller;
  private RegionReplicaOutputSink outputSink;
  private EntryBuffers entryBuffers;

  // Number of writer threads
  private int numWriterThreads;

  private int operationTimeout;

  private ExecutorService pool;

  /**
   * Skips the entries which has original seqId. Only entries persisted via distributed log replay
   * have their original seq Id fields set.
   */
  private static class SkipReplayedEditsFilter extends BaseWALEntryFilter {
    @Override
    public Entry filter(Entry entry) {
      // if orig seq id is set, skip replaying the entry
      if (entry.getKey().getOrigLogSeqNum() > 0) {
        return null;
      }
      return entry;
    }
  }

  @Override
  public WALEntryFilter getWALEntryfilter() {
    WALEntryFilter superFilter = super.getWALEntryfilter();
    WALEntryFilter skipReplayedEditsFilter = getSkipReplayedEditsFilter();

    if (superFilter == null) {
      return skipReplayedEditsFilter;
    }

    if (skipReplayedEditsFilter == null) {
      return superFilter;
    }

    ArrayList<WALEntryFilter> filters = Lists.newArrayList();
    filters.add(superFilter);
    filters.add(skipReplayedEditsFilter);
    return new ChainWALEntryFilter(filters);
  }

  protected WALEntryFilter getSkipReplayedEditsFilter() {
    return new SkipReplayedEditsFilter();
  }

  @Override
  public void init(Context context) throws IOException {
    super.init(context);

    this.conf = HBaseConfiguration.create(context.getConfiguration());
    this.tableDescriptors = context.getTableDescriptors();

    // HRS multiplies client retries by 10 globally for meta operations, but we do not want this.
    // We are resetting it here because we want default number of retries (35) rather than 10 times
    // that which makes very long retries for disabled tables etc.
    int defaultNumRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    if (defaultNumRetries > 10) {
      int mult = conf.getInt("hbase.client.serverside.retries.multiplier", 10);
      defaultNumRetries = defaultNumRetries / mult; // reset if HRS has multiplied this already
    }

    conf.setInt("hbase.client.serverside.retries.multiplier", 1);
    int numRetries = conf.getInt(CLIENT_RETRIES_NUMBER, defaultNumRetries);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, numRetries);

    this.numWriterThreads = this.conf.getInt(
      "hbase.region.replica.replication.writer.threads", 3);
    controller = new PipelineController();
    entryBuffers = new EntryBuffers(controller,
      this.conf.getInt("hbase.region.replica.replication.buffersize",
          128*1024*1024));

    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
      HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  @Override
  protected void doStart() {
    try {
      connection = (ClusterConnection) ConnectionFactory.createConnection(this.conf);
      this.pool = getDefaultThreadPool(conf);
      outputSink = new RegionReplicaOutputSink(controller, tableDescriptors, entryBuffers,
        connection, pool, numWriterThreads, operationTimeout);
      outputSink.startWriterThreads();
      super.doStart();
    } catch (IOException ex) {
      LOG.warn("Received exception while creating connection :" + ex);
      notifyFailed(ex);
    }
  }

  @Override
  protected void doStop() {
    if (outputSink != null) {
      try {
        outputSink.finishWritingAndClose();
      } catch (IOException ex) {
        LOG.warn("Got exception while trying to close OutputSink");
        LOG.warn(ex);
      }
    }
    if (this.pool != null) {
      this.pool.shutdownNow();
      try {
        // wait for 10 sec
        boolean shutdown = this.pool.awaitTermination(10000, TimeUnit.MILLISECONDS);
        if (!shutdown) {
          LOG.warn("Failed to shutdown the thread pool after 10 seconds");
        }
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted while waiting for the thread pool to shut down" + e);
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (IOException ex) {
        LOG.warn("Got exception closing connection :" + ex);
      }
    }
    super.doStop();
  }

  /**
   * Returns a Thread pool for the RPC's to region replicas. Similar to
   * Connection's thread pool.
   */
  private ExecutorService getDefaultThreadPool(Configuration conf) {
    int maxThreads = conf.getInt("hbase.region.replica.replication.threads.max", 256);
    int coreThreads = conf.getInt("hbase.region.replica.replication.threads.core", 16);
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    if (coreThreads == 0) {
      coreThreads = Runtime.getRuntime().availableProcessors() * 8;
    }
    long keepAliveTime = conf.getLong("hbase.region.replica.replication.threads.keepalivetime", 60);
    LinkedBlockingQueue<Runnable> workQueue =
        new LinkedBlockingQueue<Runnable>(maxThreads *
            conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
              HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(
      coreThreads,
      maxThreads,
      keepAliveTime,
      TimeUnit.SECONDS,
      workQueue,
      Threads.newDaemonThreadFactory(this.getClass().getSimpleName() + "-rpc-shared-"));
    tpe.allowCoreThreadTimeOut(true);
    return tpe;
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    /* A note on batching in RegionReplicaReplicationEndpoint (RRRE):
     *
     * RRRE relies on batching from two different mechanisms. The first is the batching from
     * ReplicationSource since RRRE is a ReplicationEndpoint driven by RS. RS reads from a single
     * WAL file filling up a buffer of heap size "replication.source.size.capacity"(64MB) or at most
     * "replication.source.nb.capacity" entries or until it sees the end of file (in live tailing).
     * Then RS passes all the buffered edits in this replicate() call context. RRRE puts the edits
     * to the WALSplitter.EntryBuffers which is a blocking buffer space of up to
     * "hbase.region.replica.replication.buffersize" (128MB) in size. This buffer splits the edits
     * based on regions.
     *
     * There are "hbase.region.replica.replication.writer.threads"(default 3) writer threads which
     * pick largest per-region buffer and send it to the SinkWriter (see RegionReplicaOutputSink).
     * The SinkWriter in this case will send the wal edits to all secondary region replicas in
     * parallel via a retrying rpc call. EntryBuffers guarantees that while a buffer is
     * being written to the sink, another buffer for the same region will not be made available to
     * writers ensuring regions edits are not replayed out of order.
     *
     * The replicate() call won't return until all the buffers are sent and ack'd by the sinks so
     * that the replication can assume all edits are persisted. We may be able to do a better
     * pipelining between the replication thread and output sinks later if it becomes a bottleneck.
     */

    while (this.isRunning()) {
      try {
        for (Entry entry: replicateContext.getEntries()) {
          entryBuffers.appendEntry(entry);
        }
        outputSink.flush(); // make sure everything is flushed
        ctx.getMetrics().incrLogEditsFiltered(
          outputSink.getSkippedEditsCounter().getAndSet(0));
        return true;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      } catch (IOException e) {
        LOG.warn("Received IOException while trying to replicate"
            + StringUtils.stringifyException(e));
      }
    }

    return false;
  }

  @Override
  public boolean canReplicateToSameCluster() {
    return true;
  }

  @Override
  protected WALEntryFilter getScopeWALEntryFilter() {
    // we do not care about scope. We replicate everything.
    return null;
  }

  static class RegionReplicaOutputSink extends OutputSink {
    private final RegionReplicaSinkWriter sinkWriter;
    private final TableDescriptors tableDescriptors;
    private final Cache<TableName, Boolean> memstoreReplicationEnabled;

    public RegionReplicaOutputSink(PipelineController controller, TableDescriptors tableDescriptors,
        EntryBuffers entryBuffers, ClusterConnection connection, ExecutorService pool,
        int numWriters, int operationTimeout) {
      super(controller, entryBuffers, numWriters);
      this.sinkWriter = new RegionReplicaSinkWriter(this, connection, pool, operationTimeout);
      this.tableDescriptors = tableDescriptors;

      // A cache for the table "memstore replication enabled" flag.
      // It has a default expiry of 5 sec. This means that if the table is altered
      // with a different flag value, we might miss to replicate for that amount of
      // time. But this cache avoid the slow lookup and parsing of the TableDescriptor.
      int memstoreReplicationEnabledCacheExpiryMs = connection.getConfiguration()
        .getInt("hbase.region.replica.replication.cache.memstoreReplicationEnabled.expiryMs", 5000);
      this.memstoreReplicationEnabled = CacheBuilder.newBuilder()
        .expireAfterWrite(memstoreReplicationEnabledCacheExpiryMs, TimeUnit.MILLISECONDS)
        .initialCapacity(10)
        .maximumSize(1000)
        .build();
    }

    @Override
    public void append(RegionEntryBuffer buffer) throws IOException {
      List<Entry> entries = buffer.getEntryBuffer();

      if (entries.isEmpty() || entries.get(0).getEdit().getCells().isEmpty()) {
        return;
      }

      // meta edits (e.g. flush) are always replicated.
      // data edits (e.g. put) are replicated if the table requires them.
      if (!requiresReplication(buffer.getTableName(), entries)) {
        return;
      }

      sinkWriter.append(buffer.getTableName(), buffer.getEncodedRegionName(),
        CellUtil.cloneRow(entries.get(0).getEdit().getCells().get(0)), entries);
    }

    @Override
    public boolean flush() throws IOException {
      // nothing much to do for now. Wait for the Writer threads to finish up
      // append()'ing the data.
      entryBuffers.waitUntilDrained();
      return super.flush();
    }

    @Override
    public List<Path> finishWritingAndClose() throws IOException {
      finishWriting(true);
      return null;
    }

    @Override
    public Map<byte[], Long> getOutputCounts() {
      return null; // only used in tests
    }

    @Override
    public int getNumberOfRecoveredRegions() {
      return 0;
    }

    AtomicLong getSkippedEditsCounter() {
      return skippedEdits;
    }

    /**
     * returns true if the specified entry must be replicated.
     * We should always replicate meta operations (e.g. flush)
     * and use the user HTD flag to decide whether or not replicate the memstore.
     */
    private boolean requiresReplication(final TableName tableName, final List<Entry> entries)
        throws IOException {
      // unit-tests may not the TableDescriptors, bypass the check and always replicate
      if (tableDescriptors == null) return true;

      Boolean requiresReplication = memstoreReplicationEnabled.getIfPresent(tableName);
      if (requiresReplication == null) {
        // check if the table requires memstore replication
        // some unit-test drop the table, so we should do a bypass check and always replicate.
        HTableDescriptor htd = tableDescriptors.get(tableName);
        requiresReplication = htd == null || htd.hasRegionMemstoreReplication();
        memstoreReplicationEnabled.put(tableName, requiresReplication);
      }

      // if memstore replication is not required, check the entries.
      // meta edits (e.g. flush) must be always replicated.
      if (!requiresReplication) {
        int skipEdits = 0;
        java.util.Iterator<Entry> it = entries.iterator();
        while (it.hasNext()) {
          Entry entry = it.next();
          if (entry.getEdit().isMetaEdit()) {
            requiresReplication = true;
          } else {
            it.remove();
            skipEdits++;
          }
        }
        skippedEdits.addAndGet(skipEdits);
      }
      return requiresReplication;
    }
  }

  static class RegionReplicaSinkWriter extends SinkWriter {
    RegionReplicaOutputSink sink;
    ClusterConnection connection;
    RpcControllerFactory rpcControllerFactory;
    RpcRetryingCallerFactory rpcRetryingCallerFactory;
    int operationTimeout;
    ExecutorService pool;
    Cache<TableName, Boolean> disabledAndDroppedTables;

    public RegionReplicaSinkWriter(RegionReplicaOutputSink sink, ClusterConnection connection,
        ExecutorService pool, int operationTimeout) {
      this.sink = sink;
      this.connection = connection;
      this.operationTimeout = operationTimeout;
      this.rpcRetryingCallerFactory
        = RpcRetryingCallerFactory.instantiate(connection.getConfiguration());
      this.rpcControllerFactory = RpcControllerFactory.instantiate(connection.getConfiguration());
      this.pool = pool;

      int nonExistentTableCacheExpiryMs = connection.getConfiguration()
        .getInt("hbase.region.replica.replication.cache.disabledAndDroppedTables.expiryMs", 5000);
      // A cache for non existing tables that have a default expiry of 5 sec. This means that if the
      // table is created again with the same name, we might miss to replicate for that amount of
      // time. But this cache prevents overloading meta requests for every edit from a deleted file.
      disabledAndDroppedTables = CacheBuilder.newBuilder()
        .expireAfterWrite(nonExistentTableCacheExpiryMs, TimeUnit.MILLISECONDS)
        .initialCapacity(10)
        .maximumSize(1000)
        .build();
    }

    public void append(TableName tableName, byte[] encodedRegionName, byte[] row,
        List<Entry> entries) throws IOException {

      if (disabledAndDroppedTables.getIfPresent(tableName) != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping " + entries.size() + " entries because table " + tableName
            + " is cached as a disabled or dropped table");
          for (Entry entry : entries) {
            LOG.trace("Skipping : " + entry);
          }
        }
        sink.getSkippedEditsCounter().addAndGet(entries.size());
        return;
      }

      // If the table is disabled or dropped, we should not replay the entries, and we can skip
      // replaying them. However, we might not know whether the table is disabled until we
      // invalidate the cache and check from meta
      RegionLocations locations = null;
      boolean useCache = true;
      while (true) {
        // get the replicas of the primary region
        try {
          locations = RegionReplicaReplayCallable
              .getRegionLocations(connection, tableName, row, useCache, 0);

          if (locations == null) {
            throw new HBaseIOException("Cannot locate locations for "
                + tableName + ", row:" + Bytes.toStringBinary(row));
          }
        } catch (TableNotFoundException e) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Skipping " + entries.size() + " entries because table " + tableName
              + " is dropped. Adding table to cache.");
            for (Entry entry : entries) {
              LOG.trace("Skipping : " + entry);
            }
          }
          disabledAndDroppedTables.put(tableName, Boolean.TRUE); // put to cache. Value ignored
          // skip this entry
          sink.getSkippedEditsCounter().addAndGet(entries.size());
          return;
        }

        // check whether we should still replay this entry. If the regions are changed, or the
        // entry is not coming from the primary region, filter it out.
        HRegionLocation primaryLocation = locations.getDefaultRegionLocation();
        if (!Bytes.equals(primaryLocation.getRegionInfo().getEncodedNameAsBytes(),
          encodedRegionName)) {
          if (useCache) {
            useCache = false;
            continue; // this will retry location lookup
          }
          if (LOG.isTraceEnabled()) {
            LOG.trace("Skipping " + entries.size() + " entries in table " + tableName
              + " because located region " + primaryLocation.getRegionInfo().getEncodedName()
              + " is different than the original region " + Bytes.toStringBinary(encodedRegionName)
              + " from WALEdit");
            for (Entry entry : entries) {
              LOG.trace("Skipping : " + entry);
            }
          }
          sink.getSkippedEditsCounter().addAndGet(entries.size());
          return;
        }
        break;
      }

      if (locations.size() == 1) {
        return;
      }

      ArrayList<Future<ReplicateWALEntryResponse>> tasks
        = new ArrayList<Future<ReplicateWALEntryResponse>>(locations.size() - 1);

      // All passed entries should belong to one region because it is coming from the EntryBuffers
      // split per region. But the regions might split and merge (unlike log recovery case).
      for (int replicaId = 0; replicaId < locations.size(); replicaId++) {
        HRegionLocation location = locations.getRegionLocation(replicaId);
        if (!RegionReplicaUtil.isDefaultReplica(replicaId)) {
          HRegionInfo regionInfo = location == null
              ? RegionReplicaUtil.getRegionInfoForReplica(
                locations.getDefaultRegionLocation().getRegionInfo(), replicaId)
              : location.getRegionInfo();
          RegionReplicaReplayCallable callable = new RegionReplicaReplayCallable(connection,
            rpcControllerFactory, tableName, location, regionInfo, row, entries,
            sink.getSkippedEditsCounter());
           Future<ReplicateWALEntryResponse> task = pool.submit(
             new RetryingRpcCallable<ReplicateWALEntryResponse>(rpcRetryingCallerFactory,
                 callable, operationTimeout));
           tasks.add(task);
        }
      }

      boolean tasksCancelled = false;
      for (Future<ReplicateWALEntryResponse> task : tasks) {
        try {
          task.get();
        } catch (InterruptedException e) {
          throw new InterruptedIOException(e.getMessage());
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof IOException) {
            // The table can be disabled or dropped at this time. For disabled tables, we have no
            // cheap mechanism to detect this case because meta does not contain this information.
            // HConnection.isTableDisabled() is a zk call which we cannot do for every replay RPC.
            // So instead we start the replay RPC with retries and
            // check whether the table is dropped or disabled which might cause
            // SocketTimeoutException, or RetriesExhaustedException or similar if we get IOE.
            if (cause instanceof TableNotFoundException || connection.isTableDisabled(tableName)) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Skipping " + entries.size() + " entries in table " + tableName
                  + " because received exception for dropped or disabled table", cause);
                for (Entry entry : entries) {
                  LOG.trace("Skipping : " + entry);
                }
              }
              disabledAndDroppedTables.put(tableName, Boolean.TRUE); // put to cache for later.
              if (!tasksCancelled) {
                sink.getSkippedEditsCounter().addAndGet(entries.size());
                tasksCancelled = true; // so that we do not add to skipped counter again
              }
              continue;
            }
            // otherwise rethrow
            throw (IOException)cause;
          }
          // unexpected exception
          throw new IOException(cause);
        }
      }
    }
  }

  static class RetryingRpcCallable<V> implements Callable<V> {
    RpcRetryingCallerFactory factory;
    RetryingCallable<V> callable;
    int timeout;
    public RetryingRpcCallable(RpcRetryingCallerFactory factory, RetryingCallable<V> callable,
        int timeout) {
      this.factory = factory;
      this.callable = callable;
      this.timeout = timeout;
    }
    @Override
    public V call() throws Exception {
      return factory.<V>newCaller().callWithRetries(callable, timeout);
    }
  }

  /**
   * Calls replay on the passed edits for the given set of entries belonging to the region. It skips
   * the entry if the region boundaries have changed or the region is gone.
   */
  static class RegionReplicaReplayCallable
    extends RegionAdminServiceCallable<ReplicateWALEntryResponse> {

    private final List<Entry> entries;
    private final byte[] initialEncodedRegionName;
    private final AtomicLong skippedEntries;

    public RegionReplicaReplayCallable(ClusterConnection connection,
        RpcControllerFactory rpcControllerFactory, TableName tableName,
        HRegionLocation location, HRegionInfo regionInfo, byte[] row,List<Entry> entries,
        AtomicLong skippedEntries) {
      super(connection, rpcControllerFactory, location, tableName, row, regionInfo.getReplicaId());
      this.entries = entries;
      this.skippedEntries = skippedEntries;
      this.initialEncodedRegionName = regionInfo.getEncodedNameAsBytes();
    }

    @Override
    public ReplicateWALEntryResponse call(int timeout) throws IOException {
      return replayToServer(this.entries, timeout);
    }

    private ReplicateWALEntryResponse replayToServer(List<Entry> entries, int timeout)
        throws IOException {
      // check whether we should still replay this entry. If the regions are changed, or the
      // entry is not coming form the primary region, filter it out because we do not need it.
      // Regions can change because of (1) region split (2) region merge (3) table recreated
      boolean skip = false;

      if (!Bytes.equals(location.getRegionInfo().getEncodedNameAsBytes(),
        initialEncodedRegionName)) {
        skip = true;
      }
      if (!entries.isEmpty() && !skip) {
        Entry[] entriesArray = new Entry[entries.size()];
        entriesArray = entries.toArray(entriesArray);

        // set the region name for the target region replica
        Pair<AdminProtos.ReplicateWALEntryRequest, CellScanner> p =
            ReplicationProtbufUtil.buildReplicateWALEntryRequest(entriesArray, location
                .getRegionInfo().getEncodedNameAsBytes(), null, null, null);
        try {
          PayloadCarryingRpcController controller = rpcControllerFactory.newController(p.getSecond());
          controller.setCallTimeout(timeout);
          controller.setPriority(tableName);
          return stub.replay(controller, p.getFirst());
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }
      }

      if (skip) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Skipping " + entries.size() + " entries in table " + tableName
            + " because located region " + location.getRegionInfo().getEncodedName()
            + " is different than the original region "
            + Bytes.toStringBinary(initialEncodedRegionName) + " from WALEdit");
          for (Entry entry : entries) {
            LOG.trace("Skipping : " + entry);
          }
        }
        skippedEntries.addAndGet(entries.size());
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    }
  }
}
