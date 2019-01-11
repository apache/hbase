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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hbase.thirdparty.com.google.common.cache.LoadingCache;

/**
 * A {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint} endpoint which receives the WAL
 * edits from the WAL, and sends the edits to replicas of regions.
 */
@InterfaceAudience.Private
public class RegionReplicaReplicationEndpoint extends HBaseReplicationEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(RegionReplicaReplicationEndpoint.class);

  // Can be configured differently than hbase.client.retries.number
  private static String CLIENT_RETRIES_NUMBER =
    "hbase.region.replica.replication.client.retries.number";

  private Configuration conf;
  private AsyncClusterConnection connection;
  private TableDescriptors tableDescriptors;

  private int numRetries;

  private long operationTimeoutNs;

  private LoadingCache<TableName, Optional<TableDescriptor>> tableDescriptorCache;

  private Cache<TableName, TableName> disabledTableCache;

  private final RetryCounterFactory retryCounterFactory =
    new RetryCounterFactory(Integer.MAX_VALUE, 1000, 60000);

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    this.conf = context.getConfiguration();
    this.tableDescriptors = context.getTableDescriptors();
    int memstoreReplicationEnabledCacheExpiryMs = conf
      .getInt("hbase.region.replica.replication.cache.memstoreReplicationEnabled.expiryMs", 5000);
    // A cache for the table "memstore replication enabled" flag.
    // It has a default expiry of 5 sec. This means that if the table is altered
    // with a different flag value, we might miss to replicate for that amount of
    // time. But this cache avoid the slow lookup and parsing of the TableDescriptor.
    tableDescriptorCache = CacheBuilder.newBuilder()
      .expireAfterWrite(memstoreReplicationEnabledCacheExpiryMs, TimeUnit.MILLISECONDS)
      .initialCapacity(10).maximumSize(1000)
      .build(new CacheLoader<TableName, Optional<TableDescriptor>>() {

        @Override
        public Optional<TableDescriptor> load(TableName tableName) throws Exception {
          // check if the table requires memstore replication
          // some unit-test drop the table, so we should do a bypass check and always replicate.
          return Optional.ofNullable(tableDescriptors.get(tableName));
        }
      });
    int nonExistentTableCacheExpiryMs =
      conf.getInt("hbase.region.replica.replication.cache.disabledAndDroppedTables.expiryMs", 5000);
    // A cache for non existing tables that have a default expiry of 5 sec. This means that if the
    // table is created again with the same name, we might miss to replicate for that amount of
    // time. But this cache prevents overloading meta requests for every edit from a deleted file.
    disabledTableCache = CacheBuilder.newBuilder()
      .expireAfterWrite(nonExistentTableCacheExpiryMs, TimeUnit.MILLISECONDS).initialCapacity(10)
      .maximumSize(1000).build();
    // HRS multiplies client retries by 10 globally for meta operations, but we do not want this.
    // We are resetting it here because we want default number of retries (35) rather than 10 times
    // that which makes very long retries for disabled tables etc.
    int defaultNumRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    if (defaultNumRetries > 10) {
      int mult = conf.getInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER,
        HConstants.DEFAULT_HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER);
      defaultNumRetries = defaultNumRetries / mult; // reset if HRS has multiplied this already
    }
    this.numRetries = conf.getInt(CLIENT_RETRIES_NUMBER, defaultNumRetries);
    // use the regular RPC timeout for replica replication RPC's
    this.operationTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT));
    this.connection = context.getServer().getAsyncClusterConnection();
  }

  /**
   * returns true if the specified entry must be replicated. We should always replicate meta
   * operations (e.g. flush) and use the user HTD flag to decide whether or not replicate the
   * memstore.
   */
  private boolean requiresReplication(Optional<TableDescriptor> tableDesc, Entry entry) {
    // empty edit does not need to be replicated
    if (entry.getEdit().isEmpty() || !tableDesc.isPresent()) {
      return false;
    }
    // meta edits (e.g. flush) must be always replicated
    return entry.getEdit().isMetaEdit() || tableDesc.get().hasRegionMemStoreReplication();
  }

  private void getRegionLocations(CompletableFuture<RegionLocations> future,
      TableDescriptor tableDesc, byte[] encodedRegionName, byte[] row, boolean reload) {
    FutureUtils.addListener(connection.getRegionLocations(tableDesc.getTableName(), row, reload),
      (r, e) -> {
        if (e != null) {
          future.completeExceptionally(e);
          return;
        }
        // if we are not loading from cache, just return
        if (reload) {
          future.complete(r);
          return;
        }
        // check if the number of region replicas is correct, and also the primary region name
        // matches
        if (r.size() == tableDesc.getRegionReplication() && Bytes.equals(
          r.getDefaultRegionLocation().getRegion().getEncodedNameAsBytes(), encodedRegionName)) {
          future.complete(r);
        } else {
          // reload again as the information in cache maybe stale
          getRegionLocations(future, tableDesc, encodedRegionName, row, true);
        }
      });
  }

  private void replicate(CompletableFuture<Long> future, RegionLocations locs,
      TableDescriptor tableDesc, byte[] encodedRegionName, byte[] row, List<Entry> entries) {
    if (locs.size() == 1) {
      // Could this happen?
      future.complete(Long.valueOf(entries.size()));
      return;
    }
    if (!Bytes.equals(locs.getDefaultRegionLocation().getRegion().getEncodedNameAsBytes(),
      encodedRegionName)) {
      // the region name is not equal, this usually means the region has been split or merged, so
      // give up replicating as the new region(s) should already have all the data of the parent
      // region(s).
      if (LOG.isTraceEnabled()) {
        LOG.trace(
          "Skipping {} entries in table {} because located region {} is different than" +
            " the original region {} from WALEdit",
          tableDesc.getTableName(), locs.getDefaultRegionLocation().getRegion().getEncodedName(),
          Bytes.toStringBinary(encodedRegionName));
      }
      future.complete(Long.valueOf(entries.size()));
      return;
    }
    AtomicReference<Throwable> error = new AtomicReference<>();
    AtomicInteger remainingTasks = new AtomicInteger(locs.size() - 1);
    AtomicLong skippedEdits = new AtomicLong(0);

    for (int i = 1, n = locs.size(); i < n; i++) {
      final int replicaId = i;
      FutureUtils.addListener(connection.replay(tableDesc.getTableName(),
        locs.getRegionLocation(replicaId).getRegion().getEncodedNameAsBytes(), row, entries,
        replicaId, numRetries, operationTimeoutNs), (r, e) -> {
          if (e != null) {
            LOG.warn("Failed to replicate to {}", locs.getRegionLocation(replicaId), e);
            error.compareAndSet(null, e);
          } else {
            AtomicUtils.updateMax(skippedEdits, r.longValue());
          }
          if (remainingTasks.decrementAndGet() == 0) {
            if (error.get() != null) {
              future.completeExceptionally(error.get());
            } else {
              future.complete(skippedEdits.get());
            }
          }
        });
    }
  }

  private void logSkipped(TableName tableName, List<Entry> entries, String reason) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Skipping {} entries because table {} is {}", entries.size(), tableName, reason);
      for (Entry entry : entries) {
        LOG.trace("Skipping : {}", entry);
      }
    }
  }

  private CompletableFuture<Long> replicate(TableDescriptor tableDesc, byte[] encodedRegionName,
      List<Entry> entries) {
    if (disabledTableCache.getIfPresent(tableDesc.getTableName()) != null) {
      logSkipped(tableDesc.getTableName(), entries, "cached as a disabled table");
      return CompletableFuture.completedFuture(Long.valueOf(entries.size()));
    }
    byte[] row = CellUtil.cloneRow(entries.get(0).getEdit().getCells().get(0));
    CompletableFuture<RegionLocations> locateFuture = new CompletableFuture<>();
    getRegionLocations(locateFuture, tableDesc, encodedRegionName, row, false);
    CompletableFuture<Long> future = new CompletableFuture<>();
    FutureUtils.addListener(locateFuture, (locs, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
      } else {
        replicate(future, locs, tableDesc, encodedRegionName, row, entries);
      }
    });
    return future;
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    Map<byte[], Pair<TableDescriptor, List<Entry>>> encodedRegionName2Entries =
      new TreeMap<>(Bytes.BYTES_COMPARATOR);
    long skippedEdits = 0;
    RetryCounter retryCounter = retryCounterFactory.create();
    outer: while (isRunning()) {
      encodedRegionName2Entries.clear();
      skippedEdits = 0;
      for (Entry entry : replicateContext.getEntries()) {
        Optional<TableDescriptor> tableDesc;
        try {
          tableDesc = tableDescriptorCache.get(entry.getKey().getTableName());
        } catch (ExecutionException e) {
          LOG.warn("Failed to load table descriptor for {}, attempts={}",
            entry.getKey().getTableName(), retryCounter.getAttemptTimes(), e.getCause());
          if (!retryCounter.shouldRetry()) {
            return false;
          }
          try {
            retryCounter.sleepUntilNextRetry();
          } catch (InterruptedException e1) {
            // restore the interrupted state
            Thread.currentThread().interrupt();
            return false;
          }
          continue outer;
        }
        if (!requiresReplication(tableDesc, entry)) {
          skippedEdits++;
          continue;
        }
        byte[] encodedRegionName = entry.getKey().getEncodedRegionName();
        encodedRegionName2Entries
          .computeIfAbsent(encodedRegionName, k -> Pair.newPair(tableDesc.get(), new ArrayList<>()))
          .getSecond().add(entry);
      }
      break;
    }
    // send the request to regions
    retryCounter = retryCounterFactory.create();
    while (isRunning()) {
      List<Pair<CompletableFuture<Long>, byte[]>> futureAndEncodedRegionNameList =
        new ArrayList<Pair<CompletableFuture<Long>, byte[]>>();
      for (Map.Entry<byte[], Pair<TableDescriptor, List<Entry>>> entry : encodedRegionName2Entries
        .entrySet()) {
        CompletableFuture<Long> future =
          replicate(entry.getValue().getFirst(), entry.getKey(), entry.getValue().getSecond());
        futureAndEncodedRegionNameList.add(Pair.newPair(future, entry.getKey()));
      }
      for (Pair<CompletableFuture<Long>, byte[]> pair : futureAndEncodedRegionNameList) {
        byte[] encodedRegionName = pair.getSecond();
        try {
          skippedEdits += pair.getFirst().get().longValue();
          encodedRegionName2Entries.remove(encodedRegionName);
        } catch (InterruptedException e) {
          // restore the interrupted state
          Thread.currentThread().interrupt();
          return false;
        } catch (ExecutionException e) {
          Pair<TableDescriptor, List<Entry>> tableAndEntries =
            encodedRegionName2Entries.get(encodedRegionName);
          TableName tableName = tableAndEntries.getFirst().getTableName();
          List<Entry> entries = tableAndEntries.getSecond();
          Throwable cause = e.getCause();
          // The table can be disabled or dropped at this time. For disabled tables, we have no
          // cheap mechanism to detect this case because meta does not contain this information.
          // ClusterConnection.isTableDisabled() is a zk call which we cannot do for every replay
          // RPC. So instead we start the replay RPC with retries and check whether the table is
          // dropped or disabled which might cause SocketTimeoutException, or
          // RetriesExhaustedException or similar if we get IOE.
          if (cause instanceof TableNotFoundException) {
            // add to cache that the table does not exist
            tableDescriptorCache.put(tableName, Optional.empty());
            logSkipped(tableName, entries, "dropped");
            skippedEdits += entries.size();
            encodedRegionName2Entries.remove(encodedRegionName);
            continue;
          }
          boolean disabled = false;
          try {
            disabled = connection.getAdmin().isTableDisabled(tableName).get();
          } catch (InterruptedException e1) {
            // restore the interrupted state
            Thread.currentThread().interrupt();
            return false;
          } catch (ExecutionException e1) {
            LOG.warn("Failed to test whether {} is disabled, assume it is not disabled", tableName,
              e1.getCause());
          }
          if (disabled) {
            disabledTableCache.put(tableName, tableName);
            logSkipped(tableName, entries, "disabled");
            skippedEdits += entries.size();
            encodedRegionName2Entries.remove(encodedRegionName);
            continue;
          }
          LOG.warn("Failed to replicate {} entries for region {} of table {}", entries.size(),
            Bytes.toStringBinary(encodedRegionName), tableName);
        }
      }
      // we have done
      if (encodedRegionName2Entries.isEmpty()) {
        ctx.getMetrics().incrLogEditsFiltered(skippedEdits);
        return true;
      } else {
        LOG.warn("Failed to replicate all entris, retry={}", retryCounter.getAttemptTimes());
        if (!retryCounter.shouldRetry()) {
          return false;
        }
        try {
          retryCounter.sleepUntilNextRetry();
        } catch (InterruptedException e) {
          // restore the interrupted state
          Thread.currentThread().interrupt();
          return false;
        }
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
}
