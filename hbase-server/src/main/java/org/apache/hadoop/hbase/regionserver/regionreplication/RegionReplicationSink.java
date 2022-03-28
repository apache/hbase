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
package org.apache.hadoop.hbase.regionserver.regionreplication;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.agrona.collections.IntHashSet;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FlushDescriptor.FlushAction;

/**
 * The class for replicating WAL edits to secondary replicas, one instance per region.
 */
@InterfaceAudience.Private
public class RegionReplicationSink {

  private static final Logger LOG = LoggerFactory.getLogger(RegionReplicationSink.class);

  public static final String RETRIES_NUMBER = "hbase.region.read-replica.sink.retries.number";

  public static final int RETRIES_NUMBER_DEFAULT = 3;

  public static final String RPC_TIMEOUT_MS = "hbase.region.read-replica.sink.rpc.timeout.ms";

  public static final long RPC_TIMEOUT_MS_DEFAULT = 1000;

  public static final String OPERATION_TIMEOUT_MS =
    "hbase.region.read-replica.sink.operation.timeout.ms";

  public static final long OPERATION_TIMEOUT_MS_DEFAULT = 5000;

  // the two options below are for replicating meta edits, as usually a meta edit will trigger a
  // refreshStoreFiles call at remote side so it will likely to spend more time. And also a meta
  // edit is more important for fixing inconsistent state so it worth to wait for more time.
  public static final String META_EDIT_RPC_TIMEOUT_MS =
    "hbase.region.read-replica.sink.meta-edit.rpc.timeout.ms";

  public static final long META_EDIT_RPC_TIMEOUT_MS_DEFAULT = 15000;

  public static final String META_EDIT_OPERATION_TIMEOUT_MS =
    "hbase.region.read-replica.sink.meta-edit.operation.timeout.ms";

  public static final long META_EDIT_OPERATION_TIMEOUT_MS_DEFAULT = 60000;

  public static final String BATCH_SIZE_CAPACITY = "hbase.region.read-replica.sink.size.capacity";

  public static final long BATCH_SIZE_CAPACITY_DEFAULT = 1024L * 1024;

  public static final String BATCH_COUNT_CAPACITY = "hbase.region.read-replica.sink.nb.capacity";

  public static final int BATCH_COUNT_CAPACITY_DEFAULT = 100;

  private static final class SinkEntry {

    final WALKeyImpl key;

    final WALEdit edit;

    final ServerCall<?> rpcCall;

    final long size;

    SinkEntry(WALKeyImpl key, WALEdit edit, ServerCall<?> rpcCall) {
      this.key = key;
      this.edit = edit;
      this.rpcCall = rpcCall;
      this.size = key.estimatedSerializedSizeOf() + edit.estimatedSerializedSizeOf();
      if (rpcCall != null) {
        // increase the reference count to avoid the rpc framework free the memory before we
        // actually sending them out.
        rpcCall.retainByWAL();
      }
    }

    /**
     * Should be called regardless of the result of the replicating operation. Unless you still want
     * to reuse this entry, otherwise you must call this method to release the possible off heap
     * memories.
     */
    void replicated() {
      if (rpcCall != null) {
        rpcCall.releaseByWAL();
      }
    }
  }

  private final RegionInfo primary;

  private final TableDescriptor tableDesc;

  // store it here to avoid passing it every time when calling TableDescriptor.getRegionReplication.
  private final int regionReplication;

  private final RegionReplicationBufferManager manager;

  private final RegionReplicationFlushRequester flushRequester;

  private final AsyncClusterConnection conn;

  // used to track the replicas which we failed to replicate edits to them
  // the key is the replica id, the value is the sequence id of the last failed edit
  // when we get a flush all request, we will try to remove a replica from this map, the key point
  // here is the flush sequence number must be greater than the failed sequence id, otherwise we
  // should not remove the replica from this map
  private final IntHashSet failedReplicas;

  private final Queue<SinkEntry> entries = new ArrayDeque<>();

  private final int retries;

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  private final long metaEditRpcTimeoutNs;

  private final long metaEditOperationTimeoutNs;

  private final long batchSizeCapacity;

  private final long batchCountCapacity;

  private volatile long pendingSize;

  private long lastFlushedSequenceId;

  private boolean sending;

  private boolean stopping;

  private boolean stopped;

  public RegionReplicationSink(Configuration conf, RegionInfo primary, TableDescriptor td,
    RegionReplicationBufferManager manager, Runnable flushRequester, AsyncClusterConnection conn) {
    Preconditions.checkArgument(RegionReplicaUtil.isDefaultReplica(primary), "%s is not primary",
      primary);
    this.regionReplication = td.getRegionReplication();
    Preconditions.checkArgument(this.regionReplication > 1,
      "region replication should be greater than 1 but got %s", this.regionReplication);
    this.primary = primary;
    this.tableDesc = td;
    this.manager = manager;
    this.flushRequester = new RegionReplicationFlushRequester(conf, flushRequester);
    this.conn = conn;
    this.retries = conf.getInt(RETRIES_NUMBER, RETRIES_NUMBER_DEFAULT);
    this.rpcTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(RPC_TIMEOUT_MS, RPC_TIMEOUT_MS_DEFAULT));
    this.operationTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(OPERATION_TIMEOUT_MS, OPERATION_TIMEOUT_MS_DEFAULT));
    this.metaEditRpcTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(META_EDIT_RPC_TIMEOUT_MS, META_EDIT_RPC_TIMEOUT_MS_DEFAULT));
    this.metaEditOperationTimeoutNs = TimeUnit.MILLISECONDS.toNanos(
      conf.getLong(META_EDIT_OPERATION_TIMEOUT_MS, META_EDIT_OPERATION_TIMEOUT_MS_DEFAULT));
    this.batchSizeCapacity = conf.getLong(BATCH_SIZE_CAPACITY, BATCH_SIZE_CAPACITY_DEFAULT);
    this.batchCountCapacity = conf.getInt(BATCH_COUNT_CAPACITY, BATCH_COUNT_CAPACITY_DEFAULT);
    this.failedReplicas = new IntHashSet(regionReplication - 1);
  }

  void onComplete(List<SinkEntry> sent,
    Map<Integer, MutableObject<Throwable>> replica2Error) {
    long maxSequenceId = Long.MIN_VALUE;
    long toReleaseSize = 0;
    for (SinkEntry entry : sent) {
      maxSequenceId = Math.max(maxSequenceId, entry.key.getSequenceId());
      entry.replicated();
      toReleaseSize += entry.size;
    }
    manager.decrease(toReleaseSize);
    synchronized (entries) {
      pendingSize -= toReleaseSize;
      boolean addFailedReplicas = false;
      for (Map.Entry<Integer, MutableObject<Throwable>> entry : replica2Error.entrySet()) {
        Integer replicaId = entry.getKey();
        Throwable error = entry.getValue().getValue();
        if (error != null) {
          if (maxSequenceId > lastFlushedSequenceId) {
            LOG.warn(
              "Failed to replicate to secondary replica {} for {}, since the max sequence"
                  + " id of sunk entris is {}, which is greater than the last flush SN {},"
                  + " we will stop replicating for a while and trigger a flush",
              replicaId, primary, maxSequenceId, lastFlushedSequenceId, error);
            failedReplicas.add(replicaId);
            addFailedReplicas = true;
          } else {
            LOG.warn(
              "Failed to replicate to secondary replica {} for {}, since the max sequence"
                  + " id of sunk entris is {}, which is less than or equal to the last flush SN {},"
                  + " we will not stop replicating",
              replicaId, primary, maxSequenceId, lastFlushedSequenceId, error);
          }
        }
      }

      if (addFailedReplicas) {
        flushRequester.requestFlush(maxSequenceId);
      }
      sending = false;
      if (stopping) {
        stopped = true;
        entries.notifyAll();
        return;
      }
      if (!entries.isEmpty()) {
        send();
      }
    }
  }

  private void send() {
    List<SinkEntry> toSend = new ArrayList<>();
    long totalSize = 0L;
    boolean hasMetaEdit = false;
    for (SinkEntry entry;;) {
      entry = entries.poll();
      if (entry == null) {
        break;
      }
      toSend.add(entry);
      totalSize += entry.size;
      hasMetaEdit |= entry.edit.isMetaEdit();
      if (toSend.size() >= batchCountCapacity || totalSize >= batchSizeCapacity) {
        break;
      }
    }
    int toSendReplicaCount = regionReplication - 1 - failedReplicas.size();
    if (toSendReplicaCount <= 0) {
      return;
    }
    long rpcTimeoutNsToUse;
    long operationTimeoutNsToUse;
    if (!hasMetaEdit) {
      rpcTimeoutNsToUse = rpcTimeoutNs;
      operationTimeoutNsToUse = operationTimeoutNs;
    } else {
      rpcTimeoutNsToUse = metaEditRpcTimeoutNs;
      operationTimeoutNsToUse = metaEditOperationTimeoutNs;
    }
    sending = true;
    List<WAL.Entry> walEntries =
      toSend.stream().map(e -> new WAL.Entry(e.key, e.edit)).collect(Collectors.toList());
    AtomicInteger remaining = new AtomicInteger(toSendReplicaCount);
    Map<Integer, MutableObject<Throwable>> replica2Error = new HashMap<>();
    for (int replicaId = 1; replicaId < regionReplication; replicaId++) {
      if (failedReplicas.contains(replicaId)) {
        continue;
      }
      MutableObject<Throwable> error = new MutableObject<>();
      replica2Error.put(replicaId, error);
      RegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(primary, replicaId);
      FutureUtils.addListener(
        conn.replicate(replica, walEntries, retries, rpcTimeoutNsToUse, operationTimeoutNsToUse),
        (r, e) -> {
          error.setValue(e);
          if (remaining.decrementAndGet() == 0) {
            onComplete(toSend, replica2Error);
          }
        });
    }
  }

  private boolean isStartFlushAllStores(FlushDescriptor flushDesc) {
    if (flushDesc.getAction() == FlushAction.CANNOT_FLUSH) {
      // this means the memstore is empty, which means all data before this sequence id are flushed
      // out, so it equals to a flush all, return true
      return true;
    }
    if (flushDesc.getAction() != FlushAction.START_FLUSH) {
      return false;
    }
    Set<byte[]> storesFlushed =
      flushDesc.getStoreFlushesList().stream().map(sfd -> sfd.getFamilyName().toByteArray())
        .collect(Collectors.toCollection(() -> new TreeSet<>(Bytes.BYTES_COMPARATOR)));
    if (storesFlushed.size() != tableDesc.getColumnFamilyCount()) {
      return false;
    }
    return storesFlushed.containsAll(tableDesc.getColumnFamilyNames());
  }

  Optional<FlushDescriptor> getStartFlushAllDescriptor(Cell metaCell) {
    if (!CellUtil.matchingFamily(metaCell, WALEdit.METAFAMILY)) {
      return Optional.empty();
    }
    FlushDescriptor flushDesc;
    try {
      flushDesc = WALEdit.getFlushDescriptor(metaCell);
    } catch (IOException e) {
      LOG.warn("Failed to parse FlushDescriptor from {}", metaCell);
      return Optional.empty();
    }
    if (flushDesc != null && isStartFlushAllStores(flushDesc)) {
      return Optional.of(flushDesc);
    } else {
      return Optional.empty();
    }
  }

  private long clearAllEntries() {
    long toClearSize = 0;
    for (SinkEntry entry : entries) {
      toClearSize += entry.size;
      entry.replicated();
    }
    entries.clear();
    pendingSize -= toClearSize;
    manager.decrease(toClearSize);
    return toClearSize;
  }

  /**
   * Add this edit to replication queue.
   * <p/>
   * The {@code rpcCall} is for retaining the cells if the edit is built within an rpc call and the
   * rpc call has cell scanner, which is off heap.
   */
  public void add(WALKeyImpl key, WALEdit edit, ServerCall<?> rpcCall) {
    if (!tableDesc.hasRegionMemStoreReplication() && !edit.isMetaEdit()) {
      // only replicate meta edit if region memstore replication is not enabled
      return;
    }
    synchronized (entries) {
      if (stopping) {
        return;
      }
      if (edit.isMetaEdit()) {
        // check whether we flushed all stores, which means we could drop all the previous edits,
        // and also, recover from the previous failure of some replicas
        for (Cell metaCell : edit.getCells()) {
          getStartFlushAllDescriptor(metaCell).ifPresent(flushDesc -> {
            long flushSequenceNumber = flushDesc.getFlushSequenceNumber();
            lastFlushedSequenceId = flushSequenceNumber;
            long clearedCount = entries.size();
            long clearedSize = clearAllEntries();
            if (LOG.isDebugEnabled()) {
              LOG.debug(
                "Got a flush all request with sequence id {}, clear {} pending" +
                  " entries with size {}, clear failed replicas {}",
                flushSequenceNumber, clearedCount,
                StringUtils.TraditionalBinaryPrefix.long2String(clearedSize, "", 1),
                failedReplicas);
            }
            failedReplicas.clear();
            flushRequester.recordFlush(flushSequenceNumber);
          });
        }
      }
      if (failedReplicas.size() == regionReplication - 1) {
        // this means we have marked all the replicas as failed, so just give up here
        return;
      }
      SinkEntry entry = new SinkEntry(key, edit, rpcCall);
      entries.add(entry);
      pendingSize += entry.size;
      if (manager.increase(entry.size)) {
        if (!sending) {
          send();
        }
      } else {
        // we have run out of the max pending size, drop all the edits, and mark all replicas as
        // failed
        clearAllEntries();
        for (int replicaId = 1; replicaId < regionReplication; replicaId++) {
          failedReplicas.add(replicaId);
        }
        flushRequester.requestFlush(entry.key.getSequenceId());
      }
    }
  }

  long pendingSize() {
    return pendingSize;
  }

  /**
   * Stop the replication sink.
   * <p/>
   * Usually this should only be called when you want to close a region.
   */
  public void stop() {
    synchronized (entries) {
      stopping = true;
      clearAllEntries();
      if (!sending) {
        stopped = true;
        entries.notifyAll();
      }
    }
  }

  /**
   * Make sure that we have finished all the replicating requests.
   * <p/>
   * After returning, we can make sure there will be no new replicating requests to secondary
   * replicas.
   * <p/>
   * This is used to keep the replicating order the same with the WAL edit order when writing.
   */
  public void waitUntilStopped() throws InterruptedException {
    synchronized (entries) {
      while (!stopped) {
        entries.wait();
      }
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  IntHashSet getFailedReplicas() {
    synchronized (entries) {
      return this.failedReplicas;
    }
  }
}
