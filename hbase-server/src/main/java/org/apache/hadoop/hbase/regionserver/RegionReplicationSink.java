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
package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.ipc.ServerCall;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * The class for replicating WAL edits to secondary replicas, one instance per region.
 */
@InterfaceAudience.Private
public class RegionReplicationSink {

  private static final Logger LOG = LoggerFactory.getLogger(RegionReplicationSink.class);

  public static final String MAX_PENDING_SIZE = "hbase.region.read-replica.sink.max-pending-size";

  public static final long MAX_PENDING_SIZE_DEFAULT = 10L * 1024 * 1024;

  public static final String RETRIES_NUMBER = "hbase.region.read-replica.sink.retries.number";

  public static final int RETRIES_NUMBER_DEFAULT = 3;

  public static final String RPC_TIMEOUT_MS = "hbase.region.read-replica.sink.rpc.timeout.ms";

  public static final long RPC_TIMEOUT_MS_DEFAULT = 200;

  public static final String OPERATION_TIMEOUT_MS =
    "hbase.region.read-replica.sink.operation.timeout.ms";

  public static final long OPERATION_TIMEOUT_MS_DEFAULT = 1000;

  private static final class SinkEntry {

    final WALKeyImpl key;

    final WALEdit edit;

    final ServerCall<?> rpcCall;

    SinkEntry(WALKeyImpl key, WALEdit edit, ServerCall<?> rpcCall) {
      this.key = key;
      this.edit = edit;
      this.rpcCall = rpcCall;
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

  private final int regionReplication;

  private final boolean hasRegionMemStoreReplication;

  private final Queue<SinkEntry> entries = new ArrayDeque<>();

  private final AsyncClusterConnection conn;

  private final int retries;

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  private CompletableFuture<Void> future;

  private boolean stopping;

  private boolean stopped;

  RegionReplicationSink(Configuration conf, RegionInfo primary, int regionReplication,
    boolean hasRegionMemStoreReplication, AsyncClusterConnection conn) {
    Preconditions.checkArgument(RegionReplicaUtil.isDefaultReplica(primary), "%s is not primary",
      primary);
    Preconditions.checkArgument(regionReplication > 1,
      "region replication should be greater than 1 but got %s", regionReplication);
    this.primary = primary;
    this.regionReplication = regionReplication;
    this.hasRegionMemStoreReplication = hasRegionMemStoreReplication;
    this.conn = conn;
    this.retries = conf.getInt(RETRIES_NUMBER, RETRIES_NUMBER_DEFAULT);
    this.rpcTimeoutNs =
      TimeUnit.MILLISECONDS.toNanos(conf.getLong(RPC_TIMEOUT_MS, RPC_TIMEOUT_MS_DEFAULT));
    this.operationTimeoutNs = TimeUnit.MILLISECONDS
      .toNanos(conf.getLong(OPERATION_TIMEOUT_MS, OPERATION_TIMEOUT_MS_DEFAULT));
  }

  private void send() {
    List<SinkEntry> toSend = new ArrayList<>();
    for (SinkEntry entry;;) {
      entry = entries.poll();
      if (entry == null) {
        break;
      }
      toSend.add(entry);
    }
    List<WAL.Entry> walEntries =
      toSend.stream().map(e -> new WAL.Entry(e.key, e.edit)).collect(Collectors.toList());
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int replicaId = 1; replicaId < regionReplication; replicaId++) {
      RegionInfo replica = RegionReplicaUtil.getRegionInfoForReplica(primary, replicaId);
      futures.add(conn.replicate(replica, walEntries, retries, rpcTimeoutNs, operationTimeoutNs));
    }
    future = CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    FutureUtils.addListener(future, (r, e) -> {
      if (e != null) {
        // TODO: drop pending edits and issue a flush
        LOG.warn("Failed to replicate to secondary replicas for {}", primary, e);
      }
      toSend.forEach(SinkEntry::replicated);
      synchronized (entries) {
        future = null;
        if (stopping) {
          stopped = true;
          entries.notifyAll();
          return;
        }
        if (!entries.isEmpty()) {
          send();
        }
      }
    });
  }

  /**
   * Add this edit to replication queue.
   * <p/>
   * The {@code rpcCall} is for retaining the cells if the edit is built within an rpc call and the
   * rpc call has cell scanner, which is off heap.
   */
  public void add(WALKeyImpl key, WALEdit edit, ServerCall<?> rpcCall) {
    if (!hasRegionMemStoreReplication && !edit.isMetaEdit()) {
      // only replicate meta edit if region memstore replication is not enabled
      return;
    }
    synchronized (entries) {
      if (stopping) {
        return;
      }
      // TODO: limit the total cached entries here, and we should have a global limitation, not for
      // only this region.
      entries.add(new SinkEntry(key, edit, rpcCall));
      if (future == null) {
        send();
      }
    }
  }

  /**
   * Stop the replication sink.
   * <p/>
   * Usually this should only be called when you want to close a region.
   */
  void stop() {
    synchronized (entries) {
      stopping = true;
      if (future == null) {
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
  void waitUntilStopped() throws InterruptedException {
    synchronized (entries) {
      while (!stopped) {
        entries.wait();
      }
    }
  }
}
