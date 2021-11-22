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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.protobuf.ReplicationProtobufUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;

/**
 * For replicating edits to secondary replicas.
 */
@InterfaceAudience.Private
public class AsyncRegionReplicationRetryingCaller extends AsyncRpcRetryingCaller<Void> {

  private final RegionInfo replica;

  private final Entry[] entries;

  public AsyncRegionReplicationRetryingCaller(HashedWheelTimer retryTimer,
    AsyncClusterConnectionImpl conn, int maxAttempts, long rpcTimeoutNs, long operationTimeoutNs,
    RegionInfo replica, List<Entry> entries) {
    super(retryTimer, conn, ConnectionUtils.getPriority(replica.getTable()),
      conn.connConf.getPauseNs(), conn.connConf.getPauseForCQTBENs(), maxAttempts,
      operationTimeoutNs, rpcTimeoutNs, conn.connConf.getStartLogErrorsCnt());
    this.replica = replica;
    this.entries = entries.toArray(new Entry[0]);
  }

  private void call(HRegionLocation loc) {
    AdminService.Interface stub;
    try {
      stub = conn.getAdminStub(loc.getServerName());
    } catch (IOException e) {
      onError(e,
        () -> "Get async admin stub to " + loc.getServerName() + " for " + replica + " failed",
        err -> conn.getLocator().updateCachedLocationOnError(loc, err));
      return;
    }
    Pair<ReplicateWALEntryRequest, CellScanner> pair = ReplicationProtobufUtil
      .buildReplicateWALEntryRequest(entries, replica.getEncodedNameAsBytes(), null, null, null);
    resetCallTimeout();
    controller.setCellScanner(pair.getSecond());
    stub.replicateToReplica(controller, pair.getFirst(), r -> {
      if (controller.failed()) {
        onError(controller.getFailed(),
          () -> "Call to " + loc.getServerName() + " for " + replica + " failed",
          err -> conn.getLocator().updateCachedLocationOnError(loc, err));
      } else {
        future.complete(null);
      }
    });
  }

  @Override
  protected void doCall() {
    long locateTimeoutNs;
    if (operationTimeoutNs > 0) {
      locateTimeoutNs = remainingTimeNs();
      if (locateTimeoutNs <= 0) {
        completeExceptionally();
        return;
      }
    } else {
      locateTimeoutNs = -1L;
    }
    addListener(conn.getLocator().getRegionLocation(replica.getTable(), replica.getStartKey(),
      replica.getReplicaId(), RegionLocateType.CURRENT, locateTimeoutNs), (loc, error) -> {
        if (error != null) {
          onError(error, () -> "Locate " + replica + " failed", err -> {
          });
          return;
        }
        call(loc);
      });
  }
}
