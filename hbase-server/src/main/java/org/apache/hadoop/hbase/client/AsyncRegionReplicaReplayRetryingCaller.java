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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.util.HashedWheelTimer;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;

/**
 * For replaying edits for region replica.
 * <p/>
 * The mainly difference here is that, every time after locating, we will check whether the region
 * name is equal, if not, we will give up, as this usually means the region has been split or
 * merged, and the new region(s) should already have all the data of the parent region(s).
 * <p/>
 * Notice that, the return value is the edits we skipped, as the original response message is not
 * used at upper layer.
 */
@InterfaceAudience.Private
public class AsyncRegionReplicaReplayRetryingCaller extends AsyncRpcRetryingCaller<Long> {

  private static final Logger LOG =
    LoggerFactory.getLogger(AsyncRegionReplicaReplayRetryingCaller.class);

  private final TableName tableName;

  private final byte[] encodedRegionName;

  private final byte[] row;

  private final Entry[] entries;

  private final int replicaId;

  public AsyncRegionReplicaReplayRetryingCaller(HashedWheelTimer retryTimer,
      AsyncClusterConnectionImpl conn, int maxAttempts, long operationTimeoutNs,
      TableName tableName, byte[] encodedRegionName, byte[] row, List<Entry> entries,
      int replicaId) {
    super(retryTimer, conn, ConnectionUtils.getPriority(tableName), conn.connConf.getPauseNs(),
      conn.connConf.getPauseForCQTBENs(), maxAttempts, operationTimeoutNs,
      conn.connConf.getWriteRpcTimeoutNs(), conn.connConf.getStartLogErrorsCnt());
    this.tableName = tableName;
    this.encodedRegionName = encodedRegionName;
    this.row = row;
    this.entries = entries.toArray(new Entry[0]);
    this.replicaId = replicaId;
  }

  private void call(HRegionLocation loc) {
    if (!Bytes.equals(encodedRegionName, loc.getRegion().getEncodedNameAsBytes())) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(
          "Skipping {} entries in table {} because located region {} is different than" +
            " the original region {} from WALEdit",
          entries.length, tableName, loc.getRegion().getEncodedName(),
          Bytes.toStringBinary(encodedRegionName));
        for (Entry entry : entries) {
          LOG.trace("Skipping : " + entry);
        }
      }
      future.complete(Long.valueOf(entries.length));
      return;
    }

    AdminService.Interface stub;
    try {
      stub = conn.getAdminStub(loc.getServerName());
    } catch (IOException e) {
      onError(e,
        () -> "Get async admin stub to " + loc.getServerName() + " for '" +
          Bytes.toStringBinary(row) + "' in " + loc.getRegion().getEncodedName() + " of " +
          tableName + " failed",
        err -> conn.getLocator().updateCachedLocationOnError(loc, err));
      return;
    }
    Pair<ReplicateWALEntryRequest, CellScanner> p = ReplicationProtbufUtil
      .buildReplicateWALEntryRequest(entries, encodedRegionName, null, null, null);
    resetCallTimeout();
    controller.setCellScanner(p.getSecond());
    stub.replay(controller, p.getFirst(), r -> {
      if (controller.failed()) {
        onError(controller.getFailed(),
          () -> "Call to " + loc.getServerName() + " for '" + Bytes.toStringBinary(row) + "' in " +
            loc.getRegion().getEncodedName() + " of " + tableName + " failed",
          err -> conn.getLocator().updateCachedLocationOnError(loc, err));
      } else {
        future.complete(0L);
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
    addListener(conn.getLocator().getRegionLocation(tableName, row, replicaId,
      RegionLocateType.CURRENT, locateTimeoutNs), (loc, error) -> {
        if (error != null) {
          onError(error,
            () -> "Locate '" + Bytes.toStringBinary(row) + "' in " + tableName + " failed", err -> {
            });
          return;
        }
        call(loc);
      });
  }
}
