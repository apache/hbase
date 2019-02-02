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

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;

/**
 * The implementation of AsyncClusterConnection.
 */
@InterfaceAudience.Private
class AsyncClusterConnectionImpl extends AsyncConnectionImpl implements AsyncClusterConnection {

  public AsyncClusterConnectionImpl(Configuration conf, AsyncRegistry registry, String clusterId,
      SocketAddress localAddress, User user) {
    super(conf, registry, clusterId, localAddress, user);
  }

  @Override
  public NonceGenerator getNonceGenerator() {
    return super.getNonceGenerator();
  }

  @Override
  public RpcClient getRpcClient() {
    return rpcClient;
  }

  @Override
  public AsyncRegionServerAdmin getRegionServerAdmin(ServerName serverName) {
    return new AsyncRegionServerAdmin(serverName, this);
  }

  @Override
  public CompletableFuture<FlushRegionResponse> flush(byte[] regionName,
      boolean writeFlushWALMarker) {
    RawAsyncHBaseAdmin admin = (RawAsyncHBaseAdmin) getAdmin();
    return admin.flushRegionInternal(regionName, writeFlushWALMarker);
  }

  @Override
  public CompletableFuture<Long> replay(TableName tableName, byte[] encodedRegionName, byte[] row,
      List<Entry> entries, int replicaId, int retries, long operationTimeoutNs) {
    return new AsyncRegionReplicaReplayRetryingCaller(RETRY_TIMER, this,
      ConnectionUtils.retries2Attempts(retries), operationTimeoutNs, tableName, encodedRegionName,
      row, entries, replicaId).call();
  }

  @Override
  public CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
      boolean reload) {
    return getLocator().getRegionLocations(tableName, row, RegionLocateType.CURRENT, reload, -1L);
  }

  @Override
  public CompletableFuture<String> prepareBulkLoad(TableName tableName) {
    return callerFactory.<String> single().table(tableName).row(HConstants.EMPTY_START_ROW)
      .action((controller, loc, stub) -> ConnectionUtils
        .<TableName, PrepareBulkLoadRequest, PrepareBulkLoadResponse, String> call(controller, loc,
          stub, tableName, (rn, tn) -> {
            RegionSpecifier region =
              RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, rn);
            return PrepareBulkLoadRequest.newBuilder()
              .setTableName(ProtobufUtil.toProtoTableName(tn)).setRegion(region).build();
          }, (s, c, req, done) -> s.prepareBulkLoad(c, req, done),
          (c, resp) -> resp.getBulkToken()))
      .call();
  }

  @Override
  public CompletableFuture<Boolean> bulkLoad(TableName tableName,
      List<Pair<byte[], String>> familyPaths, byte[] row, boolean assignSeqNum, Token<?> userToken,
      String bulkToken, boolean copyFiles) {
    return callerFactory.<Boolean> single().table(tableName).row(row)
      .action((controller, loc, stub) -> ConnectionUtils
        .<Void, BulkLoadHFileRequest, BulkLoadHFileResponse, Boolean> call(controller, loc, stub,
          null,
          (rn, nil) -> RequestConverter.buildBulkLoadHFileRequest(familyPaths, rn, assignSeqNum,
            userToken, bulkToken, copyFiles),
          (s, c, req, done) -> s.bulkLoadHFile(c, req, done), (c, resp) -> resp.getLoaded()))
      .call();
  }

  @Override
  public CompletableFuture<Void> cleanupBulkLoad(TableName tableName, String bulkToken) {
    return callerFactory.<Void> single().table(tableName).row(HConstants.EMPTY_START_ROW)
      .action((controller, loc, stub) -> ConnectionUtils
        .<String, CleanupBulkLoadRequest, CleanupBulkLoadResponse, Void> call(controller, loc, stub,
          bulkToken, (rn, bt) -> {
            RegionSpecifier region =
              RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME, rn);
            return CleanupBulkLoadRequest.newBuilder().setRegion(region).setBulkToken(bt).build();
          }, (s, c, req, done) -> s.cleanupBulkLoad(c, req, done), (c, resp) -> null))
      .call();
  }
}
