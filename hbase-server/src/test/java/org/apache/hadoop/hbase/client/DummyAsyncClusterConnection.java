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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.security.token.Token;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;

/**
 * Can be overridden in UT if you only want to implement part of the methods in
 * {@link AsyncClusterConnection}.
 */
public class DummyAsyncClusterConnection implements AsyncClusterConnection {

  @Override
  public Configuration getConfiguration() {
    return null;
  }

  @Override
  public AsyncTableRegionLocator getRegionLocator(TableName tableName) {
    return null;
  }

  @Override
  public void clearRegionLocationCache() {
  }

  @Override
  public AsyncTableBuilder<AdvancedScanResultConsumer> getTableBuilder(TableName tableName) {
    return null;
  }

  @Override
  public AsyncTableBuilder<ScanResultConsumer> getTableBuilder(TableName tableName,
      ExecutorService pool) {
    return null;
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder() {
    return null;
  }

  @Override
  public AsyncAdminBuilder getAdminBuilder(ExecutorService pool) {
    return null;
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName) {
    return null;
  }

  @Override
  public AsyncBufferedMutatorBuilder getBufferedMutatorBuilder(TableName tableName,
      ExecutorService pool) {
    return null;
  }

  @Override
  public CompletableFuture<Hbck> getHbck() {
    return null;
  }

  @Override
  public Hbck getHbck(ServerName masterServer) throws IOException {
    return null;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public AsyncRegionServerAdmin getRegionServerAdmin(ServerName serverName) {
    return null;
  }

  @Override
  public NonceGenerator getNonceGenerator() {
    return null;
  }

  @Override
  public RpcClient getRpcClient() {
    return null;
  }

  @Override
  public CompletableFuture<FlushRegionResponse> flush(byte[] regionName,
      boolean writeFlushWALMarker) {
    return null;
  }

  @Override
  public CompletableFuture<Long> replay(TableName tableName, byte[] encodedRegionName, byte[] row,
      List<Entry> entries, int replicaId, int numRetries, long operationTimeoutNs) {
    return null;
  }

  @Override
  public CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
      boolean reload) {
    return null;
  }

  @Override
  public CompletableFuture<String> prepareBulkLoad(TableName tableName) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> bulkLoad(TableName tableName,
      List<Pair<byte[], String>> familyPaths, byte[] row, boolean assignSeqNum, Token<?> userToken,
      String bulkToken, boolean copyFiles) {
    return null;
  }

  @Override
  public CompletableFuture<Void> cleanupBulkLoad(TableName tableName, String bulkToken) {
    return null;
  }
}
