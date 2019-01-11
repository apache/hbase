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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.FlushRegionResponse;

/**
 * The asynchronous connection for internal usage.
 */
@InterfaceAudience.Private
public interface AsyncClusterConnection extends AsyncConnection {

  /**
   * Get the admin service for the given region server.
   */
  AsyncRegionServerAdmin getRegionServerAdmin(ServerName serverName);

  /**
   * Get the nonce generator for this connection.
   */
  NonceGenerator getNonceGenerator();

  /**
   * Get the rpc client we used to communicate with other servers.
   */
  RpcClient getRpcClient();

  /**
   * Flush a region and get the response.
   */
  CompletableFuture<FlushRegionResponse> flush(byte[] regionName, boolean writeFlushWALMarker);

  /**
   * Replicate wal edits for replica regions. The return value is the edits we skipped, as the
   * original return value is useless.
   */
  CompletableFuture<Long> replay(TableName tableName, byte[] encodedRegionName, byte[] row,
      List<Entry> entries, int replicaId, int numRetries, long operationTimeoutNs);

  /**
   * Return all the replicas for a region. Used for regiong replica replication.
   */
  CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
      boolean reload);
}
