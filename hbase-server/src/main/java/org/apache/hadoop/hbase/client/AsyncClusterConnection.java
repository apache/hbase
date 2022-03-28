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
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.security.token.Token;
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
   * Return all the replicas for a region. Used for region replica replication.
   */
  CompletableFuture<RegionLocations> getRegionLocations(TableName tableName, byte[] row,
      boolean reload);

  /**
   * Return the token for this bulk load.
   */
  CompletableFuture<String> prepareBulkLoad(TableName tableName);

  /**
   * Securely bulk load a list of HFiles, passing additional list of clusters ids tracking clusters
   * where the given bulk load has already been processed (important for bulk loading replication).
   * <p/>
   * Defined as default here to avoid breaking callers who rely on the bulkLoad version that does
   * not expect additional clusterIds param.
   * @param tableName the target table
   * @param familyPaths hdfs path for the the table family dirs containg files to be loaded.
   * @param row row key.
   * @param assignSeqNum seq num for the event on WAL.
   * @param userToken user token.
   * @param bulkToken bulk load token.
   * @param copyFiles flag for copying the loaded hfiles.
   * @param clusterIds list of cluster ids where the given bulk load has already been processed.
   * @param replicate flags if the bulkload is targeted for replication.
   */
  CompletableFuture<Boolean> bulkLoad(TableName tableName, List<Pair<byte[], String>> familyPaths,
    byte[] row, boolean assignSeqNum, Token<?> userToken, String bulkToken, boolean copyFiles,
    List<String> clusterIds, boolean replicate);

  /**
   * Clean up after finishing bulk load, no matter success or not.
   */
  CompletableFuture<Void> cleanupBulkLoad(TableName tableName, String bulkToken);

  /**
   * Get live region servers from masters.
   */
  CompletableFuture<List<ServerName>> getLiveRegionServers(MasterAddressTracker masterAddrTracker,
    int count);

  /**
   * Get the bootstrap node list of another region server.
   */
  CompletableFuture<List<ServerName>> getAllBootstrapNodes(ServerName regionServer);

  /**
   * Replicate wal edits to a secondary replica.
   */
  CompletableFuture<Void> replicate(RegionInfo replica, List<Entry> entries, int numRetries,
    long rpcTimeoutNs, long operationTimeoutNs);
}
