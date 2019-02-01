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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;

/** Internal methods on Connection that should not be used by user code. */
@InterfaceAudience.Private
// NOTE: Although this class is public, this class is meant to be used directly from internal
// classes and unit tests only.
public interface ClusterConnection extends Connection {

  /**
   * Key for configuration in Configuration whose value is the class we implement making a
   * new Connection instance.
   */
  String HBASE_CLIENT_CONNECTION_IMPL = "hbase.client.connection.impl";

  /**
   * @return - true if the master server is running
   * @deprecated this has been deprecated without a replacement
   */
  @Deprecated
  boolean isMasterRunning()
      throws MasterNotRunningException, ZooKeeperConnectionException;

  /**
   * Use this api to check if the table has been created with the specified number of
   * splitkeys which was used while creating the given table.
   * Note : If this api is used after a table's region gets splitted, the api may return
   * false.
   * @param tableName
   *          tableName
   * @param splitKeys
   *          splitKeys used while creating table
   * @throws IOException
   *           if a remote or network exception occurs
   */
  boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws
      IOException;

  /**
   * A table that isTableEnabled == false and isTableDisabled == false
   * is possible. This happens when a table has a lot of regions
   * that must be processed.
   * @param tableName table name
   * @return true if the table is enabled, false otherwise
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableEnabled(TableName tableName) throws IOException;

  /**
   * @param tableName table name
   * @return true if the table is disabled, false otherwise
   * @throws IOException if a remote or network exception occurs
   */
  boolean isTableDisabled(TableName tableName) throws IOException;

  /**
   * Retrieve TableState, represent current table state.
   * @param tableName table state for
   * @return state of the table
   */
  TableState getTableState(TableName tableName)  throws IOException;

  /**
   * Returns a {@link MasterKeepAliveConnection} to the active master
   */
  MasterKeepAliveConnection getMaster() throws IOException;

  /**
   * Get the admin service for master.
   */
  AdminService.BlockingInterface getAdminForMaster() throws IOException;

  /**
   * Establishes a connection to the region server at the specified address.
   * @param serverName the region server to connect to
   * @return proxy for HRegionServer
   * @throws IOException if a remote or network exception occurs
   */
  AdminService.BlockingInterface getAdmin(final ServerName serverName) throws IOException;

  /**
   * Establishes a connection to the region server at the specified address, and returns
   * a region client protocol.
   *
   * @param serverName the region server to connect to
   * @return ClientProtocol proxy for RegionServer
   * @throws IOException if a remote or network exception occurs
   *
   */
  ClientService.BlockingInterface getClient(final ServerName serverName) throws IOException;

  /**
   * @return Nonce generator for this ClusterConnection; may be null if disabled in configuration.
   */
  NonceGenerator getNonceGenerator();

  /**
   * @return Default AsyncProcess associated with this connection.
   */
  AsyncProcess getAsyncProcess();

  /**
   * Returns a new RpcRetryingCallerFactory from the given {@link Configuration}.
   * This RpcRetryingCallerFactory lets the users create {@link RpcRetryingCaller}s which can be
   * intercepted with the configured {@link RetryingCallerInterceptor}
   * @param conf configuration
   * @return RpcRetryingCallerFactory
   */
  RpcRetryingCallerFactory getNewRpcRetryingCallerFactory(Configuration conf);

  /**
   * @return Connection's RpcRetryingCallerFactory instance
   */
  RpcRetryingCallerFactory getRpcRetryingCallerFactory();

  /**
   * @return Connection's RpcControllerFactory instance
   */
  RpcControllerFactory getRpcControllerFactory();

  /**
   * @return a ConnectionConfiguration object holding parsed configuration values
   */
  ConnectionConfiguration getConnectionConfiguration();

  /**
   * @return the current statistics tracker associated with this connection
   */
  ServerStatisticTracker getStatisticsTracker();

  /**
   * @return the configured client backoff policy
   */
  ClientBackoffPolicy getBackoffPolicy();

  /**
   * @return the MetricsConnection instance associated with this connection.
   */
  MetricsConnection getConnectionMetrics();

  /**
   * @return true when this connection uses a {@link org.apache.hadoop.hbase.codec.Codec} and so
   *         supports cell blocks.
   */
  boolean hasCellBlockSupport();

  /**
   * @return the number of region servers that are currently running
   * @throws IOException if a remote or network exception occurs
   */
  int getCurrentNrHRS() throws IOException;
}
