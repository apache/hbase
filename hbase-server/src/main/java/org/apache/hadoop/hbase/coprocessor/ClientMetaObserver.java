/*
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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Defines coprocessor hooks for interacting operations for ClientMetaService which is
 * responsible for ZooKeeperless HBase service discovery. <br>
 * <br>
 * Since most implementations will be interested in only a subset of hooks, this class uses
 * 'default' functions to avoid having to add unnecessary overrides. When the functions are
 * non-empty, it's simply to satisfy the compiler by returning value of expected (non-void) type. It
 * is done in a way that these default definitions act as no-op. So our suggestion to implementation
 * would be to not call these 'default' methods from overrides. <br>
 * <br>
 * <h3>Exception Handling</h3> For all functions, exception handling is done as follows:
 * <ul>
 * <li>Exceptions of type {@link IOException} are reported back to client.</li>
 * <li>For any other kind of exception:
 * <ul>
 * <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then the
 * server aborts.</li>
 * <li>Otherwise, coprocessor is removed from the server and
 * {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 * </ul>
 * </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface ClientMetaObserver {
  /**
   * Called before getting the cluster ID.
   * @param ctx the environment to interact with the framework
   * @throws IOException
   */
  default void preGetClusterId(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * Called after we got the cluster ID.
   * @param ctx the environment to interact with the framework
   * @param clusterId the actual cluster ID
   * @return the cluster ID which is returned to the client.
   * @throws IOException
   */
  default String postGetClusterId(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx, String clusterId)
    throws IOException {
    return clusterId;
  }

  /**
   * Called before getting the active master.
   * @param ctx the environment to interact with the framework
   * @throws IOException
   */
  default void preGetActiveMaster(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * Called after we got the active master.
   * @param ctx the environment to interact with the framework
   * @param serverName the actual active master address. It can be {@code null} if there is no
   *                   active master.
   * @return the active master address which is returned to the client. It can be {@code null}.
   * @throws IOException
   */
  default ServerName postGetActiveMaster(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx, ServerName serverName)
    throws IOException {
    return serverName;
  }

  /**
   * Called before getting the master servers.
   * @param ctx the environment to interact with the framework
   * @throws IOException
   */
  default void preGetMasters(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * Called after we got the master servers.
   * @param ctx the environment to interact with the framework
   * @param serverNames the actual master servers addresses and active statuses
   * @return the master servers addresses and active statuses which are returned to the client.
   * @throws IOException
   */
  default Map<ServerName, Boolean> postGetMasters(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx, Map<ServerName, Boolean> serverNames)
    throws IOException {
    return serverNames;
  }

  /**
   * Called before getting bootstrap nodes.
   * @param ctx the environment to interact with the framework
   * @throws IOException
   */
  default void preGetBootstrapNodes(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * Called after we got bootstrap nodes.
   * @param ctx the environment to interact with the framework
   * @param bootstrapNodes the actual bootstrap nodes
   * @return the bootstrap nodes which are returned to the client.
   * @throws IOException
   */
  default List<ServerName> postGetBootstrapNodes(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx, List<ServerName> bootstrapNodes)
    throws IOException {
    return bootstrapNodes;
  }

  /**
   * Called before getting the meta region locations.
   * @param ctx the environment to interact with the framework
   * @throws IOException
   */
  default void preGetMetaLocations(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx)
    throws IOException {
  }

  /**
   * Called after we got the meta region locations.
   * @param ctx the environment to interact with the framework
   * @param metaLocations the actual meta region locations
   * @return the meta region locations which are returned to the client.
   * @throws IOException
   */
  default List<HRegionLocation> postGetMetaLocations(
    ObserverContext<ClientMetaCoprocessorEnvironment> ctx, List<HRegionLocation> metaLocations)
    throws IOException {
    return metaLocations;
  }
}
