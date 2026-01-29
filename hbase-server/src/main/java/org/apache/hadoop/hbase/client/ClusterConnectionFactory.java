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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The factory for creating {@link AsyncClusterConnection}.
 */
@InterfaceAudience.Private
public final class ClusterConnectionFactory {

  public static final String HBASE_SERVER_CLUSTER_CONNECTION_IMPL =
    "hbase.server.cluster.connection.impl";

  private ClusterConnectionFactory() {
  }

  private static AsyncClusterConnection createAsyncClusterConnection(Configuration conf,
    ConnectionRegistry registry, SocketAddress localAddress, User user) throws IOException {
    String clusterId = FutureUtils.get(registry.getClusterId());
    org.apache.hadoop.hbase.TableName metaTableName = FutureUtils.get(registry.getMetaTableName());
    Class<? extends AsyncClusterConnection> clazz =
      conf.getClass(HBASE_SERVER_CLUSTER_CONNECTION_IMPL, AsyncClusterConnectionImpl.class,
        AsyncClusterConnection.class);
    try {
      return user
        .runAs((PrivilegedExceptionAction<? extends AsyncClusterConnection>) () -> ReflectionUtils
          .newInstance(clazz, conf, registry, clusterId, metaTableName, localAddress, user));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Create a new {@link AsyncClusterConnection} instance.
   * <p/>
   * Unlike what we have done in {@link ConnectionFactory}, here we just return an
   * {@link AsyncClusterConnection} instead of a {@link java.util.concurrent.CompletableFuture},
   * which means this method could block on fetching the cluster id. This is just used to simplify
   * the implementation, as when starting new region servers, we do not need to be event-driven. Can
   * change later if we want a {@link java.util.concurrent.CompletableFuture} here.
   */
  public static AsyncClusterConnection createAsyncClusterConnection(Configuration conf,
    SocketAddress localAddress, User user) throws IOException {
    return createAsyncClusterConnection(conf, ConnectionRegistryFactory.create(conf, user),
      localAddress, user);
  }

  /**
   * Create a new {@link AsyncClusterConnection} instance.
   * <p/>
   * This is usually used in replication, the given {@code uri} specifies the connection info of the
   * remote cluster.
   */
  public static AsyncClusterConnection createAsyncClusterConnection(URI uri, Configuration conf,
    SocketAddress localAddress, User user) throws IOException {
    ConnectionRegistry registry = uri != null
      ? ConnectionRegistryFactory.create(uri, conf, user)
      : ConnectionRegistryFactory.create(conf, user);
    return createAsyncClusterConnection(conf, registry, localAddress, user);
  }

  /**
   * Create a new {@link AsyncClusterConnection} instance to be used at server side where we have a
   * {@link ConnectionRegistryEndpoint}.
   */
  public static AsyncClusterConnection createAsyncClusterConnection(
    ConnectionRegistryEndpoint endpoint, Configuration conf, SocketAddress localAddress, User user)
    throws IOException {
    ShortCircuitConnectionRegistry registry = new ShortCircuitConnectionRegistry(endpoint);
    return createAsyncClusterConnection(conf, registry, localAddress, user);
  }
}
