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
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.ClientMetaService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetBootstrapNodesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetBootstrapNodesResponse;

/**
 * Rpc based connection registry. It will make use of the {@link ClientMetaService} to get registry
 * information.
 * <p/>
 * It needs bootstrap node list when start up, and then it will use {@link ClientMetaService} to
 * refresh the bootstrap node list periodically.
 * <p/>
 * Usually, you could set masters as the bootstrap nodes,as they will also implement the
 * {@link ClientMetaService}, and then, we will switch to use region servers after refreshing the
 * bootstrap nodes.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class RpcConnectionRegistry extends AbstractRpcBasedConnectionRegistry {

  /** Configuration key that controls the fan out of requests **/
  public static final String HEDGED_REQS_FANOUT_KEY = "hbase.client.bootstrap.hedged.fanout";

  /**
   * As end users could configure any nodes in a cluster as the initial bootstrap nodes, it is
   * possible that different end users will configure the same machine which makes the machine over
   * load. So we should have a shorter delay for the initial refresh, to let users quickly switch to
   * the bootstrap nodes we want them to connect to.
   * <p/>
   * The default value for initial refresh delay is 1/10 of periodic refresh interval.
   */
  public static final String INITIAL_REFRESH_DELAY_SECS =
    "hbase.client.bootstrap.initial_refresh_delay_secs";

  public static final String PERIODIC_REFRESH_INTERVAL_SECS =
    "hbase.client.bootstrap.refresh_interval_secs";

  public static final String MIN_SECS_BETWEEN_REFRESHES =
    "hbase.client.bootstrap.min_secs_between_refreshes";

  public static final String BOOTSTRAP_NODES = "hbase.client.bootstrap.servers";

  private static final char ADDRS_CONF_SEPARATOR = ',';

  private final String connectionString;

  RpcConnectionRegistry(Configuration conf, User user) throws IOException {
    super(conf, user, HEDGED_REQS_FANOUT_KEY, INITIAL_REFRESH_DELAY_SECS,
      PERIODIC_REFRESH_INTERVAL_SECS, MIN_SECS_BETWEEN_REFRESHES);
    connectionString = buildConnectionString(conf);
  }

  private String buildConnectionString(Configuration conf) throws UnknownHostException {
    final String configuredBootstrapNodes = conf.get(BOOTSTRAP_NODES);
    if (StringUtils.isBlank(configuredBootstrapNodes)) {
      return MasterRegistry.getConnectionString(conf);
    }
    return Splitter.on(ADDRS_CONF_SEPARATOR).trimResults().splitToStream(configuredBootstrapNodes)
      .collect(Collectors.joining(String.valueOf(ADDRS_CONF_SEPARATOR)));
  }

  @Override
  protected Set<ServerName> getBootstrapNodes(Configuration conf) throws IOException {
    // try get bootstrap nodes config first
    String configuredBootstrapNodes = conf.get(BOOTSTRAP_NODES);
    if (!StringUtils.isBlank(configuredBootstrapNodes)) {
      return Splitter.on(ADDRS_CONF_SEPARATOR).trimResults().splitToStream(configuredBootstrapNodes)
        .map(addr -> ServerName.valueOf(addr, ServerName.NON_STARTCODE))
        .collect(Collectors.toSet());
    } else {
      // otherwise, just use master addresses
      return MasterRegistry.parseMasterAddrs(conf);
    }
  }

  @Override
  public String getConnectionString() {
    return connectionString;
  }

  private static Set<ServerName> transformServerNames(GetBootstrapNodesResponse resp) {
    return resp.getServerNameList().stream().map(ProtobufUtil::toServerName)
      .collect(Collectors.toSet());
  }

  private CompletableFuture<Set<ServerName>> getBootstrapNodes() {
    return this
      .<GetBootstrapNodesResponse> call(
        (c, s, d) -> s.getBootstrapNodes(c, GetBootstrapNodesRequest.getDefaultInstance(), d),
        r -> r.getServerNameCount() != 0, "getBootstrapNodes()")
      .thenApply(RpcConnectionRegistry::transformServerNames);
  }

  @Override
  protected CompletableFuture<Set<ServerName>> fetchEndpoints() {
    return getBootstrapNodes();
  }
}
