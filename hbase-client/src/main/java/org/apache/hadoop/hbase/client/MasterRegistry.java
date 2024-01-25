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

import static org.apache.hadoop.hbase.HConstants.MASTER_ADDRS_KEY;
import static org.apache.hadoop.hbase.util.DNS.getHostname;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.DNS.ServerType;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.base.Strings;
import org.apache.hbase.thirdparty.com.google.common.net.HostAndPort;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos.GetMastersResponse;

/**
 * Master based registry implementation. Makes RPCs to the configured master addresses from config
 * {@value org.apache.hadoop.hbase.HConstants#MASTER_ADDRS_KEY}.
 * <p/>
 * It supports hedged reads, set the fan out of the requests batch by
 * {@link #MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY} to a value greater than {@code 1} will enable
 * it(the default value is {@link AbstractRpcBasedConnectionRegistry#HEDGED_REQS_FANOUT_DEFAULT}).
 * <p/>
 * @deprecated Since 2.5.0, will be removed in 4.0.0. Use {@link RpcConnectionRegistry} instead.
 */
@Deprecated
@InterfaceAudience.Private
public class MasterRegistry extends AbstractRpcBasedConnectionRegistry {

  /** Configuration key that controls the fan out of requests **/
  public static final String MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY =
    "hbase.client.master_registry.hedged.fanout";

  public static final String MASTER_REGISTRY_INITIAL_REFRESH_DELAY_SECS =
    "hbase.client.master_registry.initial_refresh_delay_secs";

  public static final String MASTER_REGISTRY_PERIODIC_REFRESH_INTERVAL_SECS =
    "hbase.client.master_registry.refresh_interval_secs";

  public static final String MASTER_REGISTRY_MIN_SECS_BETWEEN_REFRESHES =
    "hbase.client.master_registry.min_secs_between_refreshes";

  private static final String MASTER_ADDRS_CONF_SEPARATOR = ",";

  /**
   * Supplies the default master port we should use given the provided configuration.
   * @param conf Configuration to parse from.
   */
  private static int getDefaultMasterPort(Configuration conf) {
    final int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
    if (port == 0) {
      // Master port may be set to 0. We should substitute the default port in that case.
      return HConstants.DEFAULT_MASTER_PORT;
    }
    return port;
  }

  /**
   * Parses the list of master addresses from the provided configuration. Supported format is comma
   * separated host[:port] values. If no port number if specified, default master port is assumed.
   * @param conf Configuration to parse from.
   */
  public static Set<ServerName> parseMasterAddrs(Configuration conf) throws UnknownHostException {
    final int defaultPort = getDefaultMasterPort(conf);
    final Set<ServerName> masterAddrs = new HashSet<>();
    final String configuredMasters = getMasterAddr(conf);
    for (String masterAddr : Splitter.onPattern(MASTER_ADDRS_CONF_SEPARATOR)
      .split(configuredMasters)) {
      final HostAndPort masterHostPort =
        HostAndPort.fromString(masterAddr.trim()).withDefaultPort(defaultPort);
      masterAddrs.add(ServerName.valueOf(masterHostPort.toString(), ServerName.NON_STARTCODE));
    }
    Preconditions.checkArgument(!masterAddrs.isEmpty(), "At least one master address is needed");
    return masterAddrs;
  }

  private final String connectionString;

  MasterRegistry(Configuration conf, User user) throws IOException {
    super(conf, user, MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY,
      MASTER_REGISTRY_INITIAL_REFRESH_DELAY_SECS, MASTER_REGISTRY_PERIODIC_REFRESH_INTERVAL_SECS,
      MASTER_REGISTRY_MIN_SECS_BETWEEN_REFRESHES);
    connectionString = getConnectionString(conf);
  }

  @Override
  protected Set<ServerName> getBootstrapNodes(Configuration conf) throws IOException {
    return parseMasterAddrs(conf);
  }

  @Override
  protected CompletableFuture<Set<ServerName>> fetchEndpoints() {
    return getMasters();
  }

  @Override
  public String getConnectionString() {
    return connectionString;
  }

  static String getConnectionString(Configuration conf) throws UnknownHostException {
    return getMasterAddr(conf);
  }

  /**
   * Builds the default master address end point if it is not specified in the configuration.
   * <p/>
   * Will be called in {@code HBaseTestingUtility}.
   */
  public static String getMasterAddr(Configuration conf) throws UnknownHostException {
    String masterAddrFromConf = conf.get(MASTER_ADDRS_KEY);
    if (!Strings.isNullOrEmpty(masterAddrFromConf)) {
      return masterAddrFromConf;
    }
    String hostname = getHostname(conf, ServerType.MASTER);
    int port = conf.getInt(HConstants.MASTER_PORT, HConstants.DEFAULT_MASTER_PORT);
    return String.format("%s:%d", hostname, port);
  }

  private static Set<ServerName> transformServerNames(GetMastersResponse resp) {
    return resp.getMasterServersList().stream()
      .map(s -> ProtobufUtil.toServerName(s.getServerName())).collect(Collectors.toSet());
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/(.*/MasterRegistry.java|src/test/.*)")
  CompletableFuture<Set<ServerName>> getMasters() {
    return this
      .<GetMastersResponse> call(
        (c, s, d) -> s.getMasters(c, GetMastersRequest.getDefaultInstance(), d),
        r -> r.getMasterServersCount() != 0, "getMasters()")
      .thenApply(MasterRegistry::transformServerNames);
  }
}
