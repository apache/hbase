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
package org.apache.hadoop.hbase.security;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.TokenIdentifier.Kind;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.BootstrapNodeProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.LockServiceProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegistryProtos;

/**
 * Maps RPC protocol interfaces to required configuration
 */
@InterfaceAudience.Private
public class SecurityInfo {
  /** Maps RPC service names to authentication information */
  private static ConcurrentMap<String, SecurityInfo> infos = new ConcurrentHashMap<>();
  // populate info for known services
  static {
    infos.put(AdminProtos.AdminService.getDescriptor().getName(),
      new SecurityInfo(SecurityConstants.REGIONSERVER_KRB_PRINCIPAL, Kind.HBASE_AUTH_TOKEN));
    infos.put(ClientProtos.ClientService.getDescriptor().getName(),
      new SecurityInfo(SecurityConstants.REGIONSERVER_KRB_PRINCIPAL, Kind.HBASE_AUTH_TOKEN));
    infos.put(MasterService.getDescriptor().getName(),
      new SecurityInfo(SecurityConstants.MASTER_KRB_PRINCIPAL, Kind.HBASE_AUTH_TOKEN));
    infos.put(RegionServerStatusProtos.RegionServerStatusService.getDescriptor().getName(),
      new SecurityInfo(SecurityConstants.MASTER_KRB_PRINCIPAL, Kind.HBASE_AUTH_TOKEN));
    infos.put(MasterProtos.HbckService.getDescriptor().getName(),
      new SecurityInfo(SecurityConstants.MASTER_KRB_PRINCIPAL, Kind.HBASE_AUTH_TOKEN));
    infos.put(RegistryProtos.ClientMetaService.getDescriptor().getName(),
      new SecurityInfo(Kind.HBASE_AUTH_TOKEN, SecurityConstants.MASTER_KRB_PRINCIPAL,
        SecurityConstants.REGIONSERVER_KRB_PRINCIPAL));
    infos.put(BootstrapNodeProtos.BootstrapNodeService.getDescriptor().getName(),
      new SecurityInfo(SecurityConstants.REGIONSERVER_KRB_PRINCIPAL, Kind.HBASE_AUTH_TOKEN));
    infos.put(LockServiceProtos.LockService.getDescriptor().getName(),
      new SecurityInfo(SecurityConstants.MASTER_KRB_PRINCIPAL, Kind.HBASE_AUTH_TOKEN));
    // NOTE: IF ADDING A NEW SERVICE, BE SURE TO UPDATE HBasePolicyProvider ALSO ELSE
    // new Service will not be found when all is Kerberized!!!!
  }

  /**
   * Adds a security configuration for a new service name. Note that this will have no effect if the
   * service name was already registered.
   */
  public static void addInfo(String serviceName, SecurityInfo securityInfo) {
    infos.putIfAbsent(serviceName, securityInfo);
  }

  /**
   * Returns the security configuration associated with the given service name.
   */
  public static SecurityInfo getInfo(String serviceName) {
    return infos.get(serviceName);
  }

  private final List<String> serverPrincipals;
  private final Kind tokenKind;

  public SecurityInfo(String serverPrincipal, Kind tokenKind) {
    this(tokenKind, serverPrincipal);
  }

  public SecurityInfo(Kind tokenKind, String... serverPrincipal) {
    Preconditions.checkArgument(serverPrincipal.length > 0);
    this.tokenKind = tokenKind;
    this.serverPrincipals = Arrays.asList(serverPrincipal);
  }

  /**
   * Although this class is IA.Private, we leak this class in
   * {@code SaslClientAuthenticationProvider}, so need to align with the deprecation cycle for that
   * class.
   * @deprecated Since 2.5.8 and 2.6.0, will be removed in 4.0.0. Use {@link #getServerPrincipals()}
   *             instead.
   */
  @Deprecated
  public String getServerPrincipal() {
    return serverPrincipals.get(0);
  }

  public List<String> getServerPrincipals() {
    return serverPrincipals;
  }

  public Kind getTokenKind() {
    return tokenKind;
  }
}
