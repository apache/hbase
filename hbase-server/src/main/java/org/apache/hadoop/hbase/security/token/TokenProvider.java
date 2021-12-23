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
package org.apache.hadoop.hbase.security.token;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

import java.io.IOException;
import java.util.Collections;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a service for obtaining authentication tokens via the
 * {@link AuthenticationProtos} AuthenticationService coprocessor service.
 */
@CoreCoprocessor
@InterfaceAudience.Private
public class TokenProvider implements AuthenticationProtos.AuthenticationService.Interface,
    RegionCoprocessor {

  private static final Logger LOG = LoggerFactory.getLogger(TokenProvider.class);

  private AuthenticationTokenSecretManager secretManager;


  @Override
  public void start(CoprocessorEnvironment env) {
    // if running at region
    if (env instanceof RegionCoprocessorEnvironment) {
      RegionCoprocessorEnvironment regionEnv = (RegionCoprocessorEnvironment)env;
      /* Getting the RpcServer from a RegionCE is wrong. There cannot be an expectation that Region
       is hosted inside a RegionServer. If you need RpcServer, then pass in a RegionServerCE.
       TODO: FIX.
       */
      RegionServerServices rss = ((HasRegionServerServices)regionEnv).getRegionServerServices();
      RpcServerInterface server = rss.getRpcServer();
      SecretManager<?> mgr = ((RpcServer)server).getSecretManager();
      if (mgr instanceof AuthenticationTokenSecretManager) {
        secretManager = (AuthenticationTokenSecretManager)mgr;
      }
    }
  }

  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
  }

  /**
   * @param ugi A user group information.
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp(UserGroupInformation ugi) throws IOException {
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    if (authMethod != AuthenticationMethod.KERBEROS
        && authMethod != AuthenticationMethod.KERBEROS_SSL
        && authMethod != AuthenticationMethod.CERTIFICATE) {
      return false;
    }
    return true;
  }

  // AuthenticationService implementation

  @Override
  public Iterable<Service> getServices() {
    return Collections.singleton(
        AuthenticationProtos.AuthenticationService.newReflectiveService(this));
  }

  @Override
  public void getAuthenticationToken(RpcController controller,
                                     AuthenticationProtos.GetAuthenticationTokenRequest request,
                                     RpcCallback<AuthenticationProtos.GetAuthenticationTokenResponse> done) {
    AuthenticationProtos.GetAuthenticationTokenResponse.Builder response =
        AuthenticationProtos.GetAuthenticationTokenResponse.newBuilder();

    try {
      if (secretManager == null) {
        throw new IOException(
            "No secret manager configured for token authentication");
      }
      User currentUser = RpcServer.getRequestUser()
          .orElseThrow(() -> new AccessDeniedException("No authenticated user for request!"));
      UserGroupInformation ugi = currentUser.getUGI();
      if (!isAllowedDelegationTokenOp(ugi)) {
        LOG.warn("Token generation denied for user=" + currentUser.getName() + ", authMethod=" +
            ugi.getAuthenticationMethod());
        throw new AccessDeniedException(
            "Token generation only allowed for Kerberos authenticated clients");
      }

      Token<AuthenticationTokenIdentifier> token =
          secretManager.generateToken(currentUser.getName());
      response.setToken(ClientTokenUtil.toToken(token));
    } catch (IOException ioe) {
      CoprocessorRpcUtils.setControllerException(controller, ioe);
    }
    done.run(response.build());
  }

  @Override
  public void whoAmI(RpcController controller, AuthenticationProtos.WhoAmIRequest request,
      RpcCallback<AuthenticationProtos.WhoAmIResponse> done) {
    AuthenticationProtos.WhoAmIResponse.Builder response =
        AuthenticationProtos.WhoAmIResponse.newBuilder();
    RpcServer.getRequestUser().ifPresent(requestUser -> {
      response.setUsername(requestUser.getShortName());
      AuthenticationMethod method = requestUser.getUGI().getAuthenticationMethod();
      if (method != null) {
        response.setAuthMethod(method.name());
      }
    });
    done.run(response.build());
  }
}
