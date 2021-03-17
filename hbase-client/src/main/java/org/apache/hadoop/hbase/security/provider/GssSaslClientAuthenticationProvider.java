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
package org.apache.hadoop.hbase.security.provider;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

@InterfaceAudience.Private
public class GssSaslClientAuthenticationProvider extends GssSaslAuthenticationProvider
    implements SaslClientAuthenticationProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      GssSaslClientAuthenticationProvider.class);

  String getServerPrincipal(Configuration conf, SecurityInfo securityInfo, InetAddress server)
      throws IOException {
    String serverKey = securityInfo.getServerPrincipal();
    if (serverKey == null) {
      throw new IllegalArgumentException(
          "Can't obtain server Kerberos config key from SecurityInfo");
    }
    return SecurityUtil.getServerPrincipal(conf.get(serverKey),
        server.getCanonicalHostName().toLowerCase());
  }

  @Override
  public SaslClient createClient(Configuration conf, InetAddress serverAddr,
      SecurityInfo securityInfo, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException {
    String serverPrincipal = getServerPrincipal(conf, securityInfo, serverAddr);
    LOG.debug("Setting up Kerberos RPC to server={}", serverPrincipal);
    String[] names = SaslUtil.splitKerberosName(serverPrincipal);
    if (names.length != 3) {
      throw new IOException("Kerberos principal '" + serverPrincipal
          + "' does not have the expected format");
    }
    return Sasl.createSaslClient(new String[] { getSaslAuthMethod().getSaslMechanism() }, null,
        names[0], names[1], saslProps, null);
  }

  @Override
  public UserInformation getUserInfo(User user) {
    UserInformation.Builder userInfoPB = UserInformation.newBuilder();
    // Send effective user for Kerberos auth
    userInfoPB.setEffectiveUser(user.getUGI().getUserName());
    return userInfoPB.build();
  }

  @Override
  public boolean canRetry() {
    return true;
  }

  @Override
  public void relogin() throws IOException {
    // Check if UGI thinks we need to do another login
    if (UserGroupInformation.isLoginKeytabBased()) {
      UserGroupInformation.getLoginUser().reloginFromKeytab();
    } else {
      UserGroupInformation.getLoginUser().reloginFromTicketCache();
    }
  }

  @Override
  public UserGroupInformation getRealUser(User user) {
    final UserGroupInformation ugi = user.getUGI();
    // Unwrap the UGI with the real user when we're using Kerberos auth
    if (ugi != null && ugi.getRealUser() != null) {
      return ugi.getRealUser();
    }

    // Otherwise, use the UGI we were given
    return ugi;
  }
}
