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
import java.util.Map;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class GssSaslClientAuthenticationProvider extends AbstractSaslClientAuthenticationProvider {
  private static final String MECHANISM = "GSSAPI";
  private static final SaslAuthMethod SASL_AUTH_METHOD = new SaslAuthMethod(
      "KERBEROS", (byte)81, MECHANISM, AuthenticationMethod.KERBEROS);

  @Override
  public SaslClient createClient(Configuration conf, String serverPrincipal,
      Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException {
    String[] names = SaslUtil.splitKerberosName(serverPrincipal);
    if (names.length != 3) {
      throw new IOException("Kerberos principal '" + serverPrincipal
          + "' does not have the expected format");
    }
    return Sasl.createSaslClient(new String[] { MECHANISM }, null, names[0], names[1], saslProps,
        null);
  }

  @Override
  public SaslAuthMethod getSaslAuthMethod() {
    return SASL_AUTH_METHOD;
  }

  @Override
  public UserInformation getUserInfo(UserGroupInformation user) {
    UserInformation.Builder userInfoPB = UserInformation.newBuilder();
    // Send effective user for Kerberos auth
    userInfoPB.setEffectiveUser(user.getUserName());
    return userInfoPB.build();
  }

  @Override
  public boolean isKerberos() {
    return true;
  }
}
