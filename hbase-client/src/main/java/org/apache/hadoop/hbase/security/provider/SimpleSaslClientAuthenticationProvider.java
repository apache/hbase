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

import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

@InterfaceAudience.Private
public class SimpleSaslClientAuthenticationProvider extends
    SimpleSaslAuthenticationProvider implements SaslClientAuthenticationProvider {

  @Override
  public SaslClient createClient(Configuration conf, InetAddress serverAddress,
      SecurityInfo securityInfo, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException {
    return null;
  }

  @Override
  public UserInformation getUserInfo(User user) {
    final UserGroupInformation ugi = user.getUGI();
    UserInformation.Builder userInfoPB = UserInformation.newBuilder();
    // Send both effective user and real user for simple auth
    userInfoPB.setEffectiveUser(ugi.getUserName());
    if (ugi.getRealUser() != null) {
      userInfoPB.setRealUser(ugi.getRealUser().getUserName());
    }
    return userInfoPB.build();
  }
}
