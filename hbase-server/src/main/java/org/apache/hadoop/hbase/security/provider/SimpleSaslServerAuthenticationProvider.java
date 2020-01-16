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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class SimpleSaslServerAuthenticationProvider extends SimpleSaslAuthenticationProvider
    implements SaslServerAuthenticationProvider {

  @Override
  public AttemptingUserProvidingSaslServer createServer(
      SecretManager<TokenIdentifier> secretManager,
      Map<String, String> saslProps) throws IOException {
    throw new RuntimeException("HBase SIMPLE authentication doesn't use SASL");
  }

  @Override
  public boolean supportsProtocolAuthentication() {
    return true;
  }

  @Override
  public UserGroupInformation getAuthorizedUgi(String authzId,
      SecretManager<TokenIdentifier> secretManager) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(authzId);
    ugi.setAuthenticationMethod(getSaslAuthMethod().getAuthMethod());
    return ugi;
  }
}
