/*
 *  Licensed to the Apache Software Foundation (ASF) under one
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
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.auth.AuthenticateCallbackHandler;
import org.apache.hadoop.hbase.security.oauthbearer.internals.OAuthBearerSaslServerProvider;
import org.apache.hadoop.hbase.security.oauthbearer.internals.knox.OAuthBearerSignedJwtValidatorCallbackHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class OAuthBearerSaslServerAuthenticationProvider
    extends OAuthBearerSaslAuthenticationProvider
    implements SaslServerAuthenticationProvider {

  private static final Logger LOG = LoggerFactory.getLogger(
    OAuthBearerSaslServerAuthenticationProvider.class);
  private Configuration hbaseConfiguration;
  private boolean initialized = false;

  static {
    OAuthBearerSaslServerProvider.initialize(); // not part of public API
    LOG.info("OAuthBearer SASL server provider has been initialized");
  }

  @Override public void init(Configuration conf) throws IOException {
    this.hbaseConfiguration = conf;
    this.initialized = true;
  }

  @Override public AttemptingUserProvidingSaslServer createServer(
    SecretManager<TokenIdentifier> secretManager, Map<String, String> saslProps)
    throws IOException {

    if (!initialized) {
      throw new IllegalStateException(
        "OAuthBearerSaslServerAuthenticationProvider must be initialized first.");
    }

    UserGroupInformation current = UserGroupInformation.getCurrentUser();
    String fullName = current.getUserName();
    LOG.debug("Server's OAuthBearer user name is {}", fullName);
    LOG.debug("OAuthBearer saslProps = {}", saslProps);

    try {
      return current.doAs(new PrivilegedExceptionAction<AttemptingUserProvidingSaslServer>() {
        @Override
        public AttemptingUserProvidingSaslServer run() throws SaslException {
          AuthenticateCallbackHandler callbackHandler =
            new OAuthBearerSignedJwtValidatorCallbackHandler();
          callbackHandler.configure(hbaseConfiguration, getSaslAuthMethod().getSaslMechanism(),
            saslProps);
          return new AttemptingUserProvidingSaslServer(Sasl.createSaslServer(
            getSaslAuthMethod().getSaslMechanism(), null, null, saslProps,
            callbackHandler), () -> null);
        }
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException("Failed to construct OAUTHBEARER SASL server");
    }
  }

  @Override public boolean supportsProtocolAuthentication() {
    return true;
  }

  @Override public UserGroupInformation getAuthorizedUgi(String authzId,
    SecretManager<TokenIdentifier> secretManager) {
    UserGroupInformation ugi = UserGroupInformation.createRemoteUser(authzId);
    ugi.setAuthenticationMethod(getSaslAuthMethod().getAuthMethod());
    return ugi;
  }
}
