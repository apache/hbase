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
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DigestSaslServerAuthenticationProvider extends DigestSaslAuthenticationProvider
    implements SaslServerAuthenticationProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      DigestSaslServerAuthenticationProvider.class);

  private AtomicReference<UserGroupInformation> attemptingUser = new AtomicReference<>(null);

  @Override
  public AttemptingUserProvidingSaslServer createServer(
      SecretManager<TokenIdentifier> secretManager,
      Map<String, String> saslProps) throws IOException {
    if (secretManager == null) {
      throw new AccessDeniedException("Server is not configured to do DIGEST authentication.");
    }
    final SaslServer server = Sasl.createSaslServer(getSaslAuthMethod().getSaslMechanism(), null,
      SaslUtil.SASL_DEFAULT_REALM, saslProps,
      new SaslDigestCallbackHandler(secretManager, attemptingUser));

    return new AttemptingUserProvidingSaslServer(server, () -> attemptingUser.get());
  }

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  private static class SaslDigestCallbackHandler implements CallbackHandler {
    private final SecretManager<TokenIdentifier> secretManager;
    private final AtomicReference<UserGroupInformation> attemptingUser;

    public SaslDigestCallbackHandler(SecretManager<TokenIdentifier> secretManager,
        AtomicReference<UserGroupInformation> attemptingUser) {
      this.secretManager = secretManager;
      this.attemptingUser = attemptingUser;
    }

    private char[] getPassword(TokenIdentifier tokenid) throws InvalidToken {
      return SaslUtil.encodePassword(secretManager.retrievePassword(tokenid));
    }

    /** {@inheritDoc} */
    @Override
    public void handle(Callback[] callbacks) throws InvalidToken, UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          continue; // realm is ignored
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL DIGEST-MD5 Callback");
        }
      }
      if (pc != null) {
        TokenIdentifier tokenIdentifier = HBaseSaslRpcServer.getIdentifier(
            nc.getDefaultName(), secretManager);
        attemptingUser.set(tokenIdentifier.getUser());
        char[] password = getPassword(tokenIdentifier);
        if (LOG.isTraceEnabled()) {
          LOG.trace("SASL server DIGEST-MD5 callback: setting password for client: {}",
              tokenIdentifier.getUser());
        }
        pc.setPassword(password);
      }
      if (ac != null) {
        // The authentication ID is the identifier (username) of the user who authenticated via
        // SASL (the one who provided credentials). The authorization ID is who the remote user
        // "asked" to be once they authenticated. This is akin to the UGI/JAAS "doAs" notion, e.g.
        // authentication ID is the "real" user and authorization ID is the "proxy" user.
        //
        // For DelegationTokens: we do not expect any remote user with a delegation token to execute
        // any RPCs as a user other than themselves. We disallow all cases where the real user
        // does not match who the remote user wants to execute a request as someone else.
        String authenticatedUserId = ac.getAuthenticationID();
        String userRequestedToExecuteAs = ac.getAuthorizationID();
        if (authenticatedUserId.equals(userRequestedToExecuteAs)) {
          ac.setAuthorized(true);
          if (LOG.isTraceEnabled()) {
            String username = HBaseSaslRpcServer.getIdentifier(
                userRequestedToExecuteAs, secretManager).getUser().getUserName();
            LOG.trace(
              "SASL server DIGEST-MD5 callback: setting " + "canonicalized client ID: " + username);
          }
          ac.setAuthorizedID(userRequestedToExecuteAs);
        } else {
          ac.setAuthorized(false);
        }
      }
    }
  }

  @Override
  public boolean supportsProtocolAuthentication() {
    return false;
  }

  @Override
  public UserGroupInformation getAuthorizedUgi(String authzId,
      SecretManager<TokenIdentifier> secretManager) throws IOException {
    UserGroupInformation authorizedUgi;
    TokenIdentifier tokenId = HBaseSaslRpcServer.getIdentifier(authzId, secretManager);
    authorizedUgi = tokenId.getUser();
    if (authorizedUgi == null) {
      throw new AccessDeniedException(
          "Can't retrieve username from tokenIdentifier.");
    }
    authorizedUgi.addTokenIdentifier(tokenId);
    authorizedUgi.setAuthenticationMethod(getSaslAuthMethod().getAuthMethod());
    return authorizedUgi;
  }
}
