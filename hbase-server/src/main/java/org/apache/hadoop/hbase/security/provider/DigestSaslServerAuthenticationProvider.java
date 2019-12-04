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

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public class DigestSaslServerAuthenticationProvider extends DigestSaslClientAuthenticationProvider
    implements SaslServerAuthenticationProvider {
  private static final Logger LOG = LoggerFactory.getLogger(
      DigestSaslServerAuthenticationProvider.class);

  @Override
  public SaslServer createServer(SecretManager<TokenIdentifier> secretManager,
      Map<String, String> saslProps) throws IOException {
    if (secretManager == null) {
      throw new AccessDeniedException("Server is not configured to do DIGEST authentication.");
    }
    return Sasl.createSaslServer(getSaslAuthMethod().getSaslMechanism(), null,
      SaslUtil.SASL_DEFAULT_REALM, saslProps, new SaslDigestCallbackHandler(secretManager));
  }

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  private static class SaslDigestCallbackHandler implements CallbackHandler {
    private SecretManager<TokenIdentifier> secretManager;

    public SaslDigestCallbackHandler(SecretManager<TokenIdentifier> secretManager) {
      this.secretManager = secretManager;
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
        char[] password = getPassword(tokenIdentifier);
        if (LOG.isTraceEnabled()) {
          LOG.trace("SASL server DIGEST-MD5 callback: setting password " + "for client: " +
              tokenIdentifier.getUser());
        }
        pc.setPassword(password);
      }
      if (ac != null) {
        String authid = ac.getAuthenticationID();
        String authzid = ac.getAuthorizationID();
        if (authid.equals(authzid)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        if (ac.isAuthorized()) {
          if (LOG.isTraceEnabled()) {
            String username = HBaseSaslRpcServer.getIdentifier(
                authzid, secretManager).getUser().getUserName();
            LOG.trace(
              "SASL server DIGEST-MD5 callback: setting " + "canonicalized client ID: " + username);
          }
          ac.setAuthorizedID(authzid);
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
