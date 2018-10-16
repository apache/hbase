/**
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

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A utility class that encapsulates SASL logic for RPC client. Copied from
 * <code>org.apache.hadoop.security</code>
 * @since 2.0.0
 */
@InterfaceAudience.Private
public abstract class AbstractHBaseSaslRpcClient {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHBaseSaslRpcClient.class);

  private static final byte[] EMPTY_TOKEN = new byte[0];

  protected final SaslClient saslClient;

  protected final boolean fallbackAllowed;

  protected final Map<String, String> saslProps;

  /**
   * Create a HBaseSaslRpcClient for an authentication method
   * @param method the requested authentication method
   * @param token token to use if needed by the authentication method
   * @param serverPrincipal the server principal that we are trying to set the connection up to
   * @param fallbackAllowed does the client allow fallback to simple authentication
   * @throws IOException
   */
  protected AbstractHBaseSaslRpcClient(AuthMethod method, Token<? extends TokenIdentifier> token,
      String serverPrincipal, boolean fallbackAllowed) throws IOException {
    this(method, token, serverPrincipal, fallbackAllowed, "authentication");
  }

  /**
   * Create a HBaseSaslRpcClient for an authentication method
   * @param method the requested authentication method
   * @param token token to use if needed by the authentication method
   * @param serverPrincipal the server principal that we are trying to set the connection up to
   * @param fallbackAllowed does the client allow fallback to simple authentication
   * @param rpcProtection the protection level ("authentication", "integrity" or "privacy")
   * @throws IOException
   */
  protected AbstractHBaseSaslRpcClient(AuthMethod method, Token<? extends TokenIdentifier> token,
      String serverPrincipal, boolean fallbackAllowed, String rpcProtection) throws IOException {
    this.fallbackAllowed = fallbackAllowed;
    saslProps = SaslUtil.initSaslProperties(rpcProtection);
    switch (method) {
      case DIGEST:
        if (LOG.isDebugEnabled()) LOG.debug("Creating SASL " + AuthMethod.DIGEST.getMechanismName()
            + " client to authenticate to service at " + token.getService());
        saslClient = createDigestSaslClient(new String[] { AuthMethod.DIGEST.getMechanismName() },
          SaslUtil.SASL_DEFAULT_REALM, new SaslClientCallbackHandler(token));
        break;
      case KERBEROS:
        if (LOG.isDebugEnabled()) {
          LOG.debug("Creating SASL " + AuthMethod.KERBEROS.getMechanismName()
              + " client. Server's Kerberos principal name is " + serverPrincipal);
        }
        if (serverPrincipal == null || serverPrincipal.length() == 0) {
          throw new IOException("Failed to specify server's Kerberos principal name");
        }
        String[] names = SaslUtil.splitKerberosName(serverPrincipal);
        if (names.length != 3) {
          throw new IOException(
              "Kerberos principal does not have the expected format: " + serverPrincipal);
        }
        saslClient = createKerberosSaslClient(
          new String[] { AuthMethod.KERBEROS.getMechanismName() }, names[0], names[1]);
        break;
      default:
        throw new IOException("Unknown authentication method " + method);
    }
    if (saslClient == null) {
      throw new IOException("Unable to find SASL client implementation");
    }
  }

  protected SaslClient createDigestSaslClient(String[] mechanismNames, String saslDefaultRealm,
      CallbackHandler saslClientCallbackHandler) throws IOException {
    return Sasl.createSaslClient(mechanismNames, null, null, saslDefaultRealm, saslProps,
      saslClientCallbackHandler);
  }

  protected SaslClient createKerberosSaslClient(String[] mechanismNames, String userFirstPart,
      String userSecondPart) throws IOException {
    return Sasl.createSaslClient(mechanismNames, null, userFirstPart, userSecondPart, saslProps,
      null);
  }

  public byte[] getInitialResponse() throws SaslException {
    if (saslClient.hasInitialResponse()) {
      return saslClient.evaluateChallenge(EMPTY_TOKEN);
    } else {
      return EMPTY_TOKEN;
    }
  }

  public boolean isComplete() {
    return saslClient.isComplete();
  }

  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    return saslClient.evaluateChallenge(challenge);
  }

  /** Release resources used by wrapped saslClient */
  public void dispose() {
    SaslUtil.safeDispose(saslClient);
  }

  @VisibleForTesting
  static class SaslClientCallbackHandler implements CallbackHandler {
    private final String userName;
    private final char[] userPassword;

    public SaslClientCallbackHandler(Token<? extends TokenIdentifier> token) {
      this.userName = SaslUtil.encodeIdentifier(token.getIdentifier());
      this.userPassword = SaslUtil.encodePassword(token.getPassword());
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      RealmCallback rc = null;
      for (Callback callback : callbacks) {
        if (callback instanceof RealmChoiceCallback) {
          continue;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          rc = (RealmCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
        }
      }
      if (nc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client callback: setting username: " + userName);
        }
        nc.setName(userName);
      }
      if (pc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client callback: setting userPassword");
        }
        pc.setPassword(userPassword);
      }
      if (rc != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("SASL client callback: setting realm: " + rc.getDefaultText());
        }
        rc.setText(rc.getDefaultText());
      }
    }
  }
}
