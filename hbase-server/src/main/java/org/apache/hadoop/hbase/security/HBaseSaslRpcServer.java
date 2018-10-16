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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * A utility class that encapsulates SASL logic for RPC server. Copied from
 * <code>org.apache.hadoop.security</code>
 */
@InterfaceAudience.Private
public class HBaseSaslRpcServer {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseSaslRpcServer.class);

  private final SaslServer saslServer;

  private UserGroupInformation attemptingUser; // user name before auth

  public HBaseSaslRpcServer(AuthMethod method, Map<String, String> saslProps,
      SecretManager<TokenIdentifier> secretManager) throws IOException {
    switch (method) {
      case DIGEST:
        if (secretManager == null) {
          throw new AccessDeniedException("Server is not configured to do DIGEST authentication.");
        }
        saslServer = Sasl.createSaslServer(AuthMethod.DIGEST.getMechanismName(), null,
          SaslUtil.SASL_DEFAULT_REALM, saslProps, new SaslDigestCallbackHandler(secretManager));
        break;
      case KERBEROS:
        UserGroupInformation current = UserGroupInformation.getCurrentUser();
        String fullName = current.getUserName();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Kerberos principal name is " + fullName);
        }
        String[] names = SaslUtil.splitKerberosName(fullName);
        if (names.length != 3) {
          throw new AccessDeniedException(
              "Kerberos principal name does NOT have the expected " + "hostname part: " + fullName);
        }
        try {
          saslServer = current.doAs(new PrivilegedExceptionAction<SaslServer>() {
            @Override
            public SaslServer run() throws SaslException {
              return Sasl.createSaslServer(AuthMethod.KERBEROS.getMechanismName(), names[0],
                names[1], saslProps, new SaslGssCallbackHandler());
            }
          });
        } catch (InterruptedException e) {
          // should not happen
          throw new AssertionError(e);
        }
        break;
      default:
        throw new IOException("Unknown authentication method " + method);
    }
  }

  public boolean isComplete() {
    return saslServer.isComplete();
  }

  public byte[] evaluateResponse(byte[] response) throws SaslException {
    return saslServer.evaluateResponse(response);
  }

  /** Release resources used by wrapped saslServer */
  public void dispose() {
    SaslUtil.safeDispose(saslServer);
  }

  public UserGroupInformation getAttemptingUser() {
    return attemptingUser;
  }

  public byte[] wrap(byte[] buf, int off, int len) throws SaslException {
    return saslServer.wrap(buf, off, len);
  }

  public byte[] unwrap(byte[] buf, int off, int len) throws SaslException {
    return saslServer.unwrap(buf, off, len);
  }

  public String getNegotiatedQop() {
    return (String) saslServer.getNegotiatedProperty(Sasl.QOP);
  }

  public String getAuthorizationID() {
    return saslServer.getAuthorizationID();
  }

  public static <T extends TokenIdentifier> T getIdentifier(String id,
      SecretManager<T> secretManager) throws InvalidToken {
    byte[] tokenId = SaslUtil.decodeIdentifier(id);
    T tokenIdentifier = secretManager.createIdentifier();
    try {
      tokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
    } catch (IOException e) {
      throw (InvalidToken) new InvalidToken("Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  /** CallbackHandler for SASL DIGEST-MD5 mechanism */
  private class SaslDigestCallbackHandler implements CallbackHandler {
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
        TokenIdentifier tokenIdentifier = getIdentifier(nc.getDefaultName(), secretManager);
        char[] password = getPassword(tokenIdentifier);
        UserGroupInformation user = tokenIdentifier.getUser(); // may throw exception
        attemptingUser = user;
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
            String username = getIdentifier(authzid, secretManager).getUser().getUserName();
            LOG.trace(
              "SASL server DIGEST-MD5 callback: setting " + "canonicalized client ID: " + username);
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }

  /** CallbackHandler for SASL GSSAPI Kerberos mechanism */
  private static class SaslGssCallbackHandler implements CallbackHandler {

    /** {@inheritDoc} */
    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      AuthorizeCallback ac = null;
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL GSSAPI Callback");
        }
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
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "SASL server GSSAPI callback: setting " + "canonicalized client ID: " + authzid);
          }
          ac.setAuthorizedID(authzid);
        }
      }
    }
  }
}
