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

import static org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils.OAUTHBEARER_MECHANISM;

import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessController;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.auth.AuthenticateCallbackHandler;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

@InterfaceAudience.Private
public class OAuthBearerSaslClientAuthenticationProvider
  extends OAuthBearerSaslAuthenticationProvider implements SaslClientAuthenticationProvider {

  @Override
  public SaslClient createClient(Configuration conf, InetAddress serverAddr,
    SecurityInfo securityInfo, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
    Map<String, String> saslProps) throws IOException {
    AuthenticateCallbackHandler callbackHandler = new OAuthBearerSaslClientCallbackHandler();
    callbackHandler.configure(conf, getSaslAuthMethod().getSaslMechanism(), saslProps);
    return Sasl.createSaslClient(new String[] { getSaslAuthMethod().getSaslMechanism() }, null,
      null, SaslUtil.SASL_DEFAULT_REALM, saslProps, callbackHandler);
  }

  public static class OAuthBearerSaslClientCallbackHandler implements AuthenticateCallbackHandler {
    private static final Logger LOG =
      LoggerFactory.getLogger(OAuthBearerSaslClientCallbackHandler.class);
    private boolean configured = false;

    @Override
    public void configure(Configuration configs, String saslMechanism,
      Map<String, String> saslProps) {
      if (!OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
        throw new IllegalArgumentException(
          String.format("Unexpected SASL mechanism: %s", saslMechanism));
      }
      this.configured = true;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      if (!configured) {
        throw new IllegalStateException(
          "OAuthBearerSaslClientCallbackHandler handler must be configured first.");
      }

      for (Callback callback : callbacks) {
        if (callback instanceof OAuthBearerTokenCallback) {
          handleCallback((OAuthBearerTokenCallback) callback);
        } else {
          throw new UnsupportedCallbackException(callback);
        }
      }
    }

    private void handleCallback(OAuthBearerTokenCallback callback) throws IOException {
      if (callback.token() != null) {
        throw new IllegalArgumentException("Callback had a token already");
      }
      Subject subject = Subject.getSubject(AccessController.getContext());
      Set<OAuthBearerToken> privateCredentials = subject != null
        ? subject.getPrivateCredentials(OAuthBearerToken.class)
        : Collections.emptySet();
      callback.token(choosePrivateCredential(privateCredentials));
    }

    private OAuthBearerToken choosePrivateCredential(Set<OAuthBearerToken> privateCredentials)
      throws IOException {
      if (privateCredentials.size() == 0) {
        throw new IOException("No OAuth Bearer tokens in Subject's private credentials");
      }
      if (privateCredentials.size() == 1) {
        LOG.debug("Found 1 OAuthBearer token");
        return privateCredentials.iterator().next();
      } else {
        /*
         * There a very small window of time upon token refresh (on the order of milliseconds) where
         * both an old and a new token appear on the Subject's private credentials. Rather than
         * implement a lock to eliminate this window, we will deal with it by checking for the
         * existence of multiple tokens and choosing the one that has the longest lifetime. It is
         * also possible that a bug could cause multiple tokens to exist (e.g. KAFKA-7902), so
         * dealing with the unlikely possibility that occurs during normal operation also allows us
         * to deal more robustly with potential bugs.
         */
        NavigableSet<OAuthBearerToken> sortedByLifetime =
          new TreeSet<>(new Comparator<OAuthBearerToken>() {
            @Override
            public int compare(OAuthBearerToken o1, OAuthBearerToken o2) {
              return Long.compare(o1.lifetimeMs(), o2.lifetimeMs());
            }
          });
        sortedByLifetime.addAll(privateCredentials);
        if (LOG.isWarnEnabled()) {
          LOG.warn(
            "Found {} OAuth Bearer tokens in Subject's private credentials; "
              + "the oldest expires at {}, will use the newest, which expires at {}",
            sortedByLifetime.size(),
            LocalDateTime.ofInstant(Instant.ofEpochMilli(sortedByLifetime.first().lifetimeMs()),
              ZoneId.systemDefault()),
            LocalDateTime.ofInstant(Instant.ofEpochMilli(sortedByLifetime.last().lifetimeMs()),
              ZoneId.systemDefault()));
        }
        return sortedByLifetime.last();
      }
    }
  }

  @Override
  public RPCProtos.UserInformation getUserInfo(User user) {
    // Don't send user for token auth. Copied from RpcConnection.
    return null;
  }
}
