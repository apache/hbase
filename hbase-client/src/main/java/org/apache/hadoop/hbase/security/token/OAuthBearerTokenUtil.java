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
package org.apache.hadoop.hbase.security.token;

import static org.apache.hadoop.hbase.client.ConnectionFactory.ENV_OAUTHBEARER_TOKEN;
import static org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils.TOKEN_KIND;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import javax.security.auth.Subject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils;
import org.apache.hadoop.hbase.security.oauthbearer.internals.OAuthBearerSaslClientProvider;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for obtaining OAuthBearer / JWT authentication tokens.
 */
@InterfaceAudience.Public
public final class OAuthBearerTokenUtil {
  private static final Logger LOG = LoggerFactory.getLogger(OAuthBearerTokenUtil.class);

  static {
    OAuthBearerSaslClientProvider.initialize(); // not part of public API
    LOG.info("OAuthBearer SASL client provider has been initialized");
  }

  private OAuthBearerTokenUtil() {
  }

  /**
   * Add token to user's subject private credentials and a hint to provider selector to correctly
   * select OAuthBearer SASL provider.
   */
  public static void addTokenForUser(User user, String encodedToken, long lifetimeMs) {
    user.addToken(new Token<>(null, null, new Text(TOKEN_KIND), null));
    user.runAs(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        Subject subject = Subject.getSubject(AccessController.getContext());
        OAuthBearerToken jwt = new OAuthBearerToken() {
          @Override
          public String value() {
            return encodedToken;
          }

          @Override
          public long lifetimeMs() {
            return lifetimeMs;
          }

          @Override
          public String principalName() {
            return user.getName();
          }
        };
        subject.getPrivateCredentials().add(jwt);
        if (LOG.isDebugEnabled()) {
          LOG.debug("JWT token has been added to user credentials with expiry {}",
            lifetimeMs == 0 ? "0" : Instant.ofEpochMilli(lifetimeMs).toString());
        }
        return null;
      }
    });
  }

  /**
   * Check whether an OAuth Beaerer token is provided in environment variable HBASE_JWT. Parse and
   * add it to user private credentials, but only if another token is not already present.
   */
  public static void addTokenFromEnvironmentVar(User user, String token) {
    Optional<Token<?>> oauthBearerToken = user.getTokens().stream()
      .filter((t) -> new Text(OAuthBearerUtils.TOKEN_KIND).equals(t.getKind())).findFirst();

    if (oauthBearerToken.isPresent()) {
      LOG.warn("Ignoring OAuth Bearer token in " + ENV_OAUTHBEARER_TOKEN + " environment "
        + "variable, because another token is already present");
      return;
    }

    String[] tokens = token.split(",");
    if (StringUtils.isEmpty(tokens[0])) {
      return;
    }
    long lifetimeMs = 0;
    if (tokens.length > 1) {
      try {
        ZonedDateTime lifetime = ZonedDateTime.parse(tokens[1]);
        lifetimeMs = lifetime.toInstant().toEpochMilli();
      } catch (DateTimeParseException e) {
        throw new RuntimeException("Unable to parse JWT expiry: " + tokens[1], e);
      }
    } else {
      throw new RuntimeException("Expiry information of JWT is missing");
    }

    addTokenForUser(user, tokens[0], lifetimeMs);
  }
}
