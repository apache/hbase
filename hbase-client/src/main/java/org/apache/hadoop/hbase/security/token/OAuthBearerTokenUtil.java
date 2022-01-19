/**
 *
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

import java.security.AccessController;
import java.security.PrivilegedAction;
import javax.security.auth.Subject;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
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
  public static final String TOKEN_KIND = "JWT_AUTH_TOKEN";

  static {
    OAuthBearerSaslClientProvider.initialize(); // not part of public API
    LOG.info("OAuthBearer SASL client provider has been initialized");
  }

  private OAuthBearerTokenUtil() {  }

  /**
   * Add token to user's subject private credentials and a hint to provider selector
   * to correctly select OAuthBearer SASL provider.
   */
  public static void addTokenForUser(User user, String encodedToken, long lifetimeMs) {
    user.addToken(new Token<>(null, null, new Text(TOKEN_KIND), null));
    user.runAs(new PrivilegedAction<Object>() {
      @Override public Object run() {
        Subject subject = Subject.getSubject(AccessController.getContext());
        OAuthBearerToken jwt = new OAuthBearerToken() {
          @Override public String value() {
            return encodedToken;
          }

          @Override public long lifetimeMs() {
            return lifetimeMs;
          }

          @Override public String principalName() {
            return user.getName();
          }
        };
        subject.getPrivateCredentials().add(jwt);
        return null;
      }
    });
  }
}
