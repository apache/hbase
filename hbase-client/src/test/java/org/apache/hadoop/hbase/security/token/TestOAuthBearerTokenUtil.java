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
package org.apache.hadoop.hbase.security.token;

import static org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils.TOKEN_KIND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import javax.security.auth.Subject;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.junit.Test;

public class TestOAuthBearerTokenUtil {

  @Test
  public void testAddTokenFromEnvVar() {
    // Arrange
    User user = User.createUserForTesting(HBaseConfiguration.create(), "testuser", new String[] {});
    String testToken = "some_base64_encoded_stuff,2022-01-25T16:59:48.614000+00:00";

    // Act
    OAuthBearerTokenUtil.addTokenFromEnvironmentVar(user, testToken);

    // Assert
    Optional<Token<?>> oauthBearerToken = user.getTokens().stream()
      .filter((t) -> new Text(TOKEN_KIND).equals(t.getKind()))
      .findFirst();
    assertTrue("Token cannot be found in user tokens", oauthBearerToken.isPresent());
    user.runAs(new PrivilegedAction<Object>() {
      @Override public Object run() {
        Subject subject = Subject.getSubject(AccessController.getContext());
        Set<OAuthBearerToken> tokens = subject.getPrivateCredentials(OAuthBearerToken.class);
        assertFalse("Token cannot be found in subject's private credentials", tokens.isEmpty());
        OAuthBearerToken jwt = tokens.iterator().next();
        assertEquals("Invalid encoded JWT value", "some_base64_encoded_stuff", jwt.value());
        assertEquals("Invalid JWT expiry", "2022-01-25T16:59:48.614Z",
          Instant.ofEpochMilli(jwt.lifetimeMs()).toString());
        return null;
      }
    });
  }

  @Test(expected = RuntimeException.class)
  public void testAddTokenEnvVarWithoutExpiry() {
    // Arrange
    User user = User.createUserForTesting(new HBaseConfiguration(), "testuser", new String[] {});
    String testToken = "some_base64_encoded_stuff";

    // Act
    OAuthBearerTokenUtil.addTokenFromEnvironmentVar(user, testToken);

    // Assert
  }

  @Test(expected = RuntimeException.class)
  public void testAddTokenEnvVarWithInvalidExpiry() {
    // Arrange
    User user = User.createUserForTesting(new HBaseConfiguration(), "testuser", new String[] {});
    String testToken = "some_base64_encoded_stuff,foobarblahblah328742";

    // Act
    OAuthBearerTokenUtil.addTokenFromEnvironmentVar(user, testToken);

    // Assert
  }

  @Test
  public void testAddTokenEnvVarTokenAlreadyPresent() {
    // Arrange
    User user = User.createUserForTesting(new HBaseConfiguration(), "testuser", new String[] {});
    user.addToken(new Token<>(null, null, new Text(TOKEN_KIND), null));
    String testToken = "some_base64_encoded_stuff,foobarblahblah328742";

    // Act
    OAuthBearerTokenUtil.addTokenFromEnvironmentVar(user, testToken);

    // Assert
    long numberOfTokens = user.getTokens().stream()
      .filter((t) -> new Text(TOKEN_KIND).equals(t.getKind()))
      .count();
    assertEquals("Invalid number of tokens on User", 1, numberOfTokens);
    user.runAs(new PrivilegedAction<Object>() {
      @Override public Object run() {
        Subject subject = Subject.getSubject(AccessController.getContext());
        Set<OAuthBearerToken> tokens = subject.getPrivateCredentials(OAuthBearerToken.class);
        assertTrue("Token should not have been added to subject's credentials", tokens.isEmpty());
        return null;
      }
    });
  }
}
