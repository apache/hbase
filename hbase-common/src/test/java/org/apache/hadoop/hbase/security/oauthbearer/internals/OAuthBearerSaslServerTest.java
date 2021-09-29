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
package org.apache.hadoop.hbase.security.oauthbearer.internals;

import static org.apache.hadoop.hbase.security.oauthbearer.JwtTestUtils.USER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.exceptions.SaslAuthenticationException;
import org.apache.hadoop.hbase.security.auth.AuthenticateCallbackHandler;
import org.apache.hadoop.hbase.security.auth.SaslExtensions;
import org.apache.hadoop.hbase.security.oauthbearer.JwtTestUtils;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerExtensionsValidatorCallback;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerTokenMock;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.hadoop.hbase.security.oauthbearer.internals.knox.OAuthBearerConfigException;
import org.apache.hadoop.hbase.security.oauthbearer.internals.knox.OAuthBearerSignedJwtValidatorCallbackHandler;
import org.junit.Before;
import org.junit.Test;

public class OAuthBearerSaslServerTest {
  private static final Configuration CONFIGS;
  private static final AuthenticateCallbackHandler EXTENSIONS_VALIDATOR_CALLBACK_HANDLER;
  static {
    CONFIGS = new Configuration();
    EXTENSIONS_VALIDATOR_CALLBACK_HANDLER = new OAuthBearerSignedJwtValidatorCallbackHandler() {
      @Override
      public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
          if (callback instanceof OAuthBearerValidatorCallback) {
            OAuthBearerValidatorCallback validationCallback =
              (OAuthBearerValidatorCallback) callback;
            validationCallback.token(new OAuthBearerTokenMock());
          } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
            OAuthBearerExtensionsValidatorCallback extensionsCallback =
              (OAuthBearerExtensionsValidatorCallback) callback;
            extensionsCallback.valid("firstKey");
            extensionsCallback.valid("secondKey");
          } else {
            throw new UnsupportedCallbackException(callback);
          }
        }
      }
    };
  }

  private String JWT;
  private OAuthBearerSaslServer saslServer;

  @Before
  public void setUp() throws JOSEException {
    RSAKey rsaKey = JwtTestUtils.generateRSAKey();
    JWT = JwtTestUtils.createSignedJwt(rsaKey);
    OAuthBearerSignedJwtValidatorCallbackHandler validatorCallbackHandler =
      new OAuthBearerSignedJwtValidatorCallbackHandler();
    validatorCallbackHandler.configure(CONFIGS, new JWKSet(rsaKey));
    // only validate extensions "firstKey" and "secondKey"
    saslServer = new OAuthBearerSaslServer(validatorCallbackHandler);
  }

  @Test
  public void noAuthorizationIdSpecified() throws Exception {
    byte[] nextChallenge = saslServer
      .evaluateResponse(clientInitialResponse(null));
    // also asserts that no authentication error is thrown
    // if OAuthBearerExtensionsValidatorCallback is not supported
    assertTrue("Next challenge is not empty",nextChallenge.length == 0);
  }

  @Test
  public void negotiatedProperty() throws Exception {
    saslServer.evaluateResponse(clientInitialResponse(USER));
    OAuthBearerToken token =
      (OAuthBearerToken) saslServer.getNegotiatedProperty("OAUTHBEARER.token");
    assertNotNull(token);
    assertEquals(token.lifetimeMs(),
      saslServer.getNegotiatedProperty(
        OAuthBearerSaslServer.CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY));
  }

  /**
   * SASL Extensions that are validated by the callback handler should be accessible through
   * the {@code #getNegotiatedProperty()} method
   */
  @Test
  public void savesCustomExtensionAsNegotiatedProperty() throws Exception {
    Map<String, String> customExtensions = new HashMap<>();
    customExtensions.put("firstKey", "value1");
    customExtensions.put("secondKey", "value2");

    byte[] nextChallenge = saslServer
      .evaluateResponse(clientInitialResponse(null, false, customExtensions));

    assertTrue("Next challenge is not empty", nextChallenge.length == 0);
    assertEquals("value1", saslServer.getNegotiatedProperty("firstKey"));
    assertEquals("value2", saslServer.getNegotiatedProperty("secondKey"));
  }

  /**
   * SASL Extensions that were not recognized (neither validated nor invalidated)
   * by the callback handler must not be accessible through the {@code #getNegotiatedProperty()}
   * method
   */
  @Test
  public void unrecognizedExtensionsAreNotSaved() throws Exception {
    saslServer = new OAuthBearerSaslServer(EXTENSIONS_VALIDATOR_CALLBACK_HANDLER);
    Map<String, String> customExtensions = new HashMap<>();
    customExtensions.put("firstKey", "value1");
    customExtensions.put("secondKey", "value1");
    customExtensions.put("thirdKey", "value1");

    byte[] nextChallenge = saslServer
      .evaluateResponse(clientInitialResponse(null, false, customExtensions));

    assertTrue("Next challenge is not empty", nextChallenge.length == 0);
    assertNull("Extensions not recognized by the server must be ignored",
      saslServer.getNegotiatedProperty("thirdKey"));
  }

  /**
   * If the callback handler handles the `OAuthBearerExtensionsValidatorCallback`
   *  and finds an invalid extension, SaslServer should throw an authentication exception
   */
  @Test
  public void throwsAuthenticationExceptionOnInvalidExtensions() {
    OAuthBearerSignedJwtValidatorCallbackHandler invalidHandler =
      new OAuthBearerSignedJwtValidatorCallbackHandler() {
      @Override
      public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
          if (callback instanceof OAuthBearerValidatorCallback) {
            OAuthBearerValidatorCallback validationCallback =
              (OAuthBearerValidatorCallback) callback;
            validationCallback.token(new OAuthBearerTokenMock());
          } else if (callback instanceof OAuthBearerExtensionsValidatorCallback) {
            OAuthBearerExtensionsValidatorCallback extensionsCallback =
              (OAuthBearerExtensionsValidatorCallback) callback;
            extensionsCallback.error("firstKey", "is not valid");
            extensionsCallback.error("secondKey", "is not valid either");
          } else {
            throw new UnsupportedCallbackException(callback);
          }
        }
      }
    };
    saslServer = new OAuthBearerSaslServer(invalidHandler);
    Map<String, String> customExtensions = new HashMap<>();
    customExtensions.put("firstKey", "value");
    customExtensions.put("secondKey", "value");

    assertThrows(SaslAuthenticationException.class,
      () -> saslServer.evaluateResponse(clientInitialResponse(null, false, customExtensions)));
  }

  @Test
  public void authorizatonIdEqualsAuthenticationId() throws Exception {
    byte[] nextChallenge = saslServer
      .evaluateResponse(clientInitialResponse(USER));
    assertTrue("Next challenge is not empty", nextChallenge.length == 0);
  }

  @Test
  public void authorizatonIdNotEqualsAuthenticationId() {
    assertThrows(SaslAuthenticationException.class,
      () -> saslServer.evaluateResponse(clientInitialResponse(USER + "x")));
  }

  @Test
  public void illegalToken() throws Exception {
    byte[] bytes = saslServer.evaluateResponse(clientInitialResponse(null, true,
      Collections.emptyMap()));
    String challenge = new String(bytes, StandardCharsets.UTF_8);
    assertEquals("{\"status\":\"invalid_token\"}", challenge);
  }

  private byte[] clientInitialResponse(String authorizationId)
    throws OAuthBearerConfigException, IOException, UnsupportedCallbackException {
    return clientInitialResponse(authorizationId, false);
  }

  private byte[] clientInitialResponse(String authorizationId, boolean illegalToken)
    throws OAuthBearerConfigException, IOException, UnsupportedCallbackException {
    return clientInitialResponse(authorizationId, false, Collections.emptyMap());
  }

  private byte[] clientInitialResponse(String authorizationId, boolean illegalToken,
    Map<String, String> customExtensions)
    throws OAuthBearerConfigException, IOException, UnsupportedCallbackException {
    String compactSerialization = JWT;
    String tokenValue = compactSerialization + (illegalToken ? "AB" : "");
    return new OAuthBearerClientInitialResponse(tokenValue, authorizationId,
      new SaslExtensions(customExtensions)).toBytes();
  }
}
