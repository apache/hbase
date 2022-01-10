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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.exceptions.SaslAuthenticationException;
import org.apache.hadoop.hbase.security.oauthbearer.JwtTestUtils;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.hbase.security.oauthbearer.internals.knox.OAuthBearerConfigException;
import org.apache.hadoop.hbase.security.oauthbearer.internals.knox.OAuthBearerSignedJwtValidatorCallbackHandler;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class})
public class OAuthBearerSaslServerTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(OAuthBearerSaslServerTest.class);

  private static final Configuration CONFIGS;
  static {
    CONFIGS = new Configuration();
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
    byte[] bytes = saslServer.evaluateResponse(clientInitialResponse(null, true));
    String challenge = new String(bytes, StandardCharsets.UTF_8);
    assertEquals("{\"status\":\"invalid_token\"}", challenge);
  }

  private byte[] clientInitialResponse(String authorizationId)
    throws OAuthBearerConfigException {
    return clientInitialResponse(authorizationId, false);
  }

  private byte[] clientInitialResponse(String authorizationId, boolean illegalToken)
    throws OAuthBearerConfigException {
    String compactSerialization = JWT;
    String tokenValue = compactSerialization + (illegalToken ? "AB" : "");
    return new OAuthBearerClientInitialResponse(tokenValue, authorizationId).toBytes();
  }
}
