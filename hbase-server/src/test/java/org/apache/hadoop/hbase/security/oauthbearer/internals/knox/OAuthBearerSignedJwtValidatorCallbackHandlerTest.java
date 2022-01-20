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
package org.apache.hadoop.hbase.security.oauthbearer.internals.knox;

import static org.apache.hadoop.hbase.security.oauthbearer.internals.knox.OAuthBearerSignedJwtValidatorCallbackHandler.REQUIRED_AUDIENCE_OPTION;
import static org.apache.hadoop.hbase.security.oauthbearer.internals.knox.OAuthBearerSignedJwtValidatorCallbackHandler.REQUIRED_ISSUER_OPTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import java.time.LocalDate;
import java.time.ZoneId;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.security.oauthbearer.JwtTestUtils;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class})
public class OAuthBearerSignedJwtValidatorCallbackHandlerTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(OAuthBearerSignedJwtValidatorCallbackHandlerTest.class);

  private final static ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");
  private static final HBaseConfiguration EMPTY_CONFIG = new HBaseConfiguration();
  private static final HBaseConfiguration REQUIRED_AUDIENCE_CONFIG;
  static {
    REQUIRED_AUDIENCE_CONFIG = new HBaseConfiguration();
    REQUIRED_AUDIENCE_CONFIG.set(REQUIRED_AUDIENCE_OPTION, "test-audience");
  }
  private static final HBaseConfiguration REQUIRED_ISSUER_CONFIG;
  static {
    REQUIRED_ISSUER_CONFIG = new HBaseConfiguration();
    REQUIRED_ISSUER_CONFIG.set(REQUIRED_ISSUER_OPTION, "test-issuer");
  }

  private RSAKey RSA_KEY;

  @Before
  public void before() throws JOSEException {
    RSA_KEY = JwtTestUtils.generateRSAKey();
  }

  @Test
  public void validToken() throws JOSEException, UnsupportedCallbackException {
    Object validationResult = validationResult(EMPTY_CONFIG, JwtTestUtils.createSignedJwt(RSA_KEY));
    assertTrue(validationResult instanceof OAuthBearerValidatorCallback);
    assertTrue(((OAuthBearerValidatorCallback) validationResult).token()
      instanceof OAuthBearerSignedJwt);
  }

  @Test
  public void missingPrincipal()
    throws UnsupportedCallbackException, JOSEException {
    LocalDate now = LocalDate.now(ZONE_ID);
    String token = JwtTestUtils.createSignedJwt(RSA_KEY, "me", "",
      now.plusDays(1), now, "test-aud");
    confirmFailsValidation(EMPTY_CONFIG, token);
  }

  @Test
  public void tooEarlyExpirationTime() throws JOSEException, UnsupportedCallbackException {
    LocalDate now = LocalDate.now(ZONE_ID);
    String token = JwtTestUtils.createSignedJwt(RSA_KEY, "me", "",
      now.minusDays(1),
      now.minusDays(1),
      "test-aud");
    confirmFailsValidation(EMPTY_CONFIG, token);
  }

  @Test
  public void requiredAudience() throws JOSEException, UnsupportedCallbackException {
    String token = JwtTestUtils.createSignedJwtWithAudience(RSA_KEY, "test-audience");
    Object validationResult = validationResult(REQUIRED_AUDIENCE_CONFIG, token);
    assertTrue(validationResult instanceof OAuthBearerValidatorCallback);
    assertTrue(((OAuthBearerValidatorCallback) validationResult).token()
      instanceof OAuthBearerSignedJwt);
  }

  @Test
  public void missingAudience() throws JOSEException, UnsupportedCallbackException {
    String token = JwtTestUtils.createSignedJwt(RSA_KEY);
    confirmFailsValidation(REQUIRED_AUDIENCE_CONFIG, token);
  }

  @Test
  public void badAudience() throws JOSEException, UnsupportedCallbackException {
    String token = JwtTestUtils.createSignedJwtWithAudience(RSA_KEY, "bad-audience");
    confirmFailsValidation(REQUIRED_AUDIENCE_CONFIG, token);
  }

  @Test
  public void requiredIssuer() throws UnsupportedCallbackException, JOSEException {
    String token = JwtTestUtils.createSignedJwtWithIssuer(RSA_KEY, "test-issuer");
    Object validationResult = validationResult(REQUIRED_ISSUER_CONFIG, token);
    assertTrue(validationResult instanceof OAuthBearerValidatorCallback);
    assertTrue(((OAuthBearerValidatorCallback) validationResult).token()
      instanceof OAuthBearerSignedJwt);
  }

  @Test
  public void missingIssuer() throws JOSEException, UnsupportedCallbackException {
    String token = JwtTestUtils.createSignedJwt(RSA_KEY);
    confirmFailsValidation(REQUIRED_ISSUER_CONFIG, token);
  }

  @Test
  public void badIssuer() throws JOSEException, UnsupportedCallbackException {
    String token = JwtTestUtils.createSignedJwtWithIssuer(RSA_KEY, "bad-issuer");
    confirmFailsValidation(REQUIRED_ISSUER_CONFIG, token);
  }

  private void confirmFailsValidation(HBaseConfiguration config, String tokenValue)
    throws OAuthBearerConfigException, OAuthBearerIllegalTokenException,
    UnsupportedCallbackException {
    Object validationResultObj = validationResult(config, tokenValue);
    assertTrue(validationResultObj instanceof OAuthBearerValidatorCallback);
    OAuthBearerValidatorCallback callback = (OAuthBearerValidatorCallback) validationResultObj;
    assertNull(callback.token());
    assertNull(callback.errorOpenIDConfiguration());
    assertEquals("invalid_token", callback.errorStatus());
    assertNull(callback.errorScope());
  }

  private OAuthBearerValidatorCallback validationResult(HBaseConfiguration config,
    String tokenValue)
    throws UnsupportedCallbackException {
    OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(tokenValue);
    createCallbackHandler(config).handle(new Callback[] {callback});
    return callback;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private OAuthBearerSignedJwtValidatorCallbackHandler
    createCallbackHandler(HBaseConfiguration config) {
    OAuthBearerSignedJwtValidatorCallbackHandler callbackHandler =
      new OAuthBearerSignedJwtValidatorCallbackHandler();
    callbackHandler.configure(config, new JWKSet(RSA_KEY));
    return callbackHandler;
  }
}
