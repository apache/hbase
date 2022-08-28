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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.security.oauthbearer.JwtTestUtils;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class OAuthBearerSignedJwtTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(OAuthBearerSignedJwtTest.class);
  private final static ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");
  private final static int EXP_DAYS = 10;

  private JWKSet JWK_SET;
  private RSAKey RSA_KEY;

  @Before
  public void before() throws JOSEException {
    RSA_KEY = JwtTestUtils.generateRSAKey();
    JWK_SET = new JWKSet(RSA_KEY);
  }

  @Test
  public void validCompactSerialization() throws JOSEException {
    String subject = "foo";

    LocalDate issuedAt = LocalDate.now(ZONE_ID);
    LocalDate expirationTime = issuedAt.plusDays(EXP_DAYS);
    String validCompactSerialization = compactSerialization(subject, issuedAt, expirationTime);
    OAuthBearerSignedJwt jws =
      new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET).validate();
    assertEquals(5, jws.claims().size());
    assertEquals(subject, jws.claims().get("sub"));
    assertEquals(issuedAt, Date.class.cast(jws.claims().get("iat")).toInstant()
      .atZone(ZoneId.systemDefault()).toLocalDate());
    assertEquals(expirationTime, Date.class.cast(jws.claims().get("exp")).toInstant()
      .atZone(ZoneId.systemDefault()).toLocalDate());
    assertEquals(expirationTime.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli(),
      jws.lifetimeMs());
  }

  @Test
  public void missingPrincipal() throws JOSEException {
    String subject = null;
    LocalDate issuedAt = LocalDate.now(ZONE_ID);
    LocalDate expirationTime = issuedAt.plusDays(EXP_DAYS);
    String validCompactSerialization = compactSerialization(subject, issuedAt, expirationTime);
    assertThrows(OAuthBearerIllegalTokenException.class,
      () -> new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET).validate());
  }

  @Test
  public void blankPrincipalName() throws JOSEException {
    String subject = "   ";
    LocalDate issuedAt = LocalDate.now(ZONE_ID);
    LocalDate expirationTime = issuedAt.plusDays(EXP_DAYS);
    String validCompactSerialization = compactSerialization(subject, issuedAt, expirationTime);
    assertThrows(OAuthBearerIllegalTokenException.class,
      () -> new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET).validate());
  }

  @Test
  public void missingIssuer() throws JOSEException {
    String validCompactSerialization = JwtTestUtils.createSignedJwtWithIssuer(RSA_KEY, "");
    assertThrows(OAuthBearerIllegalTokenException.class,
      () -> new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET).issuer("test-issuer")
        .validate());
  }

  @Test
  public void badIssuer() throws JOSEException {
    String validCompactSerialization =
      JwtTestUtils.createSignedJwtWithIssuer(RSA_KEY, "bad-issuer");
    assertThrows(OAuthBearerIllegalTokenException.class,
      () -> new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET).issuer("test-issuer")
        .validate());
  }

  private String compactSerialization(String subject, LocalDate issuedAt, LocalDate expirationTime)
    throws JOSEException {
    return JwtTestUtils.createSignedJwt(RSA_KEY, "me", subject, expirationTime, issuedAt,
      "test-audience");
  }
}
