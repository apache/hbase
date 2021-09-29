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
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.security.oauthbearer.JwtTestUtils;
import org.junit.Before;
import org.junit.Test;

public class OAuthBearerSignedJwtTest {
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
    Date issuedAt = new Date();
    Date expirationTime = new Date(issuedAt.getTime() + 60 * 60);
    String validCompactSerialization =
      compactSerialization(subject, issuedAt, expirationTime);
    OAuthBearerSignedJwt jws = new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET)
      .validate();
    assertEquals(5, jws.claims().size());
    assertEquals(subject, jws.claims().get("sub"));
    assertEquals(DateUtils.ceiling(issuedAt, Calendar.SECOND),
      DateUtils.ceiling(Date.class.cast(jws.claims().get("iat")), Calendar.SECOND));
    assertEquals(DateUtils.ceiling(expirationTime, Calendar.SECOND),
      DateUtils.ceiling(Date.class.cast(jws.claims().get("exp")), Calendar.SECOND));
    assertEquals(DateUtils.ceiling(expirationTime, Calendar.SECOND).getTime(),
      jws.lifetimeMs());
  }

  @Test
  public void missingPrincipal() throws JOSEException {
    String subject = null;
    Date issuedAt = new Date();
    Date expirationTime = new Date(issuedAt.getTime() + 60 * 60);
    String validCompactSerialization =
      compactSerialization(subject, issuedAt, expirationTime);
    assertThrows(OAuthBearerIllegalTokenException.class,
      () -> new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET).validate());
  }

  @Test
  public void blankPrincipalName() throws JOSEException {
    String subject = "   ";
    Date issuedAt = new Date();
    Date expirationTime = new Date(issuedAt.getTime() + 60 * 60);
    String validCompactSerialization =
      compactSerialization(subject, issuedAt, expirationTime);
    assertThrows(OAuthBearerIllegalTokenException.class,
      () -> new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET).validate());
  }

  @Test
  public void missingIssuer() throws JOSEException {
    String validCompactSerialization =
      JwtTestUtils.createSignedJwtWithIssuer(RSA_KEY, "");
    assertThrows(OAuthBearerIllegalTokenException.class,
      () -> new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET)
        .issuer("test-issuer")
        .validate());
  }

  @Test
  public void badIssuer() throws JOSEException {
    String validCompactSerialization =
      JwtTestUtils.createSignedJwtWithIssuer(RSA_KEY, "bad-issuer");
    assertThrows(OAuthBearerIllegalTokenException.class,
      () -> new OAuthBearerSignedJwt(validCompactSerialization, JWK_SET)
        .issuer("test-issuer")
        .validate());
  }


  private String compactSerialization(String subject, Date issuedAt, Date expirationTime)
    throws JOSEException {
    return JwtTestUtils.createSignedJwt(RSA_KEY, "me", subject,
        expirationTime, issuedAt, "test-audience");
  }
}
