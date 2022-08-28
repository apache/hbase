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
package org.apache.hadoop.hbase.security.oauthbearer;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Public
public final class JwtTestUtils {
  private final static ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");
  public static final String USER = "user";

  public static RSAKey generateRSAKey() throws JOSEException {
    RSAKeyGenerator rsaKeyGenerator = new RSAKeyGenerator(2048);
    return rsaKeyGenerator.keyID("1").generate();
  }

  public static String createSignedJwt(RSAKey rsaKey, String issuer, String subject,
    LocalDate expirationTime, LocalDate issueTime, String audience) throws JOSEException {
    JWSHeader jwsHeader = new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT)
      .keyID(rsaKey.getKeyID()).build();
    JWTClaimsSet payload = new JWTClaimsSet.Builder().issuer(issuer).subject(subject)
      .issueTime(java.sql.Date.valueOf(issueTime))
      .expirationTime(java.sql.Date.valueOf(expirationTime)).audience(audience).build();
    SignedJWT signedJwt = new SignedJWT(jwsHeader, payload);
    signedJwt.sign(new RSASSASigner(rsaKey));
    return signedJwt.serialize();
  }

  public static String createSignedJwt(RSAKey rsaKey) throws JOSEException {
    LocalDateTime now = LocalDateTime.now(ZONE_ID);
    JWSHeader jwsHeader = new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT)
      .keyID(rsaKey.getKeyID()).build();
    JWTClaimsSet payload = new JWTClaimsSet.Builder().subject(USER)
      .expirationTime(java.sql.Timestamp.valueOf(now.plusDays(1))).build();
    SignedJWT signedJwt = new SignedJWT(jwsHeader, payload);
    signedJwt.sign(new RSASSASigner(rsaKey));
    return signedJwt.serialize();
  }

  public static String createSignedJwtWithAudience(RSAKey rsaKey, String aud) throws JOSEException {
    LocalDateTime now = LocalDateTime.now(ZONE_ID);
    JWSHeader jwsHeader = new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT)
      .keyID(rsaKey.getKeyID()).build();
    JWTClaimsSet payload = new JWTClaimsSet.Builder().subject(USER)
      .expirationTime(java.sql.Timestamp.valueOf(now.plusDays(1))).audience(aud).build();
    SignedJWT signedJwt = new SignedJWT(jwsHeader, payload);
    signedJwt.sign(new RSASSASigner(rsaKey));
    return signedJwt.serialize();
  }

  public static String createSignedJwtWithIssuer(RSAKey rsaKey, String iss) throws JOSEException {
    LocalDateTime now = LocalDateTime.now(ZONE_ID);
    JWSHeader jwsHeader = new JWSHeader.Builder(JWSAlgorithm.RS256).type(JOSEObjectType.JWT)
      .keyID(rsaKey.getKeyID()).build();
    JWTClaimsSet payload = new JWTClaimsSet.Builder().subject(USER)
      .expirationTime(java.sql.Timestamp.valueOf(now.plusDays(1))).issuer(iss).build();
    SignedJWT signedJwt = new SignedJWT(jwsHeader, payload);
    signedJwt.sign(new RSASSASigner(rsaKey));
    return signedJwt.serialize();
  }

  private JwtTestUtils() {
    // empty
  }
}
