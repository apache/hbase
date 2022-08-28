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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.JWTParser;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Signed JWT implementation for OAuth Bearer authentication mech of SASL.
 * <p/>
 * This class is based on Kafka's Unsecured JWS token implementation.
 */
@InterfaceAudience.Public
public class OAuthBearerSignedJwt implements OAuthBearerToken {
  private final String compactSerialization;
  private final JWKSet jwkSet;

  private JWTClaimsSet claims;
  private long lifetime;
  private int maxClockSkewSeconds = 0;
  private String requiredAudience;
  private String requiredIssuer;

  /**
   * Constructor base64 encoded JWT token and JWK Set.
   * <p/>
   * @param compactSerialization the compact serialization to parse as a signed JWT
   * @param jwkSet               the key set which the signature of this JWT should be verified with
   */
  public OAuthBearerSignedJwt(String compactSerialization, JWKSet jwkSet) {
    this.jwkSet = jwkSet;
    this.compactSerialization = Objects.requireNonNull(compactSerialization);
  }

  @Override
  public String value() {
    return compactSerialization;
  }

  @Override
  public String principalName() {
    return claims.getSubject();
  }

  @Override
  public long lifetimeMs() {
    return lifetime;
  }

  /**
   * Return the JWT Claim Set as a {@code Map}
   * @return the (always non-null but possibly empty) claims
   */
  public Map<String, Object> claims() {
    return claims.getClaims();
  }

  /**
   * Set required audience, as per
   * <a href="https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.3"> RFC7519 Section
   * 4.1.3</a>
   */
  public OAuthBearerSignedJwt audience(String aud) {
    this.requiredAudience = aud;
    return this;
  }

  /**
   * Set required issuer, as per
   * <a href="https://datatracker.ietf.org/doc/html/rfc7519#section-4.1.1"> RFC7519 Section
   * 4.1.1</a>
   */
  public OAuthBearerSignedJwt issuer(String iss) {
    this.requiredIssuer = iss;
    return this;
  }

  /**
   * Set maximum clock skew in seconds.
   * @param value New value
   */
  public OAuthBearerSignedJwt maxClockSkewSeconds(int value) {
    this.maxClockSkewSeconds = value;
    return this;
  }

  /**
   * This method provides a single method for validating the JWT for use in request processing.
   * <p/>
   * @throws OAuthBearerIllegalTokenException if the compact serialization is not a valid JWT
   *                                          (meaning it did not have 3 dot-separated Base64URL
   *                                          sections with a digital signature; or the header or
   *                                          claims either are not valid Base 64 URL encoded values
   *                                          or are not JSON after decoding; or the mandatory
   *                                          '{@code alg}' header value is missing)
   */
  public OAuthBearerSignedJwt validate() {
    try {
      this.claims = validateToken(compactSerialization);
      Date expirationTimeSeconds = claims.getExpirationTime();
      if (expirationTimeSeconds == null) {
        throw new OAuthBearerIllegalTokenException(
          OAuthBearerValidationResult.newFailure("No expiration time in JWT"));
      }
      lifetime = expirationTimeSeconds.toInstant().toEpochMilli();
      String principalName = claims.getSubject();
      if (StringUtils.isBlank(principalName)) {
        throw new OAuthBearerIllegalTokenException(
          OAuthBearerValidationResult.newFailure("No principal name in JWT claim"));
      }
      return this;
    } catch (ParseException | BadJOSEException | JOSEException e) {
      throw new OAuthBearerIllegalTokenException(
        OAuthBearerValidationResult.newFailure("Token validation failed: " + e.getMessage()), e);
    }
  }

  private JWTClaimsSet validateToken(String jwtToken)
    throws BadJOSEException, JOSEException, ParseException {
    JWT jwt = JWTParser.parse(jwtToken);
    ConfigurableJWTProcessor<SecurityContext> jwtProcessor = new DefaultJWTProcessor<>();

    Set<String> requiredClaims = new HashSet<>();
    JWTClaimsSet.Builder jwtClaimsSetBuilder = new JWTClaimsSet.Builder();

    // Audience
    if (!StringUtils.isBlank(requiredAudience)) {
      requiredClaims.add("aud");
      jwtClaimsSetBuilder.audience(requiredAudience);
    }

    // Issuer
    if (!StringUtils.isBlank(requiredIssuer)) {
      requiredClaims.add("iss");
      jwtClaimsSetBuilder.issuer(requiredIssuer);
    }

    // Subject / Principal is always required
    requiredClaims.add("sub");

    DefaultJWTClaimsVerifier<SecurityContext> jwtClaimsSetVerifier =
      new DefaultJWTClaimsVerifier<>(jwtClaimsSetBuilder.build(), requiredClaims);
    jwtClaimsSetVerifier.setMaxClockSkew(maxClockSkewSeconds);
    jwtProcessor.setJWTClaimsSetVerifier(jwtClaimsSetVerifier);

    JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(
      (JWSAlgorithm) jwt.getHeader().getAlgorithm(), new ImmutableJWKSet<>(jwkSet));
    jwtProcessor.setJWSKeySelector(keySelector);
    return jwtProcessor.process(jwtToken, null);
  }
}
