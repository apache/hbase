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

import static org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils.OAUTHBEARER_MECHANISM;
import com.nimbusds.jose.jwk.JWKSet;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.Map;
import java.util.Objects;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.auth.AuthenticateCallbackHandler;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@code CallbackHandler} that recognizes
 * {@link OAuthBearerValidatorCallback} and validates a secure (signed) OAuth 2
 * bearer token (JWT).
 *
 * It requires a valid JWK Set to be initialized at startup which holds the available
 * RSA public keys that JWT signature can be validated with. The Set can be initialized
 * via an URL or a local file.
 *
 * It requires there to be an <code>"exp" (Expiration Time)</code>
 * claim of type Number. If <code>"iat" (Issued At)</code> or
 * <code>"nbf" (Not Before)</code> claims are present each must be a number that
 * precedes the Expiration Time claim, and if both are present the Not Before
 * claim must not precede the Issued At claim. It also accepts the following
 * options, none of which are required:
 * <ul>
 * <li>{@code hbase.security.oauth.jwt.jwks.url} set to a non-empty value if you
 * wish to initialize the JWK Set via an URL. HTTPS URLs must have valid certificates.
 * </li>
 * <li>{@code hbase.security.oauth.jwt.jwks.file} set to a non-empty value if you
 * wish to initialize the JWK Set from a local JSON file.
 * </li>
 * <li>{@code hbase.security.oauth.jwt.audience} set to a String value which
 * you want the desired audience ("aud") the JWT to have.</li>
 * <li>{@code hbase.security.oauth.jwt.issuer} set to a String value which
 * you want the issuer ("iss") of the JWT has to be.</li>
 * <li>{@code hbase.security.oauth.jwt.allowableclockskewseconds} set to a positive integer
 * value if you wish to allow up to some number of positive seconds of
 * clock skew (the default is 0)</li>
 * </ul>
 *
 * This class is based on Kafka's OAuthBearerUnsecuredValidatorCallbackHandler.
 */
@InterfaceAudience.Public
public class OAuthBearerSignedJwtValidatorCallbackHandler implements AuthenticateCallbackHandler {
  private static final Logger LOG =
    LoggerFactory.getLogger(OAuthBearerSignedJwtValidatorCallbackHandler.class);
  private static final String OPTION_PREFIX = "hbase.security.oauth.jwt.";
  private static final String JWKS_URL = OPTION_PREFIX + "jwks.url";
  private static final String JWKS_FILE = OPTION_PREFIX + "jwks.file";
  private static final String ALLOWABLE_CLOCK_SKEW_SECONDS_OPTION =
    OPTION_PREFIX + "allowableclockskewseconds";
  static final String REQUIRED_AUDIENCE_OPTION = OPTION_PREFIX + "audience";
  static final String REQUIRED_ISSUER_OPTION = OPTION_PREFIX + "issuer";
  private Configuration hBaseConfiguration;
  private JWKSet jwkSet;
  private boolean configured = false;

  @Override
  public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
    if (!configured) {
      throw new RuntimeException(
        "OAuthBearerSignedJwtValidatorCallbackHandler must be configured first.");
    }

    for (Callback callback : callbacks) {
      if (callback instanceof OAuthBearerValidatorCallback) {
        OAuthBearerValidatorCallback validationCallback = (OAuthBearerValidatorCallback) callback;
        try {
          handleCallback(validationCallback);
        } catch (OAuthBearerIllegalTokenException e) {
          LOG.error("Signed JWT token validation error: {}", e.getMessage());
          OAuthBearerValidationResult failureReason = e.reason();
          String failureScope = failureReason.failureScope();
          validationCallback.error(failureScope != null ? "insufficient_scope" : "invalid_token",
            failureScope, failureReason.failureOpenIdConfig());
        }
      } else {
        throw new UnsupportedCallbackException(callback);
      }
    }
  }

  @Override public void configure(Configuration configs, String saslMechanism,
    Map<String, String> saslProps) {
    if (!OAUTHBEARER_MECHANISM.equals(saslMechanism)) {
      throw new IllegalArgumentException(
        String.format("Unexpected SASL mechanism: %s", saslMechanism));
    }

    this.hBaseConfiguration = configs;

    try {
      loadJwkSet();
    } catch (IOException | ParseException e) {
      throw new RuntimeException("Unable to initialize JWK Set", e);
    }

    configured = true;
  }

  @InterfaceAudience.Private
  public void configure(Configuration configs, JWKSet jwkSet) {
    this.hBaseConfiguration = Objects.requireNonNull(configs);
    this.jwkSet = Objects.requireNonNull(jwkSet);
    this.configured = true;
  }

  private void handleCallback(OAuthBearerValidatorCallback callback) {
    String tokenValue = callback.tokenValue();
    if (tokenValue == null) {
      throw new IllegalArgumentException("Callback missing required token value");
    }
    OAuthBearerSignedJwt signedJwt = new OAuthBearerSignedJwt(tokenValue, jwkSet)
      .audience(requiredAudience())
      .issuer(requiredIssuer())
      .maxClockSkewSeconds(allowableClockSkewSeconds())
      .validate();

    LOG.info("Successfully validated token with principal {}: {}", signedJwt.principalName(),
      signedJwt.claims());
    callback.token(signedJwt);
  }

  private String requiredAudience() {
    return hBaseConfiguration.get(REQUIRED_AUDIENCE_OPTION);
  }

  private String requiredIssuer() {
    return hBaseConfiguration.get(REQUIRED_ISSUER_OPTION);
  }

  private int allowableClockSkewSeconds() {
    String allowableClockSkewSecondsValue = hBaseConfiguration.get(
      ALLOWABLE_CLOCK_SKEW_SECONDS_OPTION);
    int allowableClockSkewSeconds = 0;
    try {
      allowableClockSkewSeconds = StringUtils.isBlank(allowableClockSkewSecondsValue)
        ? 0 : Integer.parseInt(allowableClockSkewSecondsValue.trim());
    } catch (NumberFormatException e) {
      throw new OAuthBearerConfigException(e.getMessage(), e);
    }
    if (allowableClockSkewSeconds < 0) {
      throw new OAuthBearerConfigException(
        String.format("Allowable clock skew seconds must not be negative: %s",
          allowableClockSkewSecondsValue));
    }
    return allowableClockSkewSeconds;
  }

  private void loadJwkSet() throws IOException, ParseException {
    String jwksFile = hBaseConfiguration.get(JWKS_FILE);
    String jwksUrl = hBaseConfiguration.get(JWKS_URL);

    if (StringUtils.isBlank(jwksFile) && StringUtils.isBlank(jwksUrl)) {
      throw new RuntimeException("Failed to initialize JWKS db. "
        + JWKS_FILE + " or " + JWKS_URL + " must be specified in the config.");
    }

    if (!StringUtils.isBlank(jwksFile)) {
      this.jwkSet = JWKSet.load(new File(jwksFile));
      LOG.debug("JWKS db initialized from file: {}", jwksFile);
      return;
    }

    this.jwkSet = JWKSet.load(new URL(jwksUrl));
    LOG.debug("JWKS db initialized from URL: {}", jwksUrl);
  }
}
