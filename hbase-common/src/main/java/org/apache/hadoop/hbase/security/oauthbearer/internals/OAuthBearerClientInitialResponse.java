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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.security.sasl.SaslException;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OAuthBearer SASL client's initial message to the server.
 *
 * This class has been copy-and-pasted from Kafka codebase.
 */
@InterfaceAudience.Public
public class OAuthBearerClientInitialResponse {
  private static final Logger LOG = LoggerFactory.getLogger(OAuthBearerClientInitialResponse.class);
  static final String SEPARATOR = "\u0001";

  private static final String SASLNAME = "(?:[\\x01-\\x7F&&[^=,]]|=2C|=3D)+";
  private static final String KEY = "[A-Za-z]+";
  private static final String VALUE = "[\\x21-\\x7E \t\r\n]+";

  private static final String KVPAIRS = String.format("(%s=%s%s)*", KEY, VALUE, SEPARATOR);
  private static final Pattern AUTH_PATTERN =
    Pattern.compile("(?<scheme>[\\w]+)[ ]+(?<token>[-_\\.a-zA-Z0-9]+)");
  private static final Pattern CLIENT_INITIAL_RESPONSE_PATTERN = Pattern.compile(
    String.format("n,(a=(?<authzid>%s))?,%s(?<kvpairs>%s)%s",
      SASLNAME, SEPARATOR, KVPAIRS, SEPARATOR));
  public static final String AUTH_KEY = "auth";

  private final String tokenValue;
  private final String authorizationId;

  public OAuthBearerClientInitialResponse(byte[] response) throws SaslException {
    LOG.trace("Client initial response parsing started");
    String responseMsg = new String(response, StandardCharsets.UTF_8);
    Matcher matcher = CLIENT_INITIAL_RESPONSE_PATTERN.matcher(responseMsg);
    if (!matcher.matches()) {
      throw new SaslException("Invalid OAUTHBEARER client first message");
    }
    LOG.trace("Client initial response matches pattern");
    String authzid = matcher.group("authzid");
    this.authorizationId = authzid == null ? "" : authzid;
    String kvPairs = matcher.group("kvpairs");
    Map<String, String> properties = OAuthBearerUtils.parseMap(kvPairs, "=", SEPARATOR);
    String auth = properties.get(AUTH_KEY);
    if (auth == null) {
      throw new SaslException("Invalid OAUTHBEARER client first message: 'auth' not specified");
    }
    LOG.trace("Auth key found in client initial response");
    properties.remove(AUTH_KEY);
    Matcher authMatcher = AUTH_PATTERN.matcher(auth);
    if (!authMatcher.matches()) {
      throw new SaslException("Invalid OAUTHBEARER client first message: invalid 'auth' format");
    }
    LOG.trace("Client initial response auth matches pattern");
    if (!"bearer".equalsIgnoreCase(authMatcher.group("scheme"))) {
      String msg = String.format("Invalid scheme in OAUTHBEARER client first message: %s",
        matcher.group("scheme"));
      throw new SaslException(msg);
    }
    this.tokenValue = authMatcher.group("token");
    LOG.trace("Client initial response parsing finished");
  }

  /**
   * Constructor
   *
   * @param tokenValue
   *            the mandatory token value
   * @throws SaslException
   *             if any extension name or value fails to conform to the required
   *             regular expression as defined by the specification, or if the
   *             reserved {@code auth} appears as a key
   */
  public OAuthBearerClientInitialResponse(String tokenValue) {
    this(tokenValue, "");
  }

  /**
   * Constructor
   *
   * @param tokenValue
   *            the mandatory token value
   * @param authorizationId
   *            the optional authorization ID
   * @throws SaslException
   *             if any extension name or value fails to conform to the required
   *             regular expression as defined by the specification, or if the
   *             reserved {@code auth} appears as a key
   */
  public OAuthBearerClientInitialResponse(String tokenValue, String authorizationId) {
    this.tokenValue = Objects.requireNonNull(tokenValue, "token value must not be null");
    this.authorizationId = authorizationId == null ? "" : authorizationId;
  }

  public byte[] toBytes() {
    String authzid = authorizationId.isEmpty() ? "" : "a=" + authorizationId;

    String message = String.format("n,%s,%sauth=Bearer %s%s%s", authzid,
      SEPARATOR, tokenValue, SEPARATOR, SEPARATOR);

    return Bytes.toBytes(message);
  }

  /**
   * Return the always non-null token value
   *
   * @return the always non-null toklen value
   */
  public String tokenValue() {
    return tokenValue;
  }

  /**
   * Return the always non-null authorization ID
   *
   * @return the always non-null authorization ID
   */
  public String authorizationId() {
    return authorizationId;
  }
}
