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

import static org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils.OAUTHBEARER_MECHANISM;
import com.nimbusds.jose.shaded.json.JSONObject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.exceptions.SaslAuthenticationException;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.auth.AuthenticateCallbackHandler;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerToken;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerUtils;
import org.apache.hadoop.hbase.security.oauthbearer.OAuthBearerValidatorCallback;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code SaslServer} implementation for SASL/OAUTHBEARER in Kafka. An instance
 * of {@link OAuthBearerToken} is available upon successful authentication via
 * the negotiated property "{@code OAUTHBEARER.token}"; the token could be used
 * in a custom authorizer (to authorize based on JWT claims rather than ACLs,
 * for example).
 */
@InterfaceAudience.Public
public class OAuthBearerSaslServer implements SaslServer {
  public static final Logger LOG = LoggerFactory.getLogger(OAuthBearerSaslServer.class);
  private static final String NEGOTIATED_PROPERTY_KEY_TOKEN = OAUTHBEARER_MECHANISM + ".token";
  private static final String INTERNAL_ERROR_ON_SERVER =
    "Authentication could not be performed due to an internal error on the server";
  static final String CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY =
    "CREDENTIAL.LIFETIME.MS";

  private final AuthenticateCallbackHandler callbackHandler;

  private boolean complete;
  private OAuthBearerToken tokenForNegotiatedProperty = null;
  private String errorMessage = null;

  public OAuthBearerSaslServer(CallbackHandler callbackHandler) {
    if (!(callbackHandler instanceof AuthenticateCallbackHandler)) {
      throw new IllegalArgumentException(
        String.format("Callback handler must be castable to %s: %s",
          AuthenticateCallbackHandler.class.getName(), callbackHandler.getClass().getName()));
    }
    this.callbackHandler = (AuthenticateCallbackHandler) callbackHandler;
  }

  /**
   * @throws SaslAuthenticationException
   *             if access token cannot be validated
   *             <p>
   *             <b>Note:</b> This method may throw
   *             {@link SaslAuthenticationException} to provide custom error
   *             messages to clients. But care should be taken to avoid including
   *             any information in the exception message that should not be
   *             leaked to unauthenticated clients. It may be safer to throw
   *             {@link SaslException} in some cases so that a standard error
   *             message is returned to clients.
   *             </p>
   */
  @Override
  public byte[] evaluateResponse(byte[] response)
    throws SaslException, SaslAuthenticationException {
    try {
      if (response.length == 1 && response[0] == OAuthBearerSaslClient.BYTE_CONTROL_A &&
        errorMessage != null) {
        LOG.error("Received %x01 response from client after it received our error");
        throw new SaslAuthenticationException(errorMessage);
      }
      errorMessage = null;

      OAuthBearerClientInitialResponse clientResponse;
      clientResponse = new OAuthBearerClientInitialResponse(response);

      return process(clientResponse.tokenValue(), clientResponse.authorizationId());
    } catch (SaslAuthenticationException e) {
      LOG.error("SASL authentication error", e);
      throw e;
    } catch (Exception e) {
      LOG.error("SASL server problem", e);
      throw e;
    }
  }

  @Override
  public String getAuthorizationID() {
    if (!complete) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    return tokenForNegotiatedProperty.principalName();
  }

  @Override
  public String getMechanismName() {
    return OAUTHBEARER_MECHANISM;
  }

  @Override
  public Object getNegotiatedProperty(String propName) {
    if (!complete) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    if (NEGOTIATED_PROPERTY_KEY_TOKEN.equals(propName)) {
      return tokenForNegotiatedProperty;
    }
    if (CREDENTIAL_LIFETIME_MS_SASL_NEGOTIATED_PROPERTY_KEY.equals(propName)) {
      return tokenForNegotiatedProperty.lifetimeMs();
    }
    if (Sasl.QOP.equals(propName)) {
      return SaslUtil.QualityOfProtection.AUTHENTICATION.getSaslQop();
    }
    return null;
  }

  @Override
  public boolean isComplete() {
    return complete;
  }

  @Override
  public byte[] unwrap(byte[] incoming, int offset, int len) {
    if (!complete) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    return Arrays.copyOfRange(incoming, offset, offset + len);
  }

  @Override
  public byte[] wrap(byte[] outgoing, int offset, int len) {
    if (!complete) {
      throw new IllegalStateException("Authentication exchange has not completed");
    }
    return Arrays.copyOfRange(outgoing, offset, offset + len);
  }

  @Override
  public void dispose() {
    complete = false;
    tokenForNegotiatedProperty = null;
  }

  private byte[] process(String tokenValue, String authorizationId)
    throws SaslException {
    OAuthBearerValidatorCallback callback = new OAuthBearerValidatorCallback(tokenValue);
    try {
      callbackHandler.handle(new Callback[] {callback});
    } catch (IOException | UnsupportedCallbackException e) {
      handleCallbackError(e);
    }
    OAuthBearerToken token = callback.token();
    if (token == null) {
      errorMessage = jsonErrorResponse(callback.errorStatus(), callback.errorScope(),
        callback.errorOpenIDConfiguration());
      LOG.error("JWT token validation error: {}", errorMessage);
      return errorMessage.getBytes(StandardCharsets.UTF_8);
    }
    /*
     * We support the client specifying an authorization ID as per the SASL
     * specification, but it must match the principal name if it is specified.
     */
    if (!authorizationId.isEmpty() && !authorizationId.equals(token.principalName())) {
      throw new SaslAuthenticationException(String.format(
        "Authentication failed: Client requested an authorization id (%s) that is different from "
          + "the token's principal name (%s)",
        authorizationId, token.principalName()));
    }

    tokenForNegotiatedProperty = token;
    complete = true;
    LOG.debug("Successfully authenticate User={}", token.principalName());
    return new byte[0];
  }

  private static String jsonErrorResponse(String errorStatus, String errorScope,
    String errorOpenIDConfiguration) {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("status", errorStatus);
    if (!StringUtils.isBlank(errorScope)) {
      jsonObject.put("scope", errorScope);
    }
    if (!StringUtils.isBlank(errorOpenIDConfiguration)) {
      jsonObject.put("openid-configuration", errorOpenIDConfiguration);
    }
    return jsonObject.toJSONString();
  }

  private void handleCallbackError(Exception e) throws SaslException {
    String msg = String.format("%s: %s", INTERNAL_ERROR_ON_SERVER, e.getMessage());
    LOG.debug(msg, e);
    throw new SaslException(msg);
  }

  public static class OAuthBearerSaslServerFactory implements SaslServerFactory {
    @Override
    public SaslServer createSaslServer(String mechanism, String protocol, String serverName,
      Map<String, ?> props, CallbackHandler callbackHandler) {
      String[] mechanismNamesCompatibleWithPolicy = getMechanismNames(props);
      for (String s : mechanismNamesCompatibleWithPolicy) {
        if (s.equals(mechanism)) {
          return new OAuthBearerSaslServer(callbackHandler);
        }
      }
      return null;
    }

    @Override
    public String[] getMechanismNames(Map<String, ?> props) {
      return OAuthBearerUtils.mechanismNamesCompatibleWithPolicy(props);
    }
  }
}
