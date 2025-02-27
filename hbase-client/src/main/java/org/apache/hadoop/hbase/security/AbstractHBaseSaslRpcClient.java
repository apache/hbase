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
package org.apache.hadoop.hbase.security;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A utility class that encapsulates SASL logic for RPC client. Copied from
 * <code>org.apache.hadoop.security</code>
 * @since 2.0.0
 */
@InterfaceAudience.Private
public abstract class AbstractHBaseSaslRpcClient {
  private static final byte[] EMPTY_TOKEN = new byte[0];

  protected final SaslClient saslClient;

  protected final boolean fallbackAllowed;

  protected final Map<String, String> saslProps;

  /**
   * Create a HBaseSaslRpcClient for an authentication method
   * @param conf             the configuration object
   * @param provider         the authentication provider
   * @param token            token to use if needed by the authentication method
   * @param serverAddr       the address of the hbase service
   * @param servicePrincipal the service principal to use if needed by the authentication method
   * @param fallbackAllowed  does the client allow fallback to simple authentication
   */
  protected AbstractHBaseSaslRpcClient(Configuration conf,
    SaslClientAuthenticationProvider provider, Token<? extends TokenIdentifier> token,
    InetAddress serverAddr, String servicePrincipal, boolean fallbackAllowed) throws IOException {
    this(conf, provider, token, serverAddr, servicePrincipal, fallbackAllowed, "authentication");
  }

  /**
   * Create a HBaseSaslRpcClient for an authentication method
   * @param conf             configuration object
   * @param provider         the authentication provider
   * @param token            token to use if needed by the authentication method
   * @param serverAddr       the address of the hbase service
   * @param servicePrincipal the service principal to use if needed by the authentication method
   * @param fallbackAllowed  does the client allow fallback to simple authentication
   * @param rpcProtection    the protection level ("authentication", "integrity" or "privacy")
   */
  protected AbstractHBaseSaslRpcClient(Configuration conf,
    SaslClientAuthenticationProvider provider, Token<? extends TokenIdentifier> token,
    InetAddress serverAddr, String servicePrincipal, boolean fallbackAllowed, String rpcProtection)
    throws IOException {
    this.fallbackAllowed = fallbackAllowed;
    saslProps = SaslUtil.initSaslProperties(rpcProtection);

    saslClient =
      provider.createClient(conf, serverAddr, servicePrincipal, token, fallbackAllowed, saslProps);
    if (saslClient == null) {
      throw new IOException(
        "Authentication provider " + provider.getClass() + " returned a null SaslClient");
    }
  }

  /**
   * Computes the initial response a client sends to a server to begin the SASL challenge/response
   * handshake. If the client's SASL mechanism does not have an initial response, an empty token
   * will be returned without querying the evaluateChallenge method, as an authentication processing
   * must be started by client.
   * @return The client's initial response to send the server (which may be empty).
   */
  public byte[] getInitialResponse() throws SaslException {
    if (saslClient.hasInitialResponse()) {
      return saslClient.evaluateChallenge(EMPTY_TOKEN);
    }
    return EMPTY_TOKEN;
  }

  public boolean isComplete() {
    return saslClient.isComplete();
  }

  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    return saslClient.evaluateChallenge(challenge);
  }

  /**
   * Check that SASL has successfully negotiated a QOP according to the requested rpcProtection
   * @throws IOException if the negotiated QOP is insufficient
   */
  protected void verifyNegotiatedQop() throws IOException {
    SaslUtil.verifyNegotiatedQop(saslProps.get(Sasl.QOP),
      (String) saslClient.getNegotiatedProperty(Sasl.QOP));
  }

  /** Release resources used by wrapped saslClient */
  public void dispose() {
    SaslUtil.safeDispose(saslClient);
  }
}
