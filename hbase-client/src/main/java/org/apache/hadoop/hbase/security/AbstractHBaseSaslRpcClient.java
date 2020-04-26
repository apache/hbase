/**
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
   * @param conf the configuration object
   * @param provider the authentication provider
   * @param token token to use if needed by the authentication method
   * @param serverAddr the address of the hbase service
   * @param securityInfo the security details for the remote hbase service
   * @param fallbackAllowed does the client allow fallback to simple authentication
   * @throws IOException
   */
  protected AbstractHBaseSaslRpcClient(Configuration conf,
      SaslClientAuthenticationProvider provider, Token<? extends TokenIdentifier> token,
      InetAddress serverAddr, SecurityInfo securityInfo, boolean fallbackAllowed)
          throws IOException {
    this(conf, provider, token, serverAddr, securityInfo, fallbackAllowed, "authentication");
  }

  /**
   * Create a HBaseSaslRpcClient for an authentication method
   * @param conf configuration object
   * @param provider the authentication provider
   * @param token token to use if needed by the authentication method
   * @param serverAddr the address of the hbase service
   * @param securityInfo the security details for the remote hbase service
   * @param fallbackAllowed does the client allow fallback to simple authentication
   * @param rpcProtection the protection level ("authentication", "integrity" or "privacy")
   * @throws IOException
   */
  protected AbstractHBaseSaslRpcClient(Configuration conf,
      SaslClientAuthenticationProvider provider, Token<? extends TokenIdentifier> token,
      InetAddress serverAddr, SecurityInfo securityInfo, boolean fallbackAllowed,
      String rpcProtection) throws IOException {
    this.fallbackAllowed = fallbackAllowed;
    saslProps = SaslUtil.initSaslProperties(rpcProtection);

    saslClient = provider.createClient(
        conf, serverAddr, securityInfo, token, fallbackAllowed, saslProps);
    if (saslClient == null) {
      throw new IOException("Authentication provider " + provider.getClass()
          + " returned a null SaslClient");
    }
  }

  public byte[] getInitialResponse() throws SaslException {
    if (saslClient.hasInitialResponse()) {
      return saslClient.evaluateChallenge(EMPTY_TOKEN);
    } else {
      return EMPTY_TOKEN;
    }
  }

  public boolean isComplete() {
    return saslClient.isComplete();
  }

  public byte[] evaluateChallenge(byte[] challenge) throws SaslException {
    return saslClient.evaluateChallenge(challenge);
  }

  /** Release resources used by wrapped saslClient */
  public void dispose() {
    SaslUtil.safeDispose(saslClient);
  }
}
