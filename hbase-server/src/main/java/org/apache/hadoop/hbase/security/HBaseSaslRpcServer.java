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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.security.provider.AttemptingUserProvidingSaslServer;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProvider;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A utility class that encapsulates SASL logic for RPC server. Copied from
 * <code>org.apache.hadoop.security</code>
 */
@InterfaceAudience.Private
public class HBaseSaslRpcServer {

  private final AttemptingUserProvidingSaslServer serverWithProvider;
  private final SaslServer saslServer;
  private CryptoAES cryptoAES;
  private final Map<String, String> saslProps;

  public HBaseSaslRpcServer(SaslServerAuthenticationProvider provider,
    Map<String, String> saslProps, SecretManager<TokenIdentifier> secretManager)
    throws IOException {
    serverWithProvider = provider.createServer(secretManager, saslProps);
    saslServer = serverWithProvider.getServer();
    this.saslProps = saslProps;
  }

  public boolean isComplete() {
    return saslServer.isComplete();
  }

  public byte[] evaluateResponse(byte[] response) throws SaslException {
    return saslServer.evaluateResponse(response);
  }

  /** Release resources used by wrapped saslServer */
  public void dispose() {
    SaslUtil.safeDispose(saslServer);
  }

  public void switchToCryptoAES(CryptoAES cryptoAES) {
    this.cryptoAES = cryptoAES;
  }

  public String getAttemptingUser() {
    return serverWithProvider.getAttemptingUser().map(Object::toString).orElse("Unknown");
  }

  public byte[] wrap(byte[] buf, int off, int len) throws SaslException {
    if (cryptoAES != null) {
      return cryptoAES.wrap(buf, off, len);
    } else {
      return saslServer.wrap(buf, off, len);
    }
  }

  public byte[] unwrap(byte[] buf, int off, int len) throws SaslException {
    if (cryptoAES != null) {
      return cryptoAES.unwrap(buf, off, len);
    } else {
      return saslServer.unwrap(buf, off, len);
    }
  }

  public String getNegotiatedQop() {
    return (String) saslServer.getNegotiatedProperty(Sasl.QOP);
  }

  public String getRequestedQop() {
    return (String) saslProps.get(Sasl.QOP);
  }

  public String getAuthorizationID() {
    return saslServer.getAuthorizationID();
  }

  public static <T extends TokenIdentifier> T getIdentifier(String id,
    SecretManager<T> secretManager) throws InvalidToken {
    byte[] tokenId = SaslUtil.decodeIdentifier(id);
    T tokenIdentifier = secretManager.createIdentifier();
    try {
      tokenIdentifier.readFields(new DataInputStream(new ByteArrayInputStream(tokenId)));
    } catch (IOException e) {
      throw (InvalidToken) new InvalidToken("Can't de-serialize tokenIdentifier").initCause(e);
    }
    return tokenIdentifier;
  }

  /**
   * Unwrap InvalidToken exception, otherwise return the one passed in.
   */
  public static Throwable unwrap(Throwable e) {
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof InvalidToken) {
        return cause;
      }
      cause = cause.getCause();
    }
    return e;
  }
}
