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

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;

import javax.security.sasl.Sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Implement SASL logic for async rpc client.
 */
@InterfaceAudience.Private
public class AsyncHBaseSaslRpcClient extends AbstractHBaseSaslRpcClient {
  private static final Log LOG = LogFactory.getLog(AsyncHBaseSaslRpcClient.class);

  public AsyncHBaseSaslRpcClient(AuthMethod method, Token<? extends TokenIdentifier> token,
      String serverPrincipal, boolean fallbackAllowed, String rpcProtection) throws IOException {
    super(method, token, serverPrincipal, fallbackAllowed, rpcProtection);
  }

  public void setupSaslHandler(ChannelPipeline p) {
    String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
    if (LOG.isDebugEnabled()) {
      LOG.debug("SASL client context established. Negotiated QoP: " + qop);
    }
    if (qop == null || "auth".equalsIgnoreCase(qop)) {
      return;
    }
    // add wrap and unwrap handlers to pipeline.
    p.addFirst(new SaslWrapHandler(saslClient),
      new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
      new SaslUnwrapHandler(saslClient));
  }
}
