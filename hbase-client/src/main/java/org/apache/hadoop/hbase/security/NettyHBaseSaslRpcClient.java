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

import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import java.io.IOException;
import java.net.InetAddress;

import javax.security.sasl.Sasl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implement SASL logic for netty rpc client.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class NettyHBaseSaslRpcClient extends AbstractHBaseSaslRpcClient {
  private static final Logger LOG = LoggerFactory.getLogger(NettyHBaseSaslRpcClient.class);

  public NettyHBaseSaslRpcClient(Configuration conf, SaslClientAuthenticationProvider provider,
      Token<? extends TokenIdentifier> token, InetAddress serverAddr, SecurityInfo securityInfo,
      boolean fallbackAllowed, String rpcProtection) throws IOException {
    super(conf, provider, token, serverAddr, securityInfo, fallbackAllowed, rpcProtection);
  }

  public void setupSaslHandler(ChannelPipeline p) {
    String qop = (String) saslClient.getNegotiatedProperty(Sasl.QOP);
    LOG.trace("SASL client context established. Negotiated QoP {}", qop);
    if (qop == null || "auth".equalsIgnoreCase(qop)) {
      return;
    }
    // add wrap and unwrap handlers to pipeline.
    p.addFirst(new SaslWrapHandler(saslClient),
      new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
      new SaslUnwrapHandler(saslClient));
  }

  public String getSaslQOP() {
    return (String) saslClient.getNegotiatedProperty(Sasl.QOP);
  }
}
