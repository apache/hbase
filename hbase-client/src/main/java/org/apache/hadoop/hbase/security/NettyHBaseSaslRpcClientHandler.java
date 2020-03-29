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

import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.ipc.FallbackDisallowedException;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Implement SASL logic for netty rpc client.
 * @since 2.0.0
 */
@InterfaceAudience.Private
public class NettyHBaseSaslRpcClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Logger LOG = LoggerFactory.getLogger(NettyHBaseSaslRpcClientHandler.class);

  private final Promise<Boolean> saslPromise;

  private final UserGroupInformation ugi;

  private final NettyHBaseSaslRpcClient saslRpcClient;

  private final Configuration conf;

  private final SaslClientAuthenticationProvider provider;

  // flag to mark if Crypto AES encryption is enable
  private boolean needProcessConnectionHeader = false;

  /**
   * @param saslPromise {@code true} if success, {@code false} if server tells us to fallback to
   *          simple.
   */
  public NettyHBaseSaslRpcClientHandler(Promise<Boolean> saslPromise, UserGroupInformation ugi,
      SaslClientAuthenticationProvider provider, Token<? extends TokenIdentifier> token,
      InetAddress serverAddr, SecurityInfo securityInfo, boolean fallbackAllowed,
      Configuration conf) throws IOException {
    this.saslPromise = saslPromise;
    this.ugi = ugi;
    this.conf = conf;
    this.provider = provider;
    this.saslRpcClient = new NettyHBaseSaslRpcClient(conf, provider, token, serverAddr,
        securityInfo, fallbackAllowed, conf.get(
        "hbase.rpc.protection", SaslUtil.QualityOfProtection.AUTHENTICATION.name().toLowerCase()));
  }

  private void writeResponse(ChannelHandlerContext ctx, byte[] response) {
    LOG.trace("Sending token size={} from initSASLContext.", response.length);
    ctx.writeAndFlush(
      ctx.alloc().buffer(4 + response.length).writeInt(response.length).writeBytes(response));
  }

  private void tryComplete(ChannelHandlerContext ctx) {
    if (!saslRpcClient.isComplete()) {
      return;
    }

    // HBASE-23881 Clearly log when the client thinks that the SASL negotiation is complete.
    if (LOG.isTraceEnabled()) {
      LOG.trace("SASL negotiation for {} is complete", provider.getSaslAuthMethod().getName());
    }

    saslRpcClient.setupSaslHandler(ctx.pipeline());
    setCryptoAESOption();

    saslPromise.setSuccess(true);
  }

  private void setCryptoAESOption() {
    boolean saslEncryptionEnabled = SaslUtil.QualityOfProtection.PRIVACY.
        getSaslQop().equalsIgnoreCase(saslRpcClient.getSaslQOP());
    needProcessConnectionHeader = saslEncryptionEnabled && conf.getBoolean(
        "hbase.rpc.crypto.encryption.aes.enabled", false);
  }

  public boolean isNeedProcessConnectionHeader() {
    return needProcessConnectionHeader;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) {
    try {
      byte[] initialResponse = ugi.doAs(new PrivilegedExceptionAction<byte[]>() {

        @Override
        public byte[] run() throws Exception {
          return saslRpcClient.getInitialResponse();
        }
      });
      if (initialResponse != null) {
        writeResponse(ctx, initialResponse);
      } else {
        LOG.trace("SASL initialResponse was null, not sending response to server.");
      }
      // HBASE-23881 We do not want to check if the SaslClient thinks the handshake is
      // complete as, at this point, we've not heard a back from the server with it's reply
      // to our first challenge response. We should wait for at least one reply
      // from the server before calling negotiation complete.
      //
      // Each SASL mechanism has its own handshake. Some mechanisms calculate a single client buffer
      // to be sent to the server while others have multiple exchanges to negotiate authentication. GSSAPI(Kerberos)
      // and DIGEST-MD5 both are examples of mechanisms which have multiple steps. Mechanisms which have multiple steps
      // will not return true on `SaslClient#isComplete()` until the handshake has fully completed. Mechanisms which
      // only send a single buffer may return true on `isComplete()` after that initial response is calculated.
    } catch (Exception e) {
      // the exception thrown by handlerAdded will not be passed to the exceptionCaught below
      // because netty will remove a handler if handlerAdded throws an exception.
      exceptionCaught(ctx, e);
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    int len = msg.readInt();
    if (len == SaslUtil.SWITCH_TO_SIMPLE_AUTH) {
      saslRpcClient.dispose();
      if (saslRpcClient.fallbackAllowed) {
        saslPromise.trySuccess(false);
      } else {
        saslPromise.tryFailure(new FallbackDisallowedException());
      }
      return;
    }
    LOG.trace("Reading input token size={} for processing by initSASLContext", len);
    final byte[] challenge = new byte[len];
    msg.readBytes(challenge);
    byte[] response = ugi.doAs(new PrivilegedExceptionAction<byte[]>() {

      @Override
      public byte[] run() throws Exception {
        return saslRpcClient.evaluateChallenge(challenge);
      }
    });
    if (response != null) {
      writeResponse(ctx, response);
    } else {
      LOG.trace("SASL challenge response was empty, not sending response to server.");
    }
    tryComplete(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    saslRpcClient.dispose();
    saslPromise.tryFailure(new ConnectionClosedException("Connection closed"));
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    saslRpcClient.dispose();
    saslPromise.tryFailure(cause);
  }
}
