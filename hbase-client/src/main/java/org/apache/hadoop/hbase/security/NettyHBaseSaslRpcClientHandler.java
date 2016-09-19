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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.FallbackDisallowedException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

/**
 * Implement SASL logic for netty rpc client.
 */
@InterfaceAudience.Private
public class NettyHBaseSaslRpcClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Log LOG = LogFactory.getLog(NettyHBaseSaslRpcClientHandler.class);

  private final Promise<Boolean> saslPromise;

  private final UserGroupInformation ugi;

  private final NettyHBaseSaslRpcClient saslRpcClient;

  /**
   * @param saslPromise {@code true} if success, {@code false} if server tells us to fallback to
   *          simple.
   */
  public NettyHBaseSaslRpcClientHandler(Promise<Boolean> saslPromise, UserGroupInformation ugi,
      AuthMethod method, Token<? extends TokenIdentifier> token, String serverPrincipal,
      boolean fallbackAllowed, String rpcProtection) throws IOException {
    this.saslPromise = saslPromise;
    this.ugi = ugi;
    this.saslRpcClient = new NettyHBaseSaslRpcClient(method, token, serverPrincipal,
        fallbackAllowed, rpcProtection);
  }

  private void writeResponse(ChannelHandlerContext ctx, byte[] response) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Will send token of size " + response.length + " from initSASLContext.");
    }
    ctx.writeAndFlush(
      ctx.alloc().buffer(4 + response.length).writeInt(response.length).writeBytes(response));
  }

  private void tryComplete(ChannelHandlerContext ctx) {
    if (!saslRpcClient.isComplete()) {
      return;
    }
    saslRpcClient.setupSaslHandler(ctx.pipeline());
    saslPromise.setSuccess(true);
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
      }
      tryComplete(ctx);
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("Will read input token of size " + len + " for processing by initSASLContext");
    }
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
    }
    tryComplete(ctx);
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    saslRpcClient.dispose();
    saslPromise.tryFailure(new IOException("Connection closed"));
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    saslRpcClient.dispose();
    saslPromise.tryFailure(cause);
  }
}
