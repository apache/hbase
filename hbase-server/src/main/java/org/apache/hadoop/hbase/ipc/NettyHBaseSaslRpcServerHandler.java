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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUnwrapHandler;
import org.apache.hadoop.hbase.security.SaslWrapHandler;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Implement SASL negotiation logic for rpc server.
 */
class NettyHBaseSaslRpcServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Logger LOG = LoggerFactory.getLogger(NettyHBaseSaslRpcServerHandler.class);

  static final String DECODER_NAME = "SaslNegotiationDecoder";

  private final NettyRpcServer rpcServer;

  private final NettyServerRpcConnection conn;

  NettyHBaseSaslRpcServerHandler(NettyRpcServer rpcServer, NettyServerRpcConnection conn) {
    this.rpcServer = rpcServer;
    this.conn = conn;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    LOG.debug("Read input token of size={} for processing by saslServer.evaluateResponse()",
      msg.readableBytes());
    HBaseSaslRpcServer saslServer = conn.getOrCreateSaslServer();
    byte[] saslToken = new byte[msg.readableBytes()];
    msg.readBytes(saslToken, 0, saslToken.length);
    byte[] replyToken = saslServer.evaluateResponse(saslToken);
    if (replyToken != null) {
      LOG.debug("Will send token of size {} from saslServer.", replyToken.length);
      conn.doRawSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null, null);
    }
    if (saslServer.isComplete()) {
      conn.finishSaslNegotiation();
      String qop = saslServer.getNegotiatedQop();
      boolean useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
      ChannelPipeline p = ctx.pipeline();
      if (useWrap) {
        p.addBefore(DECODER_NAME, null, new SaslWrapHandler(saslServer::wrap))
          .addBefore(NettyRpcServerResponseEncoder.NAME, null,
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
          .addBefore(NettyRpcServerResponseEncoder.NAME, null,
            new SaslUnwrapHandler(saslServer::unwrap));
      }
      conn.setupHandler();
      p.remove(this);
      p.remove(DECODER_NAME);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    LOG.error("Error when doing SASL handshade, provider={}", conn.provider, cause);
    Throwable sendToClient = HBaseSaslRpcServer.unwrap(cause);
    conn.doRawSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
      sendToClient.getLocalizedMessage());
    rpcServer.metrics.authenticationFailure();
    String clientIP = this.toString();
    // attempting user could be null
    RpcServer.AUDITLOG.warn("{}{}: {}", RpcServer.AUTH_FAILED_FOR, clientIP,
      conn.saslServer != null ? conn.saslServer.getAttemptingUser() : "Unknown");
    NettyFutureUtils.safeClose(ctx);
  }
}
