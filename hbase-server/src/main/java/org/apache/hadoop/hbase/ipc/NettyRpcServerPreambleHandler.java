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

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.ipc.ServerRpcConnection.PreambleResponse;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.handler.codec.FixedLengthFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * Handle connection preamble.
 * @since 2.0.0`
 */
@InterfaceAudience.Private
class NettyRpcServerPreambleHandler extends SimpleChannelInboundHandler<ByteBuf> {

  static final String DECODER_NAME = "preambleDecoder";

  private final NettyRpcServer rpcServer;
  private final NettyServerRpcConnection conn;
  private boolean processPreambleError;

  public NettyRpcServerPreambleHandler(NettyRpcServer rpcServer, NettyServerRpcConnection conn) {
    this.rpcServer = rpcServer;
    this.conn = conn;
  }

  static FixedLengthFrameDecoder createDecoder() {
    FixedLengthFrameDecoder preambleDecoder = new FixedLengthFrameDecoder(6);
    preambleDecoder.setSingleDecode(true);
    return preambleDecoder;
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    if (processPreambleError) {
      // if we failed to process preamble, we will close the connection immediately, but it is
      // possible that we have already received some bytes after the 'preamble' so when closing, the
      // netty framework will still pass them here. So we set a flag here to just skip processing
      // these broken messages.
      return;
    }
    ByteBuffer buf = ByteBuffer.allocate(msg.readableBytes());
    msg.readBytes(buf);
    buf.flip();
    PreambleResponse resp = conn.processPreamble(buf);
    if (resp == PreambleResponse.CLOSE) {
      processPreambleError = true;
      conn.close();
      return;
    }
    if (resp == PreambleResponse.CONTINUE) {
      // we use a single decode decoder, so here we need to replace it with a new one so it will
      // decode a new preamble header again
      ctx.pipeline().replace(DECODER_NAME, DECODER_NAME, createDecoder());
      return;
    }
    // resp == PreambleResponse.SUCCEED
    ChannelPipeline p = ctx.pipeline();
    if (conn.useSasl) {
      LengthFieldBasedFrameDecoder decoder =
        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4);
      decoder.setSingleDecode(true);
      p.addBefore(NettyRpcServerResponseEncoder.NAME, NettyHBaseSaslRpcServerHandler.DECODER_NAME,
        decoder).addBefore(NettyRpcServerResponseEncoder.NAME, null,
          new NettyHBaseSaslRpcServerHandler(rpcServer, conn));
    } else {
      conn.setupHandler();
    }
    // add first and then remove, so the single decode decoder will pass the remaining bytes to the
    // handler above.
    p.remove(this);
    p.remove(DECODER_NAME);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    NettyRpcServer.LOG.warn("Connection {}; caught unexpected downstream exception.",
      ctx.channel().remoteAddress(), cause);
    NettyFutureUtils.safeClose(ctx);
  }
}
