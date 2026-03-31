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

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.util.NettyFutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufInputStream;
import org.apache.hbase.thirdparty.io.netty.buffer.Unpooled;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPipeline;
import org.apache.hbase.thirdparty.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.hbase.thirdparty.io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.ReadTimeoutHandler;

/**
 * Used to decode preamble calls.
 */
@InterfaceAudience.Private
class PreambleCallHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private final NettyRpcConnection conn;

  private final byte[] preambleHeader;

  private final Call preambleCall;

  PreambleCallHandler(NettyRpcConnection conn, byte[] preambleHeader, Call preambleCall) {
    this.conn = conn;
    this.preambleHeader = preambleHeader;
    this.preambleCall = preambleCall;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    NettyFutureUtils.safeWriteAndFlush(ctx,
      Unpooled.directBuffer(preambleHeader.length).writeBytes(preambleHeader));
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf buf) throws Exception {
    try {
      conn.readResponse(new ByteBufInputStream(buf), new HashMap<>(), preambleCall,
        remoteExc -> exceptionCaught(ctx, remoteExc));
    } finally {
      ChannelPipeline p = ctx.pipeline();
      p.remove("PreambleCallReadTimeoutHandler");
      p.remove("PreambleCallFrameDecoder");
      p.remove(this);
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    preambleCall.setException(new ConnectionClosedException("Connection closed"));
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    preambleCall.setException(IPCUtil.toIOE(cause));
  }

  public static void setup(ChannelPipeline pipeline, int readTimeoutMs, NettyRpcConnection conn,
    byte[] preambleHeader, Call preambleCall) {
    // we do not use single decode here, as for a preamble call, we do not expect the server side
    // will return multiple responses
    pipeline
      .addBefore(BufferCallBeforeInitHandler.NAME, "PreambleCallReadTimeoutHandler",
        new ReadTimeoutHandler(readTimeoutMs, TimeUnit.MILLISECONDS))
      .addBefore(BufferCallBeforeInitHandler.NAME, "PreambleCallFrameDecoder",
        new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4))
      .addBefore(BufferCallBeforeInitHandler.NAME, "PreambleCallHandler",
        new PreambleCallHandler(conn, preambleHeader, preambleCall));
  }
}
