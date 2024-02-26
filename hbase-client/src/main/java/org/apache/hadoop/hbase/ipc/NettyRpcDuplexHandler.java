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

import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufInputStream;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBufOutputStream;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelDuplexHandler;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelFuture;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelPromise;
import org.apache.hbase.thirdparty.io.netty.handler.timeout.IdleStateEvent;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.PromiseCombiner;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * The netty rpc handler.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcDuplexHandler extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(NettyRpcDuplexHandler.class);

  private final NettyRpcConnection conn;

  private final CellBlockBuilder cellBlockBuilder;

  private final Codec codec;

  private final CompressionCodec compressor;

  private final Map<Integer, Call> id2Call = new HashMap<>();

  public NettyRpcDuplexHandler(NettyRpcConnection conn, CellBlockBuilder cellBlockBuilder,
    Codec codec, CompressionCodec compressor) {
    this.conn = conn;
    this.cellBlockBuilder = cellBlockBuilder;
    this.codec = codec;
    this.compressor = compressor;

  }

  private void writeRequest(ChannelHandlerContext ctx, Call call, ChannelPromise promise)
    throws IOException {
    id2Call.put(call.id, call);
    ByteBuf cellBlock = cellBlockBuilder.buildCellBlock(codec, compressor, call.cells, ctx.alloc());
    CellBlockMeta cellBlockMeta;
    if (cellBlock != null) {
      CellBlockMeta.Builder cellBlockMetaBuilder = CellBlockMeta.newBuilder();
      cellBlockMetaBuilder.setLength(cellBlock.writerIndex());
      cellBlockMeta = cellBlockMetaBuilder.build();
    } else {
      cellBlockMeta = null;
    }
    RequestHeader requestHeader = IPCUtil.buildRequestHeader(call, cellBlockMeta);
    int sizeWithoutCellBlock = IPCUtil.getTotalSizeWhenWrittenDelimited(requestHeader, call.param);
    int totalSize =
      cellBlock != null ? sizeWithoutCellBlock + cellBlock.writerIndex() : sizeWithoutCellBlock;
    ByteBuf buf = ctx.alloc().buffer(sizeWithoutCellBlock + 4);
    buf.writeInt(totalSize);
    try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf)) {
      requestHeader.writeDelimitedTo(bbos);
      if (call.param != null) {
        call.param.writeDelimitedTo(bbos);
      }
      if (cellBlock != null) {
        ChannelPromise withoutCellBlockPromise = ctx.newPromise();
        ctx.write(buf, withoutCellBlockPromise);
        ChannelPromise cellBlockPromise = ctx.newPromise();
        ctx.write(cellBlock, cellBlockPromise);
        PromiseCombiner combiner = new PromiseCombiner(ctx.executor());
        combiner.addAll((ChannelFuture) withoutCellBlockPromise, cellBlockPromise);
        combiner.finish(promise);
      } else {
        ctx.write(buf, promise);
      }
      call.callStats.setRequestSizeBytes(totalSize);
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
    throws Exception {
    if (msg instanceof Call) {
      Call call = (Call) msg;
      try (Scope scope = call.span.makeCurrent()) {
        writeRequest(ctx, call, promise);
      }
    } else {
      ctx.write(msg, promise);
    }
  }

  private void readResponse(ChannelHandlerContext ctx, ByteBuf buf) throws IOException {
    try {
      conn.readResponse(new ByteBufInputStream(buf), id2Call, null,
        remoteExc -> exceptionCaught(ctx, remoteExc));
    } catch (IOException e) {
      // In netty, the decoding the frame based, when reaching here we have already read a full
      // frame, so hitting exception here does not mean the stream decoding is broken, thus we do
      // not need to throw the exception out and close the connection.
      LOG.warn("failed to process response", e);
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      try {
        readResponse(ctx, buf);
      } finally {
        buf.release();
      }
    } else {
      super.channelRead(ctx, msg);
    }
  }

  private void cleanupCalls(IOException error) {
    for (Call call : id2Call.values()) {
      call.setException(error);
    }
    id2Call.clear();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (!id2Call.isEmpty()) {
      cleanupCalls(new ConnectionClosedException("Connection closed"));
    }
    conn.shutdown();
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (!id2Call.isEmpty()) {
      cleanupCalls(IPCUtil.toIOE(cause));
    }
    conn.shutdown();
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent idleEvt = (IdleStateEvent) evt;
      switch (idleEvt.state()) {
        case WRITER_IDLE:
          if (id2Call.isEmpty()) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("shutdown connection to " + conn.remoteId().address
                + " because idle for a long time");
            }
            // It may happen that there are still some pending calls in the event loop queue and
            // they will get a closed channel exception. But this is not a big deal as it rarely
            // rarely happens and the upper layer could retry immediately.
            conn.shutdown();
          }
          break;
        default:
          LOG.warn("Unrecognized idle state " + idleEvt.state());
          break;
      }
    } else if (evt instanceof CallEvent) {
      // just remove the call for now until we add other call event other than timeout and cancel.
      id2Call.remove(((CallEvent) evt).call.id);
    } else {
      ctx.fireUserEventTriggered(evt);
    }
  }
}
