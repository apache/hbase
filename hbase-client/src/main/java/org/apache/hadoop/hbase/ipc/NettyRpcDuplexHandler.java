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
package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message.Builder;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.TextFormat;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.PromiseCombiner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.ipc.RemoteException;

/**
 * The netty rpc handler.
 */
@InterfaceAudience.Private
class NettyRpcDuplexHandler extends ChannelDuplexHandler {

  private static final Log LOG = LogFactory.getLog(NettyRpcDuplexHandler.class);

  private final NettyRpcConnection conn;

  private final CellBlockBuilder cellBlockBuilder;

  private final Codec codec;

  private final CompressionCodec compressor;

  private final Map<Integer, Call> id2Call = new HashMap<Integer, Call>();

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
    int totalSize = cellBlock != null ? sizeWithoutCellBlock + cellBlock.writerIndex()
        : sizeWithoutCellBlock;
    ByteBuf buf = ctx.alloc().buffer(sizeWithoutCellBlock + 4);
    buf.writeInt(totalSize);
    ByteBufOutputStream bbos = new ByteBufOutputStream(buf);
    requestHeader.writeDelimitedTo(bbos);
    if (call.param != null) {
      call.param.writeDelimitedTo(bbos);
    }
    if (cellBlock != null) {
      ChannelPromise withoutCellBlockPromise = ctx.newPromise();
      ctx.write(buf, withoutCellBlockPromise);
      ChannelPromise cellBlockPromise = ctx.newPromise();
      ctx.write(cellBlock, cellBlockPromise);
      PromiseCombiner combiner = new PromiseCombiner();
      combiner.addAll(withoutCellBlockPromise, cellBlockPromise);
      combiner.finish(promise);
    } else {
      ctx.write(buf, promise);
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof Call) {
      writeRequest(ctx, (Call) msg, promise);
    } else {
      ctx.write(msg, promise);
    }
  }

  private void readResponse(ChannelHandlerContext ctx, ByteBuf buf) throws IOException {
    int totalSize = buf.readInt();
    ByteBufInputStream in = new ByteBufInputStream(buf);
    ResponseHeader responseHeader = ResponseHeader.parseDelimitedFrom(in);
    int id = responseHeader.getCallId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("got response header " + TextFormat.shortDebugString(responseHeader)
          + ", totalSize: " + totalSize + " bytes");
    }
    RemoteException remoteExc;
    if (responseHeader.hasException()) {
      ExceptionResponse exceptionResponse = responseHeader.getException();
      remoteExc = IPCUtil.createRemoteException(exceptionResponse);
      if (IPCUtil.isFatalConnectionException(exceptionResponse)) {
        // Here we will cleanup all calls so do not need to fall back, just return.
        exceptionCaught(ctx, remoteExc);
        return;
      }
    } else {
      remoteExc = null;
    }
    Call call = id2Call.remove(id);
    if (call == null) {
      // So we got a response for which we have no corresponding 'call' here on the client-side.
      // We probably timed out waiting, cleaned up all references, and now the server decides
      // to return a response. There is nothing we can do w/ the response at this stage. Clean
      // out the wire of the response so its out of the way and we can get other responses on
      // this connection.
      int readSoFar = IPCUtil.getTotalSizeWhenWrittenDelimited(responseHeader);
      int whatIsLeftToRead = totalSize - readSoFar;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unknown callId: " + id + ", skipping over this response of " + whatIsLeftToRead
            + " bytes");
      }
      return;
    }
    if (remoteExc != null) {
      call.setException(remoteExc);
      return;
    }
    Message value;
    if (call.responseDefaultType != null) {
      Builder builder = call.responseDefaultType.newBuilderForType();
      builder.mergeDelimitedFrom(in);
      value = builder.build();
    } else {
      value = null;
    }
    CellScanner cellBlockScanner;
    if (responseHeader.hasCellBlockMeta()) {
      int size = responseHeader.getCellBlockMeta().getLength();
      // Maybe we could read directly from the ByteBuf.
      // The problem here is that we do not know when to release it.
      byte[] cellBlock = new byte[size];
      buf.readBytes(cellBlock);
      cellBlockScanner = cellBlockBuilder.createCellScanner(this.codec, this.compressor, cellBlock);
    } else {
      cellBlockScanner = null;
    }
    call.setResponse(value, cellBlockScanner);
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

  private void cleanupCalls(ChannelHandlerContext ctx, IOException error) {
    for (Call call : id2Call.values()) {
      call.setException(error);
    }
    id2Call.clear();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (!id2Call.isEmpty()) {
      cleanupCalls(ctx, new IOException("Connection closed"));
    }
    conn.shutdown();
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (!id2Call.isEmpty()) {
      cleanupCalls(ctx, IPCUtil.toIOE(cause));
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
            if (LOG.isDebugEnabled()) {
              LOG.debug("shutdown connection to " + conn.remoteId().address
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