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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.io.IOException;

import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.ipc.RemoteException;

import com.google.protobuf.Message;

/**
 * Handles Hbase responses
 */
@InterfaceAudience.Private
public class AsyncServerResponseHandler extends ChannelInboundHandlerAdapter {
  private final AsyncRpcChannel channel;

  /**
   * Constructor
   *
   * @param channel on which this response handler operates
   */
  public AsyncServerResponseHandler(AsyncRpcChannel channel) {
    this.channel = channel;
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf inBuffer = (ByteBuf) msg;
    ByteBufInputStream in = new ByteBufInputStream(inBuffer);
    int totalSize = inBuffer.readableBytes();
    try {
      // Read the header
      RPCProtos.ResponseHeader responseHeader = RPCProtos.ResponseHeader.parseDelimitedFrom(in);
      int id = responseHeader.getCallId();
      AsyncCall call = channel.removePendingCall(id);
      if (call == null) {
        // So we got a response for which we have no corresponding 'call' here on the client-side.
        // We probably timed out waiting, cleaned up all references, and now the server decides
        // to return a response.  There is nothing we can do w/ the response at this stage. Clean
        // out the wire of the response so its out of the way and we can get other responses on
        // this connection.
        int readSoFar = IPCUtil.getTotalSizeWhenWrittenDelimited(responseHeader);
        int whatIsLeftToRead = totalSize - readSoFar;

        // This is done through a Netty ByteBuf which has different behavior than InputStream.
        // It does not return number of bytes read but will update pointer internally and throws an
        // exception when too many bytes are to be skipped.
        inBuffer.skipBytes(whatIsLeftToRead);
        return;
      }

      if (responseHeader.hasException()) {
        RPCProtos.ExceptionResponse exceptionResponse = responseHeader.getException();
        RemoteException re = createRemoteException(exceptionResponse);
        if (exceptionResponse.getExceptionClassName().
            equals(FatalConnectionException.class.getName())) {
          channel.close(re);
        } else {
          call.setFailed(re);
        }
      } else {
        Message value = null;
        // Call may be null because it may have timedout and been cleaned up on this side already
        if (call.responseDefaultType != null) {
          Message.Builder builder = call.responseDefaultType.newBuilderForType();
          ProtobufUtil.mergeDelimitedFrom(builder, in);
          value = builder.build();
        }
        CellScanner cellBlockScanner = null;
        if (responseHeader.hasCellBlockMeta()) {
          int size = responseHeader.getCellBlockMeta().getLength();
          byte[] cellBlock = new byte[size];
          inBuffer.readBytes(cellBlock, 0, cellBlock.length);
          cellBlockScanner = channel.client.createCellScanner(cellBlock);
        }
        call.setSuccess(value, cellBlockScanner);
        call.callStats.setResponseSizeBytes(totalSize);
      }
    } catch (IOException e) {
      // Treat this as a fatal condition and close this connection
      channel.close(e);
    } finally {
      inBuffer.release();
    }
  }

  /**
   * @param e Proto exception
   * @return RemoteException made from passed <code>e</code>
   */
  private RemoteException createRemoteException(final RPCProtos.ExceptionResponse e) {
    String innerExceptionClassName = e.getExceptionClassName();
    boolean doNotRetry = e.getDoNotRetry();
    return e.hasHostname() ?
        // If a hostname then add it to the RemoteWithExtrasException
        new RemoteWithExtrasException(innerExceptionClassName, e.getStackTrace(), e.getHostname(),
            e.getPort(), doNotRetry) :
        new RemoteWithExtrasException(innerExceptionClassName, e.getStackTrace(), doNotRetry);
  }
}
