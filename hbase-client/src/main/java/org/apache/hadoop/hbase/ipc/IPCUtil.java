/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TracingProtos.RPCTInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;

/**
 * Utility to help ipc'ing.
 */
@InterfaceAudience.Private
class IPCUtil {

  /**
   * Write out header, param, and cell block if there is one.
   * @param dos Stream to write into
   * @param header to write
   * @param param to write
   * @param cellBlock to write
   * @return Total number of bytes written.
   * @throws IOException if write action fails
   */
  public static int write(final OutputStream dos, final Message header, final Message param,
      final ByteBuffer cellBlock) throws IOException {
    // Must calculate total size and write that first so other side can read it all in in one
    // swoop. This is dictated by how the server is currently written. Server needs to change
    // if we are to be able to write without the length prefixing.
    int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header, param);
    if (cellBlock != null) {
      totalSize += cellBlock.remaining();
    }
    return write(dos, header, param, cellBlock, totalSize);
  }

  private static int write(final OutputStream dos, final Message header, final Message param,
      final ByteBuffer cellBlock, final int totalSize) throws IOException {
    // I confirmed toBytes does same as DataOutputStream#writeInt.
    dos.write(Bytes.toBytes(totalSize));
    // This allocates a buffer that is the size of the message internally.
    header.writeDelimitedTo(dos);
    if (param != null) {
      param.writeDelimitedTo(dos);
    }
    if (cellBlock != null) {
      dos.write(cellBlock.array(), 0, cellBlock.remaining());
    }
    dos.flush();
    return totalSize;
  }

  /**
   * @return Size on the wire when the two messages are written with writeDelimitedTo
   */
  public static int getTotalSizeWhenWrittenDelimited(Message... messages) {
    int totalSize = 0;
    for (Message m : messages) {
      if (m == null) {
        continue;
      }
      totalSize += m.getSerializedSize();
      totalSize += CodedOutputStream.computeRawVarint32Size(m.getSerializedSize());
    }
    Preconditions.checkArgument(totalSize < Integer.MAX_VALUE);
    return totalSize;
  }

  static RequestHeader buildRequestHeader(Call call, CellBlockMeta cellBlockMeta) {
    RequestHeader.Builder builder = RequestHeader.newBuilder();
    builder.setCallId(call.id);
    if (call.span != null) {
      builder.setTraceInfo(RPCTInfo.newBuilder().setParentId(call.span.getSpanId())
          .setTraceId(call.span.getTraceId()));
    }
    builder.setMethodName(call.md.getName());
    builder.setRequestParam(call.param != null);
    if (cellBlockMeta != null) {
      builder.setCellBlockMeta(cellBlockMeta);
    }
    // Only pass priority if there is one set.
    if (call.priority != HBaseRpcController.PRIORITY_UNSET) {
      builder.setPriority(call.priority);
    }
    builder.setTimeout(call.timeout);

    return builder.build();
  }

  /**
   * @param e exception to be wrapped
   * @return RemoteException made from passed <code>e</code>
   */
  static RemoteException createRemoteException(final ExceptionResponse e) {
    String innerExceptionClassName = e.getExceptionClassName();
    boolean doNotRetry = e.getDoNotRetry();
    return e.hasHostname() ?
    // If a hostname then add it to the RemoteWithExtrasException
        new RemoteWithExtrasException(innerExceptionClassName, e.getStackTrace(), e.getHostname(),
            e.getPort(), doNotRetry)
        : new RemoteWithExtrasException(innerExceptionClassName, e.getStackTrace(), doNotRetry);
  }

  /**
   * @return True if the exception is a fatal connection exception.
   */
  static boolean isFatalConnectionException(final ExceptionResponse e) {
    return e.getExceptionClassName().equals(FatalConnectionException.class.getName());
  }

  static IOException toIOE(Throwable t) {
    if (t instanceof IOException) {
      return (IOException) t;
    } else {
      return new IOException(t);
    }
  }

  /**
   * Takes an Exception and the address we were trying to connect to and return an IOException with
   * the input exception as the cause. The new exception provides the stack trace of the place where
   * the exception is thrown and some extra diagnostics information. If the exception is
   * ConnectException or SocketTimeoutException, return a new one of the same type; Otherwise return
   * an IOException.
   * @param addr target address
   * @param exception the relevant exception
   * @return an exception to throw
   */
  static IOException wrapException(InetSocketAddress addr, Exception exception) {
    if (exception instanceof ConnectException) {
      // connection refused; include the host:port in the error
      return (ConnectException) new ConnectException(
          "Call to " + addr + " failed on connection exception: " + exception).initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException) new SocketTimeoutException(
          "Call to " + addr + " failed because " + exception).initCause(exception);
    } else if (exception instanceof ConnectionClosingException) {
      return (ConnectionClosingException) new ConnectionClosingException(
          "Call to " + addr + " failed on local exception: " + exception).initCause(exception);
    } else {
      return (IOException) new IOException(
          "Call to " + addr + " failed on local exception: " + exception).initCause(exception);
    }
  }

  static void setCancelled(Call call) {
    call.setException(new CallCancelledException("Call id=" + call.id + ", waitTime="
        + (EnvironmentEdgeManager.currentTime() - call.getStartTime()) + ", rpcTimetout="
        + call.timeout));
  }
}
