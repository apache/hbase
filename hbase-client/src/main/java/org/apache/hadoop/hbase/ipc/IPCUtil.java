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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.util.concurrent.FastThreadLocal;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.TracingProtos.RPCTInfo;

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
      final ByteBuf cellBlock) throws IOException {
    // Must calculate total size and write that first so other side can read it all in in one
    // swoop. This is dictated by how the server is currently written. Server needs to change
    // if we are to be able to write without the length prefixing.
    int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header, param);
    if (cellBlock != null) {
      totalSize += cellBlock.readableBytes();
    }
    return write(dos, header, param, cellBlock, totalSize);
  }

  private static int write(final OutputStream dos, final Message header, final Message param,
      final ByteBuf cellBlock, final int totalSize) throws IOException {
    // I confirmed toBytes does same as DataOutputStream#writeInt.
    dos.write(Bytes.toBytes(totalSize));
    // This allocates a buffer that is the size of the message internally.
    header.writeDelimitedTo(dos);
    if (param != null) {
      param.writeDelimitedTo(dos);
    }
    if (cellBlock != null) {
      cellBlock.readBytes(dos, cellBlock.readableBytes());
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
      totalSize += CodedOutputStream.computeUInt32SizeNoTag(m.getSerializedSize());
    }
    Preconditions.checkArgument(totalSize < Integer.MAX_VALUE);
    return totalSize;
  }

  static RequestHeader buildRequestHeader(Call call, CellBlockMeta cellBlockMeta) {
    RequestHeader.Builder builder = RequestHeader.newBuilder();
    builder.setCallId(call.id);
    RPCTInfo.Builder traceBuilder = RPCTInfo.newBuilder();
    GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(),
      traceBuilder, (carrier, key, value) -> carrier.putHeaders(key, value));
    builder.setTraceInfo(traceBuilder.build());
    builder.setMethodName(call.md.getName());
    builder.setRequestParam(call.param != null);
    if (cellBlockMeta != null) {
      builder.setCellBlockMeta(cellBlockMeta);
    }
    // Only pass priority if there is one set.
    if (call.priority != HConstants.PRIORITY_UNSET) {
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
    boolean serverOverloaded = e.hasServerOverloaded() && e.getServerOverloaded();
    return e.hasHostname() ?
      // If a hostname then add it to the RemoteWithExtrasException
      new RemoteWithExtrasException(innerExceptionClassName, e.getStackTrace(), e.getHostname(),
        e.getPort(), doNotRetry, serverOverloaded) :
      new RemoteWithExtrasException(innerExceptionClassName, e.getStackTrace(), doNotRetry,
        serverOverloaded);
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

  private static String getCallTarget(Address addr, RegionInfo regionInfo) {
    return "address=" + addr +
      (regionInfo != null? ", region=" + regionInfo.getRegionNameAsString(): "");
  }

  /**
   * Takes an Exception, the address, and if pertinent, the RegionInfo for the Region we were trying
   * to connect to and returns an IOException with the input exception as the cause. The new
   * exception provides the stack trace of the place where the exception is thrown and some extra
   * diagnostics information.
   * <p/>
   * Notice that we will try our best to keep the original exception type when creating a new
   * exception, especially for the 'connection' exceptions, as it is used to determine whether this
   * is a network issue or the remote side tells us clearly what is wrong, which is important
   * deciding whether to retry. If it is not possible to create a new exception with the same type,
   * for example, the {@code error} is not an {@link IOException}, an {@link IOException} will be
   * created.
   * @param addr target address
   * @param error the relevant exception
   * @return an exception to throw
   * @see ClientExceptionsUtil#isConnectionException(Throwable)
   */
  static IOException wrapException(Address addr, RegionInfo regionInfo, Throwable error) {
    if (error instanceof ConnectException) {
      // connection refused; include the host:port in the error
      return (IOException) new ConnectException("Call to " + getCallTarget(addr, regionInfo) +
        " failed on connection exception: " + error).initCause(error);
    } else if (error instanceof SocketTimeoutException) {
      return (IOException) new SocketTimeoutException(
        "Call to " + getCallTarget(addr, regionInfo) + " failed because " + error).initCause(error);
    } else if (error instanceof ConnectionClosingException) {
      return new ConnectionClosingException("Call to " + getCallTarget(addr, regionInfo) +
        " failed on local exception: " + error, error);
    } else if (error instanceof ServerTooBusyException) {
      // we already have address in the exception message
      return (IOException) error;
    } else if (error instanceof DoNotRetryIOException) {
      // try our best to keep the original exception type
      try {
        return (IOException) error.getClass().asSubclass(DoNotRetryIOException.class)
          .getConstructor(String.class)
          .newInstance("Call to " + getCallTarget(addr, regionInfo) +
            " failed on local exception: " + error).initCause(error);
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
          | InvocationTargetException | NoSuchMethodException | SecurityException e) {
        // just ignore, will just new a DoNotRetryIOException instead below
      }
      return new DoNotRetryIOException("Call to " + getCallTarget(addr, regionInfo) +
        " failed on local exception: " + error, error);
    } else if (error instanceof ConnectionClosedException) {
      return new ConnectionClosedException("Call to " + getCallTarget(addr, regionInfo) +
        " failed on local exception: " + error, error);
    } else if (error instanceof CallTimeoutException) {
      return new CallTimeoutException("Call to " + getCallTarget(addr, regionInfo) +
        " failed on local exception: " + error, error);
    } else if (error instanceof ClosedChannelException) {
      // ClosedChannelException does not have a constructor which takes a String but it is a
      // connection exception so we keep its original type
      return (IOException) error;
    } else if (error instanceof TimeoutException) {
      // TimeoutException is not an IOException, let's convert it to TimeoutIOException.
      return new TimeoutIOException("Call to " + getCallTarget(addr, regionInfo) +
        " failed on local exception: " + error, error);
    } else {
      // try our best to keep the original exception type
      if (error instanceof IOException) {
        try {
          return (IOException) error.getClass().asSubclass(IOException.class)
            .getConstructor(String.class)
            .newInstance("Call to " + getCallTarget(addr, regionInfo) +
              " failed on local exception: " + error)
            .initCause(error);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
            | InvocationTargetException | NoSuchMethodException | SecurityException e) {
          // just ignore, will just new an IOException instead below
        }
      }
      return new HBaseIOException("Call to " + getCallTarget(addr, regionInfo) +
        " failed on local exception: " + error, error);
    }
  }

  static void setCancelled(Call call) {
    call.setException(new CallCancelledException(call.toShortString() + ", waitTime="
        + (EnvironmentEdgeManager.currentTime() - call.getStartTime()) + ", rpcTimeout="
        + call.timeout));
  }

  private static final FastThreadLocal<MutableInt> DEPTH = new FastThreadLocal<MutableInt>() {

    @Override
    protected MutableInt initialValue() throws Exception {
      return new MutableInt(0);
    }
  };

  static final int MAX_DEPTH = 4;

  static void execute(EventLoop eventLoop, Runnable action) {
    if (eventLoop.inEventLoop()) {
      // this is used to prevent stack overflow, you can see the same trick in netty's LocalChannel
      // implementation.
      MutableInt depth = DEPTH.get();
      if (depth.intValue() < MAX_DEPTH) {
        depth.increment();
        try {
          action.run();
        } finally {
          depth.decrement();
        }
      } else {
        eventLoop.execute(action);
      }
    } else {
      eventLoop.execute(action);
    }
  }
}
