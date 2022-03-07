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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.ByteBufferListOutputStream;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;

/**
 * Datastructure that holds all necessary to a method invocation and then afterward, carries
 * the result.
 */
@InterfaceAudience.Private
public abstract class ServerCall<T extends ServerRpcConnection> implements RpcCall, RpcResponse {

  protected final int id;                             // the client's call id
  protected final BlockingService service;
  protected final MethodDescriptor md;
  protected final RequestHeader header;
  protected Message param;                      // the parameter passed
  // Optional cell data passed outside of protobufs.
  protected final CellScanner cellScanner;
  protected final T connection;              // connection to client
  protected final long receiveTime;      // the time received when response is null
                                 // the time served when response is not null
  protected final int timeout;
  protected long startTime;
  protected final long deadline;// the deadline to handle this call, if exceed we can drop it.

  protected final ByteBuffAllocator bbAllocator;

  protected final CellBlockBuilder cellBlockBuilder;

  /**
   * Chain of buffers to send as response.
   */
  protected BufferChain response;

  protected final long size;                          // size of current call
  protected boolean isError;
  protected ByteBufferListOutputStream cellBlockStream = null;
  protected CallCleanup reqCleanup = null;

  protected final User user;
  protected final InetAddress remoteAddress;
  protected RpcCallback rpcCallback;

  private long responseCellSize = 0;
  private long responseBlockSize = 0;
  // cumulative size of serialized exceptions
  private long exceptionSize = 0;
  private final boolean retryImmediatelySupported;

  // This is a dirty hack to address HBASE-22539. The highest bit is for rpc ref and cleanup, and
  // the rest of the bits are for WAL reference count. We can only call release if all of them are
  // zero. The reason why we can not use a general reference counting is that, we may call cleanup
  // multiple times in the current implementation. We should fix this in the future.
  // The refCount here will start as 0x80000000 and increment with every WAL reference and decrement
  // from WAL side on release
  private final AtomicInteger reference = new AtomicInteger(0x80000000);

  private final Span span;

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "NP_NULL_ON_SOME_PATH",
      justification = "Can't figure why this complaint is happening... see below")
  ServerCall(int id, BlockingService service, MethodDescriptor md, RequestHeader header,
      Message param, CellScanner cellScanner, T connection, long size, InetAddress remoteAddress,
      long receiveTime, int timeout, ByteBuffAllocator byteBuffAllocator,
      CellBlockBuilder cellBlockBuilder, CallCleanup reqCleanup) {
    this.id = id;
    this.service = service;
    this.md = md;
    this.header = header;
    this.param = param;
    this.cellScanner = cellScanner;
    this.connection = connection;
    this.receiveTime = receiveTime;
    this.response = null;
    this.isError = false;
    this.size = size;
    if (connection != null) {
      this.user =  connection.user;
      this.retryImmediatelySupported = connection.retryImmediatelySupported;
    } else {
      this.user = null;
      this.retryImmediatelySupported = false;
    }
    this.remoteAddress = remoteAddress;
    this.timeout = timeout;
    this.deadline = this.timeout > 0 ? this.receiveTime + this.timeout : Long.MAX_VALUE;
    this.bbAllocator = byteBuffAllocator;
    this.cellBlockBuilder = cellBlockBuilder;
    this.reqCleanup = reqCleanup;
    this.span = Span.current();
  }

  /**
   * Call is done. Execution happened and we returned results to client. It is
   * now safe to cleanup.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "Presume the lock on processing request held by caller is protection enough")
  @Override
  public void done() {
    if (this.cellBlockStream != null) {
      // This will return back the BBs which we got from pool.
      this.cellBlockStream.releaseResources();
      this.cellBlockStream = null;
    }
    // If the call was run successfuly, we might have already returned the BB
    // back to pool. No worries..Then inputCellBlock will be null
    cleanup();
    span.end();
  }

  @Override
  public void cleanup() {
    for (;;) {
      int ref = reference.get();
      if ((ref & 0x80000000) == 0) {
        return;
      }
      int nextRef = ref & 0x7fffffff;
      if (reference.compareAndSet(ref, nextRef)) {
        if (nextRef == 0) {
          if (this.reqCleanup != null) {
            this.reqCleanup.run();
          }
        }
        return;
      }
    }
  }

  public void retainByWAL() {
    reference.incrementAndGet();
  }

  public void releaseByWAL() {
    // Here this method of decrementAndGet for releasing WAL reference count will work in both
    // cases - i.e. highest bit (cleanup) 1 or 0. We will be decrementing a negative or positive
    // value respectively in these 2 cases, but the logic will work the same way
    if (reference.decrementAndGet() == 0) {
      if (this.reqCleanup != null) {
        this.reqCleanup.run();
      }
    }

  }

  @Override
  public String toString() {
    return toShortString() + " param: " +
      (this.param != null? ProtobufUtil.getShortTextFormat(this.param): "") +
      " connection: " + connection.toString();
  }

  @Override
  public RequestHeader getHeader() {
    return this.header;
  }

  @Override
  public int getPriority() {
    return this.header.getPriority();
  }

  /*
   * Short string representation without param info because param itself could be huge depends on
   * the payload of a command
   */
  @Override
  public String toShortString() {
    String serviceName = this.connection.service != null ?
        this.connection.service.getDescriptorForType().getName() : "null";
    return "callId: " + this.id + " service: " + serviceName +
        " methodName: " + ((this.md != null) ? this.md.getName() : "n/a") +
        " size: " + StringUtils.TraditionalBinaryPrefix.long2String(this.size, "", 1) +
        " connection: " + connection + " deadline: " + deadline;
  }

  @Override
  public synchronized void setResponse(Message m, final CellScanner cells, Throwable t,
    String errorMsg) {
    if (this.isError) {
      return;
    }
    if (t != null) {
      this.isError = true;
      TraceUtil.setError(span, t);
    } else {
      span.setStatus(StatusCode.OK);
    }
    BufferChain bc = null;
    try {
      ResponseHeader.Builder headerBuilder = ResponseHeader.newBuilder();
      // Call id.
      headerBuilder.setCallId(this.id);
      if (t != null) {
        setExceptionResponse(t, errorMsg, headerBuilder);
      }
      // Pass reservoir to buildCellBlock. Keep reference to returne so can add it back to the
      // reservoir when finished. This is hacky and the hack is not contained but benefits are
      // high when we can avoid a big buffer allocation on each rpc.
      List<ByteBuffer> cellBlock = null;
      int cellBlockSize = 0;
      if (bbAllocator.isReservoirEnabled()) {
        this.cellBlockStream = this.cellBlockBuilder.buildCellBlockStream(this.connection.codec,
          this.connection.compressionCodec, cells, bbAllocator);
        if (this.cellBlockStream != null) {
          cellBlock = this.cellBlockStream.getByteBuffers();
          cellBlockSize = this.cellBlockStream.size();
        }
      } else {
        ByteBuffer b = this.cellBlockBuilder.buildCellBlock(this.connection.codec,
          this.connection.compressionCodec, cells);
        if (b != null) {
          cellBlockSize = b.remaining();
          cellBlock = new ArrayList<>(1);
          cellBlock.add(b);
        }
      }

      if (cellBlockSize > 0) {
        CellBlockMeta.Builder cellBlockBuilder = CellBlockMeta.newBuilder();
        // Presumes the cellBlock bytebuffer has been flipped so limit has total size in it.
        cellBlockBuilder.setLength(cellBlockSize);
        headerBuilder.setCellBlockMeta(cellBlockBuilder.build());
      }
      Message header = headerBuilder.build();
      ByteBuffer headerBuf =
          createHeaderAndMessageBytes(m, header, cellBlockSize, cellBlock);
      ByteBuffer[] responseBufs = null;
      int cellBlockBufferSize = 0;
      if (cellBlock != null) {
        cellBlockBufferSize = cellBlock.size();
        responseBufs = new ByteBuffer[1 + cellBlockBufferSize];
      } else {
        responseBufs = new ByteBuffer[1];
      }
      responseBufs[0] = headerBuf;
      if (cellBlock != null) {
        for (int i = 0; i < cellBlockBufferSize; i++) {
          responseBufs[i + 1] = cellBlock.get(i);
        }
      }
      bc = new BufferChain(responseBufs);
    } catch (IOException e) {
      RpcServer.LOG.warn("Exception while creating response " + e);
    }
    this.response = bc;
    // Once a response message is created and set to this.response, this Call can be treated as
    // done. The Responder thread will do the n/w write of this message back to client.
    if (this.rpcCallback != null) {
      try (Scope ignored = span.makeCurrent()) {
        this.rpcCallback.run();
      } catch (Exception e) {
        // Don't allow any exception here to kill this handler thread.
        RpcServer.LOG.warn("Exception while running the Rpc Callback.", e);
        TraceUtil.setError(span, e);
      }
    }
  }

  static void setExceptionResponse(Throwable t, String errorMsg,
      ResponseHeader.Builder headerBuilder) {
    ExceptionResponse.Builder exceptionBuilder = ExceptionResponse.newBuilder();
    exceptionBuilder.setExceptionClassName(t.getClass().getName());
    exceptionBuilder.setStackTrace(errorMsg);
    exceptionBuilder.setDoNotRetry(t instanceof DoNotRetryIOException);
    if (t instanceof RegionMovedException) {
      // Special casing for this exception.  This is only one carrying a payload.
      // Do this instead of build a generic system for allowing exceptions carry
      // any kind of payload.
      RegionMovedException rme = (RegionMovedException)t;
      exceptionBuilder.setHostname(rme.getHostname());
      exceptionBuilder.setPort(rme.getPort());
    } else if (t instanceof HBaseServerException) {
      HBaseServerException hse = (HBaseServerException) t;
      exceptionBuilder.setServerOverloaded(hse.isServerOverloaded());
    }
    // Set the exception as the result of the method invocation.
    headerBuilder.setException(exceptionBuilder.build());
  }

  static ByteBuffer createHeaderAndMessageBytes(Message result, Message header,
      int cellBlockSize, List<ByteBuffer> cellBlock) throws IOException {
    // Organize the response as a set of bytebuffers rather than collect it all together inside
    // one big byte array; save on allocations.
    // for writing the header, we check if there is available space in the buffers
    // created for the cellblock itself. If there is space for the header, we reuse
    // the last buffer in the cellblock. This applies to the cellblock created from the
    // pool or even the onheap cellblock buffer in case there is no pool enabled.
    // Possible reuse would avoid creating a temporary array for storing the header every time.
    ByteBuffer possiblePBBuf =
        (cellBlockSize > 0) ? cellBlock.get(cellBlock.size() - 1) : null;
    int headerSerializedSize = 0, resultSerializedSize = 0, headerVintSize = 0,
        resultVintSize = 0;
    if (header != null) {
      headerSerializedSize = header.getSerializedSize();
      headerVintSize = CodedOutputStream.computeUInt32SizeNoTag(headerSerializedSize);
    }
    if (result != null) {
      resultSerializedSize = result.getSerializedSize();
      resultVintSize = CodedOutputStream.computeUInt32SizeNoTag(resultSerializedSize);
    }
    // calculate the total size
    int totalSize = headerSerializedSize + headerVintSize
        + (resultSerializedSize + resultVintSize)
        + cellBlockSize;
    int totalPBSize = headerSerializedSize + headerVintSize + resultSerializedSize
        + resultVintSize + Bytes.SIZEOF_INT;
    // Only if the last buffer has enough space for header use it. Else allocate
    // a new buffer. Assume they are all flipped
    if (possiblePBBuf != null
        && possiblePBBuf.limit() + totalPBSize <= possiblePBBuf.capacity()) {
      // duplicate the buffer. This is where the header is going to be written
      ByteBuffer pbBuf = possiblePBBuf.duplicate();
      // get the current limit
      int limit = pbBuf.limit();
      // Position such that we write the header to the end of the buffer
      pbBuf.position(limit);
      // limit to the header size
      pbBuf.limit(totalPBSize + limit);
      // mark the current position
      pbBuf.mark();
      writeToCOS(result, header, totalSize, pbBuf);
      // reset the buffer back to old position
      pbBuf.reset();
      return pbBuf;
    } else {
      return createHeaderAndMessageBytes(result, header, totalSize, totalPBSize);
    }
  }

  private static void writeToCOS(Message result, Message header, int totalSize, ByteBuffer pbBuf)
      throws IOException {
    ByteBufferUtils.putInt(pbBuf, totalSize);
    // create COS that works on BB
    CodedOutputStream cos = CodedOutputStream.newInstance(pbBuf);
    if (header != null) {
      cos.writeMessageNoTag(header);
    }
    if (result != null) {
      cos.writeMessageNoTag(result);
    }
    cos.flush();
    cos.checkNoSpaceLeft();
  }

  private static ByteBuffer createHeaderAndMessageBytes(Message result, Message header,
      int totalSize, int totalPBSize) throws IOException {
    ByteBuffer pbBuf = ByteBuffer.allocate(totalPBSize);
    writeToCOS(result, header, totalSize, pbBuf);
    pbBuf.flip();
    return pbBuf;
  }

  protected BufferChain wrapWithSasl(BufferChain bc) throws IOException {
    if (!this.connection.useSasl) {
      return bc;
    }
    // Looks like no way around this; saslserver wants a byte array.  I have to make it one.
    // THIS IS A BIG UGLY COPY.
    byte [] responseBytes = bc.getBytes();
    byte [] token;
    // synchronization may be needed since there can be multiple Handler
    // threads using saslServer or Crypto AES to wrap responses.
    if (connection.useCryptoAesWrap) {
      // wrap with Crypto AES
      synchronized (connection.cryptoAES) {
        token = connection.cryptoAES.wrap(responseBytes, 0, responseBytes.length);
      }
    } else {
      synchronized (connection.saslServer) {
        token = connection.saslServer.wrap(responseBytes, 0, responseBytes.length);
      }
    }
    if (RpcServer.LOG.isTraceEnabled()) {
      RpcServer.LOG.trace("Adding saslServer wrapped token of size " + token.length
          + " as call response.");
    }

    ByteBuffer[] responseBufs = new ByteBuffer[2];
    responseBufs[0] = ByteBuffer.wrap(Bytes.toBytes(token.length));
    responseBufs[1] = ByteBuffer.wrap(token);
    return new BufferChain(responseBufs);
  }

  @Override
  public long disconnectSince() {
    if (!this.connection.isConnectionOpen()) {
      return EnvironmentEdgeManager.currentTime() - receiveTime;
    } else {
      return -1L;
    }
  }

  @Override
  public boolean isClientCellBlockSupported() {
    return this.connection != null && this.connection.codec != null;
  }

  @Override
  public long getResponseCellSize() {
    return responseCellSize;
  }

  @Override
  public void incrementResponseCellSize(long cellSize) {
    responseCellSize += cellSize;
  }

  @Override
  public long getResponseBlockSize() {
    return responseBlockSize;
  }

  @Override
  public void incrementResponseBlockSize(long blockSize) {
    responseBlockSize += blockSize;
  }

  @Override
  public long getResponseExceptionSize() {
    return exceptionSize;
  }
  @Override
  public void incrementResponseExceptionSize(long exSize) {
    exceptionSize += exSize;
  }

  @Override
  public long getSize() {
    return this.size;
  }

  @Override
  public long getDeadline() {
    return deadline;
  }

  @Override
  public Optional<User> getRequestUser() {
    return Optional.ofNullable(user);
  }

  @Override
  public InetAddress getRemoteAddress() {
    return remoteAddress;
  }

  @Override
  public VersionInfo getClientVersionInfo() {
    return connection.getVersionInfo();
  }

  @Override
  public synchronized void setCallBack(RpcCallback callback) {
    this.rpcCallback = callback;
  }

  @Override
  public boolean isRetryImmediatelySupported() {
    return retryImmediatelySupported;
  }

  @Override
  public BlockingService getService() {
    return service;
  }

  @Override
  public MethodDescriptor getMethod() {
    return md;
  }

  @Override
  public Message getParam() {
    return param;
  }

  @Override
  public CellScanner getCellScanner() {
    return cellScanner;
  }

  @Override
  public long getReceiveTime() {
    return receiveTime;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public void setStartTime(long t) {
    this.startTime = t;
  }

  @Override
  public int getTimeout() {
    return timeout;
  }

  @Override
  public int getRemotePort() {
    return connection.getRemotePort();
  }

  @Override
  public synchronized BufferChain getResponse() {
    if (connection.useWrap) {
      /*
       * wrapping result with SASL as the last step just before sending it out, so
       * every message must have the right increasing sequence number
       */
      try {
        return wrapWithSasl(response);
      } catch (IOException e) {
        /* it is exactly the same what setResponse() does */
        RpcServer.LOG.warn("Exception while creating response " + e);
        return null;
      }
    } else {
      return response;
    }
  }
}
