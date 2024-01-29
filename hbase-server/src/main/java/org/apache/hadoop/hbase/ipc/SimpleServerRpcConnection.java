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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.ipc.RpcServer.CallCleanup;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.io.BytesWritable;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.CodedInputStream;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/** Reads calls from a connection and queues them for handling. */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT",
    justification = "False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
@Deprecated
@InterfaceAudience.Private
class SimpleServerRpcConnection extends ServerRpcConnection {

  final SocketChannel channel;
  private ByteBuff data;
  private ByteBuffer dataLengthBuffer;
  private ByteBuffer preambleBuffer;
  private final LongAdder rpcCount = new LongAdder(); // number of outstanding rpcs
  private long lastContact;
  private final Socket socket;
  final SimpleRpcServerResponder responder;

  // If initial preamble with version and magic has been read or not.
  private boolean connectionPreambleRead = false;
  private boolean saslContextEstablished;
  private ByteBuffer unwrappedData;
  // When is this set? FindBugs wants to know! Says NP
  private ByteBuffer unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
  boolean useWrap = false;

  final ConcurrentLinkedDeque<RpcResponse> responseQueue = new ConcurrentLinkedDeque<>();
  final Lock responseWriteLock = new ReentrantLock();
  long lastSentTime = -1L;

  public SimpleServerRpcConnection(SimpleRpcServer rpcServer, SocketChannel channel,
    long lastContact) {
    super(rpcServer);
    this.channel = channel;
    this.lastContact = lastContact;
    this.data = null;
    this.dataLengthBuffer = ByteBuffer.allocate(4);
    this.socket = channel.socket();
    this.addr = socket.getInetAddress();
    if (addr == null) {
      this.hostAddress = "*Unknown*";
    } else {
      this.hostAddress = addr.getHostAddress();
    }
    this.remotePort = socket.getPort();
    if (rpcServer.socketSendBufferSize != 0) {
      try {
        socket.setSendBufferSize(rpcServer.socketSendBufferSize);
      } catch (IOException e) {
        SimpleRpcServer.LOG.warn(
          "Connection: unable to set socket send buffer size to " + rpcServer.socketSendBufferSize);
      }
    }
    this.responder = rpcServer.responder;
  }

  public void setLastContact(long lastContact) {
    this.lastContact = lastContact;
  }

  public long getLastContact() {
    return lastContact;
  }

  /* Return true if the connection has no outstanding rpc */
  boolean isIdle() {
    return rpcCount.sum() == 0;
  }

  /* Decrement the outstanding RPC count */
  protected void decRpcCount() {
    rpcCount.decrement();
  }

  /* Increment the outstanding RPC count */
  protected void incRpcCount() {
    rpcCount.increment();
  }

  private int readPreamble() throws IOException {
    if (preambleBuffer == null) {
      preambleBuffer = ByteBuffer.allocate(6);
    }
    int count = this.rpcServer.channelRead(channel, preambleBuffer);
    if (count < 0 || preambleBuffer.remaining() > 0) {
      return count;
    }
    preambleBuffer.flip();
    PreambleResponse resp = processPreamble(preambleBuffer);
    switch (resp) {
      case SUCCEED:
        preambleBuffer = null; // do not need it anymore
        connectionPreambleRead = true;
        return count;
      case CONTINUE:
        // wait for the next preamble header
        preambleBuffer.clear();
        return count;
      case CLOSE:
        return -1;
      default:
        throw new IllegalArgumentException("Unknown preamble response: " + resp);
    }
  }

  private int read4Bytes() throws IOException {
    if (this.dataLengthBuffer.remaining() > 0) {
      return this.rpcServer.channelRead(channel, this.dataLengthBuffer);
    } else {
      return 0;
    }
  }

  private void processUnwrappedData(byte[] inBuf) throws IOException, InterruptedException {
    ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(inBuf));
    // Read all RPCs contained in the inBuf, even partial ones
    while (true) {
      int count;
      if (unwrappedDataLengthBuffer.remaining() > 0) {
        count = this.rpcServer.channelRead(ch, unwrappedDataLengthBuffer);
        if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0) {
          return;
        }
      }

      if (unwrappedData == null) {
        unwrappedDataLengthBuffer.flip();
        int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

        if (unwrappedDataLength == RpcClient.PING_CALL_ID) {
          if (RpcServer.LOG.isDebugEnabled()) RpcServer.LOG.debug("Received ping message");
          unwrappedDataLengthBuffer.clear();
          continue; // ping message
        }
        unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
      }

      count = this.rpcServer.channelRead(ch, unwrappedData);
      if (count <= 0 || unwrappedData.remaining() > 0) {
        return;
      }

      if (unwrappedData.remaining() == 0) {
        unwrappedDataLengthBuffer.clear();
        unwrappedData.flip();
        processOneRpc(new SingleByteBuff(unwrappedData));
        unwrappedData = null;
      }
    }
  }

  private void saslReadAndProcess(ByteBuff saslToken) throws IOException, InterruptedException {
    if (saslContextEstablished) {
      RpcServer.LOG.trace("Read input token of size={} for processing by saslServer.unwrap()",
        saslToken.limit());
      if (!useWrap) {
        processOneRpc(saslToken);
      } else {
        byte[] b = saslToken.hasArray() ? saslToken.array() : saslToken.toBytes();
        byte[] plaintextData = saslServer.unwrap(b, 0, b.length);
        // release the request buffer as we have already unwrapped all its content
        callCleanupIfNeeded();
        processUnwrappedData(plaintextData);
      }
    } else {
      byte[] replyToken;
      try {
        try {
          getOrCreateSaslServer();
        } catch (Exception e) {
          RpcServer.LOG.error("Error when trying to create instance of HBaseSaslRpcServer "
            + "with sasl provider: " + provider, e);
          throw e;
        }
        RpcServer.LOG.debug("Created SASL server with mechanism={}",
          provider.getSaslAuthMethod().getAuthMethod());
        RpcServer.LOG.debug(
          "Read input token of size={} for processing by saslServer." + "evaluateResponse()",
          saslToken.limit());
        replyToken = saslServer
          .evaluateResponse(saslToken.hasArray() ? saslToken.array() : saslToken.toBytes());
      } catch (IOException e) {
        RpcServer.LOG.debug("Failed to execute SASL handshake", e);
        Throwable sendToClient = HBaseSaslRpcServer.unwrap(e);
        doRawSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
          sendToClient.getLocalizedMessage());
        this.rpcServer.metrics.authenticationFailure();
        String clientIP = this.toString();
        // attempting user could be null
        RpcServer.AUDITLOG.warn("{}{}: {}", RpcServer.AUTH_FAILED_FOR, clientIP,
          saslServer.getAttemptingUser());
        throw e;
      } finally {
        // release the request buffer as we have already unwrapped all its content
        callCleanupIfNeeded();
      }
      if (replyToken != null) {
        if (RpcServer.LOG.isDebugEnabled()) {
          RpcServer.LOG.debug("Will send token of size " + replyToken.length + " from saslServer.");
        }
        doRawSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null, null);
      }
      if (saslServer.isComplete()) {
        String qop = saslServer.getNegotiatedQop();
        useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
        ugi =
          provider.getAuthorizedUgi(saslServer.getAuthorizationID(), this.rpcServer.secretManager);
        RpcServer.LOG.debug(
          "SASL server context established. Authenticated client: {}. Negotiated QoP is {}", ugi,
          qop);
        this.rpcServer.metrics.authenticationSuccess();
        RpcServer.AUDITLOG.info(RpcServer.AUTH_SUCCESSFUL_FOR + ugi);
        saslContextEstablished = true;
      }
    }
  }

  /**
   * Read off the wire. If there is not enough data to read, update the connection state with what
   * we have and returns.
   * @return Returns -1 if failure (and caller will close connection), else zero or more.
   */
  public int readAndProcess() throws IOException, InterruptedException {
    // If we have not read the connection setup preamble, look to see if that is on the wire.
    if (!connectionPreambleRead) {
      int count = readPreamble();
      if (!connectionPreambleRead) {
        return count;
      }
    }

    // Try and read in an int. it will be length of the data to read (or -1 if a ping). We catch the
    // integer length into the 4-byte this.dataLengthBuffer.
    int count = read4Bytes();
    if (count < 0 || dataLengthBuffer.remaining() > 0) {
      return count;
    }

    // We have read a length and we have read the preamble. It is either the connection header
    // or it is a request.
    if (data == null) {
      dataLengthBuffer.flip();
      int dataLength = dataLengthBuffer.getInt();
      if (dataLength == RpcClient.PING_CALL_ID) {
        if (!useWrap) { // covers the !useSasl too
          dataLengthBuffer.clear();
          return 0; // ping message
        }
      }
      if (dataLength < 0) { // A data length of zero is legal.
        throw new DoNotRetryIOException(
          "Unexpected data length " + dataLength + "!! from " + getHostAddress());
      }

      if (dataLength > this.rpcServer.maxRequestSize) {
        String msg = "RPC data length of " + dataLength + " received from " + getHostAddress()
          + " is greater than max allowed " + this.rpcServer.maxRequestSize + ". Set \""
          + SimpleRpcServer.MAX_REQUEST_SIZE
          + "\" on server to override this limit (not recommended)";
        SimpleRpcServer.LOG.warn(msg);

        if (connectionHeaderRead && connectionPreambleRead) {
          incRpcCount();
          // Construct InputStream for the non-blocking SocketChannel
          // We need the InputStream because we want to read only the request header
          // instead of the whole rpc.
          ByteBuffer buf = ByteBuffer.allocate(1);
          InputStream is = new InputStream() {
            @Override
            public int read() throws IOException {
              SimpleServerRpcConnection.this.rpcServer.channelRead(channel, buf);
              buf.flip();
              int x = buf.get();
              buf.flip();
              return x;
            }
          };
          CodedInputStream cis = CodedInputStream.newInstance(is);
          int headerSize = cis.readRawVarint32();
          Message.Builder builder = RequestHeader.newBuilder();
          ProtobufUtil.mergeFrom(builder, cis, headerSize);
          RequestHeader header = (RequestHeader) builder.build();

          // Notify the client about the offending request
          SimpleServerCall reqTooBig = new SimpleServerCall(header.getCallId(), this.service, null,
            null, null, null, this, 0, this.addr, EnvironmentEdgeManager.currentTime(), 0,
            this.rpcServer.bbAllocator, this.rpcServer.cellBlockBuilder, null, responder);
          RequestTooBigException reqTooBigEx = new RequestTooBigException(msg);
          this.rpcServer.metrics.exception(reqTooBigEx);
          // Make sure the client recognizes the underlying exception
          // Otherwise, throw a DoNotRetryIOException.
          if (
            VersionInfoUtil.hasMinimumVersion(connectionHeader.getVersionInfo(),
              RequestTooBigException.MAJOR_VERSION, RequestTooBigException.MINOR_VERSION)
          ) {
            reqTooBig.setResponse(null, null, reqTooBigEx, msg);
          } else {
            reqTooBig.setResponse(null, null, new DoNotRetryIOException(msg), msg);
          }
          // In most cases we will write out the response directly. If not, it is still OK to just
          // close the connection without writing out the reqTooBig response. Do not try to write
          // out directly here, and it will cause deserialization error if the connection is slow
          // and we have a half writing response in the queue.
          reqTooBig.sendResponseIfReady();
        }
        // Close the connection
        return -1;
      }

      // Initialize this.data with a ByteBuff.
      // This call will allocate a ByteBuff to read request into and assign to this.data
      // Also when we use some buffer(s) from pool, it will create a CallCleanup instance also and
      // assign to this.callCleanup
      initByteBuffToReadInto(dataLength);

      // Increment the rpc count. This counter will be decreased when we write
      // the response. If we want the connection to be detected as idle properly, we
      // need to keep the inc / dec correct.
      incRpcCount();
    }

    count = channelDataRead(channel, data);

    if (count >= 0 && data.remaining() == 0) { // count==0 if dataLength == 0
      process();
    }

    return count;
  }

  // It creates the ByteBuff and CallCleanup and assign to Connection instance.
  private void initByteBuffToReadInto(int length) {
    this.data = rpcServer.bbAllocator.allocate(length);
    this.callCleanup = data::release;
  }

  protected int channelDataRead(ReadableByteChannel channel, ByteBuff buf) throws IOException {
    int count = buf.read(channel);
    if (count > 0) {
      this.rpcServer.metrics.receivedBytes(count);
    }
    return count;
  }

  /**
   * Process the data buffer and clean the connection state for the next call.
   */
  private void process() throws IOException, InterruptedException {
    data.rewind();
    try {
      if (skipInitialSaslHandshake) {
        skipInitialSaslHandshake = false;
        return;
      }

      if (useSasl) {
        saslReadAndProcess(data);
      } else {
        processOneRpc(data);
      }
    } catch (Exception e) {
      callCleanupIfNeeded();
      throw e;
    } finally {
      dataLengthBuffer.clear(); // Clean for the next call
      data = null; // For the GC
      this.callCleanup = null;
    }
  }

  @Override
  public synchronized void close() {
    disposeSasl();
    data = null;
    callCleanupIfNeeded();
    if (!channel.isOpen()) {
      return;
    }
    try {
      socket.shutdownOutput();
    } catch (Exception ignored) {
      if (SimpleRpcServer.LOG.isTraceEnabled()) {
        SimpleRpcServer.LOG.trace("Ignored exception", ignored);
      }
    }
    if (channel.isOpen()) {
      try {
        channel.close();
      } catch (Exception ignored) {
      }
    }
    try {
      socket.close();
    } catch (Exception ignored) {
      if (SimpleRpcServer.LOG.isTraceEnabled()) {
        SimpleRpcServer.LOG.trace("Ignored exception", ignored);
      }
    }
  }

  @Override
  public boolean isConnectionOpen() {
    return channel.isOpen();
  }

  @Override
  public SimpleServerCall createCall(int id, BlockingService service, MethodDescriptor md,
    RequestHeader header, Message param, CellScanner cellScanner, long size,
    InetAddress remoteAddress, int timeout, CallCleanup reqCleanup) {
    return new SimpleServerCall(id, service, md, header, param, cellScanner, this, size,
      remoteAddress, EnvironmentEdgeManager.currentTime(), timeout, this.rpcServer.bbAllocator,
      this.rpcServer.cellBlockBuilder, reqCleanup, this.responder);
  }

  @Override
  protected void doRespond(RpcResponse resp) throws IOException {
    responder.doRespond(this, resp);
  }
}
