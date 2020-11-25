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

import static org.apache.hadoop.hbase.ipc.IPCUtil.buildRequestHeader;
import static org.apache.hadoop.hbase.ipc.IPCUtil.createRemoteException;
import static org.apache.hadoop.hbase.ipc.IPCUtil.getTotalSizeWhenWrittenDelimited;
import static org.apache.hadoop.hbase.ipc.IPCUtil.isFatalConnectionException;
import static org.apache.hadoop.hbase.ipc.IPCUtil.setCancelled;
import static org.apache.hadoop.hbase.ipc.IPCUtil.write;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import javax.security.sasl.SaslException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.ipc.HBaseRpcController.CancellationCallback;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.security.HBaseSaslRpcClient;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProvider;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.core.TraceScope;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.Message.Builder;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback;
import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;

/**
 * Thread that reads responses and notifies callers. Each connection owns a socket connected to a
 * remote address. Calls are multiplexed through this socket: responses may be delivered out of
 * order.
 */
@InterfaceAudience.Private
class BlockingRpcConnection extends RpcConnection implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BlockingRpcConnection.class);

  private final BlockingRpcClient rpcClient;

  private final String threadName;
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
      justification = "We are always under lock actually")
  private Thread thread;

  // connected socket. protected for writing UT.
  protected Socket socket = null;
  private DataInputStream in;
  private DataOutputStream out;

  private HBaseSaslRpcClient saslRpcClient;

  // currently active calls
  private final ConcurrentMap<Integer, Call> calls = new ConcurrentHashMap<>();

  private final CallSender callSender;

  private boolean closed = false;

  private byte[] connectionHeaderPreamble;

  private byte[] connectionHeaderWithLength;

  private boolean waitingConnectionHeaderResponse = false;

  /**
   * If the client wants to interrupt its calls easily (i.e. call Thread#interrupt), it gets into a
   * java issue: an interruption during a write closes the socket/channel. A way to avoid this is to
   * use a different thread for writing. This way, on interruptions, we either cancel the writes or
   * ignore the answer if the write is already done, but we don't stop the write in the middle. This
   * adds a thread per region server in the client, so it's kept as an option.
   * <p>
   * The implementation is simple: the client threads adds their call to the queue, and then wait
   * for an answer. The CallSender blocks on the queue, and writes the calls one after the other. On
   * interruption, the client cancels its call. The CallSender checks that the call has not been
   * canceled before writing it.
   * </p>
   * When the connection closes, all the calls not yet sent are dismissed. The client thread is
   * notified with an appropriate exception, as if the call was already sent but the answer not yet
   * received.
   * </p>
   */
  private class CallSender extends Thread {

    private final Queue<Call> callsToWrite;

    private final int maxQueueSize;

    public CallSender(String name, Configuration conf) {
      int queueSize = conf.getInt("hbase.ipc.client.write.queueSize", 1000);
      callsToWrite = new ArrayDeque<>(queueSize);
      this.maxQueueSize = queueSize;
      setDaemon(true);
      setName(name + " - writer");
    }

    public void sendCall(final Call call) throws IOException {
      if (callsToWrite.size() >= maxQueueSize) {
        throw new IOException("Can't add " + call.toShortString()
            + " to the write queue. callsToWrite.size()=" + callsToWrite.size());
      }
      callsToWrite.offer(call);
      BlockingRpcConnection.this.notifyAll();
    }

    public void remove(Call call) {
      callsToWrite.remove(call);
      // By removing the call from the expected call list, we make the list smaller, but
      // it means as well that we don't know how many calls we cancelled.
      calls.remove(call.id);
      call.setException(new CallCancelledException(call.toShortString() + ", waitTime="
          + (EnvironmentEdgeManager.currentTime() - call.getStartTime()) + ", rpcTimeout="
          + call.timeout));
    }

    /**
     * Reads the call from the queue, write them on the socket.
     */
    @Override
    public void run() {
      synchronized (BlockingRpcConnection.this) {
        while (!closed) {
          if (callsToWrite.isEmpty()) {
            // We should use another monitor object here for better performance since the read
            // thread also uses ConnectionImpl.this. But this makes the locking schema more
            // complicated, can do it later as an optimization.
            try {
              BlockingRpcConnection.this.wait();
            } catch (InterruptedException e) {
            }
            // check if we need to quit, so continue the main loop instead of fallback.
            continue;
          }
          Call call = callsToWrite.poll();
          if (call.isDone()) {
            continue;
          }
          try {
            tracedWriteRequest(call);
          } catch (IOException e) {
            // exception here means the call has not been added to the pendingCalls yet, so we need
            // to fail it by our own.
            LOG.debug("call write error for {}", call.toShortString());
            call.setException(e);
            closeConn(e);
          }
        }
      }
    }

    /**
     * Cleans the call not yet sent when we finish.
     */
    public void cleanup(IOException e) {
      IOException ie = new ConnectionClosingException(
          "Connection to " + remoteId.address + " is closing.");
      for (Call call : callsToWrite) {
        call.setException(ie);
      }
      callsToWrite.clear();
    }
  }

  BlockingRpcConnection(BlockingRpcClient rpcClient, ConnectionId remoteId) throws IOException {
    super(rpcClient.conf, AbstractRpcClient.WHEEL_TIMER, remoteId, rpcClient.clusterId,
        rpcClient.userProvider.isHBaseSecurityEnabled(), rpcClient.codec, rpcClient.compressor);
    this.rpcClient = rpcClient;
    if (remoteId.getAddress().isUnresolved()) {
      throw new UnknownHostException("unknown host: " + remoteId.getAddress().getHostName());
    }

    this.connectionHeaderPreamble = getConnectionHeaderPreamble();
    ConnectionHeader header = getConnectionHeader();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(4 + header.getSerializedSize());
    DataOutputStream dos = new DataOutputStream(baos);
    dos.writeInt(header.getSerializedSize());
    header.writeTo(dos);
    assert baos.size() == 4 + header.getSerializedSize();
    this.connectionHeaderWithLength = baos.getBuffer();

    UserGroupInformation ticket = remoteId.ticket.getUGI();
    this.threadName = "BRPC Connection (" + this.rpcClient.socketFactory.hashCode() + ") to "
        + remoteId.getAddress().toString()
        + ((ticket == null) ? " from an unknown user" : (" from " + ticket.getUserName()));

    if (this.rpcClient.conf.getBoolean(BlockingRpcClient.SPECIFIC_WRITE_THREAD, false)) {
      callSender = new CallSender(threadName, this.rpcClient.conf);
      callSender.start();
    } else {
      callSender = null;
    }
  }

  // protected for write UT.
  protected void setupConnection() throws IOException {
    short ioFailures = 0;
    short timeoutFailures = 0;
    while (true) {
      try {
        this.socket = this.rpcClient.socketFactory.createSocket();
        this.socket.setTcpNoDelay(this.rpcClient.isTcpNoDelay());
        this.socket.setKeepAlive(this.rpcClient.tcpKeepAlive);
        if (this.rpcClient.localAddr != null) {
          this.socket.bind(this.rpcClient.localAddr);
        }
        NetUtils.connect(this.socket, remoteId.getAddress(), this.rpcClient.connectTO);
        this.socket.setSoTimeout(this.rpcClient.readTO);
        return;
      } catch (SocketTimeoutException toe) {
        /*
         * The max number of retries is 45, which amounts to 20s*45 = 15 minutes retries.
         */
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received exception in connection setup.\n" +
              StringUtils.stringifyException(toe));
        }
        handleConnectionFailure(timeoutFailures++, this.rpcClient.maxRetries, toe);
      } catch (IOException ie) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Received exception in connection setup.\n" +
              StringUtils.stringifyException(ie));
        }
        handleConnectionFailure(ioFailures++, this.rpcClient.maxRetries, ie);
      }
    }
  }

  /**
   * Handle connection failures If the current number of retries is equal to the max number of
   * retries, stop retrying and throw the exception; Otherwise backoff N seconds and try connecting
   * again. This Method is only called from inside setupIOstreams(), which is synchronized. Hence
   * the sleep is synchronized; the locks will be retained.
   * @param curRetries current number of retries
   * @param maxRetries max number of retries allowed
   * @param ioe failure reason
   * @throws IOException if max number of retries is reached
   */
  private void handleConnectionFailure(int curRetries, int maxRetries, IOException ioe)
      throws IOException {
    closeSocket();

    // throw the exception if the maximum number of retries is reached
    if (curRetries >= maxRetries || ExceptionUtil.isInterrupt(ioe)) {
      throw ioe;
    }

    // otherwise back off and retry
    try {
      Thread.sleep(this.rpcClient.failureSleep);
    } catch (InterruptedException ie) {
      ExceptionUtil.rethrowIfInterrupt(ie);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("Retrying connect to server: " + remoteId.getAddress() +
        " after sleeping " + this.rpcClient.failureSleep + "ms. Already tried " + curRetries +
        " time(s).");
    }
  }

  /*
   * wait till someone signals us to start reading RPC response or it is idle too long, it is marked
   * as to be closed, or the client is marked as not running.
   * @return true if it is time to read a response; false otherwise.
   */
  private synchronized boolean waitForWork() {
    // beware of the concurrent access to the calls list: we can add calls, but as well
    // remove them.
    long waitUntil = EnvironmentEdgeManager.currentTime() + this.rpcClient.minIdleTimeBeforeClose;
    for (;;) {
      if (thread == null) {
        return false;
      }
      if (!calls.isEmpty()) {
        return true;
      }
      if (EnvironmentEdgeManager.currentTime() >= waitUntil) {
        closeConn(
          new IOException("idle connection closed with " + calls.size() + " pending request(s)"));
        return false;
      }
      try {
        wait(Math.min(this.rpcClient.minIdleTimeBeforeClose, 1000));
      } catch (InterruptedException e) {
      }
    }
  }

  @Override
  public void run() {
    if (LOG.isTraceEnabled()) {
      LOG.trace(threadName + ": starting");
    }
    while (waitForWork()) {
      readResponse();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(threadName + ": stopped");
    }
  }

  private void disposeSasl() {
    if (saslRpcClient != null) {
      saslRpcClient.dispose();
      saslRpcClient = null;
    }
  }

  private boolean setupSaslConnection(final InputStream in2, final OutputStream out2)
      throws IOException {
    saslRpcClient = new HBaseSaslRpcClient(this.rpcClient.conf, provider, token,
        serverAddress, securityInfo, this.rpcClient.fallbackAllowed,
        this.rpcClient.conf.get("hbase.rpc.protection",
            QualityOfProtection.AUTHENTICATION.name().toLowerCase(Locale.ROOT)),
        this.rpcClient.conf.getBoolean(CRYPTO_AES_ENABLED_KEY, CRYPTO_AES_ENABLED_DEFAULT));
    return saslRpcClient.saslConnect(in2, out2);
  }

  /**
   * If multiple clients with the same principal try to connect to the same server at the same time,
   * the server assumes a replay attack is in progress. This is a feature of kerberos. In order to
   * work around this, what is done is that the client backs off randomly and tries to initiate the
   * connection again. The other problem is to do with ticket expiry. To handle that, a relogin is
   * attempted.
   * <p>
   * The retry logic is governed by the {@link SaslClientAuthenticationProvider#canRetry()}
   * method. Some providers have the ability to obtain new credentials and then re-attempt to
   * authenticate with HBase services. Other providers will continue to fail if they failed the
   * first time -- for those, we want to fail-fast.
   * </p>
   */
  private void handleSaslConnectionFailure(final int currRetries, final int maxRetries,
      final Exception ex, final UserGroupInformation user)
      throws IOException, InterruptedException {
    closeSocket();
    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException, InterruptedException {
        // A provider which failed authentication, but doesn't have the ability to relogin with
        // some external system (e.g. username/password, the password either works or it doesn't)
        if (!provider.canRetry()) {
          LOG.warn("Exception encountered while connecting to the server : " + ex);
          if (ex instanceof RemoteException) {
            throw (RemoteException) ex;
          }
          if (ex instanceof SaslException) {
            String msg = "SASL authentication failed."
                + " The most likely cause is missing or invalid credentials.";
            throw new RuntimeException(msg, ex);
          }
          throw new IOException(ex);
        }

        // Other providers, like kerberos, could request a new ticket from a keytab. Let
        // them try again.
        if (currRetries < maxRetries) {
          LOG.debug("Exception encountered while connecting to the server", ex);

          // Invoke the provider to perform the relogin
          provider.relogin();

          // Get rid of any old state on the SaslClient
          disposeSasl();

          // have granularity of milliseconds
          // we are sleeping with the Connection lock held but since this
          // connection instance is being used for connecting to the server
          // in question, it is okay
          Thread.sleep(ThreadLocalRandom.current().nextInt(reloginMaxBackoff) + 1);
          return null;
        } else {
          String msg = "Failed to initiate connection for "
              + UserGroupInformation.getLoginUser().getUserName() + " to "
              + securityInfo.getServerPrincipal();
          throw new IOException(msg, ex);
        }
      }
    });
  }

  private void setupIOstreams() throws IOException {
    if (socket != null) {
      // The connection is already available. Perfect.
      return;
    }

    if (this.rpcClient.failedServers.isFailedServer(remoteId.getAddress())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not trying to connect to " + remoteId.address
            + " this server is in the failed servers list");
      }
      throw new FailedServerException(
          "This server is in the failed servers list: " + remoteId.address);
    }

    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Connecting to " + remoteId.address);
      }

      short numRetries = 0;
      int reloginMaxRetries = this.rpcClient.conf.getInt("hbase.security.relogin.maxretries", 5);
      while (true) {
        setupConnection();
        InputStream inStream = NetUtils.getInputStream(socket);
        // This creates a socket with a write timeout. This timeout cannot be changed.
        OutputStream outStream = NetUtils.getOutputStream(socket, this.rpcClient.writeTO);
        // Write out the preamble -- MAGIC, version, and auth to use.
        writeConnectionHeaderPreamble(outStream);
        if (useSasl) {
          final InputStream in2 = inStream;
          final OutputStream out2 = outStream;
          UserGroupInformation ticket = provider.getRealUser(remoteId.ticket);
          boolean continueSasl;
          if (ticket == null) {
            throw new FatalConnectionException("ticket/user is null");
          }
          try {
            continueSasl = ticket.doAs(new PrivilegedExceptionAction<Boolean>() {
              @Override
              public Boolean run() throws IOException {
                return setupSaslConnection(in2, out2);
              }
            });
          } catch (Exception ex) {
            ExceptionUtil.rethrowIfInterrupt(ex);
            handleSaslConnectionFailure(numRetries++, reloginMaxRetries, ex, ticket);
            continue;
          }
          if (continueSasl) {
            // Sasl connect is successful. Let's set up Sasl i/o streams.
            inStream = saslRpcClient.getInputStream();
            outStream = saslRpcClient.getOutputStream();
          } else {
            // fall back to simple auth because server told us so.
            // do not change authMethod and useSasl here, we should start from secure when
            // reconnecting because regionserver may change its sasl config after restart.
          }
        }
        this.in = new DataInputStream(new BufferedInputStream(inStream));
        this.out = new DataOutputStream(new BufferedOutputStream(outStream));
        // Now write out the connection header
        writeConnectionHeader();
        // process the response from server for connection header if necessary
        processResponseForConnectionHeader();

        break;
      }
    } catch (Throwable t) {
      closeSocket();
      IOException e = ExceptionUtil.asInterrupt(t);
      if (e == null) {
        this.rpcClient.failedServers.addToFailedServers(remoteId.address, t);
        if (t instanceof LinkageError) {
          // probably the hbase hadoop version does not match the running hadoop version
          e = new DoNotRetryIOException(t);
        } else if (t instanceof IOException) {
          e = (IOException) t;
        } else {
          e = new IOException("Could not set up IO Streams to " + remoteId.address, t);
        }
      }
      throw e;
    }

    // start the receiver thread after the socket connection has been set up
    thread = new Thread(this, threadName);
    thread.setDaemon(true);
    thread.start();
  }

  /**
   * Write the RPC header: {@code <MAGIC WORD -- 'HBas'> <ONEBYTE_VERSION> <ONEBYTE_AUTH_TYPE>}
   */
  private void writeConnectionHeaderPreamble(OutputStream out) throws IOException {
    out.write(connectionHeaderPreamble);
    out.flush();
  }

  /**
   * Write the connection header.
   */
  private void writeConnectionHeader() throws IOException {
    boolean isCryptoAesEnable = false;
    // check if Crypto AES is enabled
    if (saslRpcClient != null) {
      boolean saslEncryptionEnabled = SaslUtil.QualityOfProtection.PRIVACY.
          getSaslQop().equalsIgnoreCase(saslRpcClient.getSaslQOP());
      isCryptoAesEnable = saslEncryptionEnabled && conf.getBoolean(
          CRYPTO_AES_ENABLED_KEY, CRYPTO_AES_ENABLED_DEFAULT);
    }

    // if Crypto AES is enabled, set transformation and negotiate with server
    if (isCryptoAesEnable) {
      waitingConnectionHeaderResponse = true;
    }
    this.out.write(connectionHeaderWithLength);
    this.out.flush();
  }

  private void processResponseForConnectionHeader() throws IOException {
    // if no response excepted, return
    if (!waitingConnectionHeaderResponse) return;
    try {
      // read the ConnectionHeaderResponse from server
      int len = this.in.readInt();
      byte[] buff = new byte[len];
      int readSize = this.in.read(buff);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Length of response for connection header:" + readSize);
      }

      RPCProtos.ConnectionHeaderResponse connectionHeaderResponse =
          RPCProtos.ConnectionHeaderResponse.parseFrom(buff);

      // Get the CryptoCipherMeta, update the HBaseSaslRpcClient for Crypto Cipher
      if (connectionHeaderResponse.hasCryptoCipherMeta()) {
        negotiateCryptoAes(connectionHeaderResponse.getCryptoCipherMeta());
      }
      waitingConnectionHeaderResponse = false;
    } catch (SocketTimeoutException ste) {
      LOG.error(HBaseMarkers.FATAL, "Can't get the connection header response for rpc timeout, "
          + "please check if server has the correct configuration to support the additional "
          + "function.", ste);
      // timeout when waiting the connection header response, ignore the additional function
      throw new IOException("Timeout while waiting connection header response", ste);
    }
  }

  private void negotiateCryptoAes(RPCProtos.CryptoCipherMeta cryptoCipherMeta)
      throws IOException {
    // initialize the Crypto AES with CryptoCipherMeta
    saslRpcClient.initCryptoCipher(cryptoCipherMeta, this.rpcClient.conf);
    // reset the inputStream/outputStream for Crypto AES encryption
    this.in = new DataInputStream(new BufferedInputStream(saslRpcClient.getInputStream()));
    this.out = new DataOutputStream(new BufferedOutputStream(saslRpcClient.getOutputStream()));
  }

  private void tracedWriteRequest(Call call) throws IOException {
    try (TraceScope ignored = TraceUtil.createTrace("RpcClientImpl.tracedWriteRequest",
          call.span)) {
      writeRequest(call);
    }
  }

  /**
   * Initiates a call by sending the parameter to the remote server. Note: this is not called from
   * the Connection thread, but by other threads.
   * @see #readResponse()
   */
  private void writeRequest(Call call) throws IOException {
    ByteBuf cellBlock = null;
    try {
      cellBlock = this.rpcClient.cellBlockBuilder.buildCellBlock(this.codec, this.compressor,
          call.cells, PooledByteBufAllocator.DEFAULT);
      CellBlockMeta cellBlockMeta;
      if (cellBlock != null) {
        cellBlockMeta = CellBlockMeta.newBuilder().setLength(cellBlock.readableBytes()).build();
      } else {
        cellBlockMeta = null;
      }
      RequestHeader requestHeader = buildRequestHeader(call, cellBlockMeta);

      setupIOstreams();

      // Now we're going to write the call. We take the lock, then check that the connection
      // is still valid, and, if so we do the write to the socket. If the write fails, we don't
      // know where we stand, we have to close the connection.
      if (Thread.interrupted()) {
        throw new InterruptedIOException();
      }

      calls.put(call.id, call); // We put first as we don't want the connection to become idle.
      // from here, we do not throw any exception to upper layer as the call has been tracked in
      // the pending calls map.
      try {
        call.callStats.setRequestSizeBytes(write(this.out, requestHeader, call.param, cellBlock));
      } catch (Throwable t) {
        if(LOG.isTraceEnabled()) {
          LOG.trace("Error while writing {}", call.toShortString());
        }
        IOException e = IPCUtil.toIOE(t);
        closeConn(e);
        return;
      }
    } finally {
      if (cellBlock != null) {
        cellBlock.release();
      }
    }
    notifyAll();
  }

  /*
   * Receive a response. Because only one receiver, so no synchronization on in.
   */
  private void readResponse() {
    Call call = null;
    boolean expectedCall = false;
    try {
      // See HBaseServer.Call.setResponse for where we write out the response.
      // Total size of the response. Unused. But have to read it in anyways.
      int totalSize = in.readInt();

      // Read the header
      ResponseHeader responseHeader = ResponseHeader.parseDelimitedFrom(in);
      int id = responseHeader.getCallId();
      call = calls.remove(id); // call.done have to be set before leaving this method
      expectedCall = (call != null && !call.isDone());
      if (!expectedCall) {
        // So we got a response for which we have no corresponding 'call' here on the client-side.
        // We probably timed out waiting, cleaned up all references, and now the server decides
        // to return a response. There is nothing we can do w/ the response at this stage. Clean
        // out the wire of the response so its out of the way and we can get other responses on
        // this connection.
        int readSoFar = getTotalSizeWhenWrittenDelimited(responseHeader);
        int whatIsLeftToRead = totalSize - readSoFar;
        IOUtils.skipFully(in, whatIsLeftToRead);
        if (call != null) {
          call.callStats.setResponseSizeBytes(totalSize);
          call.callStats
              .setCallTimeMs(EnvironmentEdgeManager.currentTime() - call.callStats.getStartTime());
        }
        return;
      }
      if (responseHeader.hasException()) {
        ExceptionResponse exceptionResponse = responseHeader.getException();
        RemoteException re = createRemoteException(exceptionResponse);
        call.setException(re);
        call.callStats.setResponseSizeBytes(totalSize);
        call.callStats
            .setCallTimeMs(EnvironmentEdgeManager.currentTime() - call.callStats.getStartTime());
        if (isFatalConnectionException(exceptionResponse)) {
          synchronized (this) {
            closeConn(re);
          }
        }
      } else {
        Message value = null;
        if (call.responseDefaultType != null) {
          Builder builder = call.responseDefaultType.newBuilderForType();
          ProtobufUtil.mergeDelimitedFrom(builder, in);
          value = builder.build();
        }
        CellScanner cellBlockScanner = null;
        if (responseHeader.hasCellBlockMeta()) {
          int size = responseHeader.getCellBlockMeta().getLength();
          byte[] cellBlock = new byte[size];
          IOUtils.readFully(this.in, cellBlock, 0, cellBlock.length);
          cellBlockScanner = this.rpcClient.cellBlockBuilder.createCellScanner(this.codec,
            this.compressor, cellBlock);
        }
        call.setResponse(value, cellBlockScanner);
        call.callStats.setResponseSizeBytes(totalSize);
        call.callStats
            .setCallTimeMs(EnvironmentEdgeManager.currentTime() - call.callStats.getStartTime());
      }
    } catch (IOException e) {
      if (expectedCall) {
        call.setException(e);
      }
      if (e instanceof SocketTimeoutException) {
        // Clean up open calls but don't treat this as a fatal condition,
        // since we expect certain responses to not make it by the specified
        // {@link ConnectionId#rpcTimeout}.
        if (LOG.isTraceEnabled()) {
          LOG.trace("ignored", e);
        }
      } else {
        synchronized (this) {
          closeConn(e);
        }
      }
    }
  }

  @Override
  protected synchronized void callTimeout(Call call) {
    // call sender
    calls.remove(call.id);
  }

  // just close socket input and output.
  private void closeSocket() {
    IOUtils.closeStream(out);
    IOUtils.closeStream(in);
    IOUtils.closeSocket(socket);
    out = null;
    in = null;
    socket = null;
  }

  // close socket, reader, and clean up all pending calls.
  private void closeConn(IOException e) {
    if (thread == null) {
      return;
    }
    thread.interrupt();
    thread = null;
    closeSocket();
    if (callSender != null) {
      callSender.cleanup(e);
    }
    for (Call call : calls.values()) {
      call.setException(e);
    }
    calls.clear();
  }

  // release all resources, the connection will not be used any more.
  @Override
  public synchronized void shutdown() {
    closed = true;
    if (callSender != null) {
      callSender.interrupt();
    }
    closeConn(new IOException("connection to " + remoteId.address + " closed"));
  }

  @Override
  public void cleanupConnection() {
    // do nothing
  }

  @Override
  public synchronized void sendRequest(final Call call, HBaseRpcController pcrc)
      throws IOException {
    pcrc.notifyOnCancel(new RpcCallback<Object>() {

      @Override
      public void run(Object parameter) {
        setCancelled(call);
        synchronized (BlockingRpcConnection.this) {
          if (callSender != null) {
            callSender.remove(call);
          } else {
            calls.remove(call.id);
          }
        }
      }
    }, new CancellationCallback() {

      @Override
      public void run(boolean cancelled) throws IOException {
        if (cancelled) {
          setCancelled(call);
          return;
        }
        scheduleTimeoutTask(call);
        if (callSender != null) {
          callSender.sendCall(call);
        } else {
          tracedWriteRequest(call);
        }
      }
    });
  }

  @Override
  public synchronized boolean isActive() {
    return thread != null;
  }
}
