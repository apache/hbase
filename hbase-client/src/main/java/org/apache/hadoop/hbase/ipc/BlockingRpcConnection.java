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

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.RpcCallback;

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
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import javax.security.sasl.SaslException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.io.ByteArrayOutputStream;
import org.apache.hadoop.hbase.ipc.HBaseRpcController.CancellationCallback;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.security.HBaseSaslRpcClient;
import org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

/**
 * Thread that reads responses and notifies callers. Each connection owns a socket connected to a
 * remote address. Calls are multiplexed through this socket: responses may be delivered out of
 * order.
 */
@InterfaceAudience.Private
class BlockingRpcConnection extends RpcConnection implements Runnable {

  private static final Log LOG = LogFactory.getLog(BlockingRpcConnection.class);

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
        throw new IOException("Can't add the call " + call.id
            + " to the write queue. callsToWrite.size()=" + callsToWrite.size());
      }
      callsToWrite.offer(call);
      BlockingRpcConnection.this.notifyAll();
    }

    public void remove(Call call) {
      callsToWrite.remove();
      // By removing the call from the expected call list, we make the list smaller, but
      // it means as well that we don't know how many calls we cancelled.
      calls.remove(call.id);
      call.setException(new CallCancelledException("Call id=" + call.id + ", waitTime="
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
            if (LOG.isDebugEnabled()) {
              LOG.debug("call write error for call #" + call.id, e);
            }
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
    this.threadName = "IPC Client (" + this.rpcClient.socketFactory.hashCode() + ") connection to "
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
        handleConnectionFailure(timeoutFailures++, this.rpcClient.maxRetries, toe);
      } catch (IOException ie) {
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

    LOG.info("Retrying connect to server: " + remoteId.getAddress() + " after sleeping "
        + this.rpcClient.failureSleep + "ms. Already tried " + curRetries + " time(s).");
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
      LOG.trace(threadName + ": starting, connections " + this.rpcClient.connections.size());
    }
    while (waitForWork()) {
      readResponse();
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(threadName + ": stopped, connections " + this.rpcClient.connections.size());
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
    saslRpcClient = new HBaseSaslRpcClient(authMethod, token, serverPrincipal,
        this.rpcClient.fallbackAllowed, this.rpcClient.conf.get("hbase.rpc.protection",
          QualityOfProtection.AUTHENTICATION.name().toLowerCase(Locale.ROOT)));
    return saslRpcClient.saslConnect(in2, out2);
  }

  /**
   * If multiple clients with the same principal try to connect to the same server at the same time,
   * the server assumes a replay attack is in progress. This is a feature of kerberos. In order to
   * work around this, what is done is that the client backs off randomly and tries to initiate the
   * connection again. The other problem is to do with ticket expiry. To handle that, a relogin is
   * attempted.
   * <p>
   * The retry logic is governed by the {@link #shouldAuthenticateOverKrb} method. In case when the
   * user doesn't have valid credentials, we don't need to retry (from cache or ticket). In such
   * cases, it is prudent to throw a runtime exception when we receive a SaslException from the
   * underlying authentication implementation, so there is no retry from other high level (for eg,
   * HCM or HBaseAdmin).
   * </p>
   */
  private void handleSaslConnectionFailure(final int currRetries, final int maxRetries,
      final Exception ex, final UserGroupInformation user)
      throws IOException, InterruptedException {
    closeSocket();
    user.doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws IOException, InterruptedException {
        if (shouldAuthenticateOverKrb()) {
          if (currRetries < maxRetries) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Exception encountered while connecting to " + "the server : " + ex);
            }
            // try re-login
            relogin();
            disposeSasl();
            // have granularity of milliseconds
            // we are sleeping with the Connection lock held but since this
            // connection instance is being used for connecting to the server
            // in question, it is okay
            Thread.sleep(ThreadLocalRandom.current().nextInt(reloginMaxBackoff) + 1);
            return null;
          } else {
            String msg = "Couldn't setup connection for "
                + UserGroupInformation.getLoginUser().getUserName() + " to " + serverPrincipal;
            LOG.warn(msg, ex);
            throw (IOException) new IOException(msg).initCause(ex);
          }
        } else {
          LOG.warn("Exception encountered while connecting to " + "the server : " + ex);
        }
        if (ex instanceof RemoteException) {
          throw (RemoteException) ex;
        }
        if (ex instanceof SaslException) {
          String msg = "SASL authentication failed."
              + " The most likely cause is missing or invalid credentials." + " Consider 'kinit'.";
          LOG.fatal(msg, ex);
          throw new RuntimeException(msg, ex);
        }
        throw new IOException(ex);
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
      final short MAX_RETRIES = 5;
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
          UserGroupInformation ticket = getUGI();
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
            handleSaslConnectionFailure(numRetries++, MAX_RETRIES, ex, ticket);
            continue;
          }
          if (continueSasl) {
            // Sasl connect is successful. Let's set up Sasl i/o streams.
            inStream = saslRpcClient.getInputStream(inStream);
            outStream = saslRpcClient.getOutputStream(outStream);
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
        break;
      }
    } catch (Throwable t) {
      closeSocket();
      IOException e = ExceptionUtil.asInterrupt(t);
      if (e == null) {
        this.rpcClient.failedServers.addToFailedServers(remoteId.address);
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
    this.out.write(connectionHeaderWithLength);
    this.out.flush();
  }

  private void tracedWriteRequest(Call call) throws IOException {
    try (TraceScope ignored = Trace.startSpan("RpcClientImpl.tracedWriteRequest", call.span)) {
      writeRequest(call);
    }
  }

  /**
   * Initiates a call by sending the parameter to the remote server. Note: this is not called from
   * the Connection thread, but by other threads.
   * @see #readResponse()
   */
  private void writeRequest(Call call) throws IOException {
    ByteBuffer cellBlock = this.rpcClient.cellBlockBuilder.buildCellBlock(this.codec,
      this.compressor, call.cells);
    CellBlockMeta cellBlockMeta;
    if (cellBlock != null) {
      cellBlockMeta = CellBlockMeta.newBuilder().setLength(cellBlock.limit()).build();
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
    // from here, we do not throw any exception to upper layer as the call has been tracked in the
    // pending calls map.
    try {
      call.callStats.setRequestSizeBytes(write(this.out, requestHeader, call.param, cellBlock));
    } catch (IOException e) {
      closeConn(e);
      return;
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