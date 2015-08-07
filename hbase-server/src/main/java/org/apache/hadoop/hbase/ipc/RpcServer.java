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

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.BoundedByteBufferPool;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslDigestCallbackHandler;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.hbase.security.SaslStatus;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.cliffc.high_scale_lib.Counter;
import org.cloudera.htrace.TraceInfo;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;
// Uses Writables doing sasl

/**
 * An RPC server that hosts protobuf described Services.
 *
 * An RpcServer instance has a Listener that hosts the socket.  Listener has fixed number
 * of Readers in an ExecutorPool, 10 by default.  The Listener does an accept and then
 * round robin a Reader is chosen to do the read.  The reader is registered on Selector.  Read does
 * total read off the channel and the parse from which it makes a Call.  The call is wrapped in a
 * CallRunner and passed to the scheduler to be run.  Reader goes back to see if more to be done
 * and loops till done.
 *
 * <p>Scheduler can be variously implemented but default simple scheduler has handlers to which it
 * has given the queues into which calls (i.e. CallRunner instances) are inserted.  Handlers run
 * taking from the queue.  They run the CallRunner#run method on each item gotten from queue
 * and keep taking while the server is up.
 *
 * CallRunner#run executes the call.  When done, asks the included Call to put itself on new
 * queue for {@link Responder} to pull from and return result to client.
 *
 * @see RpcClient
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class RpcServer implements RpcServerInterface {
  // The logging package is deliberately outside of standard o.a.h.h package so it is not on
  // by default.
  public static final Log LOG = LogFactory.getLog("org.apache.hadoop.ipc.RpcServer");
  private static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION
      = new CallQueueTooBigException();

  private final boolean authorize;
  private boolean isSecurityEnabled;

  public static final byte CURRENT_VERSION = 0;

  /**
   * How many calls/handler are allowed in the queue.
   */
  static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

  /**
   * The maximum size that we can hold in the RPC queue
   */
  private static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;

  static final int BUFFER_INITIAL_SIZE = 1024;

  private static final String WARN_DELAYED_CALLS = "hbase.ipc.warn.delayedrpc.number";

  private static final int DEFAULT_WARN_DELAYED_CALLS = 1000;

  private final int warnDelayedCalls;

  private AtomicInteger delayedCalls;
  private final IPCUtil ipcUtil;

  private static final String AUTH_FAILED_FOR = "Auth failed for ";
  private static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  private static final Log AUDITLOG = LogFactory.getLog("SecurityLogger." +
    Server.class.getName());
  protected SecretManager<TokenIdentifier> secretManager;
  protected ServiceAuthorizationManager authManager;

  /** This is set to Call object before Handler invokes an RPC and ybdie
   * after the call returns.
   */
  protected static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();

  /** Keeps MonitoredRPCHandler per handler thread. */
  static final ThreadLocal<MonitoredRPCHandler> MONITORED_RPC
      = new ThreadLocal<MonitoredRPCHandler>();

  protected final InetSocketAddress isa;
  protected int port;                             // port we listen on
  private int readThreads;                        // number of read threads
  protected int maxIdleTime;                      // the maximum idle time after
                                                  // which a client may be
                                                  // disconnected
  protected int thresholdIdleConnections;         // the number of idle
                                                  // connections after which we
                                                  // will start cleaning up idle
                                                  // connections
  int maxConnectionsToNuke;                       // the max number of
                                                  // connections to nuke
                                                  // during a cleanup

  protected MetricsHBaseServer metrics;

  protected final Configuration conf;

  private int maxQueueSize;
  protected int socketSendBufferSize;
  protected final boolean tcpNoDelay;   // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives
  protected final long purgeTimeout;    // in milliseconds

  /**
   * This flag is used to indicate to sub threads when they should go down.  When we call
   * {@link #startThreads()}, all threads started will consult this flag on whether they should
   * keep going.  It is set to false when {@link #stop()} is called.
   */
  volatile boolean running = true;

  /**
   * This flag is set to true after all threads are up and 'running' and the server is then opened
   * for business by the calle to {@link #openServer()}.
   */
  volatile boolean started = false;

  /**
   * This is a running count of the size of all outstanding calls by size.
   */
  protected final Counter callQueueSize = new Counter();

  protected final List<Connection> connectionList =
    Collections.synchronizedList(new LinkedList<Connection>());
  //maintain a list
  //of client connections
  private Listener listener = null;
  protected Responder responder = null;
  protected AuthenticationTokenSecretManager authTokenSecretMgr = null;
  protected int numConnections = 0;

  protected HBaseRPCErrorHandler errorHandler = null;

  private static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  private static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";

  /** Default value for above params */
  private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final int warnResponseTime;
  private final int warnResponseSize;
  private final Object serverInstance;
  private final List<BlockingServiceAndInterface> services;

  private final RpcScheduler scheduler;

  private UserProvider userProvider;

  private final BoundedByteBufferPool reservoir;


  /**
   * Datastructure that holds all necessary to a method invocation and then afterward, carries
   * the result.
   */
  class Call implements RpcCallContext {
    protected int id;                             // the client's call id
    protected BlockingService service;
    protected MethodDescriptor md;
    protected RequestHeader header;
    protected Message param;                      // the parameter passed
    // Optional cell data passed outside of protobufs.
    protected CellScanner cellScanner;
    protected Connection connection;              // connection to client
    protected long timestamp;      // the time received when response is null
                                   // the time served when response is not null
    /**
     * Chain of buffers to send as response.
     */
    protected BufferChain response;
    protected boolean delayResponse;
    protected Responder responder;
    protected boolean delayReturnValue;           // if the return value should be
                                                  // set at call completion
    protected long size;                          // size of current call
    protected boolean isError;
    protected TraceInfo tinfo;
    private ByteBuffer cellBlock = null;

    private User user;
    private InetAddress remoteAddress;

    /**
     * Deprecated, do not use
     */
    @Deprecated
    Call(int id, final BlockingService service, final MethodDescriptor md, RequestHeader header,
      Message param, CellScanner cellScanner, Connection connection, Responder responder,
      long size, TraceInfo tinfo) {
      this(id, service, md, header, param, cellScanner, connection, responder, size, tinfo, null);
    }

    Call(int id, final BlockingService service, final MethodDescriptor md, RequestHeader header,
         Message param, CellScanner cellScanner, Connection connection, Responder responder,
         long size, TraceInfo tinfo, InetAddress remoteAddress) {
      this.id = id;
      this.service = service;
      this.md = md;
      this.header = header;
      this.param = param;
      this.cellScanner = cellScanner;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
      this.delayResponse = false;
      this.responder = responder;
      this.isError = false;
      this.size = size;
      this.tinfo = tinfo;
      if (connection != null && connection.user != null) {
        this.user = userProvider.create(connection.user);
      } else {
        this.user = null;
      }
      this.remoteAddress = remoteAddress;
    }

    /**
     * Call is done. Execution happened and we returned results to client. It is now safe to
     * cleanup.
     */
    void done() {
      if (this.cellBlock != null) {
        // Return buffer to reservoir now we are done with it.
        reservoir.putBuffer(this.cellBlock);
        this.cellBlock = null;
      }
    }

    @Override
    public String toString() {
      return toShortString() + " param: " +
        (this.param != null? ProtobufUtil.getShortTextFormat(this.param): "") +
        " connection: " + connection.toString();
    }

    protected RequestHeader getHeader() {
      return this.header;
    }

    @Override
    public User getRequestUser() {
      return user;
    }

    @Override
    public String getRequestUserName() {
      User user = getRequestUser();
      return user == null? null: user.getShortName();
    }

    @Override
    public InetAddress getRemoteAddress() {
      return remoteAddress;
    }

    /*
     * Short string representation without param info because param itself could be huge depends on
     * the payload of a command
     */
    String toShortString() {
      String serviceName = this.connection.service != null?
        this.connection.service.getDescriptorForType().getName() : "null";
      StringBuilder sb = new StringBuilder();
      sb.append("callId: ");
      sb.append(this.id);
      sb.append(" service: ");
      sb.append(serviceName);
      sb.append(" methodName: ");
      sb.append((this.md != null) ? this.md.getName() : "");
      sb.append(" size: ");
      sb.append(StringUtils.humanReadableInt(this.size));
      sb.append(" connection: ");
      sb.append(connection.toString());
      return sb.toString();
    }

    String toTraceString() {
      String serviceName = this.connection.service != null ?
                           this.connection.service.getDescriptorForType().getName() : "";
      String methodName = (this.md != null) ? this.md.getName() : "";
      String result = serviceName + "." + methodName;
      return result;
    }

    protected synchronized void setSaslTokenResponse(ByteBuffer response) {
      this.response = new BufferChain(response);
    }

    protected synchronized void setResponse(Object m, final CellScanner cells,
        Throwable t, String errorMsg) {
      if (this.isError) return;
      if (t != null) this.isError = true;
      BufferChain bc = null;
      try {
        ResponseHeader.Builder headerBuilder = ResponseHeader.newBuilder();
        // Presume it a pb Message.  Could be null.
        Message result = (Message)m;
        // Call id.
        headerBuilder.setCallId(this.id);
        if (t != null) {
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
          }
          // Set the exception as the result of the method invocation.
          headerBuilder.setException(exceptionBuilder.build());
        }
        // Pass reservoir to buildCellBlock. Keep reference to returne so can add it back to the
        // reservoir when finished. This is hacky and the hack is not contained but benefits are
        // high when we can avoid a big buffer allocation on each rpc.
        this.cellBlock = ipcUtil.buildCellBlock(this.connection.codec,
          this.connection.compressionCodec, cells, reservoir);
        if (this.cellBlock != null) {
          CellBlockMeta.Builder cellBlockBuilder = CellBlockMeta.newBuilder();
          // Presumes the cellBlock bytebuffer has been flipped so limit has total size in it.
          cellBlockBuilder.setLength(this.cellBlock.limit());
          headerBuilder.setCellBlockMeta(cellBlockBuilder.build());
        }
        Message header = headerBuilder.build();

        // Organize the response as a set of bytebuffers rather than collect it all together inside
        // one big byte array; save on allocations.
        ByteBuffer bbHeader = IPCUtil.getDelimitedMessageAsByteBuffer(header);
        ByteBuffer bbResult = IPCUtil.getDelimitedMessageAsByteBuffer(result);
        int totalSize = bbHeader.capacity() + (bbResult == null? 0: bbResult.limit()) +
          (this.cellBlock == null? 0: this.cellBlock.limit());
        ByteBuffer bbTotalSize = ByteBuffer.wrap(Bytes.toBytes(totalSize));
        bc = new BufferChain(bbTotalSize, bbHeader, bbResult, this.cellBlock);
        if (connection.useWrap) {
          bc = wrapWithSasl(bc);
        }
      } catch (IOException e) {
        LOG.warn("Exception while creating response " + e);
      }
      this.response = bc;
    }

    private BufferChain wrapWithSasl(BufferChain bc)
        throws IOException {
      if (bc == null) return bc;
      if (!this.connection.useSasl) return bc;
      // Looks like no way around this; saslserver wants a byte array.  I have to make it one.
      // THIS IS A BIG UGLY COPY.
      byte [] responseBytes = bc.getBytes();
      byte [] token;
      // synchronization may be needed since there can be multiple Handler
      // threads using saslServer to wrap responses.
      synchronized (connection.saslServer) {
        token = connection.saslServer.wrap(responseBytes, 0, responseBytes.length);
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Adding saslServer wrapped token of size " + token.length
            + " as call response.");

      ByteBuffer bbTokenLength = ByteBuffer.wrap(Bytes.toBytes(token.length));
      ByteBuffer bbTokenBytes = ByteBuffer.wrap(token);
      return new BufferChain(bbTokenLength, bbTokenBytes);
    }

    @Override
    public synchronized void endDelay(Object result) throws IOException {
      assert this.delayResponse;
      assert this.delayReturnValue || result == null;
      this.delayResponse = false;
      delayedCalls.decrementAndGet();
      if (this.delayReturnValue) {
        this.setResponse(result, null, null, null);
      }
      this.responder.doRespond(this);
    }

    @Override
    public synchronized void endDelay() throws IOException {
      this.endDelay(null);
    }

    @Override
    public synchronized void startDelay(boolean delayReturnValue) {
      assert !this.delayResponse;
      this.delayResponse = true;
      this.delayReturnValue = delayReturnValue;
      int numDelayed = delayedCalls.incrementAndGet();
      if (numDelayed > warnDelayedCalls) {
        LOG.warn("Too many delayed calls: limit " + warnDelayedCalls + " current " + numDelayed);
      }
    }

    @Override
    public synchronized void endDelayThrowing(Throwable t) throws IOException {
      this.setResponse(null, null, t, StringUtils.stringifyException(t));
      this.delayResponse = false;
      this.sendResponseIfReady();
    }

    @Override
    public synchronized boolean isDelayed() {
      return this.delayResponse;
    }

    @Override
    public synchronized boolean isReturnValueDelayed() {
      return this.delayReturnValue;
    }

    @Override
    public boolean isClientCellBlockSupport() {
      return this.connection != null && this.connection.codec != null;
    }

    @Override
    public long disconnectSince() {
      if (!connection.channel.isOpen()) {
        return System.currentTimeMillis() - timestamp;
      } else {
        return -1L;
      }
    }

    public long getSize() {
      return this.size;
    }

    /**
     * If we have a response, and delay is not set, then respond
     * immediately.  Otherwise, do not respond to client.  This is
     * called by the RPC code in the context of the Handler thread.
     */
    public synchronized void sendResponseIfReady() throws IOException {
      if (!this.delayResponse) {
        this.responder.doRespond(this);
      }
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {

    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private Reader[] readers = null;
    private int currentReader = 0;
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private long cleanupInterval = 10000; //the minimum interval between
                                          //two cleanup runs
    private int backlogLength = conf.getInt("hbase.ipc.server.listen.queue.size",
      conf.getInt("ipc.server.listen.queue.size", 128));

    private ExecutorService readPool;

    public Listener(final String name) throws IOException {
      super(name);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), isa, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();

      readers = new Reader[readThreads];
      readPool = Executors.newFixedThreadPool(readThreads,
        new ThreadFactoryBuilder().setNameFormat(
          "RpcServer.reader=%d,port=" + port).setDaemon(true).build());
      for (int i = 0; i < readThreads; ++i) {
        Reader reader = new Reader();
        readers[i] = reader;
        readPool.execute(reader);
      }
      LOG.info(getName() + ": started " + readThreads + " reader(s).");

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("RpcServer.listener,port=" + port);
      this.setDaemon(true);
    }


    private class Reader implements Runnable {
      private volatile boolean adding = false;
      private final Selector readSelector;

      Reader() throws IOException {
        this.readSelector = Selector.open();
      }
      public void run() {
        try {
          doRunLoop();
        } finally {
          try {
            readSelector.close();
          } catch (IOException ioe) {
            LOG.error(getName() + ": error closing read selector in " + getName(), ioe);
          }
        }
      }

      private synchronized void doRunLoop() {
        while (running) {
          SelectionKey key = null;
          try {
            readSelector.select();
            while (adding) {
              this.wait(1000);
            }

            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              key = iter.next();
              iter.remove();
              if (key.isValid()) {
                if (key.isReadable()) {
                  doRead(key);
                }
              }
              key = null;
            }
          } catch (InterruptedException e) {
            if (running) {                      // unexpected -- log it
              LOG.info(getName() + ": unexpectedly interrupted: " +
                StringUtils.stringifyException(e));
            }
          } catch (IOException ex) {
            LOG.error(getName() + ": error in Reader", ex);
          }
        }
      }

      /**
       * This gets reader into the state that waits for the new channel
       * to be registered with readSelector. If it was waiting in select()
       * the thread will be woken up, otherwise whenever select() is called
       * it will return even if there is nothing to read and wait
       * in while(adding) for finishAdd call
       */
      public void startAdd() {
        adding = true;
        readSelector.wakeup();
      }

      public synchronized SelectionKey registerChannel(SocketChannel channel)
        throws IOException {
        return channel.register(readSelector, SelectionKey.OP_READ);
      }

      public synchronized void finishAdd() {
        adding = false;
        this.notify();
      }
    }

    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all
     * connections will be looked at for the cleanup.
     * @param force all connections will be looked at for cleanup
     */
    private void cleanupConnections(boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
            if (LOG.isDebugEnabled())
              LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
            closeConnection(c);
            numNuked++;
            end--;
            //noinspection UnusedAssignment
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      while (running) {
        SelectionKey key = null;
        try {
          selector.select(); // FindBugs IS2_INCONSISTENT_SYNC
          Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
          while (iter.hasNext()) {
            key = iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
              }
            } catch (IOException ignored) {
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OutOfMemoryError");
              closeCurrentConnection(key, e);
              cleanupConnections(true);
              return;
            }
          } else {
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            LOG.warn(getName() + ": OutOfMemoryError in server select", e);
            closeCurrentConnection(key, e);
            cleanupConnections(true);
            try { Thread.sleep(60000); } catch (Exception ignored) {}
          }
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }
      LOG.info(getName() + ": stopping");

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException ignored) { }

        selector= null;
        acceptChannel= null;

        // clean up all connections
        while (!connectionList.isEmpty()) {
          closeConnection(connectionList.remove(0));
        }
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress() +
                (e != null ? " on error " + e.getMessage() : ""));
          }
          closeConnection(c);
          key.attach(null);
        }
      }
    }

    InetSocketAddress getAddress() {
      return (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
    }

    void doAccept(SelectionKey key) throws IOException, OutOfMemoryError {
      Connection c;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();

      SocketChannel channel;
      while ((channel = server.accept()) != null) {
        try {
          channel.configureBlocking(false);
          channel.socket().setTcpNoDelay(tcpNoDelay);
          channel.socket().setKeepAlive(tcpKeepAlive);
        } catch (IOException ioe) {
          channel.close();
          throw ioe;
        }

        Reader reader = getReader();
        try {
          reader.startAdd();
          SelectionKey readKey = reader.registerChannel(channel);
          c = getConnection(channel, System.currentTimeMillis());
          readKey.attach(c);
          synchronized (connectionList) {
            connectionList.add(numConnections, c);
            numConnections++;
          }
          if (LOG.isDebugEnabled())
            LOG.debug(getName() + ": connection from " + c.toString() +
                "; # active connections: " + numConnections);
        } finally {
          reader.finishAdd();
        }
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {
      int count = 0;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;
      }
      c.setLastContact(System.currentTimeMillis());
      try {
        count = c.readAndProcess();
      } catch (InterruptedException ieo) {
        throw ieo;
      } catch (Exception e) {
        LOG.warn(getName() + ": count of bytes read: " + count, e);
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": DISCONNECTING client " + c.toString() +
            " because read count=" + count +
            ". Number of active connections: " + numConnections);
        }
        closeConnection(c);
        // c = null;
      } else {
        c.setLastContact(System.currentTimeMillis());
      }
    }

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.info(getName() + ": exception in closing listener socket. " + e);
        }
      }
      readPool.shutdownNow();
    }

    // The method that will return the next reader to work with
    // Simplistic implementation of round robin for now
    Reader getReader() {
      currentReader = (currentReader + 1) % readers.length;
      return readers[currentReader];
    }
  }

  // Sends responses of RPC back to clients.
  protected class Responder extends Thread {
    private final Selector writeSelector;
    private int pending;         // connections waiting to register

    Responder() throws IOException {
      this.setName("RpcServer.responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      try {
        doRunLoop();
      } finally {
        LOG.info(getName() + ": stopping");
        try {
          writeSelector.close();
        } catch (IOException ioe) {
          LOG.error(getName() + ": couldn't close write selector", ioe);
        }
      }
    }

    private void doRunLoop() {
      long lastPurgeTime = 0;   // last check for old calls.

      while (running) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(purgeTimeout);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.info(getName() + ": asyncWrite", e);
            }
          }
          long now = System.currentTimeMillis();
          if (now < lastPurgeTime + purgeTimeout) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          if (LOG.isDebugEnabled()) LOG.debug(getName() + ": checking for old call responses.");
          ArrayList<Call> calls;

          // get the list of channels from list of keys.
          synchronized (writeSelector.keys()) {
            calls = new ArrayList<Call>(writeSelector.keys().size());
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              Call call = (Call)key.attachment();
              if (call != null && key.channel() == call.connection.channel) {
                calls.add(call);
              }
            }
          }

          for(Call call : calls) {
            try {
              doPurge(call, now);
            } catch (IOException e) {
              LOG.warn(getName() + ": error in purging old calls " + e);
            }
          }
        } catch (OutOfMemoryError e) {
          if (errorHandler != null) {
            if (errorHandler.checkOOME(e)) {
              LOG.info(getName() + ": exiting on OutOfMemoryError");
              return;
            }
          } else {
            //
            // we can run out of memory if we have too many threads
            // log the event and sleep for a minute and give
            // some thread(s) a chance to finish
            //
            LOG.warn(getName() + ": OutOfMemoryError in server select", e);
            try { Thread.sleep(60000); } catch (Exception ignored) {}
          }
        } catch (Exception e) {
          LOG.warn(getName() + ": exception in Responder " +
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info(getName() + ": stopped");
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue
    // for a long time.
    //
    private void doPurge(Call call, long now) throws IOException {
      synchronized (call.connection.responseQueue) {
        Iterator<Call> iter = call.connection.responseQueue.listIterator(0);
        while (iter.hasNext()) {
          Call nextCall = iter.next();
          if (now > nextCall.timestamp + purgeTimeout) {
            closeConnection(nextCall.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private boolean processResponse(final LinkedList<Call> responseQueue, boolean inHandler)
    throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements;
      Call call = null;
      try {
        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          SocketChannel channel = call.connection.channel;
          //
          // Send as much data as we can in the non-blocking fashion
          //
          long numBytes = channelWrite(channel, call.response);
          if (numBytes < 0) {
            return true;
          }
          if (!call.response.hasRemaining()) {
            call.connection.decRpcCount();
            //noinspection RedundantIfStatement
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": callId: " + call.id + " wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then
            // insert in Selector queue.
            //
            call.connection.responseQueue.addFirst(call);

            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = System.currentTimeMillis();
              if (enqueueInSelector(call))
                done = true;
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + call.toShortString() + " partially sent, wrote " +
                numBytes + " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          LOG.warn(getName() + call.toShortString() + ": output error");
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      if (done) call.done();
      return done;
    }

    //
    // Enqueue for background thread to send responses out later.
    //
    private boolean enqueueInSelector(Call call) throws IOException {
      boolean done = false;
      incPending();
      try {
        // Wake up the thread blocked on select, only then can the call
        // to channel.register() complete.
        SocketChannel channel = call.connection.channel;
        writeSelector.wakeup();
        channel.register(writeSelector, SelectionKey.OP_WRITE, call);
      } catch (ClosedChannelException e) {
        //It's OK.  Channel might be closed else where.
        done = true;
      } finally {
        decPending();
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      // set the serve time when the response has to be sent later
      call.timestamp = System.currentTimeMillis();

      boolean doRegister = false;
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        if (call.connection.responseQueue.size() == 1) {
          doRegister = !processResponse(call.connection.responseQueue, false);
        }
      }
      if (doRegister) {
        enqueueInSelector(call);
      }
    }

    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }

  @SuppressWarnings("serial")
  public static class CallQueueTooBigException extends IOException {
    CallQueueTooBigException() {
      super();
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="VO_VOLATILE_INCREMENT",
      justification="False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
  public class Connection {
    // If initial preamble with version and magic has been read or not.
    private boolean connectionPreambleRead = false;
    // If the connection header has been read or not.
    private boolean connectionHeaderRead = false;
    protected SocketChannel channel;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    protected final LinkedList<Call> responseQueue;
    private Counter rpcCount = new Counter(); // number of outstanding rpcs
    private long lastContact;
    private InetAddress addr;
    protected Socket socket;
    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    protected String hostAddress;
    protected int remotePort;
    ConnectionHeader connectionHeader;
    /**
     * Codec the client asked use.
     */
    private Codec codec;
    /**
     * Compression codec the client asked us use.
     */
    private CompressionCodec compressionCodec;
    BlockingService service;
    protected UserGroupInformation user = null;
    private AuthMethod authMethod;
    private boolean saslContextEstablished;
    private boolean skipInitialSaslHandshake;
    private ByteBuffer unwrappedData;
    // When is this set?  FindBugs wants to know!  Says NP
    private ByteBuffer unwrappedDataLengthBuffer = ByteBuffer.allocate(4);
    boolean useSasl;
    SaslServer saslServer;
    private boolean useWrap = false;
    // Fake 'call' for failed authorization response
    private static final int AUTHROIZATION_FAILED_CALLID = -1;
    private final Call authFailedCall =
      new Call(AUTHROIZATION_FAILED_CALLID, this.service, null,
        null, null, null, this, null, 0, null, null);
    private ByteArrayOutputStream authFailedResponse =
        new ByteArrayOutputStream();
    // Fake 'call' for SASL context setup
    private static final int SASL_CALLID = -33;
    private final Call saslCall =
      new Call(SASL_CALLID, this.service, null, null, null, null, this, null, 0, null, null);

    public UserGroupInformation attemptingUser = null; // user name before auth

    public Connection(SocketChannel channel, long lastContact) {
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
      this.responseQueue = new LinkedList<Call>();
      if (socketSendBufferSize != 0) {
        try {
          socket.setSendBufferSize(socketSendBufferSize);
        } catch (IOException e) {
          LOG.warn("Connection: unable to set socket send buffer size to " +
                   socketSendBufferSize);
        }
      }
    }

    @Override
    public String toString() {
      return getHostAddress() + ":" + remotePort;
    }

    public String getHostAddress() {
      return hostAddress;
    }

    public InetAddress getHostInetAddress() {
      return addr;
    }

    public int getRemotePort() {
      return remotePort;
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    /* Return true if the connection has no outstanding rpc */
    private boolean isIdle() {
      return rpcCount.get() == 0;
    }

    /* Decrement the outstanding RPC count */
    protected void decRpcCount() {
      rpcCount.decrement();
    }

    /* Increment the outstanding RPC count */
    protected void incRpcCount() {
      rpcCount.increment();
    }

    protected boolean timedOut(long currentTime) {
      return isIdle() && currentTime - lastContact > maxIdleTime;
    }

    private UserGroupInformation getAuthorizedUgi(String authorizedId)
        throws IOException {
      if (authMethod == AuthMethod.DIGEST) {
        TokenIdentifier tokenId = HBaseSaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        UserGroupInformation ugi = tokenId.getUser();
        if (ugi == null) {
          throw new AccessDeniedException(
              "Can't retrieve username from tokenIdentifier.");
        }
        ugi.addTokenIdentifier(tokenId);
        return ugi;
      } else {
        return UserGroupInformation.createRemoteUser(authorizedId);
      }
    }

    private void saslReadAndProcess(byte[] saslToken) throws IOException,
        InterruptedException {
      if (saslContextEstablished) {
        if (LOG.isDebugEnabled())
          LOG.debug("Have read input token of size " + saslToken.length
              + " for processing by saslServer.unwrap()");

        if (!useWrap) {
          processOneRpc(saslToken);
        } else {
          byte [] plaintextData = saslServer.unwrap(saslToken, 0, saslToken.length);
          processUnwrappedData(plaintextData);
        }
      } else {
        byte[] replyToken = null;
        try {
          if (saslServer == null) {
            switch (authMethod) {
            case DIGEST:
              if (secretManager == null) {
                throw new AccessDeniedException(
                    "Server is not configured to do DIGEST authentication.");
              }
              saslServer = Sasl.createSaslServer(AuthMethod.DIGEST
                  .getMechanismName(), null, SaslUtil.SASL_DEFAULT_REALM,
                  SaslUtil.SASL_PROPS, new SaslDigestCallbackHandler(
                      secretManager, this));
              break;
            default:
              UserGroupInformation current = UserGroupInformation
              .getCurrentUser();
              String fullName = current.getUserName();
              if (LOG.isDebugEnabled()) {
                LOG.debug("Kerberos principal name is " + fullName);
              }
              final String names[] = SaslUtil.splitKerberosName(fullName);
              if (names.length != 3) {
                throw new AccessDeniedException(
                    "Kerberos principal name does NOT have the expected "
                        + "hostname part: " + fullName);
              }
              current.doAs(new PrivilegedExceptionAction<Object>() {
                @Override
                public Object run() throws SaslException {
                  saslServer = Sasl.createSaslServer(AuthMethod.KERBEROS
                      .getMechanismName(), names[0], names[1],
                      SaslUtil.SASL_PROPS, new SaslGssCallbackHandler());
                  return null;
                }
              });
            }
            if (saslServer == null)
              throw new AccessDeniedException(
                  "Unable to find SASL server implementation for "
                      + authMethod.getMechanismName());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Created SASL server with mechanism = " + authMethod.getMechanismName());
            }
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Have read input token of size " + saslToken.length
                + " for processing by saslServer.evaluateResponse()");
          }
          replyToken = saslServer.evaluateResponse(saslToken);
        } catch (IOException e) {
          IOException sendToClient = e;
          Throwable cause = e;
          while (cause != null) {
            if (cause instanceof InvalidToken) {
              sendToClient = (InvalidToken) cause;
              break;
            }
            cause = cause.getCause();
          }
          doRawSaslReply(SaslStatus.ERROR, null, sendToClient.getClass().getName(),
            sendToClient.getLocalizedMessage());
          metrics.authenticationFailure();
          String clientIP = this.toString();
          // attempting user could be null
          AUDITLOG.warn(AUTH_FAILED_FOR + clientIP + ":" + attemptingUser);
          throw e;
        }
        if (replyToken != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Will send token of size " + replyToken.length
                + " from saslServer.");
          }
          doRawSaslReply(SaslStatus.SUCCESS, new BytesWritable(replyToken), null,
              null);
        }
        if (saslServer.isComplete()) {
          String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
          useWrap = qop != null && !"auth".equalsIgnoreCase(qop);
          user = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Authenticated client: "
              + user + ". Negotiated QoP is "
              + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          metrics.authenticationSuccess();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + user);
          saslContextEstablished = true;
        }
      }
    }

    /**
     * No protobuf encoding of raw sasl messages
     */
    private void doRawSaslReply(SaslStatus status, Writable rv,
        String errorClass, String error) throws IOException {
      ByteBufferOutputStream saslResponse = null;
      DataOutputStream out = null;
      try {
        // In my testing, have noticed that sasl messages are usually
        // in the ballpark of 100-200. That's why the initial capacity is 256.
        saslResponse = new ByteBufferOutputStream(256);
        out = new DataOutputStream(saslResponse);
        out.writeInt(status.state); // write status
        if (status == SaslStatus.SUCCESS) {
          rv.write(out);
        } else {
          WritableUtils.writeString(out, errorClass);
          WritableUtils.writeString(out, error);
        }
        saslCall.setSaslTokenResponse(saslResponse.getByteBuffer());
        saslCall.responder = responder;
        saslCall.sendResponseIfReady();
      } finally {
        if (saslResponse != null) {
          saslResponse.close();
        }
        if (out != null) {
          out.close();
        }
      }
    }

    private void disposeSasl() {
      if (saslServer != null) {
        try {
          saslServer.dispose();
          saslServer = null;
        } catch (SaslException ignored) {
          // Ignored. This is being disposed of anyway.
        }
      }
    }

    /**
     * Read off the wire.
     * @return Returns -1 if failure (and caller will close connection) else return how many
     * bytes were read and processed
     * @throws IOException
     * @throws InterruptedException
     */
    public int readAndProcess() throws IOException, InterruptedException {
      while (true) {
        // Try and read in an int.  If new connection, the int will hold the 'HBas' HEADER.  If it
        // does, read in the rest of the connection preamble, the version and the auth method.
        // Else it will be length of the data to read (or -1 if a ping).  We catch the integer
        // length into the 4-byte this.dataLengthBuffer.
        int count;
        if (this.dataLengthBuffer.remaining() > 0) {
          count = channelRead(channel, this.dataLengthBuffer);
          if (count < 0 || this.dataLengthBuffer.remaining() > 0) {
            return count;
          }
        }
        // If we have not read the connection setup preamble, look to see if that is on the wire.
        if (!connectionPreambleRead) {
          // Check for 'HBas' magic.
          this.dataLengthBuffer.flip();
          if (!HConstants.RPC_HEADER.equals(dataLengthBuffer)) {
            return doBadPreambleHandling("Expected HEADER=" +
              Bytes.toStringBinary(HConstants.RPC_HEADER.array()) +
              " but received HEADER=" + Bytes.toStringBinary(dataLengthBuffer.array()) +
              " from " + toString());
          }
          // Now read the next two bytes, the version and the auth to use.
          ByteBuffer versionAndAuthBytes = ByteBuffer.allocate(2);
          count = channelRead(channel, versionAndAuthBytes);
          if (count < 0 || versionAndAuthBytes.remaining() > 0) {
            return count;
          }
          int version = versionAndAuthBytes.get(0);
          byte authbyte = versionAndAuthBytes.get(1);
          this.authMethod = AuthMethod.valueOf(authbyte);
          if (version != CURRENT_VERSION) {
            String msg = getFatalConnectionString(version, authbyte);
            return doBadPreambleHandling(msg, new WrongVersionException(msg));
          }
          if (authMethod == null) {
            String msg = getFatalConnectionString(version, authbyte);
            return doBadPreambleHandling(msg, new BadAuthException(msg));
          }
          if (isSecurityEnabled && authMethod == AuthMethod.SIMPLE) {
            AccessDeniedException ae = new AccessDeniedException("Authentication is required");
            setupResponse(authFailedResponse, authFailedCall, ae, ae.getMessage());
            responder.doRespond(authFailedCall);
            throw ae;
          }
          if (!isSecurityEnabled && authMethod != AuthMethod.SIMPLE) {
            doRawSaslReply(SaslStatus.SUCCESS, new IntWritable(
                SaslUtil.SWITCH_TO_SIMPLE_AUTH), null, null);
            authMethod = AuthMethod.SIMPLE;
            // client has already sent the initial Sasl message and we
            // should ignore it. Both client and server should fall back
            // to simple auth from now on.
            skipInitialSaslHandshake = true;
          }
          if (authMethod != AuthMethod.SIMPLE) {
            useSasl = true;
          }
          connectionPreambleRead = true;
          // Preamble checks out. Go around again to read actual connection header.
          dataLengthBuffer.clear();
          continue;
        }
        // We have read a length and we have read the preamble.  It is either the connection header
        // or it is a request.
        if (data == null) {
          dataLengthBuffer.flip();
          int dataLength = dataLengthBuffer.getInt();
          if (dataLength == RpcClient.PING_CALL_ID) {
            if (!useWrap) { //covers the !useSasl too
              dataLengthBuffer.clear();
              return 0;  //ping message
            }
          }
          if (dataLength < 0) {
            throw new IllegalArgumentException("Unexpected data length "
                + dataLength + "!! from " + getHostAddress());
          }
          data = ByteBuffer.allocate(dataLength);
          incRpcCount();  // Increment the rpc count
        }
        count = channelRead(channel, data);
        if (count < 0) {
          return count;
        } else if (data.remaining() == 0) {
          dataLengthBuffer.clear();
          data.flip();
          if (skipInitialSaslHandshake) {
            data = null;
            skipInitialSaslHandshake = false;
            continue;
          }
          boolean headerRead = connectionHeaderRead;
          if (useSasl) {
            saslReadAndProcess(data.array());
          } else {
            processOneRpc(data.array());
          }
          this.data = null;
          if (!headerRead) {
            continue;
          }
        } else if (count > 0) {
          // We got some data and there is more to read still; go around again.
          if (LOG.isTraceEnabled()) LOG.trace("Continue to read rest of data " + data.remaining());
          continue;
        }
        return count;
      }
    }

    private String getFatalConnectionString(final int version, final byte authByte) {
      return "serverVersion=" + CURRENT_VERSION +
      ", clientVersion=" + version + ", authMethod=" + authByte +
      ", authSupported=" + (authMethod != null) + " from " + toString();
    }

    private int doBadPreambleHandling(final String msg) throws IOException {
      return doBadPreambleHandling(msg, new FatalConnectionException(msg));
    }

    private int doBadPreambleHandling(final String msg, final Exception e) throws IOException {
      LOG.warn(msg);
      Call fakeCall = new Call(-1, null, null, null, null, null, this, responder, -1, null, null);
      setupResponse(null, fakeCall, e, msg);
      responder.doRespond(fakeCall);
      // Returning -1 closes out the connection.
      return -1;
    }

    // Reads the connection header following version
    private void processConnectionHeader(byte[] buf) throws IOException {
      this.connectionHeader = ConnectionHeader.parseFrom(buf);
      String serviceName = connectionHeader.getServiceName();
      if (serviceName == null) throw new EmptyServiceNameException();
      this.service = getService(services, serviceName);
      if (this.service == null) throw new UnknownServiceException(serviceName);
      setupCellBlockCodecs(this.connectionHeader);
      UserGroupInformation protocolUser = createUser(connectionHeader);
      if (!useSasl) {
        user = protocolUser;
        if (user != null) {
          user.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod);
        }
      } else {
        // user is authenticated
        user.setAuthenticationMethod(authMethod.authenticationMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However,
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(user.getUserName()))) {
          if (authMethod == AuthMethod.DIGEST) {
            // Not allowed to doAs if token authentication is used
            throw new AccessDeniedException("Authenticated user (" + user
                + ") doesn't match what the client claims to be ("
                + protocolUser + ")");
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = user;
            user = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
            // Now the user is a proxy user, set Authentication method Proxy.
            user.setAuthenticationMethod(AuthenticationMethod.PROXY);
          }
        }
      }
      if (connectionHeader.hasVersionInfo()) {
        AUDITLOG.info("Connection from " + this.hostAddress + " port: " + this.remotePort
            + " with version info: "
            + TextFormat.shortDebugString(connectionHeader.getVersionInfo()));
      } else {
        AUDITLOG.info("Connection from " + this.hostAddress + " port: " + this.remotePort
            + " with unknown version info");
      }
    }

    /**
     * Set up cell block codecs
     * @param header
     * @throws FatalConnectionException
     */
    private void setupCellBlockCodecs(final ConnectionHeader header)
    throws FatalConnectionException {
      // TODO: Plug in other supported decoders.
      if (!header.hasCellBlockCodecClass()) return;
      String className = header.getCellBlockCodecClass();
      if (className == null || className.length() == 0) return;
      try {
        this.codec = (Codec)Class.forName(className).newInstance();
      } catch (Exception e) {
        throw new UnsupportedCellCodecException(className, e);
      }
      if (!header.hasCellBlockCompressorClass()) return;
      className = header.getCellBlockCompressorClass();
      try {
        this.compressionCodec = (CompressionCodec)Class.forName(className).newInstance();
      } catch (Exception e) {
        throw new UnsupportedCompressionCodecException(className, e);
      }
    }

    private void processUnwrappedData(byte[] inBuf) throws IOException,
    InterruptedException {
      ReadableByteChannel ch = Channels.newChannel(new ByteArrayInputStream(inBuf));
      // Read all RPCs contained in the inBuf, even partial ones
      while (true) {
        int count = -1;
        if (unwrappedDataLengthBuffer.remaining() > 0) {
          count = channelRead(ch, unwrappedDataLengthBuffer);
          if (count <= 0 || unwrappedDataLengthBuffer.remaining() > 0)
            return;
        }

        if (unwrappedData == null) {
          unwrappedDataLengthBuffer.flip();
          int unwrappedDataLength = unwrappedDataLengthBuffer.getInt();

          if (unwrappedDataLength == RpcClient.PING_CALL_ID) {
            if (LOG.isDebugEnabled())
              LOG.debug("Received ping message");
            unwrappedDataLengthBuffer.clear();
            continue; // ping message
          }
          unwrappedData = ByteBuffer.allocate(unwrappedDataLength);
        }

        count = channelRead(ch, unwrappedData);
        if (count <= 0 || unwrappedData.remaining() > 0)
          return;

        if (unwrappedData.remaining() == 0) {
          unwrappedDataLengthBuffer.clear();
          unwrappedData.flip();
          processOneRpc(unwrappedData.array());
          unwrappedData = null;
        }
      }
    }

    private void processOneRpc(byte[] buf) throws IOException, InterruptedException {
      if (connectionHeaderRead) {
        processRequest(buf);
      } else {
        processConnectionHeader(buf);
        this.connectionHeaderRead = true;
        if (!authorizeConnection()) {
          // Throw FatalConnectionException wrapping ACE so client does right thing and closes
          // down the connection instead of trying to read non-existent retun.
          throw new AccessDeniedException("Connection from " + this + " for service " +
            connectionHeader.getServiceName() + " is unauthorized for user: " + user);
        }
      }
    }

    /**
     * @param buf Has the request header and the request param and optionally encoded data buffer
     * all in this one array.
     * @throws IOException
     * @throws InterruptedException
     */
    protected void processRequest(byte[] buf) throws IOException, InterruptedException {
      long totalRequestSize = buf.length;
      int offset = 0;
      // Here we read in the header.  We avoid having pb
      // do its default 4k allocation for CodedInputStream.  We force it to use backing array.
      CodedInputStream cis = CodedInputStream.newInstance(buf, offset, buf.length);
      int headerSize = cis.readRawVarint32();
      offset = cis.getTotalBytesRead();
      Message.Builder builder = RequestHeader.newBuilder();
      ProtobufUtil.mergeFrom(builder, buf, offset, headerSize);
      RequestHeader header = (RequestHeader) builder.build();
      offset += headerSize;
      int id = header.getCallId();
      if (LOG.isTraceEnabled()) {
        LOG.trace("RequestHeader " + TextFormat.shortDebugString(header) +
          " totalRequestSize: " + totalRequestSize + " bytes");
      }
      // Enforcing the call queue size, this triggers a retry in the client
      // This is a bit late to be doing this check - we have already read in the total request.
      if ((totalRequestSize + callQueueSize.get()) > maxQueueSize) {
        final Call callTooBig =
          new Call(id, this.service, null, null, null, null, this,
            responder, totalRequestSize, null, null);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, callTooBig, CALL_QUEUE_TOO_BIG_EXCEPTION,
            "Call queue is full on " + getListenerAddress() +
                ", is hbase.ipc.server.max.callqueue.size too small?");
        responder.doRespond(callTooBig);
        return;
      }
      MethodDescriptor md = null;
      Message param = null;
      CellScanner cellScanner = null;
      try {
        if (header.hasRequestParam() && header.getRequestParam()) {
          md = this.service.getDescriptorForType().findMethodByName(header.getMethodName());
          if (md == null) throw new UnsupportedOperationException(header.getMethodName());
          builder = this.service.getRequestPrototype(md).newBuilderForType();
          // To read the varint, I need an inputstream; might as well be a CIS.
          cis = CodedInputStream.newInstance(buf, offset, buf.length);
          int paramSize = cis.readRawVarint32();
          offset += cis.getTotalBytesRead();
          if (builder != null) {
            ProtobufUtil.mergeFrom(builder, buf, offset, paramSize);
            param = builder.build();
          }
          offset += paramSize;
        }
        if (header.hasCellBlockMeta()) {
          cellScanner = ipcUtil.createCellScanner(this.codec, this.compressionCodec,
            buf, offset, buf.length);
        }
      } catch (Throwable t) {
        String msg = getListenerAddress() + " is unable to read call parameter from client " +
            getHostAddress();
        LOG.warn(msg, t);

        metrics.exception(t);

        // probably the hbase hadoop version does not match the running hadoop version
        if (t instanceof LinkageError) {
          t = new DoNotRetryIOException(t);
        }
        // If the method is not present on the server, do not retry.
        if (t instanceof UnsupportedOperationException) {
          t = new DoNotRetryIOException(t);
        }

        final Call readParamsFailedCall =
          new Call(id, this.service, null, null, null, null, this,
            responder, totalRequestSize, null, null);
        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        setupResponse(responseBuffer, readParamsFailedCall, t,
          msg + "; " + t.getMessage());
        responder.doRespond(readParamsFailedCall);
        return;
      }

      TraceInfo traceInfo = header.hasTraceInfo()
          ? new TraceInfo(header.getTraceInfo().getTraceId(), header.getTraceInfo().getParentId())
          : null;
      Call call = new Call(id, this.service, md, header, param, cellScanner, this, responder,
              totalRequestSize,
              traceInfo, RpcServer.getRemoteIp());
      scheduler.dispatch(new CallRunner(RpcServer.this, call));
    }

    private boolean authorizeConnection() throws IOException {
      try {
        // If auth method is DIGEST, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (user != null && user.getRealUser() != null
            && (authMethod != AuthMethod.DIGEST)) {
          ProxyUsers.authorize(user, this.getHostAddress(), conf);
        }
        authorize(user, connectionHeader, getHostInetAddress());
        if (LOG.isDebugEnabled()) {
          LOG.debug("Authorized " + TextFormat.shortDebugString(connectionHeader));
        }
        metrics.authorizationSuccess();
      } catch (AuthorizationException ae) {
        LOG.debug("Connection authorization failed: " + ae.getMessage(), ae);
        metrics.authorizationFailure();
        setupResponse(authFailedResponse, authFailedCall,
          new AccessDeniedException(ae), ae.getMessage());
        responder.doRespond(authFailedCall);
        return false;
      }
      return true;
    }

    protected synchronized void close() {
      disposeSasl();
      data = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception ignored) {} // FindBugs DE_MIGHT_IGNORE
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception ignored) {}
      }
      try {socket.close();} catch(Exception ignored) {}
    }

    private UserGroupInformation createUser(ConnectionHeader head) {
      UserGroupInformation ugi = null;

      if (!head.hasUserInfo()) {
        return null;
      }
      UserInformation userInfoProto = head.getUserInfo();
      String effectiveUser = null;
      if (userInfoProto.hasEffectiveUser()) {
        effectiveUser = userInfoProto.getEffectiveUser();
      }
      String realUser = null;
      if (userInfoProto.hasRealUser()) {
        realUser = userInfoProto.getRealUser();
      }
      if (effectiveUser != null) {
        if (realUser != null) {
          UserGroupInformation realUserUgi =
              UserGroupInformation.createRemoteUser(realUser);
          ugi = UserGroupInformation.createProxyUser(effectiveUser, realUserUgi);
        } else {
          ugi = UserGroupInformation.createRemoteUser(effectiveUser);
        }
      }
      return ugi;
    }
  }

  /**
   * Datastructure for passing a {@link BlockingService} and its associated class of
   * protobuf service interface.  For example, a server that fielded what is defined
   * in the client protobuf service would pass in an implementation of the client blocking service
   * and then its ClientService.BlockingInterface.class.  Used checking connection setup.
   */
  public static class BlockingServiceAndInterface {
    private final BlockingService service;
    private final Class<?> serviceInterface;
    public BlockingServiceAndInterface(final BlockingService service,
        final Class<?> serviceInterface) {
      this.service = service;
      this.serviceInterface = serviceInterface;
    }
    public Class<?> getServiceInterface() {
      return this.serviceInterface;
    }
    public BlockingService getBlockingService() {
      return this.service;
    }
  }

  /**
   * Constructs a server listening on the named port and address.
   * @param serverInstance hosting instance of {@link Server}. We will do authentications if an
   * instance else pass null for no authentication check.
   * @param name Used keying this rpc servers' metrics and for naming the Listener thread.
   * @param services A list of services.
   * @param isa Where to listen
   * @param conf
   * @throws IOException
   */
  public RpcServer(final Server serverInstance, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress isa, Configuration conf,
      RpcScheduler scheduler)
      throws IOException {
    this.serverInstance = serverInstance;
    this.reservoir = new BoundedByteBufferPool(
      conf.getInt("hbase.ipc.server.reservoir.max.buffer.size",  1024 * 1024),
      conf.getInt("hbase.ipc.server.reservoir.initial.buffer.size", 16 * 1024),
      // Make the max twice the number of handlers to be safe.
      conf.getInt("hbase.ipc.server.reservoir.initial.max",
        conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
          HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT) * 2));
    this.services = services;
    this.isa = isa;
    this.conf = conf;
    this.socketSendBufferSize = 0;
    this.maxQueueSize = conf.getInt("hbase.ipc.server.max.callqueue.size",
      conf.getInt("ipc.server.max.callqueue.size", DEFAULT_MAX_CALLQUEUE_SIZE));
    this.readThreads = conf.getInt("hbase.ipc.server.read.threadpool.size",
      conf.getInt("ipc.server.read.threadpool.size", 10));
    this.maxIdleTime = 2 * conf.getInt("hbase.ipc.client.connection.maxidletime",
      conf.getInt("ipc.client.connection.maxidletime", 1000));
    this.maxConnectionsToNuke = conf.getInt("hbase.ipc.client.kill.max",
      conf.getInt("ipc.client.kill.max", 10));
    this.thresholdIdleConnections = conf.getInt("hbase.ipc.client.idlethreshold",
      conf.getInt("ipc.client.idlethreshold", 4000));
    this.purgeTimeout = conf.getLong("hbase.ipc.client.call.purge.timeout",
      conf.getLong("ipc.client.call.purge.timeout",
        2 * HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
    this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME, DEFAULT_WARN_RESPONSE_TIME);
    this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE, DEFAULT_WARN_RESPONSE_SIZE);

    // Start the listener here and let it bind to the port
    listener = new Listener(name);
    this.port = listener.getAddress().getPort();

    this.metrics = new MetricsHBaseServer(name, new MetricsHBaseServerWrapperImpl(this));
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.server.tcpnodelay", true);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.server.tcpkeepalive",
      conf.getBoolean("ipc.server.tcpkeepalive", true));

    this.warnDelayedCalls = conf.getInt(WARN_DELAYED_CALLS, DEFAULT_WARN_DELAYED_CALLS);
    this.delayedCalls = new AtomicInteger(0);
    this.ipcUtil = new IPCUtil(conf);


    // Create the responder here
    responder = new Responder();
    this.authorize = conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    this.userProvider = UserProvider.instantiate(conf);
    this.isSecurityEnabled = userProvider.isHBaseSecurityEnabled();
    if (isSecurityEnabled) {
      HBaseSaslRpcServer.init(conf);
    }
    this.scheduler = scheduler;
    this.scheduler.init(new RpcSchedulerContext(this));
  }

  /**
   * Subclasses of HBaseServer can override this to provide their own
   * Connection implementations.
   */
  protected Connection getConnection(SocketChannel channel, long time) {
    return new Connection(channel, time);
  }

  /**
   * Setup response for the RPC Call.
   *
   * @param response buffer to serialize the response into
   * @param call {@link Call} to which we are setting up the response
   * @param error error message, if the call failed
   * @param t
   * @throws IOException
   */
  private void setupResponse(ByteArrayOutputStream response, Call call, Throwable t, String error)
  throws IOException {
    if (response != null) response.reset();
    call.setResponse(null, null, t, error);
  }

  protected void closeConnection(Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection)) {
        numConnections--;
      }
    }
    connection.close();
  }

  Configuration getConf() {
    return conf;
  }

  /** Sets the socket buffer size used for responding to RPCs.
   * @param size send size
   */
  @Override
  public void setSocketSendBufSize(int size) { this.socketSendBufferSize = size; }

  /** Starts the service.  Must be called before any calls will be handled. */
  @Override
  public void start() {
    startThreads();
    openServer();
  }

  /**
   * Open a previously started server.
   */
  @Override
  public void openServer() {
    this.started = true;
  }

  @Override
  public boolean isStarted() {
    return this.started;
  }

  /**
   * Starts the service threads but does not allow requests to be responded yet.
   * Client will get {@link ServerNotRunningYetException} instead.
   */
  @Override
  public synchronized void startThreads() {
    authTokenSecretMgr = createSecretManager();
    if (authTokenSecretMgr != null) {
      setSecretManager(authTokenSecretMgr);
      authTokenSecretMgr.start();
    }
    this.authManager = new ServiceAuthorizationManager();
    HBasePolicyProvider.init(conf, authManager);
    responder.start();
    listener.start();
    scheduler.start();
  }

  @Override
  public void refreshAuthManager(PolicyProvider pp) {
    // Ignore warnings that this should be accessed in a static way instead of via an instance;
    // it'll break if you go via static route.
    this.authManager.refresh(this.conf, pp);
  }

  private AuthenticationTokenSecretManager createSecretManager() {
    if (!isSecurityEnabled) return null;
    if (serverInstance == null) return null;
    if (!(serverInstance instanceof org.apache.hadoop.hbase.Server)) return null;
    org.apache.hadoop.hbase.Server server = (org.apache.hadoop.hbase.Server)serverInstance;
    Configuration conf = server.getConfiguration();
    long keyUpdateInterval =
        conf.getLong("hbase.auth.key.update.interval", 24*60*60*1000);
    long maxAge =
        conf.getLong("hbase.auth.token.max.lifetime", 7*24*60*60*1000);
    return new AuthenticationTokenSecretManager(conf, server.getZooKeeper(),
        server.getServerName().toString(), keyUpdateInterval, maxAge);
  }

  public SecretManager<? extends TokenIdentifier> getSecretManager() {
    return this.secretManager;
  }

  @SuppressWarnings("unchecked")
  public void setSecretManager(SecretManager<? extends TokenIdentifier> secretManager) {
    this.secretManager = (SecretManager<TokenIdentifier>) secretManager;
  }

  /**
   * This is a server side method, which is invoked over RPC. On success
   * the return response has protobuf response payload. On failure, the
   * exception name and the stack trace are returned in the protobuf response.
   */
  public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
      Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
  throws IOException {
    try {
      status.setRPC(md.getName(), new Object[]{param}, receiveTime);
      // TODO: Review after we add in encoded data blocks.
      status.setRPCPacket(param);
      status.resume("Servicing call");
      //get an instance of the method arg type
      long startTime = System.currentTimeMillis();
      PayloadCarryingRpcController controller = new PayloadCarryingRpcController(cellScanner);
      Message result = service.callBlockingMethod(md, controller, param);
      long endTime = System.currentTimeMillis();
      int processingTime = (int) (endTime - startTime);
      int qTime = (int) (startTime - receiveTime);
      int totalTime = (int) (endTime - receiveTime);
      if (LOG.isTraceEnabled()) {
        LOG.trace(CurCall.get().toString() +
            ", response " + TextFormat.shortDebugString(result) +
            " queueTime: " + qTime +
            " processingTime: " + processingTime +
            " totalTime: " + totalTime);
      }
      metrics.dequeuedCall(qTime);
      metrics.processedCall(processingTime);
      metrics.totalCall(totalTime);
      long responseSize = result.getSerializedSize();
      // log any RPC responses that are slower than the configured warn
      // response time or larger than configured warning size
      boolean tooSlow = (processingTime > warnResponseTime && warnResponseTime > -1);
      boolean tooLarge = (responseSize > warnResponseSize && warnResponseSize > -1);
      if (tooSlow || tooLarge) {
        // when tagging, we let TooLarge trump TooSmall to keep output simple
        // note that large responses will often also be slow.
        StringBuilder buffer = new StringBuilder(256);
        buffer.append(md.getName());
        buffer.append("(");
        buffer.append(param.getClass().getName());
        buffer.append(")");
        logResponse(new Object[]{param},
            md.getName(), buffer.toString(), (tooLarge ? "TooLarge" : "TooSlow"),
            status.getClient(), startTime, processingTime, qTime,
            responseSize);
      }
      return new Pair<Message, CellScanner>(result,
        controller != null? controller.cellScanner(): null);
    } catch (Throwable e) {
      // The above callBlockingMethod will always return a SE.  Strip the SE wrapper before
      // putting it on the wire.  Its needed to adhere to the pb Service Interface but we don't
      // need to pass it over the wire.
      if (e instanceof ServiceException) e = e.getCause();

      // increment the number of requests that were exceptions.
      metrics.exception(e);

      if (e instanceof LinkageError) throw new DoNotRetryIOException(e);
      if (e instanceof IOException) throw (IOException)e;
      LOG.error("Unexpected throwable object ", e);
      throw new IOException(e.getMessage(), e);
    }
  }

  /**
   * Logs an RPC response to the LOG file, producing valid JSON objects for
   * client Operations.
   * @param params The parameters received in the call.
   * @param methodName The name of the method invoked
   * @param call The string representation of the call
   * @param tag  The tag that will be used to indicate this event in the log.
   * @param clientAddress   The address of the client who made this call.
   * @param startTime       The time that the call was initiated, in ms.
   * @param processingTime  The duration that the call took to run, in ms.
   * @param qTime           The duration that the call spent on the queue
   *                        prior to being initiated, in ms.
   * @param responseSize    The size in bytes of the response buffer.
   */
  void logResponse(Object[] params, String methodName, String call, String tag,
      String clientAddress, long startTime, int processingTime, int qTime,
      long responseSize)
          throws IOException {
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<String, Object>();
    responseInfo.put("starttimems", startTime);
    responseInfo.put("processingtimems", processingTime);
    responseInfo.put("queuetimems", qTime);
    responseInfo.put("responsesize", responseSize);
    responseInfo.put("client", clientAddress);
    responseInfo.put("class", serverInstance == null? "": serverInstance.getClass().getSimpleName());
    responseInfo.put("method", methodName);
    if (params.length == 2 && serverInstance instanceof HRegionServer &&
        params[0] instanceof byte[] &&
        params[1] instanceof Operation) {
      // if the slow process is a query, we want to log its table as well
      // as its own fingerprint
      TableName tableName = TableName.valueOf(
          HRegionInfo.parseRegionName((byte[]) params[0])[0]);
      responseInfo.put("table", tableName.getNameAsString());
      // annotate the response map with operation details
      responseInfo.putAll(((Operation) params[1]).toMap());
      // report to the log file
      LOG.warn("(operation" + tag + "): " +
               MAPPER.writeValueAsString(responseInfo));
    } else if (params.length == 1 && serverInstance instanceof HRegionServer &&
        params[0] instanceof Operation) {
      // annotate the response map with operation details
      responseInfo.putAll(((Operation) params[0]).toMap());
      // report to the log file
      LOG.warn("(operation" + tag + "): " +
               MAPPER.writeValueAsString(responseInfo));
    } else {
      // can't get JSON details, so just report call.toString() along with
      // a more generic tag.
      responseInfo.put("call", call);
      LOG.warn("(response" + tag + "): " + MAPPER.writeValueAsString(responseInfo));
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  @Override
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (authTokenSecretMgr != null) {
      authTokenSecretMgr.stop();
      authTokenSecretMgr = null;
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    scheduler.stop();
    notifyAll();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   * @throws InterruptedException e
   */
  @Override
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  @Override
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }

  /**
   * Set the handler for calling out of RPC for error conditions.
   * @param handler the handler implementation
   */
  @Override
  public void setErrorHandler(HBaseRPCErrorHandler handler) {
    this.errorHandler = handler;
  }

  @Override
  public HBaseRPCErrorHandler getErrorHandler() {
    return this.errorHandler;
  }

  /**
   * Returns the metrics instance for reporting RPC call statistics
   */
  public MetricsHBaseServer getMetrics() {
    return metrics;
  }

  @Override
  public void addCallSize(final long diff) {
    this.callQueueSize.add(diff);
  }

  /**
   * Authorize the incoming client connection.
   *
   * @param user client user
   * @param connection incoming connection
   * @param addr InetAddress of incoming connection
   * @throws org.apache.hadoop.security.authorize.AuthorizationException when the client isn't authorized to talk the protocol
   */
  @SuppressWarnings("static-access")
  public void authorize(UserGroupInformation user, ConnectionHeader connection, InetAddress addr)
  throws AuthorizationException {
    if (authorize) {
      Class<?> c = getServiceInterface(services, connection.getServiceName());
      this.authManager.authorize(user != null ? user : null, c, getConf(), addr);
    }
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  private static int NIO_BUFFER_LIMIT = 64 * 1024; //should not be more than 64KB.

  /**
   * This is a wrapper around {@link java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * buffer increases. This also minimizes extra copies in NIO layer
   * as a result of multiple write operations required to write a large
   * buffer.
   *
   * @param channel writable byte channel to write to
   * @param bufferChain Chain of buffers to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
   */
  protected long channelWrite(GatheringByteChannel channel, BufferChain bufferChain)
  throws IOException {
    long count =  bufferChain.write(channel, NIO_BUFFER_LIMIT);
    if (count > 0) this.metrics.sentBytes(count);
    return count;
  }

  /**
   * This is a wrapper around {@link java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)}.
   * If the amount of data is large, it writes to channel in smaller chunks.
   * This is to avoid jdk from creating many direct buffers as the size of
   * ByteBuffer increases. There should not be any performance degredation.
   *
   * @param channel writable byte channel to write on
   * @param buffer buffer to write
   * @return number of bytes written
   * @throws java.io.IOException e
   * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
   */
  protected int channelRead(ReadableByteChannel channel,
                                   ByteBuffer buffer) throws IOException {

    int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
           channel.read(buffer) : channelIO(channel, null, buffer);
    if (count > 0) {
      metrics.receivedBytes(count);
    }
    return count;
  }

  /**
   * Helper for {@link #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)}
   * and {@link #channelWrite(java.nio.channels.WritableByteChannel, java.nio.ByteBuffer)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @param readCh read channel
   * @param writeCh write channel
   * @param buf buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
   * @see #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
   * @see #channelWrite(java.nio.channels.WritableByteChannel, java.nio.ByteBuffer)
   */
  private static int channelIO(ReadableByteChannel readCh,
                               WritableByteChannel writeCh,
                               ByteBuffer buf) throws IOException {

    int originalLimit = buf.limit();
    int initialRemaining = buf.remaining();
    int ret = 0;

    while (buf.remaining() > 0) {
      try {
        int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
        buf.limit(buf.position() + ioSize);

        ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

        if (ret < ioSize) {
          break;
        }

      } finally {
        buf.limit(originalLimit);
      }
    }

    int nBytes = initialRemaining - buf.remaining();
    return (nBytes > 0) ? nBytes : ret;
  }

  /**
   * Needed for features such as delayed calls.  We need to be able to store the current call
   * so that we can complete it later or ask questions of what is supported by the current ongoing
   * call.
   * @return An RpcCallConext backed by the currently ongoing call (gotten from a thread local)
   */
  public static RpcCallContext getCurrentCall() {
    return CurCall.get();
  }

  public static boolean isInRpcCallContext() {
    return CurCall.get() != null;
  }

  /**
   * Returns the user credentials associated with the current RPC request or
   * <code>null</code> if no credentials were provided.
   * @return A User
   */
  public static User getRequestUser() {
    RpcCallContext ctx = getCurrentCall();
    return ctx == null? null: ctx.getRequestUser();
  }

  /**
   * Returns the username for any user associated with the current RPC
   * request or <code>null</code> if no user is set.
   */
  public static String getRequestUserName() {
    User user = getRequestUser();
    return user == null? null: user.getShortName();
  }

  /**
   * @return Address of remote client if a request is ongoing, else null
   */
  public static InetAddress getRemoteAddress() {
    RpcCallContext ctx = getCurrentCall();
    return ctx == null? null: ctx.getRemoteAddress();
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available service instances
   * @return Matching BlockingServiceAndInterface pair
   */
  static BlockingServiceAndInterface getServiceAndInterface(
      final List<BlockingServiceAndInterface> services, final String serviceName) {
    for (BlockingServiceAndInterface bs : services) {
      if (bs.getBlockingService().getDescriptorForType().getName().equals(serviceName)) {
        return bs;
      }
    }
    return null;
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available services and their service interfaces.
   * @return Service interface class for <code>serviceName</code>
   */
  static Class<?> getServiceInterface(
      final List<BlockingServiceAndInterface> services,
      final String serviceName) {
    BlockingServiceAndInterface bsasi =
        getServiceAndInterface(services, serviceName);
    return bsasi == null? null: bsasi.getServiceInterface();
  }

  /**
   * @param serviceName Some arbitrary string that represents a 'service'.
   * @param services Available services and their service interfaces.
   * @return BlockingService that goes with the passed <code>serviceName</code>
   */
  static BlockingService getService(
      final List<BlockingServiceAndInterface> services,
      final String serviceName) {
    BlockingServiceAndInterface bsasi =
        getServiceAndInterface(services, serviceName);
    return bsasi == null? null: bsasi.getBlockingService();
  }

  /** Returns the remote side ip address when invoked inside an RPC
   *  Returns null incase of an error.
   *  @return InetAddress
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null && call.connection.socket != null) {
      return call.connection.socket.getInetAddress();
    }
    return null;
  }

  /**
   * A convenience method to bind to a given address and report
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  public static void bind(ServerSocket socket, InetSocketAddress address,
                          int backlog) throws IOException {
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      BindException bindException =
        new BindException("Problem binding to " + address + " : " +
            e.getMessage());
      bindException.initCause(e);
      throw bindException;
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: " +
                                       address.getHostName());
      }
      throw e;
    }
  }

  public RpcScheduler getScheduler() {
    return scheduler;
  }
}
