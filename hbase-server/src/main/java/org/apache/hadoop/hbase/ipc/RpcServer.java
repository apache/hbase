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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.client.VersionInfoUtil;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.io.BoundedByteBufferPool;
import org.apache.hadoop.hbase.io.ByteBufferInputStream;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.VersionInfo;
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
import org.apache.hadoop.hbase.util.Counter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
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
import org.apache.htrace.TraceInfo;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.BlockingService;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;

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
 * queue for Responder to pull from and return result to client.
 *
 * @see RpcClientImpl
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class RpcServer implements RpcServerInterface, ConfigurationObserver {
  // LOG is being used in CallRunner and the log level is being changed in tests
  public static final Log LOG = LogFactory.getLog(RpcServer.class);
  private static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION
      = new CallQueueTooBigException();

  private final boolean authorize;
  private boolean isSecurityEnabled;

  public static final byte CURRENT_VERSION = 0;

  /**
   * Whether we allow a fallback to SIMPLE auth for insecure clients when security is enabled.
   */
  public static final String FALLBACK_TO_INSECURE_CLIENT_AUTH =
          "hbase.ipc.server.fallback-to-simple-auth-allowed";

  /**
   * How many calls/handler are allowed in the queue.
   */
  static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

  /**
   * The maximum size that we can hold in the RPC queue
   */
  private static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;

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

  protected final InetSocketAddress bindAddress;
  protected int port;                             // port we listen on
  protected InetSocketAddress address;            // inet address we listen on
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
   * {@link #start()}, all threads started will consult this flag on whether they should
   * keep going.  It is set to false when {@link #stop()} is called.
   */
  volatile boolean running = true;

  /**
   * This flag is set to true after all threads are up and 'running' and the server is then opened
   * for business by the call to {@link #start()}.
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

  static final String MAX_REQUEST_SIZE = "hbase.ipc.max.request.size";
  private static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  private static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";

  /** Default value for above params */
  private static final int DEFAULT_MAX_REQUEST_SIZE = DEFAULT_MAX_CALLQUEUE_SIZE / 4; // 256M
  private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final int maxRequestSize;
  private final int warnResponseTime;
  private final int warnResponseSize;
  private final Server server;
  private final List<BlockingServiceAndInterface> services;

  private final RpcScheduler scheduler;

  private UserProvider userProvider;

  private final BoundedByteBufferPool reservoir;

  private volatile boolean allowFallbackToSimpleAuth;

  /**
   * Datastructure that holds all necessary to a method invocation and then afterward, carries
   * the result.
   */
  @InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
  @InterfaceStability.Evolving
  public class Call implements RpcCallContext {
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
    protected Responder responder;

    protected long size;                          // size of current call
    protected boolean isError;
    protected TraceInfo tinfo;
    private ByteBuffer cellBlock = null;

    private User user;
    private InetAddress remoteAddress;
    private RpcCallback callback;

    private long responseCellSize = 0;
    private long responseBlockSize = 0;
    private boolean retryImmediatelySupported;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
        justification="Can't figure why this complaint is happening... see below")
    Call(int id, final BlockingService service, final MethodDescriptor md, RequestHeader header,
         Message param, CellScanner cellScanner, Connection connection, Responder responder,
         long size, TraceInfo tinfo, final InetAddress remoteAddress) {
      this.id = id;
      this.service = service;
      this.md = md;
      this.header = header;
      this.param = param;
      this.cellScanner = cellScanner;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
      this.responder = responder;
      this.isError = false;
      this.size = size;
      this.tinfo = tinfo;
      this.user = connection == null? null: connection.user; // FindBugs: NP_NULL_ON_SOME_PATH
      this.remoteAddress = remoteAddress;
      this.retryImmediatelySupported =
          connection == null? null: connection.retryImmediatelySupported;
    }

    /**
     * Call is done. Execution happened and we returned results to client. It is now safe to
     * cleanup.
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
        justification="Presume the lock on processing request held by caller is protection enough")
    void done() {
      if (this.cellBlock != null && reservoir != null) {
        // Return buffer to reservoir now we are done with it.
        reservoir.putBuffer(this.cellBlock);
        this.cellBlock = null;
      }
      this.connection.decRpcCount();  // Say that we're done with this call.
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

    public boolean hasPriority() {
      return this.header.hasPriority();
    }

    public int getPriority() {
      return this.header.getPriority();
    }

    /*
     * Short string representation without param info because param itself could be huge depends on
     * the payload of a command
     */
    String toShortString() {
      String serviceName = this.connection.service != null ?
          this.connection.service.getDescriptorForType().getName() : "null";
      return "callId: " + this.id + " service: " + serviceName +
          " methodName: " + ((this.md != null) ? this.md.getName() : "n/a") +
          " size: " + StringUtils.TraditionalBinaryPrefix.long2String(this.size, "", 1) +
          " connection: " + connection.toString();
    }

    String toTraceString() {
      String serviceName = this.connection.service != null ?
                           this.connection.service.getDescriptorForType().getName() : "";
      String methodName = (this.md != null) ? this.md.getName() : "";
      return serviceName + "." + methodName;
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

        byte[] b = createHeaderAndMessageBytes(result, header);

        bc = new BufferChain(ByteBuffer.wrap(b), this.cellBlock);

        if (connection.useWrap) {
          bc = wrapWithSasl(bc);
        }
      } catch (IOException e) {
        LOG.warn("Exception while creating response " + e);
      }
      this.response = bc;
      // Once a response message is created and set to this.response, this Call can be treated as
      // done. The Responder thread will do the n/w write of this message back to client.
      if (this.callback != null) {
        try {
          this.callback.run();
        } catch (Exception e) {
          // Don't allow any exception here to kill this handler thread.
          LOG.warn("Exception while running the Rpc Callback.", e);
        }
      }
    }

    private byte[] createHeaderAndMessageBytes(Message result, Message header)
        throws IOException {
      // Organize the response as a set of bytebuffers rather than collect it all together inside
      // one big byte array; save on allocations.
      int headerSerializedSize = 0, resultSerializedSize = 0, headerVintSize = 0,
          resultVintSize = 0;
      if (header != null) {
        headerSerializedSize = header.getSerializedSize();
        headerVintSize = CodedOutputStream.computeRawVarint32Size(headerSerializedSize);
      }
      if (result != null) {
        resultSerializedSize = result.getSerializedSize();
        resultVintSize = CodedOutputStream.computeRawVarint32Size(resultSerializedSize);
      }
      // calculate the total size
      int totalSize = headerSerializedSize + headerVintSize
          + (resultSerializedSize + resultVintSize)
          + (this.cellBlock == null ? 0 : this.cellBlock.limit());
      // The byte[] should also hold the totalSize of the header, message and the cellblock
      byte[] b = new byte[headerSerializedSize + headerVintSize + resultSerializedSize
          + resultVintSize + Bytes.SIZEOF_INT];
      // The RpcClient expects the int to be in a format that code be decoded by
      // the DataInputStream#readInt(). Hence going with the Bytes.toBytes(int)
      // form of writing int.
      Bytes.putInt(b, 0, totalSize);
      CodedOutputStream cos = CodedOutputStream.newInstance(b, Bytes.SIZEOF_INT,
          b.length - Bytes.SIZEOF_INT);
      if (header != null) {
        cos.writeMessageNoTag(header);
      }
      if (result != null) {
        cos.writeMessageNoTag(result);
      }
      cos.flush();
      cos.checkNoSpaceLeft();
      return b;
    }

    private BufferChain wrapWithSasl(BufferChain bc)
        throws IOException {
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
      if (LOG.isTraceEnabled()) {
        LOG.trace("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      }

      ByteBuffer bbTokenLength = ByteBuffer.wrap(Bytes.toBytes(token.length));
      ByteBuffer bbTokenBytes = ByteBuffer.wrap(token);
      return new BufferChain(bbTokenLength, bbTokenBytes);
    }

    @Override
    public boolean isClientCellBlockSupported() {
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

    public synchronized void sendResponseIfReady() throws IOException {
      this.responder.doRespond(this);
    }

    public UserGroupInformation getRemoteUser() {
      return connection.ugi;
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

    @Override
    public VersionInfo getClientVersionInfo() {
      return connection.getVersionInfo();
    }

    @Override
    public synchronized void setCallBack(RpcCallback callback) {
      this.callback = callback;
    }

    @Override
    public boolean isRetryImmediatelySupported() {
      return retryImmediatelySupported;
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
    private int backlogLength;

    private ExecutorService readPool;

    public Listener(final String name) throws IOException {
      super(name);
      backlogLength = conf.getInt("hbase.ipc.server.listen.queue.size", 128);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the binding addrees (can be different from the default interface)
      bind(acceptChannel.socket(), bindAddress, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      address = (InetSocketAddress)acceptChannel.socket().getLocalSocketAddress();
      // create a selector;
      selector= Selector.open();

      readers = new Reader[readThreads];
      readPool = Executors.newFixedThreadPool(readThreads,
        new ThreadFactoryBuilder().setNameFormat(
          "RpcServer.reader=%d,bindAddress=" + bindAddress.getHostName() +
          ",port=" + port).setDaemon(true)
        .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
      for (int i = 0; i < readThreads; ++i) {
        Reader reader = new Reader();
        readers[i] = reader;
        readPool.execute(reader);
      }
      LOG.info(getName() + ": started " + readThreads + " reader(s) listening on port=" + port);

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
      @Override
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
          try {
            readSelector.select();
            while (adding) {
              this.wait(1000);
            }

            Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              iter.remove();
              if (key.isValid()) {
                if (key.isReadable()) {
                  doRead(key);
                }
              }
            }
          } catch (InterruptedException e) {
            LOG.debug("Interrupted while sleeping");
            return;
          } catch (IOException ex) {
            LOG.info(getName() + ": IOException in Reader", ex);
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
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IS2_INCONSISTENT_SYNC",
      justification="selector access is not synchronized; seems fine but concerned changing " +
        "it will have per impact")
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
              if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
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
            try {
              Thread.sleep(60000);
            } catch (InterruptedException ex) {
              LOG.debug("Interrupted while sleeping");
              return;
            }
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
        } catch (IOException ignored) {
          if (LOG.isTraceEnabled()) LOG.trace("ignored", ignored);
        }

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
      return address;
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
      int count;
      Connection c = (Connection) key.attachment();
      if (c == null) {
        return;
      }
      c.setLastContact(System.currentTimeMillis());
      try {
        count = c.readAndProcess();

        if (count > 0) {
          c.setLastContact(System.currentTimeMillis());
        }

      } catch (InterruptedException ieo) {
        throw ieo;
      } catch (Exception e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": Caught exception while reading:", e);
        }
        count = -1; //so that the (count < 0) block is executed
      }
      if (count < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": DISCONNECTING client " + c.toString() +
              " because read count=" + count +
              ". Number of active connections: " + numConnections);
        }
        closeConnection(c);
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
    private final Set<Connection> writingCons =
        Collections.newSetFromMap(new ConcurrentHashMap<Connection, Boolean>());

    Responder() throws IOException {
      this.setName("RpcServer.responder");
      this.setDaemon(true);
      this.setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER);
      writeSelector = Selector.open(); // create a selector
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

    /**
     * Take the list of the connections that want to write, and register them
     * in the selector.
     */
    private void registerWrites() {
      Iterator<Connection> it = writingCons.iterator();
      while (it.hasNext()) {
        Connection c = it.next();
        it.remove();
        SelectionKey sk = c.channel.keyFor(writeSelector);
        try {
          if (sk == null) {
            try {
              c.channel.register(writeSelector, SelectionKey.OP_WRITE, c);
            } catch (ClosedChannelException e) {
              // ignore: the client went away.
              if (LOG.isTraceEnabled()) LOG.trace("ignored", e);
            }
          } else {
            sk.interestOps(SelectionKey.OP_WRITE);
          }
        } catch (CancelledKeyException e) {
          // ignore: the client went away.
          if (LOG.isTraceEnabled()) LOG.trace("ignored", e);
        }
      }
    }

    /**
     * Add a connection to the list that want to write,
     */
    public void registerForWrite(Connection c) {
      if (writingCons.add(c)) {
        writeSelector.wakeup();
      }
    }

    private void doRunLoop() {
      long lastPurgeTime = 0;   // last check for old calls.
      while (running) {
        try {
          registerWrites();
          int keyCt = writeSelector.select(purgeTimeout);
          if (keyCt == 0) {
            continue;
          }

          Set<SelectionKey> keys = writeSelector.selectedKeys();
          Iterator<SelectionKey> iter = keys.iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.debug(getName() + ": asyncWrite", e);
            }
          }

          lastPurgeTime = purge(lastPurgeTime);

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
            try {
              Thread.sleep(60000);
            } catch (InterruptedException ex) {
              LOG.debug("Interrupted while sleeping");
              return;
            }
          }
        } catch (Exception e) {
          LOG.warn(getName() + ": exception in Responder " +
              StringUtils.stringifyException(e), e);
        }
      }
      LOG.info(getName() + ": stopped");
    }

    /**
     * If there were some calls that have not been sent out for a
     * long time, we close the connection.
     * @return the time of the purge.
     */
    private long purge(long lastPurgeTime) {
      long now = System.currentTimeMillis();
      if (now < lastPurgeTime + purgeTimeout) {
        return lastPurgeTime;
      }

      ArrayList<Connection> conWithOldCalls = new ArrayList<Connection>();
      // get the list of channels from list of keys.
      synchronized (writeSelector.keys()) {
        for (SelectionKey key : writeSelector.keys()) {
          Connection connection = (Connection) key.attachment();
          if (connection == null) {
            throw new IllegalStateException("Coding error: SelectionKey key without attachment.");
          }
          Call call = connection.responseQueue.peekFirst();
          if (call != null && now > call.timestamp + purgeTimeout) {
            conWithOldCalls.add(call.connection);
          }
        }
      }

      // Seems safer to close the connection outside of the synchronized loop...
      for (Connection connection : conWithOldCalls) {
        closeConnection(connection);
      }

      return now;
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Connection connection = (Connection) key.attachment();
      if (connection == null) {
        throw new IOException("doAsyncWrite: no connection");
      }
      if (key.channel() != connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      if (processAllResponses(connection)) {
        try {
          // We wrote everything, so we don't need to be told when the socket is ready for
          //  write anymore.
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

    /**
     * Process the response for this call. You need to have the lock on
     * {@link org.apache.hadoop.hbase.ipc.RpcServer.Connection#responseWriteLock}
     *
     * @param call the call
     * @return true if we proceed the call fully, false otherwise.
     * @throws IOException
     */
    private boolean processResponse(final Call call) throws IOException {
      boolean error = true;
      try {
        // Send as much data as we can in the non-blocking fashion
        long numBytes = channelWrite(call.connection.channel, call.response);
        if (numBytes < 0) {
          throw new HBaseIOException("Error writing on the socket " +
            "for the call:" + call.toShortString());
        }
        error = false;
      } finally {
        if (error) {
          LOG.debug(getName() + call.toShortString() + ": output error -- closing");
          closeConnection(call.connection);
        }
      }

      if (!call.response.hasRemaining()) {
        call.done();
        return true;
      } else {
        return false; // Socket can't take more, we will have to come back.
      }
    }

    /**
     * Process all the responses for this connection
     *
     * @return true if all the calls were processed or that someone else is doing it.
     * false if there * is still some work to do. In this case, we expect the caller to
     * delay us.
     * @throws IOException
     */
    private boolean processAllResponses(final Connection connection) throws IOException {
      // We want only one writer on the channel for a connection at a time.
      connection.responseWriteLock.lock();
      try {
        for (int i = 0; i < 20; i++) {
          // protection if some handlers manage to need all the responder
          Call call = connection.responseQueue.pollFirst();
          if (call == null) {
            return true;
          }
          if (!processResponse(call)) {
            connection.responseQueue.addFirst(call);
            return false;
          }
        }
      } finally {
        connection.responseWriteLock.unlock();
      }

      return connection.responseQueue.isEmpty();
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      boolean added = false;

      // If there is already a write in progress, we don't wait. This allows to free the handlers
      //  immediately for other tasks.
      if (call.connection.responseQueue.isEmpty() && call.connection.responseWriteLock.tryLock()) {
        try {
          if (call.connection.responseQueue.isEmpty()) {
            // If we're alone, we can try to do a direct call to the socket. It's
            //  an optimisation to save on context switches and data transfer between cores..
            if (processResponse(call)) {
              return; // we're done.
            }
            // Too big to fit, putting ahead.
            call.connection.responseQueue.addFirst(call);
            added = true; // We will register to the selector later, outside of the lock.
          }
        } finally {
          call.connection.responseWriteLock.unlock();
        }
      }

      if (!added) {
        call.connection.responseQueue.addLast(call);
      }
      call.responder.registerForWrite(call.connection);

      // set the serve time when the response has to be sent later
      call.timestamp = System.currentTimeMillis();
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
    protected final ConcurrentLinkedDeque<Call> responseQueue = new ConcurrentLinkedDeque<Call>();
    private final Lock responseWriteLock = new ReentrantLock();
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
    private static final int AUTHORIZATION_FAILED_CALLID = -1;
    private final Call authFailedCall = new Call(AUTHORIZATION_FAILED_CALLID, null, null, null,
        null, null, this, null, 0, null, null);
    private ByteArrayOutputStream authFailedResponse =
        new ByteArrayOutputStream();
    // Fake 'call' for SASL context setup
    private static final int SASL_CALLID = -33;
    private final Call saslCall = new Call(SASL_CALLID, null, null, null, null, null, this, null,
        0, null, null);

    // was authentication allowed with a fallback to simple auth
    private boolean authenticatedWithFallback;

    private boolean retryImmediatelySupported = false;

    public UserGroupInformation attemptingUser = null; // user name before auth
    protected User user = null;
    protected UserGroupInformation ugi = null;

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

    public VersionInfo getVersionInfo() {
      if (connectionHeader.hasVersionInfo()) {
        return connectionHeader.getVersionInfo();
      }
      return null;
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
      UserGroupInformation authorizedUgi;
      if (authMethod == AuthMethod.DIGEST) {
        TokenIdentifier tokenId = HBaseSaslRpcServer.getIdentifier(authorizedId,
            secretManager);
        authorizedUgi = tokenId.getUser();
        if (authorizedUgi == null) {
          throw new AccessDeniedException(
              "Can't retrieve username from tokenIdentifier.");
        }
        authorizedUgi.addTokenIdentifier(tokenId);
      } else {
        authorizedUgi = UserGroupInformation.createRemoteUser(authorizedId);
      }
      authorizedUgi.setAuthenticationMethod(authMethod.authenticationMethod.getAuthMethod());
      return authorizedUgi;
    }

    private void saslReadAndProcess(ByteBuffer saslToken) throws IOException,
        InterruptedException {
      if (saslContextEstablished) {
        if (LOG.isTraceEnabled())
          LOG.trace("Have read input token of size " + saslToken.limit()
              + " for processing by saslServer.unwrap()");

        if (!useWrap) {
          processOneRpc(saslToken);
        } else {
          byte[] b = saslToken.array();
          byte [] plaintextData = saslServer.unwrap(b, saslToken.position(), saslToken.limit());
          processUnwrappedData(plaintextData);
        }
      } else {
        byte[] replyToken;
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
                  HBaseSaslRpcServer.getSaslProps(), new SaslDigestCallbackHandler(
                      secretManager, this));
              break;
            default:
              UserGroupInformation current = UserGroupInformation.getCurrentUser();
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
                      HBaseSaslRpcServer.getSaslProps(), new SaslGssCallbackHandler());
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
            LOG.debug("Have read input token of size " + saslToken.limit()
                + " for processing by saslServer.evaluateResponse()");
          }
          replyToken = saslServer.evaluateResponse(saslToken.array());
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
          ugi = getAuthorizedUgi(saslServer.getAuthorizationID());
          if (LOG.isDebugEnabled()) {
            LOG.debug("SASL server context established. Authenticated client: "
              + ugi + ". Negotiated QoP is "
              + saslServer.getNegotiatedProperty(Sasl.QOP));
          }
          metrics.authenticationSuccess();
          AUDITLOG.info(AUTH_SUCCESSFUL_FOR + ugi);
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

    private int readPreamble() throws IOException {
      int count;
      // Check for 'HBas' magic.
      this.dataLengthBuffer.flip();
      if (!Arrays.equals(HConstants.RPC_HEADER, dataLengthBuffer.array())) {
        return doBadPreambleHandling("Expected HEADER=" +
            Bytes.toStringBinary(HConstants.RPC_HEADER) +
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
        if (allowFallbackToSimpleAuth) {
          metrics.authenticationFallback();
          authenticatedWithFallback = true;
        } else {
          AccessDeniedException ae = new AccessDeniedException("Authentication is required");
          setupResponse(authFailedResponse, authFailedCall, ae, ae.getMessage());
          responder.doRespond(authFailedCall);
          throw ae;
        }
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

      dataLengthBuffer.clear();
      connectionPreambleRead = true;
      return count;
    }

    private int read4Bytes() throws IOException {
      if (this.dataLengthBuffer.remaining() > 0) {
        return channelRead(channel, this.dataLengthBuffer);
      } else {
        return 0;
      }
    }


    /**
     * Read off the wire. If there is not enough data to read, update the connection state with
     *  what we have and returns.
     * @return Returns -1 if failure (and caller will close connection), else zero or more.
     * @throws IOException
     * @throws InterruptedException
     */
    public int readAndProcess() throws IOException, InterruptedException {
      // Try and read in an int.  If new connection, the int will hold the 'HBas' HEADER.  If it
      // does, read in the rest of the connection preamble, the version and the auth method.
      // Else it will be length of the data to read (or -1 if a ping).  We catch the integer
      // length into the 4-byte this.dataLengthBuffer.
      int count = read4Bytes();
      if (count < 0 || dataLengthBuffer.remaining() > 0) {
        return count;
      }

      // If we have not read the connection setup preamble, look to see if that is on the wire.
      if (!connectionPreambleRead) {
        count = readPreamble();
        if (!connectionPreambleRead) {
          return count;
        }

        count = read4Bytes();
        if (count < 0 || dataLengthBuffer.remaining() > 0) {
          return count;
        }
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
        if (dataLength < 0) { // A data length of zero is legal.
          throw new DoNotRetryIOException("Unexpected data length "
              + dataLength + "!! from " + getHostAddress());
        }

        if (dataLength > maxRequestSize) {
          throw new DoNotRetryIOException("RPC data length of " + dataLength + " received from "
              + getHostAddress() + " is greater than max allowed " + maxRequestSize + ". Set \""
              + MAX_REQUEST_SIZE + "\" on server to override this limit (not recommended)");
        }

        data = ByteBuffer.allocate(dataLength);

        // Increment the rpc count. This counter will be decreased when we write
        //  the response.  If we want the connection to be detected as idle properly, we
        //  need to keep the inc / dec correct.
        incRpcCount();
      }

      count = channelRead(channel, data);

      if (count >= 0 && data.remaining() == 0) { // count==0 if dataLength == 0
        process();
      }

      return count;
    }

    /**
     * Process the data buffer and clean the connection state for the next call.
     */
    private void process() throws IOException, InterruptedException {
      data.flip();
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

      } finally {
        dataLengthBuffer.clear(); // Clean for the next call
        data = null; // For the GC
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
    private void processConnectionHeader(ByteBuffer buf) throws IOException {
      this.connectionHeader = ConnectionHeader.parseFrom(
        new ByteBufferInputStream(buf));
      String serviceName = connectionHeader.getServiceName();
      if (serviceName == null) throw new EmptyServiceNameException();
      this.service = getService(services, serviceName);
      if (this.service == null) throw new UnknownServiceException(serviceName);
      setupCellBlockCodecs(this.connectionHeader);
      UserGroupInformation protocolUser = createUser(connectionHeader);
      if (!useSasl) {
        ugi = protocolUser;
        if (ugi != null) {
          ugi.setAuthenticationMethod(AuthMethod.SIMPLE.authenticationMethod);
        }
        // audit logging for SASL authenticated users happens in saslReadAndProcess()
        if (authenticatedWithFallback) {
          LOG.warn("Allowed fallback to SIMPLE auth for " + ugi
              + " connecting from " + getHostAddress());
        }
        AUDITLOG.info(AUTH_SUCCESSFUL_FOR + ugi);
      } else {
        // user is authenticated
        ugi.setAuthenticationMethod(authMethod.authenticationMethod);
        //Now we check if this is a proxy user case. If the protocol user is
        //different from the 'user', it is a proxy user scenario. However,
        //this is not allowed if user authenticated with DIGEST.
        if ((protocolUser != null)
            && (!protocolUser.getUserName().equals(ugi.getUserName()))) {
          if (authMethod == AuthMethod.DIGEST) {
            // Not allowed to doAs if token authentication is used
            throw new AccessDeniedException("Authenticated user (" + ugi
                + ") doesn't match what the client claims to be ("
                + protocolUser + ")");
          } else {
            // Effective user can be different from authenticated user
            // for simple auth or kerberos auth
            // The user is the real user. Now we create a proxy user
            UserGroupInformation realUser = ugi;
            ugi = UserGroupInformation.createProxyUser(protocolUser
                .getUserName(), realUser);
            // Now the user is a proxy user, set Authentication method Proxy.
            ugi.setAuthenticationMethod(AuthenticationMethod.PROXY);
          }
        }
      }
      if (connectionHeader.hasVersionInfo()) {
        // see if this connection will support RetryImmediatelyException
        retryImmediatelySupported = VersionInfoUtil.hasMinimumVersion(getVersionInfo(), 1, 2);

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
        int count;
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
          processOneRpc(unwrappedData);
          unwrappedData = null;
        }
      }
    }


    private void processOneRpc(ByteBuffer buf) throws IOException, InterruptedException {
      if (connectionHeaderRead) {
        processRequest(buf);
      } else {
        processConnectionHeader(buf);
        this.connectionHeaderRead = true;
        if (!authorizeConnection()) {
          // Throw FatalConnectionException wrapping ACE so client does right thing and closes
          // down the connection instead of trying to read non-existent retun.
          throw new AccessDeniedException("Connection from " + this + " for service " +
            connectionHeader.getServiceName() + " is unauthorized for user: " + ugi);
        }
        this.user = userProvider.create(this.ugi);
      }
    }

    /**
     * @param buf Has the request header and the request param and optionally encoded data buffer
     * all in this one array.
     * @throws IOException
     * @throws InterruptedException
     */
    protected void processRequest(ByteBuffer buf) throws IOException, InterruptedException {
      long totalRequestSize = buf.limit();
      int offset = 0;
      // Here we read in the header.  We avoid having pb
      // do its default 4k allocation for CodedInputStream.  We force it to use backing array.
      CodedInputStream cis = CodedInputStream.newInstance(buf.array(), offset, buf.limit());
      int headerSize = cis.readRawVarint32();
      offset = cis.getTotalBytesRead();
      Message.Builder builder = RequestHeader.newBuilder();
      ProtobufUtil.mergeFrom(builder, cis, headerSize);
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
            "Call queue is full on " + server.getServerName() +
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
          cis.resetSizeCounter();
          int paramSize = cis.readRawVarint32();
          offset += cis.getTotalBytesRead();
          if (builder != null) {
            ProtobufUtil.mergeFrom(builder, cis, paramSize);
            param = builder.build();
          }
          offset += paramSize;
        }
        if (header.hasCellBlockMeta()) {
          buf.position(offset);
          cellScanner = ipcUtil.createCellScannerReusingBuffers(this.codec, this.compressionCodec, buf);
        }
      } catch (Throwable t) {
        InetSocketAddress address = getListenerAddress();
        String msg = (address != null ? address : "(channel closed)") +
            " is unable to read call parameter from client " + getHostAddress();
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
              totalRequestSize, traceInfo, this.addr);

      if (!scheduler.dispatch(new CallRunner(RpcServer.this, call))) {
        callQueueSize.add(-1 * call.getSize());

        ByteArrayOutputStream responseBuffer = new ByteArrayOutputStream();
        metrics.exception(CALL_QUEUE_TOO_BIG_EXCEPTION);
        setupResponse(responseBuffer, call, CALL_QUEUE_TOO_BIG_EXCEPTION,
            "Call queue is full on " + server.getServerName() +
                ", too many items queued ?");
        responder.doRespond(call);
      }
    }

    private boolean authorizeConnection() throws IOException {
      try {
        // If auth method is DIGEST, the token was obtained by the
        // real user for the effective user, therefore not required to
        // authorize real user. doAs is allowed only for simple or kerberos
        // authentication
        if (ugi != null && ugi.getRealUser() != null
            && (authMethod != AuthMethod.DIGEST)) {
          ProxyUsers.authorize(ugi, this.getHostAddress(), conf);
        }
        authorize(ugi, connectionHeader, getHostInetAddress());
        metrics.authorizationSuccess();
      } catch (AuthorizationException ae) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connection authorization failed: " + ae.getMessage(), ae);
        }
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
      try {socket.shutdownOutput();} catch(Exception ignored) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Ignored exception", ignored);
        }
      }
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception ignored) {}
      }
      try {
        socket.close();
      } catch(Exception ignored) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Ignored exception", ignored);
        }
      }
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
   * @param server hosting instance of {@link Server}. We will do authentications if an
   * instance else pass null for no authentication check.
   * @param name Used keying this rpc servers' metrics and for naming the Listener thread.
   * @param services A list of services.
   * @param bindAddress Where to listen
   * @param conf
   * @param scheduler
   */
  public RpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      RpcScheduler scheduler)
      throws IOException {
    if (conf.getBoolean("hbase.ipc.server.reservoir.enabled", true)) {
      this.reservoir = new BoundedByteBufferPool(
          conf.getInt("hbase.ipc.server.reservoir.max.buffer.size", 1024 * 1024),
          conf.getInt("hbase.ipc.server.reservoir.initial.buffer.size", 16 * 1024),
          // Make the max twice the number of handlers to be safe.
          conf.getInt("hbase.ipc.server.reservoir.initial.max",
              conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
                  HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT) * 2));
    } else {
      reservoir = null;
    }
    this.server = server;
    this.services = services;
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.socketSendBufferSize = 0;
    this.maxQueueSize =
      this.conf.getInt("hbase.ipc.server.max.callqueue.size", DEFAULT_MAX_CALLQUEUE_SIZE);
    this.readThreads = conf.getInt("hbase.ipc.server.read.threadpool.size", 10);
    this.maxIdleTime = 2 * conf.getInt("hbase.ipc.client.connection.maxidletime", 1000);
    this.maxConnectionsToNuke = conf.getInt("hbase.ipc.client.kill.max", 10);
    this.thresholdIdleConnections = conf.getInt("hbase.ipc.client.idlethreshold", 4000);
    this.purgeTimeout = conf.getLong("hbase.ipc.client.call.purge.timeout",
      2 * HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME, DEFAULT_WARN_RESPONSE_TIME);
    this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE, DEFAULT_WARN_RESPONSE_SIZE);

    this.maxRequestSize = conf.getInt(MAX_REQUEST_SIZE, DEFAULT_MAX_REQUEST_SIZE);

    // Start the listener here and let it bind to the port
    listener = new Listener(name);
    this.port = listener.getAddress().getPort();

    this.metrics = new MetricsHBaseServer(name, new MetricsHBaseServerWrapperImpl(this));
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.server.tcpnodelay", true);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.server.tcpkeepalive", true);

    this.ipcUtil = new IPCUtil(conf);


    // Create the responder here
    responder = new Responder();
    this.authorize = conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    this.userProvider = UserProvider.instantiate(conf);
    this.isSecurityEnabled = userProvider.isHBaseSecurityEnabled();
    if (isSecurityEnabled) {
      HBaseSaslRpcServer.init(conf);
    }
    initReconfigurable(conf);

    this.scheduler = scheduler;
    this.scheduler.init(new RpcSchedulerContext(this));
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    initReconfigurable(newConf);
    if (scheduler instanceof ConfigurationObserver) {
      ((ConfigurationObserver)scheduler).onConfigurationChange(newConf);
    }
  }

  private void initReconfigurable(Configuration confToLoad) {
    this.allowFallbackToSimpleAuth = confToLoad.getBoolean(FALLBACK_TO_INSECURE_CLIENT_AUTH, false);
    if (isSecurityEnabled && allowFallbackToSimpleAuth) {
      LOG.warn("********* WARNING! *********");
      LOG.warn("This server is configured to allow connections from INSECURE clients");
      LOG.warn("(" + FALLBACK_TO_INSECURE_CLIENT_AUTH + " = true).");
      LOG.warn("While this option is enabled, client identities cannot be secured, and user");
      LOG.warn("impersonation is possible!");
      LOG.warn("For secure operation, please disable SIMPLE authentication as soon as possible,");
      LOG.warn("by setting " + FALLBACK_TO_INSECURE_CLIENT_AUTH + " = false in hbase-site.xml");
      LOG.warn("****************************");
    }
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

  @Override
  public boolean isStarted() {
    return this.started;
  }

  /** Starts the service.  Must be called before any calls will be handled. */
  @Override
  public synchronized void start() {
    if (started) return;
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
    started = true;
  }

  @Override
  public synchronized void refreshAuthManager(PolicyProvider pp) {
    // Ignore warnings that this should be accessed in a static way instead of via an instance;
    // it'll break if you go via static route.
    this.authManager.refresh(this.conf, pp);
  }

  private AuthenticationTokenSecretManager createSecretManager() {
    if (!isSecurityEnabled) return null;
    if (server == null) return null;
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
  @Override
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
      long requestSize = param.getSerializedSize();
      long responseSize = result.getSerializedSize();
      metrics.dequeuedCall(qTime);
      metrics.processedCall(processingTime);
      metrics.totalCall(totalTime);
      metrics.receivedRequest(requestSize);
      metrics.sentResponse(responseSize);
      // log any RPC responses that are slower than the configured warn
      // response time or larger than configured warning size
      boolean tooSlow = (processingTime > warnResponseTime && warnResponseTime > -1);
      boolean tooLarge = (responseSize > warnResponseSize && warnResponseSize > -1);
      if (tooSlow || tooLarge) {
        // when tagging, we let TooLarge trump TooSmall to keep output simple
        // note that large responses will often also be slow.
        logResponse(new Object[]{param},
            md.getName(), md.getName() + "(" + param.getClass().getName() + ")",
            (tooLarge ? "TooLarge" : "TooSlow"),
            status.getClient(), startTime, processingTime, qTime,
            responseSize);
      }
      return new Pair<Message, CellScanner>(result, controller.cellScanner());
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
    responseInfo.put("class", server == null? "": server.getClass().getSimpleName());
    responseInfo.put("method", methodName);
    if (params.length == 2 && server instanceof HRegionServer &&
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
    } else if (params.length == 1 && server instanceof HRegionServer &&
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
   * Return the socket (ip+port) on which the RPC server is listening to. May return null if
   * the listener channel is closed.
   * @return the socket (ip+port) on which the RPC server is listening to, or null if this
   * information cannot be determined
   */
  @Override
  public synchronized InetSocketAddress getListenerAddress() {
    if (listener == null) {
      return null;
    }
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
  @Override
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
   * @throws org.apache.hadoop.security.authorize.AuthorizationException
   *         when the client isn't authorized to talk the protocol
   */
  public synchronized void authorize(UserGroupInformation user, ConnectionHeader connection,
      InetAddress addr)
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
   * and {@link #channelWrite(GatheringByteChannel, BufferChain)}. Only
   * one of readCh or writeCh should be non-null.
   *
   * @param readCh read channel
   * @param writeCh write channel
   * @param buf buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
   * @see #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
   * @see #channelWrite(GatheringByteChannel, BufferChain)
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
   * @return An RpcCallContext backed by the currently ongoing call (gotten from a thread local)
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

  static MonitoredRPCHandler getStatus() {
    // It is ugly the way we park status up in RpcServer.  Let it be for now.  TODO.
    MonitoredRPCHandler status = RpcServer.MONITORED_RPC.get();
    if (status != null) {
      return status;
    }
    status = TaskMonitor.get().createRPCStatus(Thread.currentThread().getName());
    status.pause("Waiting for a call");
    RpcServer.MONITORED_RPC.set(status);
    return status;
  }

  /** Returns the remote side ip address when invoked inside an RPC
   *  Returns null incase of an error.
   *  @return InetAddress
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null && call.connection != null && call.connection.socket != null) {
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

  @Override
  public RpcScheduler getScheduler() {
    return scheduler;
  }
}
