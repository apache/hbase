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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.LongAdder;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslServer;

import org.apache.commons.crypto.cipher.CryptoCipherFactory;
import org.apache.commons.crypto.random.CryptoRandom;
import org.apache.commons.crypto.random.CryptoRandomFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RequestTooBigException;
import org.apache.hadoop.hbase.io.ByteBufferListOutputStream;
import org.apache.hadoop.hbase.io.ByteBufferPool;
import org.apache.hadoop.hbase.io.crypto.aes.CryptoAES;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.MultiByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.BlockingService;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteInput;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.CodedOutputStream;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.Message;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.TextFormat;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.VersionInfo;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.TraceInfo;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.annotations.VisibleForTesting;

/**
 * An RPC server that hosts protobuf described Services.
 *
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public abstract class RpcServer implements RpcServerInterface,
    ConfigurationObserver {
  // LOG is being used in CallRunner and the log level is being changed in tests
  public static final Log LOG = LogFactory.getLog(RpcServer.class);
  protected static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION
      = new CallQueueTooBigException();

  private final boolean authorize;
  protected boolean isSecurityEnabled;

  public static final byte CURRENT_VERSION = 0;

  /**
   * Whether we allow a fallback to SIMPLE auth for insecure clients when security is enabled.
   */
  public static final String FALLBACK_TO_INSECURE_CLIENT_AUTH =
          "hbase.ipc.server.fallback-to-simple-auth-allowed";

  /**
   * How many calls/handler are allowed in the queue.
   */
  protected static final int DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER = 10;

  protected final CellBlockBuilder cellBlockBuilder;

  protected static final String AUTH_FAILED_FOR = "Auth failed for ";
  protected static final String AUTH_SUCCESSFUL_FOR = "Auth successful for ";
  protected static final Log AUDITLOG = LogFactory.getLog("SecurityLogger."
      + Server.class.getName());
  protected SecretManager<TokenIdentifier> secretManager;
  protected ServiceAuthorizationManager authManager;

  /** This is set to Call object before Handler invokes an RPC and ybdie
   * after the call returns.
   */
  protected static final ThreadLocal<RpcCall> CurCall =
      new ThreadLocal<RpcCall>();

  /** Keeps MonitoredRPCHandler per handler thread. */
  protected static final ThreadLocal<MonitoredRPCHandler> MONITORED_RPC
      = new ThreadLocal<MonitoredRPCHandler>();

  protected final InetSocketAddress bindAddress;

  protected MetricsHBaseServer metrics;

  protected final Configuration conf;

  /**
   * Maximum size in bytes of the currently queued and running Calls. If a new Call puts us over
   * this size, then we will reject the call (after parsing it though). It will go back to the
   * client and client will retry. Set this size with "hbase.ipc.server.max.callqueue.size". The
   * call queue size gets incremented after we parse a call and before we add it to the queue of
   * calls for the scheduler to use. It get decremented after we have 'run' the Call. The current
   * size is kept in {@link #callQueueSizeInBytes}.
   * @see #callQueueSizeInBytes
   * @see #DEFAULT_MAX_CALLQUEUE_SIZE
   */
  protected final long maxQueueSizeInBytes;
  protected static final int DEFAULT_MAX_CALLQUEUE_SIZE = 1024 * 1024 * 1024;

  /**
   * This is a running count of the size in bytes of all outstanding calls whether currently
   * executing or queued waiting to be run.
   */
  protected final LongAdder callQueueSizeInBytes = new LongAdder();

  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives

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

  protected AuthenticationTokenSecretManager authTokenSecretMgr = null;

  protected HBaseRPCErrorHandler errorHandler = null;

  protected static final String MAX_REQUEST_SIZE = "hbase.ipc.max.request.size";
  protected static final RequestTooBigException REQUEST_TOO_BIG_EXCEPTION =
      new RequestTooBigException();

  protected static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  protected static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";

  /**
   * Minimum allowable timeout (in milliseconds) in rpc request's header. This
   * configuration exists to prevent the rpc service regarding this request as timeout immediately.
   */
  protected static final String MIN_CLIENT_REQUEST_TIMEOUT = "hbase.ipc.min.client.request.timeout";
  protected static final int DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT = 20;

  /** Default value for above params */
  protected static final int DEFAULT_MAX_REQUEST_SIZE = DEFAULT_MAX_CALLQUEUE_SIZE / 4; // 256M
  protected static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  protected static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  protected static final ObjectMapper MAPPER = new ObjectMapper();

  protected final int maxRequestSize;
  protected final int warnResponseTime;
  protected final int warnResponseSize;

  protected final int minClientRequestTimeout;

  protected final Server server;
  protected final List<BlockingServiceAndInterface> services;

  protected final RpcScheduler scheduler;

  protected UserProvider userProvider;

  protected final ByteBufferPool reservoir;
  // The requests and response will use buffers from ByteBufferPool, when the size of the
  // request/response is at least this size.
  // We make this to be 1/6th of the pool buffer size.
  protected final int minSizeForReservoirUse;

  protected volatile boolean allowFallbackToSimpleAuth;

  /**
   * Used to get details for scan with a scanner_id<br/>
   * TODO try to figure out a better way and remove reference from regionserver package later.
   */
  private RSRpcServices rsRpcServices;

  /**
   * Datastructure that holds all necessary to a method invocation and then afterward, carries
   * the result.
   */
  @InterfaceStability.Evolving
  @InterfaceAudience.Private
  public abstract class Call implements RpcCall {
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
    protected int timeout;
    protected long startTime;
    protected long deadline;// the deadline to handle this call, if exceed we can drop it.

    /**
     * Chain of buffers to send as response.
     */
    protected BufferChain response;

    protected long size;                          // size of current call
    protected boolean isError;
    protected TraceInfo tinfo;
    protected ByteBufferListOutputStream cellBlockStream = null;
    protected CallCleanup reqCleanup = null;

    protected User user;
    protected InetAddress remoteAddress;
    protected RpcCallback rpcCallback;

    private long responseCellSize = 0;
    private long responseBlockSize = 0;
    // cumulative size of serialized exceptions
    private long exceptionSize = 0;
    private boolean retryImmediatelySupported;

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH",
        justification="Can't figure why this complaint is happening... see below")
    Call(int id, final BlockingService service, final MethodDescriptor md,
        RequestHeader header, Message param, CellScanner cellScanner,
        Connection connection, long size, TraceInfo tinfo,
        final InetAddress remoteAddress, int timeout, CallCleanup reqCleanup) {
      this.id = id;
      this.service = service;
      this.md = md;
      this.header = header;
      this.param = param;
      this.cellScanner = cellScanner;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
      this.isError = false;
      this.size = size;
      this.tinfo = tinfo;
      this.user = connection == null? null: connection.user; // FindBugs: NP_NULL_ON_SOME_PATH
      this.remoteAddress = remoteAddress;
      this.retryImmediatelySupported =
          connection == null? null: connection.retryImmediatelySupported;
      this.timeout = timeout;
      this.deadline = this.timeout > 0 ? this.timestamp + this.timeout : Long.MAX_VALUE;
      this.reqCleanup = reqCleanup;
    }

    /**
     * Call is done. Execution happened and we returned results to client. It is
     * now safe to cleanup.
     */
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC",
        justification = "Presume the lock on processing request held by caller is protection enough")
    void done() {
      if (this.cellBlockStream != null) {
        // This will return back the BBs which we got from pool.
        this.cellBlockStream.releaseResources();
        this.cellBlockStream = null;
      }
      // If the call was run successfuly, we might have already returned the BB
      // back to pool. No worries..Then inputCellBlock will be null
      cleanup();
    }

    @Override
    public void cleanup() {
      if (this.reqCleanup != null) {
        this.reqCleanup.run();
        this.reqCleanup = null;
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
          " connection: " + connection.toString() +
          " deadline: " + deadline;
    }

    protected synchronized void setSaslTokenResponse(ByteBuffer response) {
      ByteBuffer[] responseBufs = new ByteBuffer[1];
      responseBufs[0] = response;
      this.response = new BufferChain(responseBufs);
    }

    protected synchronized void setConnectionHeaderResponse(ByteBuffer response) {
      ByteBuffer[] responseBufs = new ByteBuffer[1];
      responseBufs[0] = response;
      this.response = new BufferChain(responseBufs);
    }

    @Override
    public synchronized void setResponse(Message m, final CellScanner cells,
        Throwable t, String errorMsg) {
      if (this.isError) return;
      if (t != null) this.isError = true;
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
        if (reservoir != null) {
          this.cellBlockStream = cellBlockBuilder.buildCellBlockStream(this.connection.codec,
              this.connection.compressionCodec, cells, reservoir);
          if (this.cellBlockStream != null) {
            cellBlock = this.cellBlockStream.getByteBuffers();
            cellBlockSize = this.cellBlockStream.size();
          }
        } else {
          ByteBuffer b = cellBlockBuilder.buildCellBlock(this.connection.codec,
              this.connection.compressionCodec, cells);
          if (b != null) {
            cellBlockSize = b.remaining();
            cellBlock = new ArrayList<ByteBuffer>(1);
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
        if (connection.useWrap) {
          bc = wrapWithSasl(bc);
        }
      } catch (IOException e) {
        LOG.warn("Exception while creating response " + e);
      }
      this.response = bc;
      // Once a response message is created and set to this.response, this Call can be treated as
      // done. The Responder thread will do the n/w write of this message back to client.
      if (this.rpcCallback != null) {
        try {
          this.rpcCallback.run();
        } catch (Exception e) {
          // Don't allow any exception here to kill this handler thread.
          LOG.warn("Exception while running the Rpc Callback.", e);
        }
      }
    }

    private void setExceptionResponse(Throwable t, String errorMsg,
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
      }
      // Set the exception as the result of the method invocation.
      headerBuilder.setException(exceptionBuilder.build());
    }

    private ByteBuffer createHeaderAndMessageBytes(Message result, Message header,
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
        headerVintSize = CodedOutputStream.computeRawVarint32Size(headerSerializedSize);
      }
      if (result != null) {
        resultSerializedSize = result.getSerializedSize();
        resultVintSize = CodedOutputStream.computeRawVarint32Size(resultSerializedSize);
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

    private void writeToCOS(Message result, Message header, int totalSize, ByteBuffer pbBuf)
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

    private ByteBuffer createHeaderAndMessageBytes(Message result, Message header,
        int totalSize, int totalPBSize) throws IOException {
      ByteBuffer pbBuf = ByteBuffer.allocate(totalPBSize);
      writeToCOS(result, header, totalSize, pbBuf);
      pbBuf.flip();
      return pbBuf;
    }

    private BufferChain wrapWithSasl(BufferChain bc)
        throws IOException {
      if (!this.connection.useSasl) return bc;
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
      if (LOG.isTraceEnabled()) {
        LOG.trace("Adding saslServer wrapped token of size " + token.length
            + " as call response.");
      }

      ByteBuffer[] responseBufs = new ByteBuffer[2];
      responseBufs[0] = ByteBuffer.wrap(Bytes.toBytes(token.length));
      responseBufs[1] = ByteBuffer.wrap(token);
      return new BufferChain(responseBufs);
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
      return timestamp;
    }

    @Override
    public void setReceiveTime(long t) {
      this.timestamp = t;
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
    public TraceInfo getTraceInfo() {
      return tinfo;
    }

  }

  @FunctionalInterface
  protected static interface CallCleanup {
    void run();
  }

  /** Reads calls from a connection and queues them for handling. */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
      value="VO_VOLATILE_INCREMENT",
      justification="False positive according to http://sourceforge.net/p/findbugs/bugs/1032/")
  public abstract class Connection {
    // If initial preamble with version and magic has been read or not.
    protected boolean connectionPreambleRead = false;
    // If the connection header has been read or not.
    protected boolean connectionHeaderRead = false;

    protected CallCleanup callCleanup;

    // Cache the remote host & port info so that even if the socket is
    // disconnected, we can say where it used to connect to.
    protected String hostAddress;
    protected int remotePort;
    protected InetAddress addr;
    protected ConnectionHeader connectionHeader;

    /**
     * Codec the client asked use.
     */
    protected Codec codec;
    /**
     * Compression codec the client asked us use.
     */
    protected CompressionCodec compressionCodec;
    protected BlockingService service;

    protected AuthMethod authMethod;
    protected boolean saslContextEstablished;
    protected boolean skipInitialSaslHandshake;

    protected boolean useSasl;
    protected SaslServer saslServer;
    protected CryptoAES cryptoAES;
    protected boolean useWrap = false;
    protected boolean useCryptoAesWrap = false;
    // Fake 'call' for failed authorization response
    protected static final int AUTHORIZATION_FAILED_CALLID = -1;

    protected ByteArrayOutputStream authFailedResponse =
        new ByteArrayOutputStream();
    // Fake 'call' for SASL context setup
    protected static final int SASL_CALLID = -33;

    // Fake 'call' for connection header response
    protected static final int CONNECTION_HEADER_RESPONSE_CALLID = -34;

    // was authentication allowed with a fallback to simple auth
    protected boolean authenticatedWithFallback;

    protected boolean retryImmediatelySupported = false;

    public UserGroupInformation attemptingUser = null; // user name before auth
    protected User user = null;
    protected UserGroupInformation ugi = null;

    public Connection() {
      this.callCleanup = null;
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

    public VersionInfo getVersionInfo() {
      if (connectionHeader.hasVersionInfo()) {
        return connectionHeader.getVersionInfo();
      }
      return null;
    }

    protected String getFatalConnectionString(final int version, final byte authByte) {
      return "serverVersion=" + CURRENT_VERSION +
      ", clientVersion=" + version + ", authMethod=" + authByte +
      ", authSupported=" + (authMethod != null) + " from " + toString();
    }

    protected UserGroupInformation getAuthorizedUgi(String authorizedId)
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

    /**
     * Set up cell block codecs
     * @throws FatalConnectionException
     */
    protected void setupCellBlockCodecs(final ConnectionHeader header)
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

    /**
     * Set up cipher for rpc encryption with Apache Commons Crypto
     *
     * @throws FatalConnectionException
     */
    protected void setupCryptoCipher(final ConnectionHeader header,
        RPCProtos.ConnectionHeaderResponse.Builder chrBuilder)
        throws FatalConnectionException {
      // If simple auth, return
      if (saslServer == null) return;
      // check if rpc encryption with Crypto AES
      String qop = (String) saslServer.getNegotiatedProperty(Sasl.QOP);
      boolean isEncryption = SaslUtil.QualityOfProtection.PRIVACY
          .getSaslQop().equalsIgnoreCase(qop);
      boolean isCryptoAesEncryption = isEncryption && conf.getBoolean(
          "hbase.rpc.crypto.encryption.aes.enabled", false);
      if (!isCryptoAesEncryption) return;
      if (!header.hasRpcCryptoCipherTransformation()) return;
      String transformation = header.getRpcCryptoCipherTransformation();
      if (transformation == null || transformation.length() == 0) return;
       // Negotiates AES based on complete saslServer.
       // The Crypto metadata need to be encrypted and send to client.
      Properties properties = new Properties();
      // the property for SecureRandomFactory
      properties.setProperty(CryptoRandomFactory.CLASSES_KEY,
          conf.get("hbase.crypto.sasl.encryption.aes.crypto.random",
              "org.apache.commons.crypto.random.JavaCryptoRandom"));
      // the property for cipher class
      properties.setProperty(CryptoCipherFactory.CLASSES_KEY,
          conf.get("hbase.rpc.crypto.encryption.aes.cipher.class",
              "org.apache.commons.crypto.cipher.JceCipher"));

      int cipherKeyBits = conf.getInt(
          "hbase.rpc.crypto.encryption.aes.cipher.keySizeBits", 128);
      // generate key and iv
      if (cipherKeyBits % 8 != 0) {
        throw new IllegalArgumentException("The AES cipher key size in bits" +
            " should be a multiple of byte");
      }
      int len = cipherKeyBits / 8;
      byte[] inKey = new byte[len];
      byte[] outKey = new byte[len];
      byte[] inIv = new byte[len];
      byte[] outIv = new byte[len];

      try {
        // generate the cipher meta data with SecureRandom
        CryptoRandom secureRandom = CryptoRandomFactory.getCryptoRandom(properties);
        secureRandom.nextBytes(inKey);
        secureRandom.nextBytes(outKey);
        secureRandom.nextBytes(inIv);
        secureRandom.nextBytes(outIv);

        // create CryptoAES for server
        cryptoAES = new CryptoAES(transformation, properties,
            inKey, outKey, inIv, outIv);
        // create SaslCipherMeta and send to client,
        //  for client, the [inKey, outKey], [inIv, outIv] should be reversed
        RPCProtos.CryptoCipherMeta.Builder ccmBuilder = RPCProtos.CryptoCipherMeta.newBuilder();
        ccmBuilder.setTransformation(transformation);
        ccmBuilder.setInIv(getByteString(outIv));
        ccmBuilder.setInKey(getByteString(outKey));
        ccmBuilder.setOutIv(getByteString(inIv));
        ccmBuilder.setOutKey(getByteString(inKey));
        chrBuilder.setCryptoCipherMeta(ccmBuilder);
        useCryptoAesWrap = true;
      } catch (GeneralSecurityException | IOException ex) {
        throw new UnsupportedCryptoException(ex.getMessage(), ex);
      }
    }

    private ByteString getByteString(byte[] bytes) {
      // return singleton to reduce object allocation
      return (bytes.length == 0) ? ByteString.EMPTY : ByteString.copyFrom(bytes);
    }

    protected UserGroupInformation createUser(ConnectionHeader head) {
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

    public abstract boolean isConnectionOpen();

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
      int poolBufSize = conf.getInt(ByteBufferPool.BUFFER_SIZE_KEY,
          ByteBufferPool.DEFAULT_BUFFER_SIZE);
      // The max number of buffers to be pooled in the ByteBufferPool. The default value been
      // selected based on the #handlers configured. When it is read request, 2 MB is the max size
      // at which we will send back one RPC request. Means max we need 2 MB for creating the
      // response cell block. (Well it might be much lesser than this because in 2 MB size calc, we
      // include the heap size overhead of each cells also.) Considering 2 MB, we will need
      // (2 * 1024 * 1024) / poolBufSize buffers to make the response cell block. Pool buffer size
      // is by default 64 KB.
      // In case of read request, at the end of the handler process, we will make the response
      // cellblock and add the Call to connection's response Q and a single Responder thread takes
      // connections and responses from that one by one and do the socket write. So there is chances
      // that by the time a handler originated response is actually done writing to socket and so
      // released the BBs it used, the handler might have processed one more read req. On an avg 2x
      // we consider and consider that also for the max buffers to pool
      int bufsForTwoMB = (2 * 1024 * 1024) / poolBufSize;
      int maxPoolSize = conf.getInt(ByteBufferPool.MAX_POOL_SIZE_KEY,
          conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,
              HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT) * bufsForTwoMB * 2);
      this.reservoir = new ByteBufferPool(poolBufSize, maxPoolSize);
      this.minSizeForReservoirUse = getMinSizeForReservoirUse(this.reservoir);
    } else {
      reservoir = null;
      this.minSizeForReservoirUse = Integer.MAX_VALUE;// reservoir itself not in place.
    }
    this.server = server;
    this.services = services;
    this.bindAddress = bindAddress;
    this.conf = conf;
    // See declaration above for documentation on what this size is.
    this.maxQueueSizeInBytes =
      this.conf.getLong("hbase.ipc.server.max.callqueue.size", DEFAULT_MAX_CALLQUEUE_SIZE);

    this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME, DEFAULT_WARN_RESPONSE_TIME);
    this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE, DEFAULT_WARN_RESPONSE_SIZE);
    this.minClientRequestTimeout = conf.getInt(MIN_CLIENT_REQUEST_TIMEOUT,
        DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT);
    this.maxRequestSize = conf.getInt(MAX_REQUEST_SIZE, DEFAULT_MAX_REQUEST_SIZE);

    this.metrics = new MetricsHBaseServer(name, new MetricsHBaseServerWrapperImpl(this));
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.server.tcpnodelay", true);
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.server.tcpkeepalive", true);

    this.cellBlockBuilder = new CellBlockBuilder(conf);

    this.authorize = conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
    this.userProvider = UserProvider.instantiate(conf);
    this.isSecurityEnabled = userProvider.isHBaseSecurityEnabled();
    if (isSecurityEnabled) {
      HBaseSaslRpcServer.init(conf);
    }

    this.scheduler = scheduler;
  }

  @VisibleForTesting
  static int getMinSizeForReservoirUse(ByteBufferPool pool) {
    return pool.getBufferSize() / 6;
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    initReconfigurable(newConf);
    if (scheduler instanceof ConfigurationObserver) {
      ((ConfigurationObserver) scheduler).onConfigurationChange(newConf);
    }
  }

  protected void initReconfigurable(Configuration confToLoad) {
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

  Configuration getConf() {
    return conf;
  }

  @Override
  public boolean isStarted() {
    return this.started;
  }

  @Override
  public synchronized void refreshAuthManager(PolicyProvider pp) {
    // Ignore warnings that this should be accessed in a static way instead of via an instance;
    // it'll break if you go via static route.
    this.authManager.refresh(this.conf, pp);
  }

  protected AuthenticationTokenSecretManager createSecretManager() {
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
  public Pair<Message, CellScanner> call(RpcCall call,
      MonitoredRPCHandler status) throws IOException {
    try {
      MethodDescriptor md = call.getMethod();
      Message param = call.getParam();
      status.setRPC(md.getName(), new Object[]{param},
        call.getReceiveTime());
      // TODO: Review after we add in encoded data blocks.
      status.setRPCPacket(param);
      status.resume("Servicing call");
      //get an instance of the method arg type
      HBaseRpcController controller = new HBaseRpcControllerImpl(call.getCellScanner());
      controller.setCallTimeout(call.getTimeout());
      Message result = call.getService().callBlockingMethod(md, controller, param);
      long receiveTime = call.getReceiveTime();
      long startTime = call.getStartTime();
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
      // Use the raw request call size for now.
      long requestSize = call.getSize();
      long responseSize = result.getSerializedSize();
      if (call.isClientCellBlockSupported()) {
        // Include the payload size in HBaseRpcController
        responseSize += call.getResponseCellSize();
      }

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
        logResponse(param,
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
      if (e instanceof ServiceException) {
        if (e.getCause() == null) {
          LOG.debug("Caught a ServiceException with null cause", e);
        } else {
          e = e.getCause();
        }
      }

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
   * @param param The parameters received in the call.
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
  void logResponse(Message param, String methodName, String call, String tag,
      String clientAddress, long startTime, int processingTime, int qTime,
      long responseSize) throws IOException {
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<String, Object>();
    responseInfo.put("starttimems", startTime);
    responseInfo.put("processingtimems", processingTime);
    responseInfo.put("queuetimems", qTime);
    responseInfo.put("responsesize", responseSize);
    responseInfo.put("client", clientAddress);
    responseInfo.put("class", server == null? "": server.getClass().getSimpleName());
    responseInfo.put("method", methodName);
    responseInfo.put("call", call);
    responseInfo.put("param", ProtobufUtil.getShortTextFormat(param));
    if (param instanceof ClientProtos.ScanRequest && rsRpcServices != null) {
      ClientProtos.ScanRequest request = ((ClientProtos.ScanRequest) param);
      if (request.hasScannerId()) {
        long scannerId = request.getScannerId();
        String scanDetails = rsRpcServices.getScanDetailsWithId(scannerId);
        if (scanDetails != null) {
          responseInfo.put("scandetails", scanDetails);
        }
      }
    }
    LOG.warn("(response" + tag + "): " + MAPPER.writeValueAsString(responseInfo));
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
    this.callQueueSizeInBytes.add(diff);
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
  public synchronized void authorize(UserGroupInformation user,
      ConnectionHeader connection, InetAddress addr)
      throws AuthorizationException {
    if (authorize) {
      Class<?> c = getServiceInterface(services, connection.getServiceName());
      this.authManager.authorize(user != null ? user : null, c, getConf(), addr);
    }
  }

  /**
   * This is extracted to a static method for better unit testing. We try to get buffer(s) from pool
   * as much as possible.
   *
   * @param pool The ByteBufferPool to use
   * @param minSizeForPoolUse Only for buffer size above this, we will try to use pool. Any buffer
   *           need of size below this, create on heap ByteBuffer.
   * @param reqLen Bytes count in request
   */
  @VisibleForTesting
  static Pair<ByteBuff, CallCleanup> allocateByteBuffToReadInto(ByteBufferPool pool,
      int minSizeForPoolUse, int reqLen) {
    ByteBuff resultBuf;
    List<ByteBuffer> bbs = new ArrayList<ByteBuffer>((reqLen / pool.getBufferSize()) + 1);
    int remain = reqLen;
    ByteBuffer buf = null;
    while (remain >= minSizeForPoolUse && (buf = pool.getBuffer()) != null) {
      bbs.add(buf);
      remain -= pool.getBufferSize();
    }
    ByteBuffer[] bufsFromPool = null;
    if (bbs.size() > 0) {
      bufsFromPool = new ByteBuffer[bbs.size()];
      bbs.toArray(bufsFromPool);
    }
    if (remain > 0) {
      bbs.add(ByteBuffer.allocate(remain));
    }
    if (bbs.size() > 1) {
      ByteBuffer[] items = new ByteBuffer[bbs.size()];
      bbs.toArray(items);
      resultBuf = new MultiByteBuff(items);
    } else {
      // We are backed by single BB
      resultBuf = new SingleByteBuff(bbs.get(0));
    }
    resultBuf.limit(reqLen);
    if (bufsFromPool != null) {
      final ByteBuffer[] bufsFromPoolFinal = bufsFromPool;
      return new Pair<ByteBuff, RpcServer.CallCleanup>(resultBuf, () -> {
        // Return back all the BBs to pool
        for (int i = 0; i < bufsFromPoolFinal.length; i++) {
          pool.putbackBuffer(bufsFromPoolFinal[i]);
        }
      });
    }
    return new Pair<ByteBuff, RpcServer.CallCleanup>(resultBuf, null);
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
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  abstract public int getNumOpenConnections();

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
  protected static BlockingServiceAndInterface getServiceAndInterface(
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
  protected static Class<?> getServiceInterface(
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
  protected static BlockingService getService(
      final List<BlockingServiceAndInterface> services,
      final String serviceName) {
    BlockingServiceAndInterface bsasi =
        getServiceAndInterface(services, serviceName);
    return bsasi == null? null: bsasi.getBlockingService();
  }

  protected static MonitoredRPCHandler getStatus() {
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
    RpcCall call = CurCall.get();
    if (call != null) {
      return call.getRemoteAddress();
    }
    return null;
  }

  @Override
  public RpcScheduler getScheduler() {
    return scheduler;
  }

  @Override
  public void setRsRpcServices(RSRpcServices rsRpcServices) {
    this.rsRpcServices = rsRpcServices;
  }

  protected static class ByteBuffByteInput extends ByteInput {

    private ByteBuff buf;
    private int offset;
    private int length;

    ByteBuffByteInput(ByteBuff buf, int offset, int length) {
      this.buf = buf;
      this.offset = offset;
      this.length = length;
    }

    @Override
    public byte read(int offset) {
      return this.buf.get(getAbsoluteOffset(offset));
    }

    private int getAbsoluteOffset(int offset) {
      return this.offset + offset;
    }

    @Override
    public int read(int offset, byte[] out, int outOffset, int len) {
      this.buf.get(getAbsoluteOffset(offset), out, outOffset, len);
      return len;
    }

    @Override
    public int read(int offset, ByteBuffer out) {
      int len = out.remaining();
      this.buf.get(out, getAbsoluteOffset(offset), len);
      return len;
    }

    @Override
    public int size() {
      return this.length;
    }
  }
}