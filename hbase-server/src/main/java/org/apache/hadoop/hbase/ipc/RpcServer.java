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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.namequeues.RpcLogDetails;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.security.HBasePolicyProvider;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSecretManager;
import org.apache.hadoop.hbase.util.GsonUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.gson.Gson;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingService;
import org.apache.hbase.thirdparty.com.google.protobuf.Descriptors.MethodDescriptor;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ConnectionHeader;

/**
 * An RPC server that hosts protobuf described Services.
 *
 */
@InterfaceAudience.Private
public abstract class RpcServer implements RpcServerInterface,
    ConfigurationObserver {
  // LOG is being used in CallRunner and the log level is being changed in tests
  public static final Logger LOG = LoggerFactory.getLogger(RpcServer.class);
  protected static final CallQueueTooBigException CALL_QUEUE_TOO_BIG_EXCEPTION
      = new CallQueueTooBigException();

  private static final String MULTI_GETS = "multi.gets";
  private static final String MULTI_MUTATIONS = "multi.mutations";
  private static final String MULTI_SERVICE_CALLS = "multi.service_calls";

  private final boolean authorize;
  private final boolean isOnlineLogProviderEnabled;
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
  protected static final Logger AUDITLOG = LoggerFactory.getLogger("SecurityLogger."
      + Server.class.getName());
  protected SecretManager<TokenIdentifier> secretManager;
  protected final Map<String, String> saslProps;

  protected ServiceAuthorizationManager authManager;

  /** This is set to Call object before Handler invokes an RPC and ybdie
   * after the call returns.
   */
  protected static final ThreadLocal<RpcCall> CurCall = new ThreadLocal<>();

  /** Keeps MonitoredRPCHandler per handler thread. */
  protected static final ThreadLocal<MonitoredRPCHandler> MONITORED_RPC = new ThreadLocal<>();

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

  public static final String MAX_REQUEST_SIZE = "hbase.ipc.max.request.size";

  protected static final String WARN_RESPONSE_TIME = "hbase.ipc.warn.response.time";
  protected static final String WARN_RESPONSE_SIZE = "hbase.ipc.warn.response.size";

  /**
   * Minimum allowable timeout (in milliseconds) in rpc request's header. This
   * configuration exists to prevent the rpc service regarding this request as timeout immediately.
   */
  protected static final String MIN_CLIENT_REQUEST_TIMEOUT = "hbase.ipc.min.client.request.timeout";
  protected static final int DEFAULT_MIN_CLIENT_REQUEST_TIMEOUT = 20;

  /** Default value for above params */
  public static final int DEFAULT_MAX_REQUEST_SIZE = DEFAULT_MAX_CALLQUEUE_SIZE / 4; // 256M
  protected static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
  protected static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

  protected static final int DEFAULT_TRACE_LOG_MAX_LENGTH = 1000;
  protected static final String TRACE_LOG_MAX_LENGTH = "hbase.ipc.trace.log.max.length";
  protected static final String KEY_WORD_TRUNCATED = " <TRUNCATED>";

  protected static final Gson GSON = GsonUtil.createGsonWithDisableHtmlEscaping().create();

  protected final int maxRequestSize;
  protected final int warnResponseTime;
  protected final int warnResponseSize;

  protected final int minClientRequestTimeout;

  protected final Server server;
  protected final List<BlockingServiceAndInterface> services;

  protected final RpcScheduler scheduler;

  protected UserProvider userProvider;

  protected final ByteBuffAllocator bbAllocator;

  protected volatile boolean allowFallbackToSimpleAuth;

  /**
   * Used to get details for scan with a scanner_id<br/>
   * TODO try to figure out a better way and remove reference from regionserver package later.
   */
  private RSRpcServices rsRpcServices;


  /**
   * Use to add online slowlog responses
   */
  private NamedQueueRecorder namedQueueRecorder;

  @FunctionalInterface
  protected interface CallCleanup {
    void run();
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
   * @param reservoirEnabled Enable ByteBufferPool or not.
   */
  public RpcServer(final Server server, final String name,
      final List<BlockingServiceAndInterface> services,
      final InetSocketAddress bindAddress, Configuration conf,
      RpcScheduler scheduler, boolean reservoirEnabled) throws IOException {
    this.bbAllocator = ByteBuffAllocator.create(conf, reservoirEnabled);
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
      saslProps = SaslUtil.initSaslProperties(conf.get("hbase.rpc.protection",
        QualityOfProtection.AUTHENTICATION.name().toLowerCase(Locale.ROOT)));
    } else {
      saslProps = Collections.emptyMap();
    }

    this.isOnlineLogProviderEnabled = conf.getBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY,
      HConstants.DEFAULT_ONLINE_LOG_PROVIDER_ENABLED);
    this.scheduler = scheduler;
  }

  @Override
  public void onConfigurationChange(Configuration newConf) {
    initReconfigurable(newConf);
    if (scheduler instanceof ConfigurationObserver) {
      ((ConfigurationObserver) scheduler).onConfigurationChange(newConf);
    }
    if (authorize) {
      refreshAuthManager(newConf, new HBasePolicyProvider());
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
  public synchronized void refreshAuthManager(Configuration conf, PolicyProvider pp) {
    // Ignore warnings that this should be accessed in a static way instead of via an instance;
    // it'll break if you go via static route.
    System.setProperty("hadoop.policy.file", "hbase-policy.xml");
    this.authManager.refresh(conf, pp);
    LOG.info("Refreshed hbase-policy.xml successfully");
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    LOG.info("Refreshed super and proxy users successfully");
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
        final String userName = call.getRequestUserName().orElse(StringUtils.EMPTY);
        // when tagging, we let TooLarge trump TooSmall to keep output simple
        // note that large responses will often also be slow.
        logResponse(param,
          md.getName(), md.getName() + "(" + param.getClass().getName() + ")",
          tooLarge, tooSlow,
          status.getClient(), startTime, processingTime, qTime,
          responseSize, userName);
        if (this.namedQueueRecorder != null && this.isOnlineLogProviderEnabled) {
          // send logs to ring buffer owned by slowLogRecorder
          final String className =
            server == null ? StringUtils.EMPTY : server.getClass().getSimpleName();
          this.namedQueueRecorder.addRecord(
            new RpcLogDetails(call, param, status.getClient(), responseSize, className, tooSlow,
              tooLarge));
        }
      }
      return new Pair<>(result, controller.cellScanner());
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
   * @param tooLarge To indicate if the event is tooLarge
   * @param tooSlow To indicate if the event is tooSlow
   * @param clientAddress The address of the client who made this call.
   * @param startTime The time that the call was initiated, in ms.
   * @param processingTime The duration that the call took to run, in ms.
   * @param qTime The duration that the call spent on the queue
   *   prior to being initiated, in ms.
   * @param responseSize The size in bytes of the response buffer.
   * @param userName UserName of the current RPC Call
   */
  void logResponse(Message param, String methodName, String call, boolean tooLarge,
      boolean tooSlow, String clientAddress, long startTime, int processingTime, int qTime,
      long responseSize, String userName) {
    final String className = server == null ? StringUtils.EMPTY :
      server.getClass().getSimpleName();
    // base information that is reported regardless of type of call
    Map<String, Object> responseInfo = new HashMap<>();
    responseInfo.put("starttimems", startTime);
    responseInfo.put("processingtimems", processingTime);
    responseInfo.put("queuetimems", qTime);
    responseInfo.put("responsesize", responseSize);
    responseInfo.put("client", clientAddress);
    responseInfo.put("class", className);
    responseInfo.put("method", methodName);
    responseInfo.put("call", call);
    // The params could be really big, make sure they don't kill us at WARN
    String stringifiedParam = ProtobufUtil.getShortTextFormat(param);
    if (stringifiedParam.length() > 150) {
      // Truncate to 1000 chars if TRACE is on, else to 150 chars
      stringifiedParam = truncateTraceLog(stringifiedParam);
    }
    responseInfo.put("param", stringifiedParam);
    if (param instanceof ClientProtos.ScanRequest && rsRpcServices != null) {
      ClientProtos.ScanRequest request = ((ClientProtos.ScanRequest) param);
      String scanDetails;
      if (request.hasScannerId()) {
        long scannerId = request.getScannerId();
        scanDetails = rsRpcServices.getScanDetailsWithId(scannerId);
      } else {
        scanDetails = rsRpcServices.getScanDetailsWithRequest(request);
      }
      if (scanDetails != null) {
        responseInfo.put("scandetails", scanDetails);
      }
    }
    if (param instanceof ClientProtos.MultiRequest) {
      int numGets = 0;
      int numMutations = 0;
      int numServiceCalls = 0;
      ClientProtos.MultiRequest multi = (ClientProtos.MultiRequest)param;
      for (ClientProtos.RegionAction regionAction : multi.getRegionActionList()) {
        for (ClientProtos.Action action: regionAction.getActionList()) {
          if (action.hasMutation()) {
            numMutations++;
          }
          if (action.hasGet()) {
            numGets++;
          }
          if (action.hasServiceCall()) {
            numServiceCalls++;
          }
        }
      }
      responseInfo.put(MULTI_GETS, numGets);
      responseInfo.put(MULTI_MUTATIONS, numMutations);
      responseInfo.put(MULTI_SERVICE_CALLS, numServiceCalls);
    }
    final String tag = (tooLarge && tooSlow) ? "TooLarge & TooSlow"
      : (tooSlow ? "TooSlow" : "TooLarge");
    LOG.warn("(response" + tag + "): " + GSON.toJson(responseInfo));
  }


  /**
   * Truncate to number of chars decided by conf hbase.ipc.trace.log.max.length
   * if TRACE is on else to 150 chars Refer to Jira HBASE-20826 and HBASE-20942
   * @param strParam stringifiedParam to be truncated
   * @return truncated trace log string
   */
  String truncateTraceLog(String strParam) {
    if (LOG.isTraceEnabled()) {
      int traceLogMaxLength = getConf().getInt(TRACE_LOG_MAX_LENGTH, DEFAULT_TRACE_LOG_MAX_LENGTH);
      int truncatedLength =
          strParam.length() < traceLogMaxLength ? strParam.length() : traceLogMaxLength;
      String truncatedFlag = truncatedLength == strParam.length() ? "" : KEY_WORD_TRUNCATED;
      return strParam.subSequence(0, truncatedLength) + truncatedFlag;
    }
    return strParam.subSequence(0, 150) + KEY_WORD_TRUNCATED;
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
   * @param user client user
   * @param connection incoming connection
   * @param addr InetAddress of incoming connection
   * @throws AuthorizationException when the client isn't authorized to talk the protocol
   */
  public synchronized void authorize(UserGroupInformation user, ConnectionHeader connection,
      InetAddress addr) throws AuthorizationException {
    if (authorize) {
      Class<?> c = getServiceInterface(services, connection.getServiceName());
      authManager.authorize(user, c, getConf(), addr);
    }
  }

  /**
   * When the read or write buffer size is larger than this limit, i/o will be
   * done in chunks of this size. Most RPC requests and responses would be
   * be smaller.
   */
  protected static final int NIO_BUFFER_LIMIT = 64 * 1024; //should not be more than 64KB.

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
   * Helper for {@link #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)}.
   * Only one of readCh or writeCh should be non-null.
   *
   * @param readCh read channel
   * @param writeCh write channel
   * @param buf buffer to read or write into/out of
   * @return bytes written
   * @throws java.io.IOException e
   * @see #channelRead(java.nio.channels.ReadableByteChannel, java.nio.ByteBuffer)
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
  public static Optional<RpcCall> getCurrentCall() {
    return Optional.ofNullable(CurCall.get());
  }

  public static boolean isInRpcCallContext() {
    return CurCall.get() != null;
  }

  /**
   * Used by {@link org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore}. For
   * master's rpc call, it may generate new procedure and mutate the region which store procedure.
   * There are some check about rpc when mutate region, such as rpc timeout check. So unset the rpc
   * call to avoid the rpc check.
   * @return the currently ongoing rpc call
   */
  public static Optional<RpcCall> unsetCurrentCall() {
    Optional<RpcCall> rpcCall = getCurrentCall();
    CurCall.set(null);
    return rpcCall;
  }


  /**
   * Used by {@link org.apache.hadoop.hbase.procedure2.store.region.RegionProcedureStore}. Set the
   * rpc call back after mutate region.
   */
  public static void setCurrentCall(RpcCall rpcCall) {
    CurCall.set(rpcCall);
  }

  /**
   * Returns the user credentials associated with the current RPC request or not present if no
   * credentials were provided.
   * @return A User
   */
  public static Optional<User> getRequestUser() {
    Optional<RpcCall> ctx = getCurrentCall();
    return ctx.isPresent() ? ctx.get().getRequestUser() : Optional.empty();
  }

  /**
   * The number of open RPC conections
   * @return the number of open rpc connections
   */
  abstract public int getNumOpenConnections();

  /**
   * Returns the username for any user associated with the current RPC
   * request or not present if no user is set.
   */
  public static Optional<String> getRequestUserName() {
    return getRequestUser().map(User::getShortName);
  }

  /**
   * @return Address of remote client if a request is ongoing, else null
   */
  public static Optional<InetAddress> getRemoteAddress() {
    return getCurrentCall().map(RpcCall::getRemoteAddress);
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
  public ByteBuffAllocator getByteBuffAllocator() {
    return this.bbAllocator;
  }

  @Override
  public void setRsRpcServices(RSRpcServices rsRpcServices) {
    this.rsRpcServices = rsRpcServices;
  }

  @Override
  public void setNamedQueueRecorder(NamedQueueRecorder namedQueueRecorder) {
    this.namedQueueRecorder = namedQueueRecorder;
  }

  protected boolean needAuthorization() {
    return authorize;
  }
}
