/**
 * Copyright 2010 The Apache Software Foundation
 *
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Operation;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ParamFormatHelper;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.UserGroupInformation;
import org.codehaus.jackson.map.ObjectMapper;

/** A simple RPC mechanism.
 *
 * This is a local hbase copy of the hadoop RPC so we can do things like
 * address HADOOP-414 for hbase-only and try other hbase-specific
 * optimizations like using our own version of ObjectWritable.  Class has been
 * renamed to avoid confusing it w/ hadoop versions.
 * <p>
 *
 *
 * A <i>protocol</i> is a Java interface.  All parameters and return types must
 * be one of:
 *
 * <ul> <li>a primitive type, <code>boolean</code>, <code>byte</code>,
 * <code>char</code>, <code>short</code>, <code>int</code>, <code>long</code>,
 * <code>float</code>, <code>double</code>, or <code>void</code>; or</li>
 *
 * <li>a {@link String}; or</li>
 *
 * <li>a {@link Writable}; or</li>
 *
 * <li>an array of the above types</li> </ul>
 *
 * All methods in the protocol should throw only IOException.  No field data of
 * the protocol instance is transmitted.
 */
public class HBaseRPC {
  protected static final Log LOG = LogFactory.getLog(HBaseRPC.class.getName());
  
  private final static Map<InetSocketAddress, Long> versions =
      new ConcurrentHashMap<InetSocketAddress, Long>();

  private HBaseRPC() {
    super();
  }                                  // no public ctor

  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    private String methodName;
    @SuppressWarnings("unchecked")
    private Class[] parameterClasses;
    private Object[] parameters;
    private Configuration conf;

    /** default constructor */
    public Invocation() {
      super();
    }

    /**
     * @param method method to call
     * @param parameters parameters of call
     */
    public Invocation(Method method, Object[] parameters) {
      this.methodName = method.getName();
      this.parameterClasses = method.getParameterTypes();
      this.parameters = parameters;
    }

    /** @return The name of the method invoked. */
    public String getMethodName() { return methodName; }

    /** @return The parameter classes. */
    @SuppressWarnings("unchecked")
    public Class[] getParameterClasses() { return parameterClasses; }

    /** @return The parameter instances. */
    public Object[] getParameters() { return parameters; }

    public void readFields(DataInput in) throws IOException {
      methodName = in.readUTF();
      
      int parameterLength = in.readInt();
      if (parameterLength < 0 || parameterLength > HConstants.IPC_CALL_PARAMETER_LENGTH_MAX) {
        String error = "Invalid parameter length: " +  parameterLength +
        " for the method " + methodName;
        LOG.error(error);
        throw new IllegalArgumentException(error);
      }
      parameters = new Object[parameterLength];
      parameterClasses = new Class[parameters.length];
      HbaseObjectWritable objectWritable = new HbaseObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = HbaseObjectWritable.readObject(in, objectWritable,
          this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.methodName);
      out.writeInt(parameterClasses.length);
      for (int i = 0; i < parameterClasses.length; i++) {
        HbaseObjectWritable.writeObject(out, parameters[i], parameterClasses[i],
                                   conf);
      }
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder(256);
      buffer.append(methodName);
      buffer.append("(");
      for (int i = 0; i < parameters.length; i++) {
        if (i != 0)
          buffer.append(", ");
        buffer.append(parameters[i]);
      }
      buffer.append(")");
      return buffer.toString();
    }

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return this.conf;
    }
  }

  /* Cache a client using its socket factory as the hash key */
  static private class ClientCache {
    private Map<SocketFactory, HBaseClient> clients =
      new HashMap<SocketFactory, HBaseClient>();

    protected ClientCache() {}

    /**
     * Construct & cache an IPC client with the user-provided SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @param factory socket factory
     * @return an IPC client
     */
    protected synchronized HBaseClient getClient(Configuration conf,
        SocketFactory factory) {
      // Construct & cache client.  The configuration is only used for timeout,
      // and Clients have connection pools.  So we can either (a) lose some
      // connection pooling and leak sockets, or (b) use the same timeout for all
      // configurations.  Since the IPC is usually intended globally, not
      // per-job, we choose (a).
      HBaseClient client = clients.get(factory);
      if (client == null) {
        // Make an hbase client instead of hadoop Client.
        client = new HBaseClient(conf, factory);
        clients.put(factory, client);
      }
      return client;
    }

    /**
     * Construct & cache an IPC client with the default SocketFactory
     * if no cached client exists.
     *
     * @param conf Configuration
     * @return an IPC client
     */
    protected synchronized HBaseClient getClient(Configuration conf) {
      return getClient(conf, SocketFactory.getDefault());
    }

    /**
     * Stop a RPC client connection
     * @param client client to stop
     */
    protected void stopClient(HBaseClient client) {
      synchronized (this) {
        clients.remove(client.getSocketFactory());
      }
      client.stop();
    }
    
    protected synchronized void stopClients() {
      for (Map.Entry<SocketFactory, HBaseClient> e : clients.entrySet()) {
        e.getValue().stop();
      }
      clients.clear();
    }
  }

  protected final static ClientCache CLIENTS = new ClientCache();

  private static class Invoker implements InvocationHandler {
    private InetSocketAddress address;
    private UserGroupInformation ticket;
    private HBaseClient client;
    private boolean isClosed = false;
    final private int rpcTimeout;
    public HBaseRPCOptions options;

    /**
     * @param address address for invoker
     * @param ticket ticket
     * @param conf configuration
     * @param factory socket factory
     */
    public Invoker(InetSocketAddress address, UserGroupInformation ticket,
                   Configuration conf, SocketFactory factory, 
                   int rpcTimeout, HBaseRPCOptions options) {
      this.address = address;
      this.ticket = ticket;
      this.client = CLIENTS.getClient(conf, factory);
      this.rpcTimeout = rpcTimeout;
      this.options = options;
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      final boolean isTraceEnabled = LOG.isTraceEnabled();
      long startTime = 0;
      if (isTraceEnabled) {
        startTime = System.currentTimeMillis();
      }
      HbaseObjectWritable value = client.call(new Invocation(method, args),
          address, ticket, rpcTimeout, options);
      if (isTraceEnabled) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.trace("Call: " + method.getName() + " " + callTime);
      }
      return value.get();
    }

    /* close the IPC client that's responsible for this invoker's RPCs */
    synchronized protected void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }
  }
  
  public static void stopClients () {
    CLIENTS.stopClients();
  }

  /**
   * A version mismatch for the RPC protocol.
   */
  @SuppressWarnings("serial")
  public static class VersionMismatch extends IOException {
    private String interfaceName;
    private long clientVersion;
    private long serverVersion;

    /**
     * Create a version mismatch exception
     * @param interfaceName the name of the protocol mismatch
     * @param clientVersion the client's version of the protocol
     * @param serverVersion the server's version of the protocol
     */
    public VersionMismatch(String interfaceName, long clientVersion,
                           long serverVersion) {
      super("Protocol " + interfaceName + " version mismatch. (client = " +
            clientVersion + ", server = " + serverVersion + ")");
      this.interfaceName = interfaceName;
      this.clientVersion = clientVersion;
      this.serverVersion = serverVersion;
    }

    /**
     * Get the interface name
     * @return the java class name
     *          (eg. org.apache.hadoop.mapred.InterTrackerProtocol)
     */
    public String getInterfaceName() {
      return interfaceName;
    }

    /**
     * @return the client's preferred version
     */
    public long getClientVersion() {
      return clientVersion;
    }

    /**
     * @return the server's agreed to version.
     */
    public long getServerVersion() {
      return serverVersion;
    }
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout timeout for each RPC
   * @return proxy
   * @throws IOException e
   */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf,
      SocketFactory factory, int rpcTimeout, HBaseRPCOptions options) throws IOException {
    return getProxy(protocol, clientVersion, addr, null, conf, factory,
        rpcTimeout, options);
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param ticket ticket
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout timeout for each RPC
   * @return proxy
   * @throws IOException e
   */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory, int rpcTimeout, HBaseRPCOptions options)
  throws IOException {
    VersionedProtocol proxy =
        (VersionedProtocol) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[]{protocol},
            new Invoker(addr, ticket, conf, factory, rpcTimeout, options));

    Long serverVersion = versions.get (addr);
    if (serverVersion == null) {
      serverVersion = proxy.getProtocolVersion(protocol.getName(), clientVersion);
      versions.put (addr, serverVersion);
    }
    
    if ((long) serverVersion == clientVersion) {
      return proxy;
    }
    throw new VersionMismatch(protocol.getName(), clientVersion,
                              (long) serverVersion);
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   *
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param conf configuration
   * @param rpcTimeout timeout for each RPC
   * @return a proxy instance
   * @throws IOException e
   */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf,
      int rpcTimeout, HBaseRPCOptions options)
      throws IOException {

    return getProxy(protocol, clientVersion, addr, conf, 
        SocketFactory.getDefault(), rpcTimeout, options);
  }
  
  /* this is needed for unit tests. some tests start multiple
   * region servers using a single HBaseClient object. we need to
   * reference count so the first region server to shut down
   * doesn't shut down the HBaseClient object
   */
  static AtomicInteger numProxies = new AtomicInteger (0);
  public static void startProxy () {
    numProxies.incrementAndGet();
  }

  /**
   * Stop this proxy and release its invoker's resource
   * @param proxy the proxy to be stopped
   */
  public static void stopProxy(VersionedProtocol proxy) {
    numProxies.decrementAndGet();
    if (proxy!=null && numProxies.get () <= 0) {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    }
  }

  /**
   * Construct a server for a protocol implementation instance listening on a
   * port and address.
   *
   * @param instance instance
   * @param bindAddress bind address
   * @param port port to bind to
   * @param conf configuration
   * @return Server
   * @throws IOException e
   */
  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf)
    throws IOException {
    return getServer(instance, bindAddress, port, 1, false, conf);
  }

  /**
   * Construct a server for a protocol implementation instance listening on a
   * port and address.
   *
   * @param instance instance
   * @param bindAddress bind address
   * @param port port to bind to
   * @param numHandlers number of handlers to start
   * @param verbose verbose flag
   * @param conf configuration
   * @return Server
   * @throws IOException e
   */
  public static Server getServer(final Object instance, final String bindAddress, final int port,
                                 final int numHandlers,
                                 final boolean verbose, Configuration conf)
    throws IOException {
    return new Server(instance, conf, bindAddress, port, numHandlers, verbose);
  }

  /** An RPC Server. */
  public static class Server extends HBaseServer {
    private Object instance;
    private Class<?> implementation;
    private boolean verbose;
    ParamFormatHelper paramFormatHelper;

    private static final String WARN_RESPONSE_TIME =
      "hbase.ipc.warn.response.time";
    private static final String WARN_RESPONSE_SIZE =
      "hbase.ipc.warn.response.size";

    /** Default value for above params */
    private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
    private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

    private final int warnResponseTime;
    private final int warnResponseSize;

    /**
     * Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @throws IOException e
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port)
      throws IOException {
      this(instance, conf,  bindAddress, port, 1, false);
    }

    public static String classNameBase(String className) {
      String[] names = className.split("\\.", -1);
      if (names == null || names.length == 0) {
        return className;
      }
      return names[names.length-1];
    }

    /** Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @param numHandlers the number of method handler threads to run
     * @param verbose whether each call should be logged
     * @throws IOException e
     */
    public Server(Object instance, Configuration conf, String bindAddress,  int port,
                  int numHandlers, boolean verbose) throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf,
          classNameBase(instance.getClass().getName()));
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;
      this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME,
          DEFAULT_WARN_RESPONSE_TIME);
      this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE,
          DEFAULT_WARN_RESPONSE_SIZE);

      // Create a param formatter for TaskMonitor to use to pretty print
      //  the arguments passed over RPC. See ScanParamsFormatter for an example
      paramFormatHelper = new ParamFormatHelper(instance);
    }

    /**
     * Gets info about a specific RPC call given an specific method and
     * arguments it takes. Relies on the method in the RPC handler having an
     * ParamFormat annotation on it. If there is no pretty printer, this returns
     * null
     * @param method the method of this.instance to get the info on
     * @param params the params that would be passed to this function
     * @return A map from String to object of information on this RPC call
     */
    @Override
    public Map<String, Object> getParamFormatMap(Method method, Object[] params) {
      if (paramFormatHelper != null) {
        return paramFormatHelper.getMap(method, params);
      }
      return null;
    }

    @Override
    public Writable call(Writable param, long receivedTime,
        MonitoredRPCHandler status) throws IOException {
      long startTime = 0L;
      Invocation call = null;
      int qTime = -1;
      try {
        call = (Invocation)param;
        if(call.getMethodName() == null) {
          throw new IOException("Could not find requested method, the usual " +
              "cause is a version mismatch between client and server.");
        }
        Call callInfo = HRegionServer.callContext.get();
        if (verbose) trace("Call: " + call);
        Method method = implementation.getMethod(call.getMethodName(),
                call.getParameterClasses());
        status.setRPC(call.getMethodName(), call.getParameters(), receivedTime, method);
        status.setRPCPacket(param);
        status.resume("Servicing call");
        startTime = System.currentTimeMillis();
        qTime = (int) (startTime - receivedTime);
        ProfilingData pData = callInfo == null ? null : callInfo.getProfilingData();
        if (pData != null) {
          pData.addString(ProfilingData.RPC_METHOD_NAME, call.getMethodName ());
          pData.addLong(ProfilingData.QUEUED_TIME_MS, qTime);
        }
        Object value = method.invoke(instance, call.getParameters());
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Served: " + call.getMethodName() +
            " queueTime= " + qTime +
            " procesingTime= " + processingTime);
        }
        rpcMetrics.rpcQueueTime.inc(qTime);
        rpcMetrics.rpcProcessingTime.inc(processingTime);
        rpcMetrics.inc(call.getMethodName(), processingTime);
        if (verbose) trace("Return: " + value);

        HbaseObjectWritable retVal =
          new HbaseObjectWritable(method.getReturnType(), value);
        long responseSize = retVal.getWritableSize();
        // log any RPC responses that are slower than the configured warn
        // response time or larger than configured warning size
        boolean tooSlow = (processingTime > warnResponseTime
            && warnResponseTime > -1);
        boolean tooLarge = (responseSize > warnResponseSize
            && warnResponseSize > -1);
        if (tooSlow || tooLarge) {
          // when tagging, we let TooLarge trump TooSmall to keep output simple
          // note that large responses will often also be slow.
          logResponse(call, (tooLarge ? "TooLarge" : "TooSlow"),
              status.getClient(), startTime, processingTime, qTime,
              responseSize);
          if (tooSlow) {
            // increment global slow RPC response counter
            rpcMetrics.inc("slowResponse.", processingTime);
            HRegion.incrNumericPersistentMetric("slowResponse.all.cumulative", 1);
            HRegion.incrNumericPersistentMetric("slowResponse."
                + call.getMethodName() + ".cumulative", 1);
          }
          if (tooLarge) {
            // increment global slow RPC response counter
            rpcMetrics.inc("largeResponse.kb.", (int)(responseSize/1024));
            HRegion.incrNumericPersistentMetric("largeResponse.all.cumulative", 1);
            HRegion.incrNumericPersistentMetric("largeResponse."
                + call.getMethodName() + ".cumulative", 1);
          }
        }
        if (processingTime > 1000) {
          // we use a hard-coded one second period so that we can clearly
          // indicate the time period we're warning about in the name of the
          // metric itself
          rpcMetrics.inc(call.getMethodName() + ".aboveOneSec.",
              processingTime);
          HRegion.incrNumericPersistentMetric(call.getMethodName() +
              ".aboveOneSec.cumulative", 1);
        }

        return retVal;
      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          if (target instanceof CallInterruptedException) {
            int processingTime = (int) (System.currentTimeMillis() - startTime);
            // put -1 for response size since we are not going to return a response
            logResponse(call, "INTERRUPTED", status.getClient(), startTime, processingTime, qTime, -1);
          }
          throw (IOException) target;
        }
        IOException ioe = new IOException(target.toString());
        ioe.setStackTrace(target.getStackTrace());
        throw ioe;
      } catch (Throwable e) {
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }

    /**
     * Logs an RPC response to the LOG file, producing valid JSON objects for
     * client Operations.
     * @param call The call to log.
     * @param tag  The tag that will be used to indicate this event in the log.
     * @param client          The address of the client who made this call.
     * @param startTime       The time that the call was initiated, in ms.
     * @param processingTime  The duration that the call took to run, in ms.
     * @param qTime           The duration that the call spent on the queue
     *                        prior to being initiated, in ms.
     * @param responseSize    The size in bytes of the response buffer.
     */
    private void logResponse(Invocation call, String tag, String clientAddress,
        long startTime, int processingTime, int qTime, long responseSize)
      throws IOException {
      Object params[] = call.getParameters();
      // for JSON encoding
      ObjectMapper mapper = new ObjectMapper();
      // base information that is reported regardless of type of call
      Map<String, Object> responseInfo = new HashMap<String, Object>();
      responseInfo.put("starttimems", startTime);
      responseInfo.put("processingtimems", processingTime);
      responseInfo.put("queuetimems", qTime);
      responseInfo.put("responsesize", responseSize);
      responseInfo.put("client", clientAddress);
      responseInfo.put("class", instance.getClass().getSimpleName());
      responseInfo.put("method", call.getMethodName());

      Call callContext = HRegionServer.callContext.get();
      ProfilingData pData = callContext == null ? null : callContext.getProfilingData();
      if (pData != null) {
        responseInfo.put("profilingData", pData.toString());
      }

      if (params.length == 2 && instance instanceof HRegionServer &&
          params[0] instanceof byte[] &&
          params[1] instanceof Operation) {
        // if the slow process is a query, we want to log its table as well
        // as its own fingerprint
        byte [] tableName =
          HRegionInfo.parseRegionName((byte[]) params[0])[0];
        responseInfo.put("table", Bytes.toStringBinary(tableName));
        // annotate the response map with operation details
        responseInfo.putAll(((Operation) params[1]).toMap());
        // report to the log file
        LOG.warn("(operation" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
      } else if (params.length == 1 && instance instanceof HRegionServer &&
          params[0] instanceof Operation) {
        // annotate the response map with operation details
        responseInfo.putAll(((Operation) params[0]).toMap());
        // report to the log file
        LOG.warn("(operation" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
      } else {
        // can't get JSON details, so just report call.toString() along with
        // a more generic tag.
        responseInfo.put("call", call.toString());
        LOG.warn("(response" + tag + "): " +
            mapper.writeValueAsString(responseInfo));
      }
    }
  }

  protected static void trace(String value) {
    String v = value;
    if (v != null && v.length() > 55)
      v = v.substring(0, 55)+"...";
    LOG.trace(v);
  }
}
