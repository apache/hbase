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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

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
  // Leave this out in the hadoop ipc package but keep class name.  Do this
  // so that we dont' get the logging of this class's invocations by doing our
  // blanket enabling DEBUG on the o.a.h.h. package.
  protected static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.HbaseRPC");

  private HBaseRPC() {
    super();
  }                                  // no public ctor


  /** A method invocation, including the method name and its parameters.*/
  private static class Invocation implements Writable, Configurable {
    // Here, for hbase, we maintain two static maps of method names to code and
    // vice versa.
    private static final Map<Byte, String> CODE_TO_METHODNAME =
      new HashMap<Byte, String>();
    private static final Map<String, Byte> METHODNAME_TO_CODE =
      new HashMap<String, Byte>();
    // Special code that means 'not-encoded'.
    private static final byte NOT_ENCODED = 0;
    static {
      byte code = NOT_ENCODED + 1;
      code = addToMap(VersionedProtocol.class, code);
      code = addToMap(HMasterInterface.class, code);
      code = addToMap(HMasterRegionInterface.class, code);
      code = addToMap(TransactionalRegionInterface.class, code);
    }
    // End of hbase modifications.

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
     * @param method
     * @param parameters
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
      byte code = in.readByte();
      methodName = CODE_TO_METHODNAME.get(Byte.valueOf(code));
      parameters = new Object[in.readInt()];
      parameterClasses = new Class[parameters.length];
      HbaseObjectWritable objectWritable = new HbaseObjectWritable();
      for (int i = 0; i < parameters.length; i++) {
        parameters[i] = HbaseObjectWritable.readObject(in, objectWritable,
          this.conf);
        parameterClasses[i] = objectWritable.getDeclaredClass();
      }
    }

    public void write(DataOutput out) throws IOException {
      writeMethodNameCode(out, this.methodName);
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
    
    // Hbase additions.
    private static void addToMap(final String name, final byte code) {
      if (METHODNAME_TO_CODE.containsKey(name)) {
        return;
      }
      METHODNAME_TO_CODE.put(name, Byte.valueOf(code));
      CODE_TO_METHODNAME.put(Byte.valueOf(code), name);
    }
    
    /*
     * @param c Class whose methods we'll add to the map of methods to codes
     * (and vice versa).
     * @param code Current state of the byte code.
     * @return State of <code>code</code> when this method is done.
     */
    private static byte addToMap(final Class<?> c, final byte code) {
      byte localCode = code;
      Method [] methods = c.getMethods();
      // There are no guarantees about the order in which items are returned in
      // so do a sort (Was seeing that sort was one way on one server and then
      // another on different server).
      Arrays.sort(methods, new Comparator<Method>() {
        public int compare(Method left, Method right) {
          return left.getName().compareTo(right.getName());
        }
      });
      for (int i = 0; i < methods.length; i++) {
        addToMap(methods[i].getName(), localCode++);
      }
      return localCode;
    }

    /*
     * Write out the code byte for passed Class.
     * @param out
     * @param c
     * @throws IOException
     */
    static void writeMethodNameCode(final DataOutput out, final String methodname)
    throws IOException {
      Byte code = METHODNAME_TO_CODE.get(methodname);
      if (code == null) {
        LOG.error("Unsupported type " + methodname);
        throw new UnsupportedOperationException("No code for unexpected " +
          methodname);
      }
      out.writeByte(code.byteValue());
    }
    // End of hbase additions.
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
        client = new HBaseClient(HbaseObjectWritable.class, conf, factory);
        clients.put(factory, client);
      } else {
        client.incCount();
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
     * A RPC client is closed only when its reference count becomes zero.
     */
    protected void stopClient(HBaseClient client) {
      synchronized (this) {
        client.decCount();
        if (client.isZeroReference()) {
          clients.remove(client.getSocketFactory());
        }
      }
      if (client.isZeroReference()) {
        client.stop();
      }
    }
  }

  protected final static ClientCache CLIENTS = new ClientCache();
  
  private static class Invoker implements InvocationHandler {
    private InetSocketAddress address;
    private UserGroupInformation ticket;
    private HBaseClient client;
    private boolean isClosed = false;

    /**
     * @param address
     * @param ticket
     * @param conf
     * @param factory
     */
    public Invoker(InetSocketAddress address, UserGroupInformation ticket, 
                   Configuration conf, SocketFactory factory) {
      this.address = address;
      this.ticket = ticket;
      this.client = CLIENTS.getClient(conf, factory);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      final boolean logDebug = LOG.isDebugEnabled();
      long startTime = 0;
      if (logDebug) {
        startTime = System.currentTimeMillis();
      }
      HbaseObjectWritable value = (HbaseObjectWritable)
        client.call(new Invocation(method, args), address, ticket);
      if (logDebug) {
        long callTime = System.currentTimeMillis() - startTime;
        LOG.debug("Call: " + method.getName() + " " + callTime);
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
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @param maxAttempts
   * @return proxy
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static VersionedProtocol waitForProxy(Class protocol,
                                               long clientVersion,
                                               InetSocketAddress addr,
                                               Configuration conf,
                                               int maxAttempts,
                                               long timeout
                                               ) throws IOException {
    // HBase does limited number of reconnects which is different from hadoop.
    long startTime = System.currentTimeMillis();
    IOException ioe;
    int reconnectAttempts = 0;
    while (true) {
      try {
        return getProxy(protocol, clientVersion, addr, conf);
      } catch(ConnectException se) {  // namenode has not been started
        LOG.info("Server at " + addr + " not available yet, Zzzzz...");
        ioe = se;
        if (maxAttempts >= 0 && ++reconnectAttempts >= maxAttempts) {
          LOG.info("Server at " + addr + " could not be reached after " +
                  reconnectAttempts + " tries, giving up.");
          throw new RetriesExhaustedException(addr.toString(), "unknown".getBytes(),
                  "unknown".getBytes(), reconnectAttempts - 1,
                  new ArrayList<Throwable>());
      }
      } catch(SocketTimeoutException te) {  // namenode is busy
        LOG.info("Problem connecting to server: " + addr);
        ioe = te;
      }
      // check if timed out
      if (System.currentTimeMillis()-timeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }

  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @param factory
   * @return proxy
   * @throws IOException
   */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf,
      SocketFactory factory) throws IOException {
    return getProxy(protocol, clientVersion, addr, null, conf, factory);
  }
  
  /**
   * Construct a client-side proxy object that implements the named protocol,
   * talking to a server at the named address.
   *
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param ticket
   * @param conf
   * @param factory
   * @return proxy
   * @throws IOException
   */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, UserGroupInformation ticket,
      Configuration conf, SocketFactory factory)
  throws IOException {    
    VersionedProtocol proxy =
        (VersionedProtocol) Proxy.newProxyInstance(
            protocol.getClassLoader(), new Class[] { protocol },
            new Invoker(addr, ticket, conf, factory));
    long serverVersion = proxy.getProtocolVersion(protocol.getName(), 
                                                  clientVersion);
    if (serverVersion == clientVersion) {
      return proxy;
    }
    throw new VersionMismatch(protocol.getName(), clientVersion, 
                              serverVersion);
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   * 
   * @param protocol
   * @param clientVersion
   * @param addr
   * @param conf
   * @return a proxy instance
   * @throws IOException
   */
  public static VersionedProtocol getProxy(Class<?> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf)
      throws IOException {

    return getProxy(protocol, clientVersion, addr, conf, NetUtils
        .getDefaultSocketFactory(conf));
  }

  /**
   * Stop this proxy and release its invoker's resource
   * @param proxy the proxy to be stopped
   */
  public static void stopProxy(VersionedProtocol proxy) {
    if (proxy!=null) {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    }
  }

  /**
   * Expert: Make multiple, parallel calls to a set of servers.
   *
   * @param method
   * @param params
   * @param addrs
   * @param conf
   * @return values
   * @throws IOException
   */
  public static Object[] call(Method method, Object[][] params,
                              InetSocketAddress[] addrs, Configuration conf)
    throws IOException {

    Invocation[] invocations = new Invocation[params.length];
    for (int i = 0; i < params.length; i++)
      invocations[i] = new Invocation(method, params[i]);
    HBaseClient client = CLIENTS.getClient(conf);
    try {
    Writable[] wrappedValues = client.call(invocations, addrs);
    
    if (method.getReturnType() == Void.TYPE) {
      return null;
    }

    Object[] values =
      (Object[])Array.newInstance(method.getReturnType(), wrappedValues.length);
    for (int i = 0; i < values.length; i++)
      if (wrappedValues[i] != null)
        values[i] = ((HbaseObjectWritable)wrappedValues[i]).get();
    
    return values;
    } finally {
      CLIENTS.stopClient(client);
    }
  }

  /**
   * Construct a server for a protocol implementation instance listening on a
   * port and address.
   *
   * @param instance
   * @param bindAddress
   * @param port
   * @param conf
   * @return Server
   * @throws IOException
   */
  public static Server getServer(final Object instance, final String bindAddress, final int port, Configuration conf) 
    throws IOException {
    return getServer(instance, bindAddress, port, 1, false, conf);
  }

  /**
   * Construct a server for a protocol implementation instance listening on a
   * port and address.
   *
   * @param instance
   * @param bindAddress
   * @param port
   * @param numHandlers
   * @param verbose
   * @param conf
   * @return Server
   * @throws IOException
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

    /**
     * Construct an RPC server.
     * @param instance the instance whose methods will be called
     * @param conf the configuration to use
     * @param bindAddress the address to bind on to listen for connection
     * @param port the port to listen for connections on
     * @throws IOException
     */
    public Server(Object instance, Configuration conf, String bindAddress, int port) 
      throws IOException {
      this(instance, conf,  bindAddress, port, 1, false);
    }
    
    private static String classNameBase(String className) {
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
     * @throws IOException
     */
    public Server(Object instance, Configuration conf, String bindAddress,  int port,
                  int numHandlers, boolean verbose) throws IOException {
      super(bindAddress, port, Invocation.class, numHandlers, conf, classNameBase(instance.getClass().getName()));
      this.instance = instance;
      this.implementation = instance.getClass();
      this.verbose = verbose;
    }

    @Override
    public Writable call(Writable param, long receivedTime) throws IOException {
      try {
        Invocation call = (Invocation)param;
        if (verbose) log("Call: " + call);
        Method method =
          implementation.getMethod(call.getMethodName(),
                                   call.getParameterClasses());

        long startTime = System.currentTimeMillis();
        Object value = method.invoke(instance, call.getParameters());
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime-receivedTime);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Served: " + call.getMethodName() +
            " queueTime= " + qTime +
            " procesingTime= " + processingTime);
          rpcMetrics.rpcQueueTime.inc(qTime);
          rpcMetrics.rpcProcessingTime.inc(processingTime);
        }
        rpcMetrics.rpcQueueTime.inc(qTime);
        rpcMetrics.rpcProcessingTime.inc(processingTime);
        rpcMetrics.inc(call.getMethodName(), processingTime);
        if (verbose) log("Return: "+value);

        return new HbaseObjectWritable(method.getReturnType(), value);

      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
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
  }

  protected static void log(String value) {
    String v = value;
    if (v != null && v.length() > 55)
      v = v.substring(0, 55)+"...";
    LOG.info(v);
  }

  public static void connect(Socket socket, 
      SocketAddress endpoint, 
      int timeout) throws IOException {
    if (socket == null || endpoint == null || timeout < 0) {
      throw new IllegalArgumentException("Illegal argument for connect()");
    }
    SocketChannel ch = socket.getChannel();
    if (ch == null) {
      // let the default implementation handle it.
      socket.connect(endpoint, timeout);
    } else {
      connect(ch, endpoint, timeout);
    }
  }

  private static SelectorPool selector = new SelectorPool();

  public static void connect(SocketChannel channel, SocketAddress endpoint,
      int timeout) throws IOException {
    
    boolean blockingOn = channel.isBlocking();
    if (blockingOn) {
      channel.configureBlocking(false);
    }
    
    try { 
      if (channel.connect(endpoint)) {
        return;
      }

      long timeoutLeft = timeout;
      long endTime = (timeout > 0) ? (System.currentTimeMillis() + timeout): 0;
      
      while (true) {
        // we might have to call finishConnect() more than once
        // for some channels (with user level protocols)
        
        int ret = selector.select((SelectableChannel)channel, 
                                  SelectionKey.OP_CONNECT, timeoutLeft);
        
        if (ret > 0 && channel.finishConnect()) {
          return;
        }
        
        if (ret == 0 ||
            (timeout > 0 &&  
              (timeoutLeft = (endTime - System.currentTimeMillis())) <= 0)) {
          throw new SocketTimeoutException(
                    timeoutExceptionString(channel, timeout, 
                                           SelectionKey.OP_CONNECT));
        }
      }
    } catch (IOException e) {
      // javadoc for SocketChannel.connect() says channel should be closed.
      try {
        channel.close();
      } catch (IOException ignored) {}
      throw e;
    } finally {
      if (blockingOn && channel.isOpen()) {
        channel.configureBlocking(true);
      }
    }
  }

  private static String timeoutExceptionString(SelectableChannel channel,
      long timeout, int ops) {

    String waitingFor;
    switch(ops) {

    case SelectionKey.OP_READ :
      waitingFor = "read"; break;

    case SelectionKey.OP_WRITE :
      waitingFor = "write"; break;      

    case SelectionKey.OP_CONNECT :
      waitingFor = "connect"; break;

    default :
      waitingFor = "" + ops;  
    }

    return timeout + " millis timeout while " +
      "waiting for channel to be ready for " + 
      waitingFor + ". ch : " + channel;    
  }

  /**
   * This maintains a pool of selectors. These selectors are closed
   * once they are idle (unused) for a few seconds.
   */
  private static class SelectorPool {
    
    private static class SelectorInfo {
      Selector              selector;
      long                  lastActivityTime;
      LinkedList<SelectorInfo> queue; 
      
      void close() {
        if (selector != null) {
          try {
            selector.close();
          } catch (IOException e) {
            LOG.warn("Unexpected exception while closing selector : " +
                     StringUtils.stringifyException(e));
          }
        }
      }    
    }
    
    private static class ProviderInfo {
      SelectorProvider provider;
      LinkedList<SelectorInfo> queue; // lifo
      ProviderInfo next;
    }
    
    private static final long IDLE_TIMEOUT = 10 * 1000; // 10 seconds.
    
    private ProviderInfo providerList = null;
    
    /**
     * Waits on the channel with the given timeout using one of the 
     * cached selectors. It also removes any cached selectors that are
     * idle for a few seconds.
     * 
     * @param channel
     * @param ops
     * @param timeout
     * @return
     * @throws IOException
     */
    int select(SelectableChannel channel, int ops, long timeout) 
                                                   throws IOException {
     
      SelectorInfo info = get(channel);
      
      SelectionKey key = null;
      int ret = 0;
      
      try {
        while (true) {
          long start = (timeout == 0) ? 0 : System.currentTimeMillis();

          key = channel.register(info.selector, ops);
          ret = info.selector.select(timeout);
          
          if (ret != 0) {
            return ret;
          }
          
          /* Sometimes select() returns 0 much before timeout for 
           * unknown reasons. So select again if required.
           */
          if (timeout > 0) {
            timeout -= System.currentTimeMillis() - start;
            if (timeout <= 0) {
              return 0;
            }
          }
          
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Interruped while waiting for " +
                                             "IO on channel " + channel +
                                             ". " + timeout + 
                                             " millis timeout left.");
          }
        }
      } finally {
        if (key != null) {
          key.cancel();
        }
        
        //clear the canceled key.
        try {
          info.selector.selectNow();
        } catch (IOException e) {
          LOG.info("Unexpected Exception while clearing selector : " +
                   StringUtils.stringifyException(e));
          // don't put the selector back.
          info.close();
          return ret; 
        }
        
        release(info);
      }
    }
    
    /**
     * Takes one selector from end of LRU list of free selectors.
     * If there are no selectors awailable, it creates a new selector.
     * Also invokes trimIdleSelectors(). 
     * 
     * @param channel
     * @return 
     * @throws IOException
     */
    private synchronized SelectorInfo get(SelectableChannel channel) 
                                                         throws IOException {
      SelectorInfo selInfo = null;
      
      SelectorProvider provider = channel.provider();
      
      // pick the list : rarely there is more than one provider in use.
      ProviderInfo pList = providerList;
      while (pList != null && pList.provider != provider) {
        pList = pList.next;
      }      
      if (pList == null) {
        //LOG.info("Creating new ProviderInfo : " + provider.toString());
        pList = new ProviderInfo();
        pList.provider = provider;
        pList.queue = new LinkedList<SelectorInfo>();
        pList.next = providerList;
        providerList = pList;
      }
      
      LinkedList<SelectorInfo> queue = pList.queue;
      
      if (queue.isEmpty()) {
        Selector selector = provider.openSelector();
        selInfo = new SelectorInfo();
        selInfo.selector = selector;
        selInfo.queue = queue;
      } else {
        selInfo = queue.removeLast();
      }
      
      trimIdleSelectors(System.currentTimeMillis());
      return selInfo;
    }
    
    /**
     * puts selector back at the end of LRU list of free selectos.
     * Also invokes trimIdleSelectors().
     * 
     * @param info
     */
    private synchronized void release(SelectorInfo info) {
      long now = System.currentTimeMillis();
      trimIdleSelectors(now);
      info.lastActivityTime = now;
      info.queue.addLast(info);
    }
    
    /**
     * Closes selectors that are idle for IDLE_TIMEOUT (10 sec). It does not
     * traverse the whole list, just over the one that have crossed 
     * the timeout.
     */
    private void trimIdleSelectors(long now) {
      long cutoff = now - IDLE_TIMEOUT;
      
      for(ProviderInfo pList=providerList; pList != null; pList=pList.next) {
        if (pList.queue.isEmpty()) {
          continue;
        }
        for(Iterator<SelectorInfo> it = pList.queue.iterator(); it.hasNext();) {
          SelectorInfo info = it.next();
          if (info.lastActivityTime > cutoff) {
            break;
          }
          it.remove();
          info.close();
        }
      }
    }
  }

}
