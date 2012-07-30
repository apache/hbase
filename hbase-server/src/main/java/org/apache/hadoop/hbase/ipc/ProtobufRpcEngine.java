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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.DataOutputOutputStream;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestBody;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.*;
import org.apache.hadoop.hbase.util.Objects;
import org.apache.hadoop.hbase.util.ProtoUtil;

import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
/**
 * The {@link RpcEngine} implementation for ProtoBuf-based RPCs.
 */
@InterfaceAudience.Private
class ProtobufRpcEngine implements RpcEngine {
  private static final Log LOG =
      LogFactory.getLog("org.apache.hadoop.hbase.ipc.ProtobufRpcEngine");
  protected final static ClientCache CLIENTS = new ClientCache();
  @Override
  public VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol, long clientVersion,
      InetSocketAddress addr, User ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout) throws IOException {
    final Invoker invoker = new Invoker(protocol, addr, ticket, conf, factory,
        rpcTimeout);
    return (VersionedProtocol)Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[]{protocol}, invoker);
  }

  @Override
  public void stopProxy(VersionedProtocol proxy) {
    if (proxy!=null) {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    }
  }

  @Override
  public Server getServer(Class<? extends VersionedProtocol> protocol,
      Object instance, Class<?>[] ifaces, String bindAddress, int port,
      int numHandlers, int metaHandlerCount, boolean verbose,
      Configuration conf, int highPriorityLevel) throws IOException {
    return new Server(instance, ifaces, conf, bindAddress, port, numHandlers,
        metaHandlerCount, verbose, highPriorityLevel);
  }
  private static class Invoker implements InvocationHandler {
    private final Map<String, Message> returnTypes =
        new ConcurrentHashMap<String, Message>();
    private Class<? extends VersionedProtocol> protocol;
    private InetSocketAddress address;
    private User ticket;
    private HBaseClient client;
    private boolean isClosed = false;
    final private int rpcTimeout;
    private final long clientProtocolVersion;

    public Invoker(Class<? extends VersionedProtocol> protocol,
        InetSocketAddress addr, User ticket, Configuration conf,
        SocketFactory factory, int rpcTimeout) throws IOException {
      this.protocol = protocol;
      this.address = addr;
      this.ticket = ticket;
      this.client = CLIENTS.getClient(conf, factory, RpcResponseWritable.class);
      this.rpcTimeout = rpcTimeout;
      Long version = Invocation.PROTOCOL_VERSION.get(protocol);
      if (version != null) {
        this.clientProtocolVersion = version;
      } else {
        try {
          this.clientProtocolVersion = HBaseRPC.getProtocolVersion(protocol);
        } catch (NoSuchFieldException e) {
          throw new RuntimeException("Exception encountered during " +
                                      protocol, e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Exception encountered during " +
                                      protocol, e);
        }
      }
    }

    private RpcRequestBody constructRpcRequest(Method method,
        Object[] params) throws ServiceException {
      RpcRequestBody rpcRequest;
      RpcRequestBody.Builder builder = RpcRequestBody.newBuilder();
      builder.setMethodName(method.getName());
      Message param;
      int length = params.length;
      if (length == 2) {
        // RpcController + Message in the method args
        // (generated code from RPC bits in .proto files have RpcController)
        param = (Message)params[1];
      } else if (length == 1) { // Message
        param = (Message)params[0];
      } else {
        throw new ServiceException("Too many parameters for request. Method: ["
            + method.getName() + "]" + ", Expected: 2, Actual: "
            + params.length);
      }
      builder.setRequest(param.toByteString());
      builder.setClientProtocolVersion(clientProtocolVersion);
      rpcRequest = builder.build();
      return rpcRequest;
    }

    /**
     * This is the client side invoker of RPC method. It only throws
     * ServiceException, since the invocation proxy expects only
     * ServiceException to be thrown by the method in case protobuf service.
     *
     * ServiceException has the following causes:
     * <ol>
     * <li>Exceptions encountered on the client side in this method are
     * set as cause in ServiceException as is.</li>
     * <li>Exceptions from the server are wrapped in RemoteException and are
     * set as cause in ServiceException</li>
     * </ol>
     *
     * Note that the client calling protobuf RPC methods, must handle
     * ServiceException by getting the cause from the ServiceException. If the
     * cause is RemoteException, then unwrap it to get the exception thrown by
     * the server.
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws ServiceException {
      long startTime = 0;
      if (LOG.isDebugEnabled()) {
        startTime = System.currentTimeMillis();
      }

      RpcRequestBody rpcRequest = constructRpcRequest(method, args);
      RpcResponseWritable val = null;
      try {
        val = (RpcResponseWritable) client.call(
            new RpcRequestWritable(rpcRequest), address, protocol, ticket,
            rpcTimeout);

        if (LOG.isDebugEnabled()) {
          long callTime = System.currentTimeMillis() - startTime;
          LOG.debug("Call: " + method.getName() + " " + callTime);
        }

        Message protoType = null;
        protoType = getReturnProtoType(method);
        Message returnMessage;
        returnMessage = protoType.newBuilderForType()
            .mergeFrom(val.responseMessage).build();
        return returnMessage;
      } catch (Throwable e) {
        throw new ServiceException(e);
      }
    }

    synchronized protected void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    private Message getReturnProtoType(Method method) throws Exception {
      if (returnTypes.containsKey(method.getName())) {
        return returnTypes.get(method.getName());
      }

      Class<?> returnType = method.getReturnType();
      Method newInstMethod = returnType.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      Message protoType = (Message) newInstMethod.invoke(null, (Object[]) null);
      returnTypes.put(method.getName(), protoType);
      return protoType;
    }
  }

  /**
   * Writable Wrapper for Protocol Buffer Requests
   */
  private static class RpcRequestWritable implements Writable {
    RpcRequestBody message;

    @SuppressWarnings("unused")
    public RpcRequestWritable() {
    }

    RpcRequestWritable(RpcRequestBody message) {
      this.message = message;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      ((Message)message).writeDelimitedTo(
          DataOutputOutputStream.constructOutputStream(out));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = ProtoUtil.readRawVarint32(in);
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      message = RpcRequestBody.parseFrom(bytes);
    }
    
    public int getSerializedSize() {
      return message.getSerializedSize();
    }

    @Override
    public String toString() {
      return " Client Protocol Version: " +
          message.getClientProtocolVersion() + " MethodName: " +
          message.getMethodName();
    }
  }

  /**
   * Writable Wrapper for Protocol Buffer Responses
   */
  private static class RpcResponseWritable implements Writable {
    byte[] responseMessage;

    @SuppressWarnings("unused")
    public RpcResponseWritable() {
    }

    public RpcResponseWritable(Message message) {
      this.responseMessage = message.toByteArray();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(responseMessage.length);
      out.write(responseMessage);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int length = in.readInt();
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      responseMessage = bytes;
    }
  }
  public static class Server extends WritableRpcEngine.Server {
    boolean verbose;
    Object instance;
    Class<?> implementation;
    private static final String WARN_RESPONSE_TIME =
        "hbase.ipc.warn.response.time";
    private static final String WARN_RESPONSE_SIZE =
        "hbase.ipc.warn.response.size";

    /** Default value for above params */
    private static final int DEFAULT_WARN_RESPONSE_TIME = 10000; // milliseconds
    private static final int DEFAULT_WARN_RESPONSE_SIZE = 100 * 1024 * 1024;

    /** Names for suffixed metrics */
    private static final String ABOVE_ONE_SEC_METRIC = ".aboveOneSec.";

    private final int warnResponseTime;
    private final int warnResponseSize;
    public Server(Object instance, final Class<?>[] ifaces,
        Configuration conf, String bindAddress,  int port,
        int numHandlers, int metaHandlerCount, boolean verbose,
        int highPriorityLevel)
        throws IOException {
      super(instance, ifaces, RpcRequestWritable.class, conf, bindAddress, port,
          numHandlers, metaHandlerCount, verbose, highPriorityLevel);
      this.verbose = verbose;
      this.instance = instance;
      this.implementation = instance.getClass();
      // create metrics for the advertised interfaces this server implements.
      String [] metricSuffixes = new String [] {ABOVE_ONE_SEC_METRIC};
      this.rpcMetrics.createMetrics(ifaces, false, metricSuffixes);

      this.warnResponseTime = conf.getInt(WARN_RESPONSE_TIME,
          DEFAULT_WARN_RESPONSE_TIME);
      this.warnResponseSize = conf.getInt(WARN_RESPONSE_SIZE,
          DEFAULT_WARN_RESPONSE_SIZE);
    }
    private final Map<String, Message> methodArg =
        new ConcurrentHashMap<String, Message>();
    private final Map<String, Method> methodInstances =
        new ConcurrentHashMap<String, Method>();
    @Override
    /**
     * This is a server side method, which is invoked over RPC. On success
     * the return response has protobuf response payload. On failure, the
     * exception name and the stack trace are returned in the protobuf response.
     */
    public Writable call(Class<? extends VersionedProtocol> protocol,
        Writable writableRequest, long receiveTime, MonitoredRPCHandler status)
        throws IOException {
      try {
        RpcRequestWritable request = (RpcRequestWritable) writableRequest;
        RpcRequestBody rpcRequest = request.message;
        String methodName = rpcRequest.getMethodName();
        Method method = getMethod(protocol, methodName);
        if (method == null) {
          throw new HBaseRPC.UnknownProtocolException("Method " + methodName +
              " doesn't exist in protocol " + protocol.getName());
        }

        /**
         * RPCs for a particular interface (ie protocol) are done using a
         * IPC connection that is setup using rpcProxy.
         * The rpcProxy's has a declared protocol name that is
         * sent form client to server at connection time.
         */
        //TODO: use the clientVersion to do protocol compatibility checks, and
        //this could be used here to handle complex use cases like deciding
        //which implementation of the protocol should be used to service the
        //current request, etc. Ideally, we shouldn't land up in a situation
        //where we need to support such a use case.
        //For now the clientVersion field is simply ignored 
        long clientVersion = rpcRequest.getClientProtocolVersion();

        if (verbose) {
          LOG.info("Call: protocol name=" + protocol.getName() +
              ", method=" + methodName);
        }

        status.setRPC(rpcRequest.getMethodName(), 
            new Object[]{rpcRequest.getRequest()}, receiveTime);
        status.setRPCPacket(writableRequest);
        status.resume("Servicing call");        
        //get an instance of the method arg type
        Message protoType = getMethodArgType(method);
        Message param = protoType.newBuilderForType()
            .mergeFrom(rpcRequest.getRequest()).build();
        Message result;
        Object impl = null;
        if (protocol.isAssignableFrom(this.implementation)) {
          impl = this.instance;
        } else {
          throw new HBaseRPC.UnknownProtocolException(protocol);
        }

        long startTime = System.currentTimeMillis();
        if (method.getParameterTypes().length == 2) {
          // RpcController + Message in the method args
          // (generated code from RPC bits in .proto files have RpcController)
          result = (Message)method.invoke(impl, null, param);
        } else if (method.getParameterTypes().length == 1) {
          // Message (hand written code usually has only a single argument)
          result = (Message)method.invoke(impl, param);
        } else {
          throw new ServiceException("Too many parameters for method: ["
              + method.getName() + "]" + ", allowed (at most): 2, Actual: "
              + method.getParameterTypes().length);
        }
        int processingTime = (int) (System.currentTimeMillis() - startTime);
        int qTime = (int) (startTime-receiveTime);
        if (TRACELOG.isDebugEnabled()) {
          TRACELOG.debug("Call #" + CurCall.get().id +
              "; Served: " + protocol.getSimpleName()+"#"+method.getName() +
              " queueTime=" + qTime +
              " processingTime=" + processingTime +
              " contents=" + Objects.describeQuantity(param));
        }
        rpcMetrics.rpcQueueTime.inc(qTime);
        rpcMetrics.rpcProcessingTime.inc(processingTime);
        rpcMetrics.inc(method.getName(), processingTime);
        if (verbose) {
          WritableRpcEngine.log("Return: "+result, LOG);
        }
        long responseSize = result.getSerializedSize();
        // log any RPC responses that are slower than the configured warn
        // response time or larger than configured warning size
        boolean tooSlow = (processingTime > warnResponseTime
            && warnResponseTime > -1);
        boolean tooLarge = (responseSize > warnResponseSize
            && warnResponseSize > -1);
        if (tooSlow || tooLarge) {
          // when tagging, we let TooLarge trump TooSmall to keep output simple
          // note that large responses will often also be slow.
          StringBuilder buffer = new StringBuilder(256);
          buffer.append(methodName);
          buffer.append("(");
          buffer.append(param.getClass().getName());
          buffer.append(")");
          buffer.append(", client version="+clientVersion);
          logResponse(new Object[]{rpcRequest.getRequest()}, 
              methodName, buffer.toString(), (tooLarge ? "TooLarge" : "TooSlow"),
              status.getClient(), startTime, processingTime, qTime,
              responseSize);
          // provides a count of log-reported slow responses
          if (tooSlow) {
            rpcMetrics.rpcSlowResponseTime.inc(processingTime);
          }
        }
        if (processingTime > 1000) {
          // we use a hard-coded one second period so that we can clearly
          // indicate the time period we're warning about in the name of the
          // metric itself
          rpcMetrics.inc(method.getName() + ABOVE_ONE_SEC_METRIC,
              processingTime);
        }
        return new RpcResponseWritable(result);
      } catch (InvocationTargetException e) {
        Throwable target = e.getTargetException();
        if (target instanceof IOException) {
          throw (IOException)target;
        }
        if (target instanceof ServiceException) {
          throw ProtobufUtil.getRemoteException((ServiceException)target);
        }
        IOException ioe = new IOException(target.toString());
        ioe.setStackTrace(target.getStackTrace());
        throw ioe;
      } catch (Throwable e) {
        if (!(e instanceof IOException)) {
          LOG.error("Unexpected throwable object ", e);
        }
        IOException ioe = new IOException(e.toString());
        ioe.setStackTrace(e.getStackTrace());
        throw ioe;
      }
    }

    private Method getMethod(Class<? extends VersionedProtocol> protocol,
        String methodName) {
      Method method = methodInstances.get(methodName);
      if (method != null) {
        return method;
      }
      Method[] methods = protocol.getMethods();
      LOG.warn("Methods length : " + methods.length);
      for (Method m : methods) {
        if (m.getName().equals(methodName)) {
          m.setAccessible(true);
          methodInstances.put(methodName, m);
          return m;
        }
      }
      return null;
    }

    private Message getMethodArgType(Method method) throws Exception {
      Message protoType = methodArg.get(method.getName());
      if (protoType != null) {
        return protoType;
      }

      Class<?>[] args = method.getParameterTypes();
      Class<?> arg;
      if (args.length == 2) {
        // RpcController + Message in the method args
        // (generated code from RPC bits in .proto files have RpcController)
        arg = args[1];
      } else if (args.length == 1) {
        arg = args[0];
      } else {
        //unexpected
        return null;
      }
      //in the protobuf methods, args[1] is the only significant argument
      Method newInstMethod = arg.getMethod("getDefaultInstance");
      newInstMethod.setAccessible(true);
      protoType = (Message) newInstMethod.invoke(null, (Object[]) null);
      methodArg.put(method.getName(), protoType);
      return protoType;
    }
  }
}