/**
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

import com.google.protobuf.Message;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MasterAdminProtocol;
import org.apache.hadoop.hbase.MasterMonitorProtocol;
import org.apache.hadoop.hbase.RegionServerStatusProtocol;
import org.apache.hadoop.hbase.client.AdminProtocol;
import org.apache.hadoop.hbase.client.ClientProtocol;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RpcRequestBody;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.ipc.RemoteException;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProtobufRpcClientEngine implements RpcClientEngine {

  private static final Log LOG =
      LogFactory.getLog("org.apache.hadoop.hbase.ipc.ProtobufRpcClientEngine");

  ProtobufRpcClientEngine() {
    super();
  }

  protected final static ClientCache CLIENTS = new ClientCache();
  @Override
  public VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol, long clientVersion,
      InetSocketAddress addr, User ticket, Configuration conf,
      SocketFactory factory, int rpcTimeout) throws IOException {
    final Invoker invoker = new Invoker(protocol, addr, ticket, conf, factory,
        rpcTimeout);
    return (VersionedProtocol) Proxy.newProxyInstance(
        protocol.getClassLoader(), new Class[]{protocol}, invoker);
  }

  @Override
  public void stopProxy(VersionedProtocol proxy) {
    if (proxy!=null) {
      ((Invoker)Proxy.getInvocationHandler(proxy)).close();
    }
  }

  static class Invoker implements InvocationHandler {
    private static final Map<String, Message> returnTypes =
        new ConcurrentHashMap<String, Message>();
    private Class<? extends VersionedProtocol> protocol;
    private InetSocketAddress address;
    private User ticket;
    private HBaseClient client;
    private boolean isClosed = false;
    final private int rpcTimeout;
    private final long clientProtocolVersion;

    // For generated protocol classes which don't have VERSION field,
    // such as protobuf interfaces.
    static final Map<Class<?>, Long>
      PROTOCOL_VERSION = new HashMap<Class<?>, Long>();
    static {
      PROTOCOL_VERSION.put(ClientService.BlockingInterface.class,
        Long.valueOf(ClientProtocol.VERSION));
      PROTOCOL_VERSION.put(AdminService.BlockingInterface.class,
        Long.valueOf(AdminProtocol.VERSION));
      PROTOCOL_VERSION.put(RegionServerStatusService.BlockingInterface.class,
        Long.valueOf(RegionServerStatusProtocol.VERSION));
      PROTOCOL_VERSION.put(MasterMonitorProtocol.class,Long.valueOf(MasterMonitorProtocol.VERSION));
      PROTOCOL_VERSION.put(MasterAdminProtocol.class,Long.valueOf(MasterAdminProtocol.VERSION));
    }

    public Invoker(Class<? extends VersionedProtocol> protocol,
                   InetSocketAddress addr, User ticket, Configuration conf,
                   SocketFactory factory, int rpcTimeout) throws IOException {
      this.protocol = protocol;
      this.address = addr;
      this.ticket = ticket;
      this.client = CLIENTS.getClient(conf, factory);
      this.rpcTimeout = rpcTimeout;
      Long version = PROTOCOL_VERSION.get(protocol);
      if (version != null) {
        this.clientProtocolVersion = version;
      } else {
        try {
          this.clientProtocolVersion = HBaseClientRPC.getProtocolVersion(protocol);
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
      builder.setRequestClassName(param.getClass().getName());
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
      Message val = null;
      try {
        val = client.call(rpcRequest, address, protocol, ticket, rpcTimeout);

        if (LOG.isDebugEnabled()) {
          long callTime = System.currentTimeMillis() - startTime;
          if (LOG.isTraceEnabled()) LOG.trace("Call: " + method.getName() + " " + callTime);
        }
        return val;
      } catch (Throwable e) {
        if (e instanceof RemoteException) {
          Throwable cause = ((RemoteException)e).unwrapRemoteException();
          throw new ServiceException(cause);
        }
        throw new ServiceException(e);
      }
    }

    synchronized protected void close() {
      if (!isClosed) {
        isClosed = true;
        CLIENTS.stopClient(client);
      }
    }

    static Message getReturnProtoType(Method method) throws Exception {
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

}
