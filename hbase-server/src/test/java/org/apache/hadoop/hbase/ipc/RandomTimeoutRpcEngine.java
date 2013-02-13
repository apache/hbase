/**
 * Copyright The Apache Software Foundation
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

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.SocketFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.IpcProtocol;
import org.apache.hadoop.hbase.security.User;

import com.google.protobuf.ServiceException;

/**
 * RpcEngine that random throws a SocketTimeoutEngine for testing.
 * Make sure to call setProtocolEngine to have the client actually use the RpcEngine
 * for a specific protocol
 */
public class RandomTimeoutRpcEngine extends ProtobufRpcClientEngine {

  private static final Random RANDOM = new Random(System.currentTimeMillis());
  public static double chanceOfTimeout = 0.3;
  private static AtomicInteger invokations = new AtomicInteger();

  public RandomTimeoutRpcEngine(Configuration conf) {
    super(conf);
  }

  @Override
  public <T extends IpcProtocol> T getProxy(
      Class<T> protocol, InetSocketAddress addr, Configuration conf, int rpcTimeout)
  throws IOException {
    // Start up the requested-for proxy so we can pass-through calls to the underlying
    // RpcEngine.  Also instantiate and return our own proxy (RandomTimeoutInvocationHandler)
    // that will either throw exceptions or pass through to the underlying proxy.
    T actualProxy = super.getProxy(protocol, addr, conf, rpcTimeout);
    RandomTimeoutInvocationHandler invoker =
      new RandomTimeoutInvocationHandler(actualProxy);
    T wrapperProxy = (T)Proxy.newProxyInstance(
      protocol.getClassLoader(), new Class[]{protocol}, invoker);
    return wrapperProxy;
  }

  /**
   * @return the number of times the invoker has been invoked
   */
  public static int getNumberOfInvocations() {
    return invokations.get();
  }

  static class RandomTimeoutInvocationHandler implements InvocationHandler {
    private IpcProtocol actual = null;

    public RandomTimeoutInvocationHandler(IpcProtocol actual) {
      this.actual = actual;
    }

    public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
      RandomTimeoutRpcEngine.invokations.getAndIncrement();
      if (RANDOM.nextFloat() < chanceOfTimeout) {
        // throw a ServiceException, becuase that is the only exception type that
      	// {@link ProtobufRpcEngine} throws.  If this RpcEngine is used with a different
      	// "actual" type, this may not properly mimic the underlying RpcEngine.
        throw new ServiceException(new SocketTimeoutException("fake timeout"));
      }
      return Proxy.getInvocationHandler(actual).invoke(proxy, method, args);
    }
  }
}