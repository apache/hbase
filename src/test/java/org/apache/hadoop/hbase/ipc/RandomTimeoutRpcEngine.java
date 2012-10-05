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
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Random;
import org.apache.hadoop.hbase.ipc.VersionedProtocol;
import javax.net.SocketFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;

/**
 * RpcEngine that random throws a SocketTimeoutEngine for testing.
 * Make sure to call setProtocolEngine to have the client actually use the RpcEngine
 * for a specific protocol
 */
public class RandomTimeoutRpcEngine extends WritableRpcEngine {

  private static final Random RANDOM = new Random(System.currentTimeMillis());
  public static double chanceOfTimeout = 0.3;

  public VersionedProtocol getProxy(
      Class<? extends VersionedProtocol> protocol, long clientVersion,
      InetSocketAddress addr, User ticket,
      Configuration conf, SocketFactory factory, int rpcTimeout) throws IOException {
    RandomTimeoutInvoker handler =
      new RandomTimeoutInvoker(protocol, addr, ticket, conf, factory, rpcTimeout);
    return super.getProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout, handler);
  }

  // Call this in order to set this class to run as the RpcEngine for the given protocol
  public static void setProtocolEngine(Configuration conf, Class protocol) {
    HBaseRPC.setProtocolEngine(conf, protocol, RandomTimeoutRpcEngine.class);
  }

  public static boolean isProxyForObject(Object proxy) {
    return HBaseRPC.getProxyEngine(proxy).getClass().equals(RandomTimeoutRpcEngine.class);
  }

  static class RandomTimeoutInvoker extends Invoker {

    public RandomTimeoutInvoker(Class<? extends VersionedProtocol> protocol,
        InetSocketAddress address, User ticket,
        Configuration conf, SocketFactory factory, int rpcTimeout) {
      super(protocol, address, ticket, conf, factory, rpcTimeout);
    }

    public Object invoke(Object proxy, Method method, Object[] args)
    throws Throwable {
      if (RANDOM.nextFloat() < chanceOfTimeout) {
        throw new SocketTimeoutException("fake timeout");
      }
      return super.invoke(proxy, method, args);
    }
  }
}
