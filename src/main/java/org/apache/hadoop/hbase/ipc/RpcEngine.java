/*
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

import java.lang.reflect.Method;
import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.conf.Configuration;

/** An RPC implementation. */
@InterfaceAudience.Private
public interface RpcEngine extends Configurable {

  /* Client-related methods */
  /** Construct a client-side proxy object. */
  <T extends VersionedProtocol> T getProxy(Class<T> protocol,
                                           long clientVersion, InetSocketAddress addr,
                                           Configuration conf, int rpcTimeout) throws IOException;

  /** Shutdown this instance */
  void close();

  /** Expert: Make multiple, parallel calls to a set of servers. */
  Object[] call(Method method, Object[][] params, InetSocketAddress[] addrs,
                Class<? extends VersionedProtocol> protocol,
                User ticket, Configuration conf)
    throws IOException, InterruptedException;

  /* Server-related methods */
  /** Construct a server for a protocol implementation instance. */
  RpcServer getServer(Class<? extends VersionedProtocol> protocol, Object instance,
                       Class<?>[] ifaces, String bindAddress,
                       int port, int numHandlers, int metaHandlerCount,
                       boolean verbose, Configuration conf, int highPriorityLevel)
      throws IOException;

}
