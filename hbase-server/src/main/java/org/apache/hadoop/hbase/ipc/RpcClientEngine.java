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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.IpcProtocol;
import org.apache.hadoop.hbase.security.User;

import java.io.IOException;
import java.net.InetSocketAddress;

/** An RPC implementation for the client */
@InterfaceAudience.Private
public interface RpcClientEngine {
  /** Construct a client-side proxy object. */
  <T extends IpcProtocol> T getProxy(Class<T> protocol, InetSocketAddress addr,
      Configuration conf, int rpcTimeout) throws IOException;

  /** Shutdown this instance */
  void close();
}