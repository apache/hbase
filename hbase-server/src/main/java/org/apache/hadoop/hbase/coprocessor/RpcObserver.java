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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

/**
 * Coprocessors implement this interface to observe and mediate RPC events in Master and RS
 * instances.
 * <p>
 * Since most implementations will be interested in only a subset of hooks, this class uses
 * 'default' functions to avoid having to add unnecessary overrides. When the functions are
 * non-empty, it's simply to satisfy the compiler by returning value of expected (non-void) type. It
 * is done in a way that these default definitions act as no-op. So our suggestion to implementation
 * would be to not call these 'default' methods from overrides.
 * <p>
 * <h3>Exception Handling</h3><br>
 * For all functions, exception handling is done as follows:
 * <ul>
 * <li>Exceptions of type {@link IOException} are reported back to client.</li>
 * <li>For any other kind of exception:
 * <ul>
 * <li>Be aware that this coprocessor doesn't support abortion. If the configuration
 * {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, the event will be logged, but the RPC
 * server won't be aborted.</li>
 * <li>Otherwise, coprocessor is removed from the server.</li>
 * </ul>
 * </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RpcObserver {

  /**
   * Called before authorizing connection
   * @param ctx the coprocessor instance's environment
   */
  default void preAuthorizeConnection(ObserverContext<RpcCoprocessorEnvironment> ctx,
    RPCProtos.ConnectionHeader connectionHeader, InetAddress remoteAddr) throws IOException {
  }

  /**
   * Called after successfully authorizing connection
   * @param ctx                    the coprocessor instance's environment
   * @param userName               the user name
   * @param clientCertificateChain list of peer certificates from SSL connection
   */
  default void postAuthorizeConnection(ObserverContext<RpcCoprocessorEnvironment> ctx,
    String userName, X509Certificate[] clientCertificateChain) throws IOException {
  }
}
