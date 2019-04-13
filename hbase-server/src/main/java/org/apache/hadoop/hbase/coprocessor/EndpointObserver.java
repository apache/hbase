/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.coprocessor;

import com.google.protobuf.Message;
import com.google.protobuf.Service;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Coprocessors implement this interface to observe and mediate endpoint invocations
 * on a region.
 * <br><br>
 *
 * <h3>Exception Handling</h3>
 * For all functions, exception handling is done as follows:
 * <ul>
 *   <li>Exceptions of type {@link IOException} are reported back to client.</li>
 *   <li>For any other kind of exception:
 *     <ul>
 *       <li>If the configuration {@link CoprocessorHost#ABORT_ON_ERROR_KEY} is set to true, then
 *         the server aborts.</li>
 *       <li>Otherwise, coprocessor is removed from the server and
 *         {@link org.apache.hadoop.hbase.DoNotRetryIOException} is returned to the client.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface EndpointObserver {

  /**
   * Called before an Endpoint service method is invoked.
   * The request message can be altered by returning a new instance. Throwing an
   * exception will abort the invocation.
   * Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   * @param ctx the environment provided by the region server
   * @param service the endpoint service
   * @param request  Request message expected by given {@code Service}'s method (by the name
   *   {@code methodName}).
   * @param methodName the invoked service method
   * @return the possibly modified message
   */
  default Message preEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> ctx,
      Service service, String methodName, Message request) throws IOException {
    return request;
  }

  /**
   * Called after an Endpoint service method is invoked. The response message can be
   * altered using the builder.
   * @param ctx the environment provided by the region server
   * @param service the endpoint service
   * @param methodName the invoked service method
   * @param request  Request message expected by given {@code Service}'s method (by the name
   *   {@code methodName}).
   * @param responseBuilder Builder for final response to the client, with original response from
   *   Service's method merged into it.
   */
  default void postEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> ctx,
      Service service, String methodName, Message request, Message.Builder responseBuilder)
      throws IOException {}
}
