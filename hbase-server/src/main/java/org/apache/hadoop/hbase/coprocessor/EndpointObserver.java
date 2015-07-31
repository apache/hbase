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

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;

import com.google.protobuf.Message;
import com.google.protobuf.Service;

/**
 * Coprocessors implement this interface to observe and mediate endpoint invocations
 * on a region.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface EndpointObserver extends Coprocessor {

  /**
   * Called before an Endpoint service method is invoked.
   * The request message can be altered by returning a new instance. Throwing an
   * exception will abort the invocation.
   * Calling {@link org.apache.hadoop.hbase.coprocessor.ObserverContext#bypass()} has no
   * effect in this hook.
   * @param ctx the environment provided by the region server
   * @param service the endpoint service
   * @param methodName the invoked service method
   * @param request the request message
   * @return the possibly modified message
   * @throws IOException
   */
  Message preEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> ctx, Service service,
      String methodName, Message request) throws IOException;

  /**
   * Called after an Endpoint service method is invoked. The response message can be
   * altered using the builder.
   * @param ctx the environment provided by the region server
   * @param service the endpoint service
   * @param methodName the invoked service method
   * @param request the request message
   * @param responseBuilder the response message builder
   * @throws IOException
   */
  void postEndpointInvocation(ObserverContext<RegionCoprocessorEnvironment> ctx, Service service,
      String methodName, Message request, Message.Builder responseBuilder) throws IOException;

}
