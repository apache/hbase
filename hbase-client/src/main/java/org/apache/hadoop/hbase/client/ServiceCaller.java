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
package org.apache.hadoop.hbase.client;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Delegate to a protobuf rpc call.
 * <p>
 * Usually, it is just a simple lambda expression, like:
 *
 * <pre>
 * <code>
 * (stub, controller, rpcCallback) -> {
 *   XXXRequest request = ...; // prepare the request
 *   stub.xxx(controller, request, rpcCallback);
 * }
 * </code>
 * </pre>
 *
 * And if already have the {@code request}, the lambda expression will be:
 *
 * <pre>
 * <code>
 * (stub, controller, rpcCallback) -> stub.xxx(controller, request, rpcCallback)
 * </code>
 * </pre>
 *
 * @param <S> the type of the protobuf Service you want to call.
 * @param <R> the type of the return value.
 */
@InterfaceAudience.Public
@FunctionalInterface
public interface ServiceCaller<S, R> {

  /**
   * Represent the actual protobuf rpc call.
   * @param stub the asynchronous stub
   * @param controller the rpc controller, has already been prepared for you
   * @param rpcCallback the rpc callback, has already been prepared for you
   */
  void call(S stub, RpcController controller, RpcCallback<R> rpcCallback);
}
