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
package org.apache.hadoop.hbase.client;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcChannel;

/**
 * Additional Asynchronous Admin capabilities for clients.
 */
@InterfaceAudience.Public
public final class AsyncAdminClientUtils {

  private AsyncAdminClientUtils() {
  }

  /**
   * Execute the given coprocessor call on all region servers.
   * <p>
   * The {@code stubMaker} is just a delegation to the {@code newStub} call. Usually it is only a
   * one line lambda expression, like:
   *
   * <pre>
   * channel -&gt; xxxService.newStub(channel)
   * </pre>
   *
   * @param asyncAdmin the asynchronous administrative API for HBase.
   * @param stubMaker  a delegation to the actual {@code newStub} call.
   * @param callable   a delegation to the actual protobuf rpc call. See the comment of
   *                   {@link ServiceCaller} for more details.
   * @param <S>        the type of the asynchronous stub
   * @param <R>        the type of the return value
   * @return Map of each region server to its result of the protobuf rpc call, wrapped by a
   *         {@link CompletableFuture}.
   * @see ServiceCaller
   */
  public static <S, R> CompletableFuture<Map<ServerName, Object>>
    coprocessorServiceOnAllRegionServers(AsyncAdmin asyncAdmin, Function<RpcChannel, S> stubMaker,
      ServiceCaller<S, R> callable) {
    CompletableFuture<Map<ServerName, Object>> future = new CompletableFuture<>();
    FutureUtils.addListener(asyncAdmin.getRegionServers(), (regionServers, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      Map<ServerName, Object> resultMap = new ConcurrentHashMap<>();
      for (ServerName regionServer : regionServers) {
        FutureUtils.addListener(asyncAdmin.coprocessorService(stubMaker, callable, regionServer),
          (server, err) -> {
            if (err != null) {
              resultMap.put(regionServer, err);
            } else {
              resultMap.put(regionServer, server);
            }
            if (resultMap.size() == regionServers.size()) {
              future.complete(Collections.unmodifiableMap(resultMap));
            }
          });
      }
    });
    return future;
  }
}
