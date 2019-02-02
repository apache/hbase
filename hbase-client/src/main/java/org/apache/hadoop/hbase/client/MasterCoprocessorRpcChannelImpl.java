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

import static org.apache.hadoop.hbase.util.FutureUtils.addListener;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.MasterRequestCallerBuilder;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;

/**
 * The implementation of a master based coprocessor rpc channel.
 */
@InterfaceAudience.Private
class MasterCoprocessorRpcChannelImpl implements RpcChannel {

  MasterRequestCallerBuilder<Message> callerBuilder;

  MasterCoprocessorRpcChannelImpl(MasterRequestCallerBuilder<Message> callerBuilder) {
    this.callerBuilder = callerBuilder;
  }

  private CompletableFuture<Message> rpcCall(MethodDescriptor method, Message request,
      Message responsePrototype, HBaseRpcController controller, MasterService.Interface stub) {
    CompletableFuture<Message> future = new CompletableFuture<>();
    CoprocessorServiceRequest csr =
        CoprocessorRpcUtils.getCoprocessorServiceRequest(method, request);
    stub.execMasterService(
      controller,
      csr,
      new org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback<CoprocessorServiceResponse>() {

        @Override
        public void run(CoprocessorServiceResponse resp) {
          if (controller.failed()) {
            future.completeExceptionally(controller.getFailed());
          } else {
            try {
              future.complete(CoprocessorRpcUtils.getResponse(resp, responsePrototype));
            } catch (IOException e) {
              future.completeExceptionally(e);
            }
          }
        }
      });
    return future;
  }

  @Override
  public void callMethod(MethodDescriptor method, RpcController controller, Message request,
      Message responsePrototype, RpcCallback<Message> done) {
    addListener(
      callerBuilder.action((c, s) -> rpcCall(method, request, responsePrototype, c, s)).call(),
      ((r, e) -> {
        if (e != null) {
          ((ClientCoprocessorRpcController) controller).setFailed(e);
        }
        done.run(r);
      }));
  }
}
