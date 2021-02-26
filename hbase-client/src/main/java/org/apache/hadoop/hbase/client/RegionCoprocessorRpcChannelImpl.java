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
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;

/**
 * The implementation of a region based coprocessor rpc channel.
 */
@InterfaceAudience.Private
class RegionCoprocessorRpcChannelImpl implements RpcChannel {

  private final AsyncConnectionImpl conn;

  private final TableName tableName;

  private final RegionInfo region;

  private final byte[] row;

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  RegionCoprocessorRpcChannelImpl(AsyncConnectionImpl conn, TableName tableName, RegionInfo region,
      byte[] row, long rpcTimeoutNs, long operationTimeoutNs) {
    this.conn = conn;
    this.tableName = tableName;
    this.region = region;
    this.row = row;
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.operationTimeoutNs = operationTimeoutNs;
  }

  private CompletableFuture<Message> rpcCall(MethodDescriptor method, Message request,
      Message responsePrototype, HBaseRpcController controller, HRegionLocation loc,
      ClientService.Interface stub) {
    CompletableFuture<Message> future = new CompletableFuture<>();
    if (region != null
        && !Bytes.equals(loc.getRegionInfo().getRegionName(), region.getRegionName())) {
      future.completeExceptionally(new DoNotRetryIOException(
          "Region name is changed, expected " + region.getRegionNameAsString() + ", actual "
              + loc.getRegionInfo().getRegionNameAsString()));
      return future;
    }
    CoprocessorServiceRequest csr = CoprocessorRpcUtils.getCoprocessorServiceRequest(method,
      request, row, loc.getRegionInfo().getRegionName());
    stub.execService(controller, csr,
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
      conn.callerFactory.<Message> single().table(tableName).row(row)
        .locateType(RegionLocateType.CURRENT).rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .action((c, l, s) -> rpcCall(method, request, responsePrototype, c, l, s)).call(),
      (r, e) -> {
        if (e != null) {
          ((ClientCoprocessorRpcController) controller).setFailed(e);
        }
        done.run(r);
      });
  }
}
