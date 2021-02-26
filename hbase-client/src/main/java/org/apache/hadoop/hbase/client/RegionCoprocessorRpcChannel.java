/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;

/**
 * Provides clients with an RPC connection to call Coprocessor Endpoint
 * {@link com.google.protobuf.Service}s
 * against a given table region.  An instance of this class may be obtained
 * by calling {@link org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])},
 * but should normally only be used in creating a new {@link com.google.protobuf.Service} stub to
 * call the endpoint methods.
 * @see org.apache.hadoop.hbase.client.Table#coprocessorService(byte[])
 */
@InterfaceAudience.Private
class RegionCoprocessorRpcChannel extends SyncCoprocessorRpcChannel {
  private static final Logger LOG = LoggerFactory.getLogger(RegionCoprocessorRpcChannel.class);
  private final TableName table;
  private final byte [] row;
  private final ClusterConnection conn;
  private byte[] lastRegion;
  private final int operationTimeout;
  private final RpcRetryingCallerFactory rpcCallerFactory;

  /**
   * Constructor
   * @param conn connection to use
   * @param table to connect to
   * @param row to locate region with
   */
  RegionCoprocessorRpcChannel(ClusterConnection conn, TableName table, byte[] row) {
    this.table = table;
    this.row = row;
    this.conn = conn;
    this.operationTimeout = conn.getConnectionConfiguration().getOperationTimeout();
    this.rpcCallerFactory = conn.getRpcRetryingCallerFactory();
  }

  @Override
  protected Message callExecService(final RpcController controller,
      final Descriptors.MethodDescriptor method, final Message request,
      final Message responsePrototype)
  throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Call: " + method.getName() + ", " + request.toString());
    }
    if (row == null) {
      throw new NullPointerException("Can't be null!");
    }
    ClientServiceCallable<CoprocessorServiceResponse> callable =
      new ClientServiceCallable<CoprocessorServiceResponse>(this.conn,
              this.table, this.row, this.conn.getRpcControllerFactory().newController(), HConstants.PRIORITY_UNSET) {
      @Override
      protected CoprocessorServiceResponse rpcCall() throws Exception {
        byte [] regionName = getLocation().getRegionInfo().getRegionName();
        CoprocessorServiceRequest csr =
            CoprocessorRpcUtils.getCoprocessorServiceRequest(method, request, row, regionName);
        return getStub().execService(getRpcController(), csr);
      }
    };
    CoprocessorServiceResponse result =
        this.rpcCallerFactory.<CoprocessorServiceResponse> newCaller().callWithRetries(callable,
            operationTimeout);
    this.lastRegion = result.getRegion().getValue().toByteArray();
    return CoprocessorRpcUtils.getResponse(result, responsePrototype);
  }

  /**
   * Get last region this RpcChannel communicated with
   * @return region name as byte array
   */
  public byte[] getLastRegion() {
    return lastRegion;
  }
}
