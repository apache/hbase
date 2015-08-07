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

package org.apache.hadoop.hbase.ipc;

import java.io.IOException;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RegionServerCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

/**
 * Provides clients with an RPC connection to call coprocessor endpoint {@link com.google.protobuf.Service}s
 * against a given table region.  An instance of this class may be obtained
 * by calling {@link org.apache.hadoop.hbase.client.HTable#coprocessorService(byte[])},
 * but should normally only be used in creating a new {@link com.google.protobuf.Service} stub to call the endpoint
 * methods.
 * @see org.apache.hadoop.hbase.client.HTable#coprocessorService(byte[])
 */
@InterfaceAudience.Private
public class RegionCoprocessorRpcChannel extends CoprocessorRpcChannel{
  private static Log LOG = LogFactory.getLog(RegionCoprocessorRpcChannel.class);

  private final HConnection connection;
  private final TableName table;
  private final byte[] row;
  private byte[] lastRegion;
  private int operationTimeout;

  private RpcRetryingCallerFactory rpcFactory;

  public RegionCoprocessorRpcChannel(HConnection conn, TableName table, byte[] row) {
    this.connection = conn;
    this.table = table;
    this.row = row;
    this.rpcFactory = RpcRetryingCallerFactory.instantiate(conn.getConfiguration(), null);
    this.operationTimeout = conn.getConfiguration().getInt(
        HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
  }

  @Override
  protected Message callExecService(Descriptors.MethodDescriptor method,
                                  Message request, Message responsePrototype)
      throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Call: "+method.getName()+", "+request.toString());
    }

    if (row == null) {
      throw new IllegalArgumentException("Missing row property for remote region location");
    }

    final ClientProtos.CoprocessorServiceCall call =
        ClientProtos.CoprocessorServiceCall.newBuilder()
            .setRow(ByteStringer.wrap(row))
            .setServiceName(method.getService().getFullName())
            .setMethodName(method.getName())
            .setRequest(request.toByteString()).build();
    RegionServerCallable<CoprocessorServiceResponse> callable =
        new RegionServerCallable<CoprocessorServiceResponse>(connection, table, row) {
          public CoprocessorServiceResponse call(int callTimeout) throws Exception {
            byte[] regionName = getLocation().getRegionInfo().getRegionName();
            return ProtobufUtil.execService(getStub(), call, regionName);
          }
        };
    CoprocessorServiceResponse result = rpcFactory.<CoprocessorServiceResponse> newCaller()
        .callWithRetries(callable, operationTimeout);
    Message response = null;
    if (result.getValue().hasValue()) {
      Message.Builder builder = responsePrototype.newBuilderForType();
      ProtobufUtil.mergeFrom(builder, result.getValue().getValue());
      response = builder.build();
    } else {
      response = responsePrototype.getDefaultInstanceForType();
    }
    lastRegion = result.getRegion().getValue().toByteArray();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Result is region=" + Bytes.toStringBinary(lastRegion) + ", value=" + response);
    }
    return response;
  }

  public byte[] getLastRegion() {
    return lastRegion;
  }
}
