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

import com.google.protobuf.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;

/**
 * Provides clients with an RPC connection to call coprocessor endpoint {@link Service}s
 * against a given table region.  An instance of this class may be obtained
 * by calling {@link org.apache.hadoop.hbase.client.HTable#coprocessorService(byte[])},
 * but should normally only be used in creating a new {@link Service} stub to call the endpoint
 * methods.
 * @see org.apache.hadoop.hbase.client.HTable#coprocessorService(byte[])
 */
@InterfaceAudience.Private
public class CoprocessorRpcChannel implements RpcChannel, BlockingRpcChannel {
  private static Log LOG = LogFactory.getLog(CoprocessorRpcChannel.class);

  private final HConnection connection;
  private final byte[] table;
  private final byte[] row;
  private byte[] lastRegion;

  public CoprocessorRpcChannel(HConnection conn, byte[] table, byte[] row) {
    this.connection = conn;
    this.table = table;
    this.row = row;
  }

  @Override
  public void callMethod(Descriptors.MethodDescriptor method,
                         RpcController controller,
                         Message request, Message responsePrototype,
                         RpcCallback<Message> callback) {
    Message response = null;
    try {
      response = callExecService(method, request, responsePrototype);
    } catch (IOException ioe) {
      LOG.warn("Call failed on IOException", ioe);
      ResponseConverter.setControllerException(controller, ioe);
    }
    if (callback != null) {
      callback.run(response);
    }
  }

  @Override
  public Message callBlockingMethod(Descriptors.MethodDescriptor method,
                                    RpcController controller,
                                    Message request, Message responsePrototype)
      throws ServiceException {
    try {
      return callExecService(method, request, responsePrototype);
    } catch (IOException ioe) {
      throw new ServiceException("Error calling method "+method.getFullName(), ioe);
    }
  }

  private Message callExecService(Descriptors.MethodDescriptor method,
                                  Message request, Message responsePrototype)
      throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Call: "+method.getName()+", "+request.toString());
    }

    if (row == null) {
      throw new IllegalArgumentException("Missing row property for remote region location");
    }

    final ClientProtos.CoprocessorServiceCall call =
        ClientProtos.CoprocessorServiceCall.newBuilder()
            .setRow(ByteString.copyFrom(row))
            .setServiceName(method.getService().getFullName())
            .setMethodName(method.getName())
            .setRequest(request.toByteString()).build();
    ServerCallable<ClientProtos.CoprocessorServiceResponse> callable =
        new ServerCallable<ClientProtos.CoprocessorServiceResponse>(connection, table, row) {
          public CoprocessorServiceResponse call() throws Exception {
            byte[] regionName = location.getRegionInfo().getRegionName();
            return ProtobufUtil.execService(server, call, regionName);
          }
        };
    CoprocessorServiceResponse result = callable.withRetries();
    Message response = null;
    if (result.getValue().hasValue()) {
      response = responsePrototype.newBuilderForType()
          .mergeFrom(result.getValue().getValue()).build();
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
