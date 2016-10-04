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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CleanupBulkLoadResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.DelegationToken;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.token.Token;

/**
 * Client proxy for SecureBulkLoadProtocol used in conjunction with SecureBulkLoadEndpoint
 * @deprecated Use for backward compatibility testing only. Will be removed when
 *             SecureBulkLoadEndpoint is not supported.
 */
@InterfaceAudience.Private
public class SecureBulkLoadEndpointClient {
  private Table table;

  public SecureBulkLoadEndpointClient(Table table) {
    this.table = table;
  }

  public String prepareBulkLoad(final TableName tableName) throws IOException {
    try {
      CoprocessorRpcChannel channel = table.coprocessorService(HConstants.EMPTY_START_ROW);
      SecureBulkLoadProtos.SecureBulkLoadService instance =
          ProtobufUtil.newServiceStub(SecureBulkLoadProtos.SecureBulkLoadService.class, channel);

      ServerRpcController controller = new ServerRpcController();

      CoprocessorRpcUtils.BlockingRpcCallback<PrepareBulkLoadResponse> rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<PrepareBulkLoadResponse>();

      PrepareBulkLoadRequest request =
          PrepareBulkLoadRequest.newBuilder()
          .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();

      instance.prepareBulkLoad(controller, request, rpcCallback);

      PrepareBulkLoadResponse response = rpcCallback.get();
      if (controller.failedOnException()) {
        throw controller.getFailedOn();
      }

      return response.getBulkToken();
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

  public void cleanupBulkLoad(final String bulkToken) throws IOException {
    try {
      CoprocessorRpcChannel channel = table.coprocessorService(HConstants.EMPTY_START_ROW);
      SecureBulkLoadProtos.SecureBulkLoadService instance =
          ProtobufUtil.newServiceStub(SecureBulkLoadProtos.SecureBulkLoadService.class, channel);

      ServerRpcController controller = new ServerRpcController();

      CoprocessorRpcUtils.BlockingRpcCallback<CleanupBulkLoadResponse> rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<CleanupBulkLoadResponse>();

      CleanupBulkLoadRequest request =
          CleanupBulkLoadRequest.newBuilder()
              .setBulkToken(bulkToken).build();

      instance.cleanupBulkLoad(controller,
          request,
          rpcCallback);

      if (controller.failedOnException()) {
        throw controller.getFailedOn();
      }
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

  public boolean bulkLoadHFiles(final List<Pair<byte[], String>> familyPaths,
                         final Token<?> userToken,
                         final String bulkToken,
                         final byte[] startRow) throws IOException {
    // we never want to send a batch of HFiles to all regions, thus cannot call
    // HTable#coprocessorService methods that take start and end rowkeys; see HBASE-9639
    try {
      CoprocessorRpcChannel channel = table.coprocessorService(startRow);
      SecureBulkLoadProtos.SecureBulkLoadService instance =
          ProtobufUtil.newServiceStub(SecureBulkLoadProtos.SecureBulkLoadService.class, channel);

      DelegationToken protoDT =
          DelegationToken.newBuilder().build();
      if(userToken != null) {
        protoDT =
            DelegationToken.newBuilder()
              .setIdentifier(ByteStringer.wrap(userToken.getIdentifier()))
              .setPassword(ByteStringer.wrap(userToken.getPassword()))
              .setKind(userToken.getKind().toString())
              .setService(userToken.getService().toString()).build();
      }

      List<ClientProtos.BulkLoadHFileRequest.FamilyPath> protoFamilyPaths =
          new ArrayList<ClientProtos.BulkLoadHFileRequest.FamilyPath>();
      for(Pair<byte[], String> el: familyPaths) {
        protoFamilyPaths.add(ClientProtos.BulkLoadHFileRequest.FamilyPath.newBuilder()
          .setFamily(ByteStringer.wrap(el.getFirst()))
          .setPath(el.getSecond()).build());
      }

      SecureBulkLoadProtos.SecureBulkLoadHFilesRequest request =
          SecureBulkLoadProtos.SecureBulkLoadHFilesRequest.newBuilder()
            .setFsToken(protoDT)
            .addAllFamilyPath(protoFamilyPaths)
            .setBulkToken(bulkToken).build();

      ServerRpcController controller = new ServerRpcController();
      CoprocessorRpcUtils.BlockingRpcCallback<SecureBulkLoadProtos.SecureBulkLoadHFilesResponse>
            rpcCallback =
          new CoprocessorRpcUtils.BlockingRpcCallback<SecureBulkLoadProtos.SecureBulkLoadHFilesResponse>();
      instance.secureBulkLoadHFiles(controller,
        request,
        rpcCallback);

      SecureBulkLoadProtos.SecureBulkLoadHFilesResponse response = rpcCallback.get();
      if (controller.failedOnException()) {
        throw controller.getFailedOn();
      }
      return response.getLoaded();
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

}
