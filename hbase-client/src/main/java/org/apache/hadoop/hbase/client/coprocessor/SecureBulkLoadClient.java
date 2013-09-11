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

package org.apache.hadoop.hbase.client.coprocessor;

import com.google.protobuf.ByteString;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.SecureBulkLoadProtos;
import org.apache.hadoop.hbase.security.SecureBulkLoadUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Client proxy for SecureBulkLoadProtocol
 * used in conjunction with SecureBulkLoadEndpoint
 */
public class SecureBulkLoadClient {
  private HTable table;

  public SecureBulkLoadClient(HTable table) {
    this.table = table;
  }

  public String prepareBulkLoad(final TableName tableName) throws IOException {
    try {
      return
        table.coprocessorService(SecureBulkLoadProtos.SecureBulkLoadService.class,
          HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_START_ROW,
          new Batch.Call<SecureBulkLoadProtos.SecureBulkLoadService,String>() {
            @Override
            public String call(SecureBulkLoadProtos.SecureBulkLoadService instance) throws IOException {
              ServerRpcController controller = new ServerRpcController();

              BlockingRpcCallback<SecureBulkLoadProtos.PrepareBulkLoadResponse> rpcCallback =
                  new BlockingRpcCallback<SecureBulkLoadProtos.PrepareBulkLoadResponse>();

              SecureBulkLoadProtos.PrepareBulkLoadRequest request =
                  SecureBulkLoadProtos.PrepareBulkLoadRequest.newBuilder()
                  .setTableName(ProtobufUtil.toProtoTableName(tableName)).build();

              instance.prepareBulkLoad(controller,
                  request,
                  rpcCallback);

              SecureBulkLoadProtos.PrepareBulkLoadResponse response = rpcCallback.get();
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return response.getBulkToken();
            }
          }).entrySet().iterator().next().getValue();
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

  public void cleanupBulkLoad(final String bulkToken) throws IOException {
    try {
        table.coprocessorService(SecureBulkLoadProtos.SecureBulkLoadService.class,
            HConstants.EMPTY_START_ROW,
            HConstants.EMPTY_START_ROW,
            new Batch.Call<SecureBulkLoadProtos.SecureBulkLoadService, String>() {

              @Override
              public String call(SecureBulkLoadProtos.SecureBulkLoadService instance) throws IOException {
                ServerRpcController controller = new ServerRpcController();

                BlockingRpcCallback<SecureBulkLoadProtos.CleanupBulkLoadResponse> rpcCallback =
                    new BlockingRpcCallback<SecureBulkLoadProtos.CleanupBulkLoadResponse>();

                SecureBulkLoadProtos.CleanupBulkLoadRequest request =
                    SecureBulkLoadProtos.CleanupBulkLoadRequest.newBuilder()
                        .setBulkToken(bulkToken).build();

                instance.cleanupBulkLoad(controller,
                    request,
                    rpcCallback);

                if (controller.failedOnException()) {
                  throw controller.getFailedOn();
                }
                return null;
              }
            });
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

  public boolean bulkLoadHFiles(final List<Pair<byte[], String>> familyPaths,
                         final Token<?> userToken,
                         final String bulkToken,
                         final byte[] startRow) throws IOException {
    try {
      return
        table.coprocessorService(SecureBulkLoadProtos.SecureBulkLoadService.class,
          startRow,
          startRow,
          new Batch.Call<SecureBulkLoadProtos.SecureBulkLoadService,Boolean>() {

            @Override
            public Boolean call(SecureBulkLoadProtos.SecureBulkLoadService instance) throws IOException {
              SecureBulkLoadProtos.DelegationToken protoDT =
                  SecureBulkLoadProtos.DelegationToken.newBuilder().build();
              if(userToken != null) {
                protoDT =
                  SecureBulkLoadProtos.DelegationToken.newBuilder()
                     .setIdentifier(ByteString.copyFrom(userToken.getIdentifier()))
                     .setPassword(ByteString.copyFrom(userToken.getPassword()))
                     .setKind(userToken.getKind().toString())
                     .setService(userToken.getService().toString()).build();
              }

              List<ClientProtos.BulkLoadHFileRequest.FamilyPath> protoFamilyPaths =
                  new ArrayList<ClientProtos.BulkLoadHFileRequest.FamilyPath>();
              for(Pair<byte[], String> el: familyPaths) {
                protoFamilyPaths.add(ClientProtos.BulkLoadHFileRequest.FamilyPath.newBuilder()
                    .setFamily(ByteString.copyFrom(el.getFirst()))
                    .setPath(el.getSecond()).build());
              }

              SecureBulkLoadProtos.SecureBulkLoadHFilesRequest request =
                  SecureBulkLoadProtos.SecureBulkLoadHFilesRequest.newBuilder()
                      .setFsToken(protoDT)
                      .addAllFamilyPath(protoFamilyPaths)
                      .setBulkToken(bulkToken).build();

              ServerRpcController controller = new ServerRpcController();
              BlockingRpcCallback<SecureBulkLoadProtos.SecureBulkLoadHFilesResponse> rpcCallback =
                  new BlockingRpcCallback<SecureBulkLoadProtos.SecureBulkLoadHFilesResponse>();
              instance.secureBulkLoadHFiles(controller,
                  request,
                  rpcCallback);

              SecureBulkLoadProtos.SecureBulkLoadHFilesResponse response = rpcCallback.get();
              if (controller.failedOnException()) {
                throw controller.getFailedOn();
              }
              return response.getLoaded();
            }
          }).entrySet().iterator().next().getValue();
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

  public Path getStagingPath(String bulkToken, byte[] family) throws IOException {
    return SecureBulkLoadUtil.getStagingPath(table.getConfiguration(), bulkToken, family);
  }
}
