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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CleanupBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.PrepareBulkLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.PrepareBulkLoadResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.security.SecureBulkLoadUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.token.Token;

import com.google.protobuf.ServiceException;

/**
 * Client proxy for SecureBulkLoadProtocol
 */
@InterfaceAudience.Private
public class SecureBulkLoadClient {
  private Table table;

  public SecureBulkLoadClient(Table table) {
    this.table = table;
  }

  public String prepareBulkLoad(final Connection conn) throws IOException {
    try {
      RegionServerCallable<String> callable =
          new RegionServerCallable<String>(conn, table.getName(), HConstants.EMPTY_START_ROW) {
            @Override
            public String call(int callTimeout) throws IOException {
              byte[] regionName = getLocation().getRegionInfo().getRegionName();
              RegionSpecifier region =
                  RequestConverter
                      .buildRegionSpecifier(RegionSpecifierType.REGION_NAME, regionName);
              try {
                PrepareBulkLoadRequest request =
                    PrepareBulkLoadRequest.newBuilder()
                        .setTableName(ProtobufUtil.toProtoTableName(table.getName()))
                        .setRegion(region).build();
                PrepareBulkLoadResponse response = getStub().prepareBulkLoad(null, request);
                return response.getBulkToken();
              } catch (ServiceException se) {
                throw ProtobufUtil.getRemoteException(se);
              }
            }
          };
      return RpcRetryingCallerFactory.instantiate(conn.getConfiguration(), null)
          .<String> newCaller().callWithRetries(callable, Integer.MAX_VALUE);
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

  public void cleanupBulkLoad(final Connection conn, final String bulkToken) throws IOException {
    try {
      RegionServerCallable<Void> callable =
          new RegionServerCallable<Void>(conn, table.getName(), HConstants.EMPTY_START_ROW) {
            @Override
            public Void call(int callTimeout) throws IOException {
              byte[] regionName = getLocation().getRegionInfo().getRegionName();
              RegionSpecifier region = RequestConverter.buildRegionSpecifier(
                RegionSpecifierType.REGION_NAME, regionName);
              try {
                CleanupBulkLoadRequest request =
                    CleanupBulkLoadRequest.newBuilder().setRegion(region)
                        .setBulkToken(bulkToken).build();
                getStub().cleanupBulkLoad(null, request);
              } catch (ServiceException se) {
                throw ProtobufUtil.getRemoteException(se);
              }
              return null;
            }
          };
      RpcRetryingCallerFactory.instantiate(conn.getConfiguration(), null)
          .<Void> newCaller().callWithRetries(callable, Integer.MAX_VALUE);
    } catch (Throwable throwable) {
      throw new IOException(throwable);
    }
  }

  /**
   * Securely bulk load a list of HFiles using client protocol.
   *
   * @param client
   * @param familyPaths
   * @param regionName
   * @param assignSeqNum
   * @param userToken
   * @param bulkToken
   * @return true if all are loaded
   * @throws IOException
   */
  public boolean secureBulkLoadHFiles(final ClientService.BlockingInterface client,
      final List<Pair<byte[], String>> familyPaths,
      final byte[] regionName, boolean assignSeqNum,
      final Token<?> userToken, final String bulkToken) throws IOException {
    BulkLoadHFileRequest request =
        RequestConverter.buildBulkLoadHFileRequest(familyPaths, regionName, assignSeqNum,
          userToken, bulkToken);

    try {
      BulkLoadHFileResponse response = client.bulkLoadHFile(null, request);
      return response.getLoaded();
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  public Path getStagingPath(String bulkToken, byte[] family) throws IOException {
    return SecureBulkLoadUtil.getStagingPath(table.getConfiguration(), bulkToken, family);
  }
}
