/*
 *
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

package org.apache.hadoop.hbase.client.example;

import java.io.Closeable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.generated.RefreshHFilesProtos;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This client class is for invoking the refresh HFile function deployed on the
 * Region Server side via the RefreshHFilesService.
 */
@InterfaceAudience.Private
public class RefreshHFilesClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshHFilesClient.class);
  private final Connection connection;

  /**
   * Constructor with Conf object
   *
   * @param cfg
   */
  public RefreshHFilesClient(Configuration cfg) {
    try {
      this.connection = ConnectionFactory.createConnection(cfg);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.connection != null && !this.connection.isClosed()) {
      this.connection.close();
    }
  }

  public void refreshHFiles(final TableName tableName) throws Throwable {
    try (Table table = connection.getTable(tableName)) {
      refreshHFiles(table);
    }
  }

  public void refreshHFiles(final Table table) throws Throwable {
    final RefreshHFilesProtos.RefreshHFilesRequest request = RefreshHFilesProtos.RefreshHFilesRequest
                                                               .getDefaultInstance();
    table.coprocessorService(RefreshHFilesProtos.RefreshHFilesService.class, HConstants.EMPTY_START_ROW,
                             HConstants.EMPTY_END_ROW,
                             new Batch.Call<RefreshHFilesProtos.RefreshHFilesService,
                                             RefreshHFilesProtos.RefreshHFilesResponse>() {
                               @Override
                               public RefreshHFilesProtos.RefreshHFilesResponse call(
                                 RefreshHFilesProtos.RefreshHFilesService refreshHFilesService)
                                 throws IOException {
                                 ServerRpcController controller = new ServerRpcController();
                                 BlockingRpcCallback<RefreshHFilesProtos.RefreshHFilesResponse> rpcCallback =
                                   new BlockingRpcCallback<>();
                                 refreshHFilesService.refreshHFiles(controller, request, rpcCallback);
                                 if (controller.failedOnException()) {
                                   throw controller.getFailedOn();
                                 }
                                 return rpcCallback.get();
                               }
                             });
    LOG.debug("Done refreshing HFiles");
  }
}
