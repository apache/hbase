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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;

/**
 * ClientSimpleScanner implements a sync scanner behaviour.
 * The cache is a simple list.
 * The prefetch is invoked only when the application finished processing the entire cache.
 */
@InterfaceAudience.Private
public class ClientSimpleScanner extends ClientScanner {

  public ClientSimpleScanner(Configuration configuration, Scan scan, TableName name,
      ClusterConnection connection, RpcRetryingCallerFactory rpcCallerFactory,
      RpcControllerFactory rpcControllerFactory, ExecutorService pool,
      int replicaCallTimeoutMicroSecondScan) throws IOException {
    super(configuration, scan, name, connection, rpcCallerFactory, rpcControllerFactory, pool,
        replicaCallTimeoutMicroSecondScan);
  }

  @Override
  protected void initCache() {
    initSyncCache();
  }

  @Override
  public Result next() throws IOException {
    return nextWithSyncCache();
  }
}
