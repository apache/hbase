/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.ConnectionUtils.noMoreResultsForReverseScan;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

/**
 * A reversed client scanner which support backward scanning
 */
@InterfaceAudience.Private
public class ReversedClientScanner extends ClientScanner {

  /**
   * Create a new ReversibleClientScanner for the specified table Note that the passed
   * {@link Scan}'s start row maybe changed.
   */
  public ReversedClientScanner(Configuration conf, Scan scan, TableName tableName,
      ConnectionImplementation connection, RpcRetryingCallerFactory rpcFactory,
      RpcControllerFactory controllerFactory, ExecutorService pool, int primaryOperationTimeout)
      throws IOException {
    super(conf, scan, tableName, connection, rpcFactory, controllerFactory, pool,
      primaryOperationTimeout);
  }

  @Override
  protected boolean setNewStartKey() {
    if (noMoreResultsForReverseScan(scan, currentRegion)) {
      return false;
    }
    scan.withStartRow(currentRegion.getStartKey(), false);
    return true;
  }

  @Override
  protected ReversedScannerCallable createScannerCallable() {
    return new ReversedScannerCallable(getConnection(), getTable(), scan, this.scanMetrics,
        this.rpcControllerFactory);
  }
}
