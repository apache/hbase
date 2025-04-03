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
package org.apache.hadoop.hbase.quotas;

import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

public class TestNoopOperationQuota implements OperationQuota {
  public static final TestNoopOperationQuota INSTANCE = new TestNoopOperationQuota();

  @Override
  public void checkBatchQuota(int numWrites, int numReads, boolean isAtomic)
    throws RpcThrottlingException {
  }

  @Override
  public void checkScanQuota(ClientProtos.ScanRequest scanRequest, long maxScannerResultSize,
    long maxBlockBytesScanned, long prevBlockBytesScannedDifference) throws RpcThrottlingException {

  }

  @Override
  public void close() {

  }

  @Override
  public void addGetResult(Result result) {

  }

  @Override
  public void addScanResult(List<Result> results) {

  }

  @Override
  public void addScanResultCells(List<Cell> cells) {

  }

  @Override
  public void addMutation(Mutation mutation) {

  }

  @Override
  public long getReadAvailable() {
    return 0L;
  }

  @Override
  public long getReadConsumed() {
    return 0;
  }
}
