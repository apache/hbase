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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.CheckAndMutateResult;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.quotas.ActivePolicyEnforcement;
import org.apache.hadoop.hbase.quotas.OperationQuota;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.BulkLoadHFileResponse;

/**
 * It is responsible for populating the row cache and retrieving rows from it.
 */
@org.apache.yetus.audience.InterfaceAudience.Private
public class RowCacheService {
  private final boolean enabledByConf;
  private final RowCache rowCache;

  RowCacheService(Configuration conf) {
    enabledByConf =
      conf.getFloat(HConstants.ROW_CACHE_SIZE_KEY, HConstants.ROW_CACHE_SIZE_DEFAULT) > 0;
    rowCache = new RowCacheImpl(MemorySizeUtil.getRowCacheSize(conf));
  }

  OperationStatus[] batchMutate(HRegion region, Mutation[] mArray, boolean atomic, long nonceGroup,
    long nonce) throws IOException {
    return region.batchMutate(mArray, atomic, nonceGroup, nonce);
  }

  BulkLoadHFileResponse bulkLoadHFile(RSRpcServices rsRpcServices, BulkLoadHFileRequest request)
    throws ServiceException {
    return rsRpcServices.bulkLoadHFileInternal(request);
  }

  CheckAndMutateResult checkAndMutate(HRegion region, CheckAndMutate checkAndMutate,
    long nonceGroup, long nonce) throws IOException {
    return region.checkAndMutate(checkAndMutate, nonceGroup, nonce);
  }

  RegionScannerImpl getScanner(HRegion region, Scan scan, List<Cell> results) throws IOException {
    RegionScannerImpl scanner = region.getScanner(scan);
    scanner.next(results);
    return scanner;
  }

  Result mutate(RSRpcServices rsRpcServices, HRegion region, ClientProtos.MutationProto mutation,
    OperationQuota quota, CellScanner cellScanner, long nonceGroup,
    ActivePolicyEnforcement spaceQuotaEnforcement, RpcCallContext context) throws IOException {
    return rsRpcServices.mutateInternal(mutation, region, quota, cellScanner, nonceGroup,
      spaceQuotaEnforcement, context);
  }
}
