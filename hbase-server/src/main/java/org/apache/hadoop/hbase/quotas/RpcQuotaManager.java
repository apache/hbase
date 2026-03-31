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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RpcQuotaManager {

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation. This method is specific to scans
   * because estimating a scan's workload is more complicated than estimating the workload of a
   * get/put.
   * @param region                          the region where the operation will be performed
   * @param scanRequest                     the scan to be estimated against the quota
   * @param maxScannerResultSize            the maximum bytes to be returned by the scanner
   * @param maxBlockBytesScanned            the maximum bytes scanned in a single RPC call by the
   *                                        scanner
   * @param prevBlockBytesScannedDifference the difference between BBS of the previous two next
   *                                        calls
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  OperationQuota checkScanQuota(final Region region, final ClientProtos.ScanRequest scanRequest,
    long maxScannerResultSize, long maxBlockBytesScanned, long prevBlockBytesScannedDifference)
    throws IOException, RpcThrottlingException;

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation. This method does not support
   * scans because estimating a scan's workload is more complicated than estimating the workload of
   * a get/put.
   * @param region the region where the operation will be performed
   * @param type   the operation type
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  OperationQuota checkBatchQuota(final Region region, final OperationQuota.OperationType type)
    throws IOException, RpcThrottlingException;

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation. This method does not support
   * scans because estimating a scan's workload is more complicated than estimating the workload of
   * a get/put.
   * @param region       the region where the operation will be performed
   * @param actions      the "multi" actions to perform
   * @param hasCondition whether the RegionAction has a condition
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  OperationQuota checkBatchQuota(final Region region, final List<ClientProtos.Action> actions,
    boolean hasCondition) throws IOException, RpcThrottlingException;

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation. This method does not support
   * scans because estimating a scan's workload is more complicated than estimating the workload of
   * a get/put.
   * @param region    the region where the operation will be performed
   * @param numWrites number of writes to count against quota
   * @param numReads  number of reads to count against quota
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  OperationQuota checkBatchQuota(final Region region, int numWrites, int numReads, boolean isAtomic)
    throws IOException, RpcThrottlingException;
}
