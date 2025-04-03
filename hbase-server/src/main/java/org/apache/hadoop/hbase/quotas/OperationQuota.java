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
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

/**
 * Interface that allows to check the quota available for an operation.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface OperationQuota {
  public enum OperationType {
    MUTATE,
    GET,
    SCAN,
    CHECK_AND_MUTATE
  }

  /**
   * If false, the default, then IO based throttles will consume read availability based on the
   * block bytes scanned by the given request. If true then IO based throttles will use result size
   * rather than block bytes scanned. Using block bytes scanned should be preferable to using result
   * size, because otherwise access patterns like heavily filtered scans may be able to produce a
   * significant and effectively un-throttled workload.
   */
  String USE_RESULT_SIZE_BYTES = "hbase.quota.use.result.size.bytes";
  boolean USE_RESULT_SIZE_BYTES_DEFAULT = false;

  /**
   * Checks if it is possible to execute the specified operation. The quota will be estimated based
   * on the number of operations to perform and the average size accumulated during time.
   * @param numWrites number of write operation that will be performed
   * @param numReads  number of small-read operation that will be performed
   * @throws RpcThrottlingException if the operation cannot be performed because RPC quota is
   *                                exceeded.
   */
  void checkBatchQuota(int numWrites, int numReads, boolean isAtomic) throws RpcThrottlingException;

  /**
   * Checks if it is possible to execute the scan. The quota will be estimated based on the
   * composition of the scan.
   * @param scanRequest                     the given scan operation
   * @param maxScannerResultSize            the maximum bytes to be returned by the scanner
   * @param maxBlockBytesScanned            the maximum bytes scanned in a single RPC call by the
   *                                        scanner
   * @param prevBlockBytesScannedDifference the difference between BBS of the previous two next
   *                                        calls
   * @throws RpcThrottlingException if the operation cannot be performed because RPC quota is
   *                                exceeded.
   */
  void checkScanQuota(ClientProtos.ScanRequest scanRequest, long maxScannerResultSize,
    long maxBlockBytesScanned, long prevBlockBytesScannedDifference) throws RpcThrottlingException;

  /** Cleanup method on operation completion */
  void close();

  /**
   * Add a get result. This will be used to calculate the exact quota and have a better short-read
   * average size for the next time.
   */
  void addGetResult(Result result);

  /**
   * Add a scan result. This will be used to calculate the exact quota and have a better long-read
   * average size for the next time.
   */
  void addScanResult(List<Result> results);

  /**
   * Add a scan result in the form of cells. This will be used to calculate the exact quota and have
   * a better long-read average size for the next time.
   */
  void addScanResultCells(List<Cell> cells);

  /**
   * Add a mutation result. This will be used to calculate the exact quota and have a better
   * mutation average size for the next time.
   */
  void addMutation(Mutation mutation);

  /** Returns the number of bytes available to read to avoid exceeding the quota */
  long getReadAvailable();

  /** Returns the number of bytes consumed from the quota by the operation */
  long getReadConsumed();

  /**
   * Returns the maximum result size to be returned by the given operation. This is the greater of
   * two numbers: the bytes available, or the bytes already consumed
   */
  default long getMaxResultSize() {
    return Math.max(getReadAvailable(), getReadConsumed());
  }
}
