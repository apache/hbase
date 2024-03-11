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

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DefaultOperationQuota implements OperationQuota {

  // a single scan estimate can consume no more than this proportion of the limiter's limit
  // this prevents a long-running scan from being estimated at, say, 100MB of IO against
  // a <100MB/IO throttle (because this would never succeed)
  private static final double MAX_SCAN_ESTIMATE_PROPORTIONAL_LIMIT_CONSUMPTION = 0.9;

  protected final List<QuotaLimiter> limiters;
  private final long writeCapacityUnit;
  private final long readCapacityUnit;

  // the available read/write quota size in bytes
  protected long readAvailable = 0;
  // estimated quota
  protected long writeConsumed = 0;
  protected long readConsumed = 0;
  protected long writeCapacityUnitConsumed = 0;
  protected long readCapacityUnitConsumed = 0;
  // real consumed quota
  private final long[] operationSize;
  // difference between estimated quota and real consumed quota used in close method
  // to adjust quota amount. Also used by ExceedOperationQuota which is a subclass
  // of DefaultOperationQuota
  protected long writeDiff = 0;
  protected long readDiff = 0;
  protected long writeCapacityUnitDiff = 0;
  protected long readCapacityUnitDiff = 0;
  private boolean useResultSizeBytes;
  private long blockSizeBytes;
  private long maxScanEstimate;

  public DefaultOperationQuota(final Configuration conf, final int blockSizeBytes,
    final QuotaLimiter... limiters) {
    this(conf, Arrays.asList(limiters));
    this.useResultSizeBytes =
      conf.getBoolean(OperationQuota.USE_RESULT_SIZE_BYTES, USE_RESULT_SIZE_BYTES_DEFAULT);
    this.blockSizeBytes = blockSizeBytes;
    long readSizeLimit =
      Arrays.stream(limiters).mapToLong(QuotaLimiter::getReadLimit).min().orElse(Long.MAX_VALUE);
    maxScanEstimate = Math.round(MAX_SCAN_ESTIMATE_PROPORTIONAL_LIMIT_CONSUMPTION * readSizeLimit);
  }

  /**
   * NOTE: The order matters. It should be something like [user, table, namespace, global]
   */
  public DefaultOperationQuota(final Configuration conf, final List<QuotaLimiter> limiters) {
    this.writeCapacityUnit =
      conf.getLong(QuotaUtil.WRITE_CAPACITY_UNIT_CONF_KEY, QuotaUtil.DEFAULT_WRITE_CAPACITY_UNIT);
    this.readCapacityUnit =
      conf.getLong(QuotaUtil.READ_CAPACITY_UNIT_CONF_KEY, QuotaUtil.DEFAULT_READ_CAPACITY_UNIT);
    this.limiters = limiters;
    int size = OperationType.values().length;
    operationSize = new long[size];

    for (int i = 0; i < size; ++i) {
      operationSize[i] = 0;
    }
  }

  @Override
  public void checkBatchQuota(int numWrites, int numReads) throws RpcThrottlingException {
    updateEstimateConsumeBatchQuota(numWrites, numReads);
    checkQuota(numWrites, numReads);
  }

  @Override
  public void checkScanQuota(ClientProtos.ScanRequest scanRequest, long maxScannerResultSize,
    long maxBlockBytesScanned, long prevBlockBytesScannedDifference) throws RpcThrottlingException {
    updateEstimateConsumeScanQuota(scanRequest, maxScannerResultSize, maxBlockBytesScanned,
      prevBlockBytesScannedDifference);
    checkQuota(0, 1);
  }

  private void checkQuota(long numWrites, long numReads) throws RpcThrottlingException {
    readAvailable = Long.MAX_VALUE;
    for (final QuotaLimiter limiter : limiters) {
      if (limiter.isBypass()) {
        continue;
      }

      limiter.checkQuota(numWrites, writeConsumed, numReads, readConsumed,
        writeCapacityUnitConsumed, readCapacityUnitConsumed);
      readAvailable = Math.min(readAvailable, limiter.getReadAvailable());
    }

    for (final QuotaLimiter limiter : limiters) {
      limiter.grabQuota(numWrites, writeConsumed, numReads, readConsumed, writeCapacityUnitConsumed,
        readCapacityUnitConsumed);
    }
  }

  @Override
  public void close() {
    // Adjust the quota consumed for the specified operation
    writeDiff = operationSize[OperationType.MUTATE.ordinal()] - writeConsumed;

    long resultSize =
      operationSize[OperationType.GET.ordinal()] + operationSize[OperationType.SCAN.ordinal()];
    if (useResultSizeBytes) {
      readDiff = resultSize - readConsumed;
    } else {
      long blockBytesScanned =
        RpcServer.getCurrentCall().map(RpcCall::getBlockBytesScanned).orElse(0L);
      readDiff = Math.max(blockBytesScanned, resultSize) - readConsumed;
    }

    writeCapacityUnitDiff =
      calculateWriteCapacityUnitDiff(operationSize[OperationType.MUTATE.ordinal()], writeConsumed);
    readCapacityUnitDiff = calculateReadCapacityUnitDiff(
      operationSize[OperationType.GET.ordinal()] + operationSize[OperationType.SCAN.ordinal()],
      readConsumed);

    for (final QuotaLimiter limiter : limiters) {
      if (writeDiff != 0) {
        limiter.consumeWrite(writeDiff, writeCapacityUnitDiff);
      }
      if (readDiff != 0) {
        limiter.consumeRead(readDiff, readCapacityUnitDiff);
      }
    }
  }

  @Override
  public long getReadAvailable() {
    return readAvailable;
  }

  @Override
  public long getReadConsumed() {
    return readConsumed;
  }

  @Override
  public void addGetResult(final Result result) {
    operationSize[OperationType.GET.ordinal()] += QuotaUtil.calculateResultSize(result);
  }

  @Override
  public void addScanResult(final List<Result> results) {
    operationSize[OperationType.SCAN.ordinal()] += QuotaUtil.calculateResultSize(results);
  }

  @Override
  public void addMutation(final Mutation mutation) {
    operationSize[OperationType.MUTATE.ordinal()] += QuotaUtil.calculateMutationSize(mutation);
  }

  /**
   * Update estimate quota(read/write size/capacityUnits) which will be consumed
   * @param numWrites the number of write requests
   * @param numReads  the number of read requests
   */
  protected void updateEstimateConsumeBatchQuota(int numWrites, int numReads) {
    writeConsumed = estimateConsume(OperationType.MUTATE, numWrites, 100);

    if (useResultSizeBytes) {
      readConsumed = estimateConsume(OperationType.GET, numReads, 100);
    } else {
      // assume 1 block required for reads. this is probably a low estimate, which is okay
      readConsumed = numReads > 0 ? blockSizeBytes : 0;
    }

    writeCapacityUnitConsumed = calculateWriteCapacityUnit(writeConsumed);
    readCapacityUnitConsumed = calculateReadCapacityUnit(readConsumed);
  }

  /**
   * Update estimate quota(read/write size/capacityUnits) which will be consumed
   * @param scanRequest                     the scan to be executed
   * @param maxScannerResultSize            the maximum bytes to be returned by the scanner
   * @param maxBlockBytesScanned            the maximum bytes scanned in a single RPC call by the
   *                                        scanner
   * @param prevBlockBytesScannedDifference the difference between BBS of the previous two next
   *                                        calls
   */
  protected void updateEstimateConsumeScanQuota(ClientProtos.ScanRequest scanRequest,
    long maxScannerResultSize, long maxBlockBytesScanned, long prevBlockBytesScannedDifference) {
    if (useResultSizeBytes) {
      readConsumed = estimateConsume(OperationType.SCAN, 1, 1000);
    } else {
      long estimate = getScanReadConsumeEstimate(blockSizeBytes, scanRequest.getNextCallSeq(),
        maxScannerResultSize, maxBlockBytesScanned, prevBlockBytesScannedDifference);
      readConsumed = Math.min(maxScanEstimate, estimate);
    }

    readCapacityUnitConsumed = calculateReadCapacityUnit(readConsumed);
  }

  protected static long getScanReadConsumeEstimate(long blockSizeBytes, long nextCallSeq,
    long maxScannerResultSize, long maxBlockBytesScanned, long prevBlockBytesScannedDifference) {
    /*
     * Estimating scan workload is more complicated, and if we severely underestimate workloads then
     * throttled clients will exhaust retries too quickly, and could saturate the RPC layer
     */
    if (nextCallSeq == 0) {
      // start scanners with an optimistic 1 block IO estimate
      // it is better to underestimate a large scan in the beginning
      // than to overestimate, and block, a small scan
      return blockSizeBytes;
    }

    boolean isWorkloadGrowing = prevBlockBytesScannedDifference > blockSizeBytes;
    if (isWorkloadGrowing) {
      // if nextCallSeq > 0 and the workload is growing then our estimate
      // should consider that the workload may continue to increase
      return Math.min(maxScannerResultSize, nextCallSeq * maxBlockBytesScanned);
    } else {
      // if nextCallSeq > 0 and the workload is shrinking or flat
      // then our workload has likely plateaued. We can just rely on the existing
      // maxBlockBytesScanned as our estimate in this case.
      return maxBlockBytesScanned;
    }
  }

  private long estimateConsume(final OperationType type, int numReqs, long avgSize) {
    if (numReqs > 0) {
      return avgSize * numReqs;
    }
    return 0;
  }

  private long calculateWriteCapacityUnit(final long size) {
    return (long) Math.ceil(size * 1.0 / this.writeCapacityUnit);
  }

  private long calculateReadCapacityUnit(final long size) {
    return (long) Math.ceil(size * 1.0 / this.readCapacityUnit);
  }

  private long calculateWriteCapacityUnitDiff(final long actualSize, final long estimateSize) {
    return calculateWriteCapacityUnit(actualSize) - calculateWriteCapacityUnit(estimateSize);
  }

  private long calculateReadCapacityUnitDiff(final long actualSize, final long estimateSize) {
    return calculateReadCapacityUnit(actualSize) - calculateReadCapacityUnit(estimateSize);
  }
}
