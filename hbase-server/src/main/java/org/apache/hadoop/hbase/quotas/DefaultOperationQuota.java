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

package org.apache.hadoop.hbase.quotas;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DefaultOperationQuota implements OperationQuota {

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

  public DefaultOperationQuota(final Configuration conf, final QuotaLimiter... limiters) {
    this(conf, Arrays.asList(limiters));
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
  public void checkQuota(int numWrites, int numReads, int numScans) throws RpcThrottlingException {
    updateEstimateConsumeQuota(numWrites, numReads, numScans);

    readAvailable = Long.MAX_VALUE;
    for (final QuotaLimiter limiter : limiters) {
      if (limiter.isBypass()) continue;

      limiter.checkQuota(numWrites, writeConsumed, numReads + numScans, readConsumed,
        writeCapacityUnitConsumed, readCapacityUnitConsumed);
      readAvailable = Math.min(readAvailable, limiter.getReadAvailable());
    }

    for (final QuotaLimiter limiter : limiters) {
      limiter.grabQuota(numWrites, writeConsumed, numReads + numScans, readConsumed,
        writeCapacityUnitConsumed, readCapacityUnitConsumed);
    }
  }

  @Override
  public void close() {
    // Adjust the quota consumed for the specified operation
    writeDiff = operationSize[OperationType.MUTATE.ordinal()] - writeConsumed;
    readDiff = operationSize[OperationType.GET.ordinal()]
        + operationSize[OperationType.SCAN.ordinal()] - readConsumed;
    writeCapacityUnitDiff = calculateWriteCapacityUnitDiff(
      operationSize[OperationType.MUTATE.ordinal()], writeConsumed);
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
   * @param numReads the number of read requests
   * @param numScans the number of scan requests
   */
  protected void updateEstimateConsumeQuota(int numWrites, int numReads, int numScans) {
    writeConsumed = estimateConsume(OperationType.MUTATE, numWrites, 100);
    readConsumed = estimateConsume(OperationType.GET, numReads, 100);
    readConsumed += estimateConsume(OperationType.SCAN, numScans, 1000);

    writeCapacityUnitConsumed = calculateWriteCapacityUnit(writeConsumed);
    readCapacityUnitConsumed = calculateReadCapacityUnit(readConsumed);
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
