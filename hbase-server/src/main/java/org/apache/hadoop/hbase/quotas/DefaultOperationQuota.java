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

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.quotas.OperationQuota.AvgOperationSize;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DefaultOperationQuota implements OperationQuota {
  private static final Log LOG = LogFactory.getLog(DefaultOperationQuota.class);

  private final List<QuotaLimiter> limiters;
  private long writeAvailable = 0;
  private long readAvailable = 0;
  private long writeConsumed = 0;
  private long readConsumed = 0;

  private AvgOperationSize avgOpSize = new AvgOperationSize();

  public DefaultOperationQuota(final QuotaLimiter... limiters) {
    this(Arrays.asList(limiters));
  }

  /**
   * NOTE: The order matters. It should be something like [user, table, namespace, global]
   */
  public DefaultOperationQuota(final List<QuotaLimiter> limiters) {
    this.limiters = limiters;
  }

  @Override
  public void checkQuota(int numWrites, int numReads, int numScans)
      throws ThrottlingException {
    writeConsumed = estimateConsume(OperationType.MUTATE, numWrites, 100);
    readConsumed  = estimateConsume(OperationType.GET, numReads, 100);
    readConsumed += estimateConsume(OperationType.SCAN, numScans, 1000);

    writeAvailable = Long.MAX_VALUE;
    readAvailable = Long.MAX_VALUE;
    for (final QuotaLimiter limiter: limiters) {
      if (limiter.isBypass()) continue;

      limiter.checkQuota(writeConsumed, readConsumed);
      readAvailable = Math.min(readAvailable, limiter.getReadAvailable());
      writeAvailable = Math.min(writeAvailable, limiter.getWriteAvailable());
    }

    for (final QuotaLimiter limiter: limiters) {
      limiter.grabQuota(writeConsumed, readConsumed);
    }
  }

  @Override
  public void close() {
    // Calculate and set the average size of get, scan and mutate for the current operation
    long getSize = avgOpSize.getAvgOperationSize(OperationType.GET);
    long scanSize = avgOpSize.getAvgOperationSize(OperationType.SCAN);
    long mutationSize = avgOpSize.getAvgOperationSize(OperationType.MUTATE);
    for (final QuotaLimiter limiter: limiters) {
      limiter.addOperationSize(OperationType.GET, getSize);
      limiter.addOperationSize(OperationType.SCAN, scanSize);
      limiter.addOperationSize(OperationType.MUTATE, mutationSize);
    }

    // Adjust the quota consumed for the specified operation
    long writeDiff = avgOpSize.getOperationSize(OperationType.MUTATE) - writeConsumed;
    long readDiff = (avgOpSize.getOperationSize(OperationType.GET) +
                     avgOpSize.getOperationSize(OperationType.SCAN)) - readConsumed;
    for (final QuotaLimiter limiter: limiters) {
      if (writeDiff != 0) limiter.consumeWrite(writeDiff);
      if (readDiff != 0) limiter.consumeRead(readDiff);
    }
  }

  @Override
  public long getReadAvailable() {
    return readAvailable;
  }

  @Override
  public long getWriteAvailable() {
    return writeAvailable;
  }

  @Override
  public void addGetResult(final Result result) {
    avgOpSize.addGetResult(result);
  }

  @Override
  public void addScanResult(final List<Result> results) {
    avgOpSize.addScanResult(results);
  }

  @Override
  public void addMutation(final Mutation mutation) {
    avgOpSize.addMutation(mutation);
  }

  @Override
  public long getAvgOperationSize(OperationType type) {
    return avgOpSize.getAvgOperationSize(type);
  }

  private long estimateConsume(final OperationType type, int numReqs, long avgSize) {
    if (numReqs > 0) {
      for (final QuotaLimiter limiter: limiters) {
        long size = limiter.getAvgOperationSize(type);
        if (size > 0) {
          avgSize = size;
          break;
        }
      }
      return avgSize * numReqs;
    }
    return 0;
  }
}
