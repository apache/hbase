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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;

/**
 * Internal interface used to interact with the user/table quota.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface QuotaLimiter {
  /**
   * Checks if it is possible to execute the specified operation.
   *
   * @param estimateWriteSize the write size that will be checked against the available quota
   * @param estimateReadSize the read size that will be checked against the available quota
   * @throws ThrottlingException thrown if not enough avialable resources to perform operation.
   */
  void checkQuota(long estimateWriteSize, long estimateReadSize)
    throws ThrottlingException;

  /**
   * Removes the specified write and read amount from the quota.
   * At this point the write and read amount will be an estimate,
   * that will be later adjusted with a consumeWrite()/consumeRead() call.
   *
   * @param writeSize the write size that will be removed from the current quota
   * @param readSize the read size that will be removed from the current quota
   */
  void grabQuota(long writeSize, long readSize);

  /**
   * Removes or add back some write amount to the quota.
   * (called at the end of an operation in case the estimate quota was off)
   */
  void consumeWrite(long size);

  /**
   * Removes or add back some read amount to the quota.
   * (called at the end of an operation in case the estimate quota was off)
   */
  void consumeRead(long size);

  /** @return true if the limiter is a noop */
  boolean isBypass();

    /** @return the number of bytes available to read to avoid exceeding the quota */
  long getReadAvailable();

  /** @return the number of bytes available to write to avoid exceeding the quota */
  long getWriteAvailable();

  /**
   * Add the average size of the specified operation type.
   * The average will be used as estimate for the next operations.
   */
  void addOperationSize(OperationType type, long size);

  /** @return the average data size of the specified operation */
  long getAvgOperationSize(OperationType type);
}
