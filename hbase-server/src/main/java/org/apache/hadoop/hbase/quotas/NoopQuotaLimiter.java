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
 * Noop quota limiter returned when no limiter is associated to the user/table
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class NoopQuotaLimiter implements QuotaLimiter {
  private static QuotaLimiter instance = new NoopQuotaLimiter();

  private NoopQuotaLimiter() {
    // no-op
  }

  @Override
  public void checkQuota(long estimateWriteSize, long estimateReadSize)
      throws ThrottlingException {
    // no-op
  }

  @Override
  public void grabQuota(long writeSize, long readSize) {
    // no-op
  }

  @Override
  public void consumeWrite(final long size) {
    // no-op
  }

  @Override
  public void consumeRead(final long size) {
    // no-op
  }

  @Override
  public boolean isBypass() {
    return true;
  }

  @Override
  public long getWriteAvailable() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getReadAvailable() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addOperationSize(OperationType type, long size) {
  }

  @Override
  public long getAvgOperationSize(OperationType type) {
    return -1;
  }

  @Override
  public String toString() {
    return "NoopQuotaLimiter";
  }

  public static QuotaLimiter get() {
    return instance;
  }
}
