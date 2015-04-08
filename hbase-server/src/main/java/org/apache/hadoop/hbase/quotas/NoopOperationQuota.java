/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.quotas;

import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

/**
 * Noop operation quota returned when no quota is associated to the user/table
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class NoopOperationQuota implements OperationQuota {
  private static OperationQuota instance = new NoopOperationQuota();

  private NoopOperationQuota() {
    // no-op
  }

  public static OperationQuota get() {
    return instance;
  }

  @Override
  public void checkQuota(int numWrites, int numReads, int numScans) throws ThrottlingException {
    // no-op
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public void addGetResult(final Result result) {
    // no-op
  }

  @Override
  public void addScanResult(final List<Result> results) {
    // no-op
  }

  @Override
  public void addMutation(final Mutation mutation) {
    // no-op
  }

  @Override
  public long getReadAvailable() {
    return Long.MAX_VALUE;
  }

  @Override
  public long getWriteAvailable() {
    return Long.MAX_VALUE;
  }

  @Override
  public long getAvgOperationSize(OperationType type) {
    return -1;
  }
}
