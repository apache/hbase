/*
 *
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

package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.com.google.common.annotations.VisibleForTesting;

/**
 * Class used to push numbers about ZooKeeper into the metrics subsystem. This will take a
 * single function call and turn it into multiple manipulations of the hadoop metrics system.
 */
@InterfaceAudience.Private
public class ZKMetrics implements ZKMetricsListener {
  private final MetricsZooKeeperSource source;

  public ZKMetrics() {
    this(CompatibilitySingletonFactory.getInstance(MetricsZooKeeperSource.class));
  }

  @VisibleForTesting
  public ZKMetrics(MetricsZooKeeperSource s) {
    this.source = s;
  }

  @Override
  public void registerAuthFailedException() {
    source.incrementAuthFailedCount();
  }

  @Override
  public void registerConnectionLossException() {
    source.incrementConnectionLossCount();
  }

  @Override
  public void registerDataInconsistencyException() {
    source.incrementDataInconsistencyCount();
  }

  @Override
  public void registerInvalidACLException() {
    source.incrementInvalidACLCount();
  }

  @Override
  public void registerNoAuthException() {
    source.incrementNoAuthCount();
  }

  @Override
  public void registerOperationTimeoutException() {
    source.incrementOperationTimeoutCount();
  }

  @Override
  public void registerRuntimeInconsistencyException() {
    source.incrementRuntimeInconsistencyCount();
  }

  @Override
  public void registerSessionExpiredException() {
    source.incrementSessionExpiredCount();
  }

  @Override
  public void registerSystemErrorException() {
    source.incrementSystemErrorCount();
  }

  @Override
  public void registerFailedZKCall() {
    source.incrementTotalFailedZKCalls();
  }

  @Override
  public void registerReadOperationLatency(long latency) {
    source.recordReadOperationLatency(latency);
  }

  @Override
  public void registerWriteOperationLatency(long latency) {
    source.recordWriteOperationLatency(latency);
  }

  @Override
  public void registerSyncOperationLatency(long latency) {
    source.recordSyncOperationLatency(latency);
  }
}
