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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestZKMetrics {

  @Test
  public void testRegisterExceptions() {
    MetricsZooKeeperSource zkSource = mock(MetricsZooKeeperSourceImpl.class);
    ZKMetrics metricsZK = new ZKMetrics(zkSource);
    metricsZK.registerAuthFailedException();
    metricsZK.registerConnectionLossException();
    metricsZK.registerConnectionLossException();
    metricsZK.registerDataInconsistencyException();
    metricsZK.registerInvalidACLException();
    metricsZK.registerNoAuthException();
    metricsZK.registerOperationTimeoutException();
    metricsZK.registerOperationTimeoutException();
    metricsZK.registerRuntimeInconsistencyException();
    metricsZK.registerSessionExpiredException();
    metricsZK.registerSystemErrorException();
    metricsZK.registerSystemErrorException();
    metricsZK.registerFailedZKCall();

    verify(zkSource, times(1)).incrementAuthFailedCount();
    // ConnectionLoss Exception was registered twice.
    verify(zkSource, times(2)).incrementConnectionLossCount();
    verify(zkSource, times(1)).incrementDataInconsistencyCount();
    verify(zkSource, times(1)).incrementInvalidACLCount();
    verify(zkSource, times(1)).incrementNoAuthCount();
    // OperationTimeout Exception was registered twice.
    verify(zkSource, times(2)).incrementOperationTimeoutCount();
    verify(zkSource, times(1)).incrementRuntimeInconsistencyCount();
    verify(zkSource, times(1)).incrementSessionExpiredCount();
    // SystemError Exception was registered twice.
    verify(zkSource, times(2)).incrementSystemErrorCount();
    verify(zkSource, times(1)).incrementTotalFailedZKCalls();
  }

  @Test
  public void testLatencyHistogramUpdates() {
    MetricsZooKeeperSource zkSource = mock(MetricsZooKeeperSourceImpl.class);
    ZKMetrics metricsZK = new ZKMetrics(zkSource);
    long latency = 100;

    metricsZK.registerReadOperationLatency(latency);
    metricsZK.registerReadOperationLatency(latency);
    metricsZK.registerWriteOperationLatency(latency);
    metricsZK.registerSyncOperationLatency(latency);
    // Read Operation Latency update was registered twice.
    verify(zkSource, times(2)).recordReadOperationLatency(latency);
    verify(zkSource, times(1)).recordWriteOperationLatency(latency);
    verify(zkSource, times(1)).recordSyncOperationLatency(latency);
  }
}
