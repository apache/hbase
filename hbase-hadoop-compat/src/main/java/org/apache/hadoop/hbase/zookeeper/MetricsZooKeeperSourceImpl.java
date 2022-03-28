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

package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Class that transitions metrics from MetricsZooKeeper into the metrics subsystem.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern.
 */
@InterfaceAudience.Private
public class MetricsZooKeeperSourceImpl extends BaseSourceImpl implements MetricsZooKeeperSource {

  private final MutableGaugeLong authFailedFailedOpCount;
  private final MutableGaugeLong connectionLossFailedOpCount;
  private final MutableGaugeLong dataInconsistencyFailedOpCount;
  private final MutableGaugeLong invalidACLFailedOpCount;
  private final MutableGaugeLong noAuthFailedOpCount;
  private final MutableGaugeLong operationTimeOutFailedOpCount;
  private final MutableGaugeLong runtimeInconsistencyFailedOpCount;
  private final MutableGaugeLong sessionExpiredFailedOpCount;
  private final MutableGaugeLong systemErrorFailedOpCount;
  private final MutableGaugeLong totalFailedZKCalls;

  private MutableHistogram readOpLatency;
  private MutableHistogram writeOpLatency;
  private MutableHistogram syncOpLatency;

  public MetricsZooKeeperSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsZooKeeperSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    //Create and store the metrics that will be used.
    authFailedFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_AUTHFAILED, EXCEPTION_AUTHFAILED_DESC, 0L);
    connectionLossFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_CONNECTIONLOSS, EXCEPTION_CONNECTIONLOSS_DESC, 0L);
    dataInconsistencyFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_DATAINCONSISTENCY, EXCEPTION_DATAINCONSISTENCY_DESC, 0L);
    invalidACLFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_INVALIDACL, EXCEPTION_INVALIDACL_DESC, 0L);
    noAuthFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_NOAUTH, EXCEPTION_NOAUTH_DESC, 0L);
    operationTimeOutFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_OPERATIONTIMEOUT, EXCEPTION_OPERATIONTIMEOUT_DESC, 0L);
    runtimeInconsistencyFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_RUNTIMEINCONSISTENCY, EXCEPTION_RUNTIMEINCONSISTENCY_DESC, 0L);
    sessionExpiredFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_SESSIONEXPIRED, EXCEPTION_SESSIONEXPIRED_DESC, 0L);
    systemErrorFailedOpCount = this.getMetricsRegistry().newGauge(
            EXCEPTION_SYSTEMERROR, EXCEPTION_SYSTEMERROR_DESC, 0L);
    totalFailedZKCalls = this.getMetricsRegistry().newGauge(
            TOTAL_FAILED_ZK_CALLS, TOTAL_FAILED_ZK_CALLS_DESC, 0L);

    readOpLatency = this.getMetricsRegistry().newHistogram(
            READ_OPERATION_LATENCY_NAME, READ_OPERATION_LATENCY_DESC);
    writeOpLatency = this.getMetricsRegistry().newHistogram(
            WRITE_OPERATION_LATENCY_NAME, WRITE_OPERATION_LATENCY_DESC);
    syncOpLatency = this.getMetricsRegistry().newHistogram(
            SYNC_OPERATION_LATENCY_NAME, SYNC_OPERATION_LATENCY_DESC);
  }

  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    super.getMetrics(metricsCollector, all);
    clearZKExceptionMetrics();
  }

  private void clearZKExceptionMetrics() {
    //Reset the exception metrics.
    clearMetricIfNotNull(authFailedFailedOpCount);
    clearMetricIfNotNull(connectionLossFailedOpCount);
    clearMetricIfNotNull(dataInconsistencyFailedOpCount);
    clearMetricIfNotNull(invalidACLFailedOpCount);
    clearMetricIfNotNull(noAuthFailedOpCount);
    clearMetricIfNotNull(operationTimeOutFailedOpCount);
    clearMetricIfNotNull(runtimeInconsistencyFailedOpCount);
    clearMetricIfNotNull(sessionExpiredFailedOpCount);
    clearMetricIfNotNull(systemErrorFailedOpCount);
    clearMetricIfNotNull(totalFailedZKCalls);
  }

  private static void clearMetricIfNotNull(MutableGaugeLong metric) {
    if (metric != null) {
      metric.set(0L);
    }
  }

  @Override
  public void incrementAuthFailedCount() {
    authFailedFailedOpCount.incr();
  }

  @Override
  public void incrementConnectionLossCount() {
    connectionLossFailedOpCount.incr();
  }

  @Override
  public void incrementDataInconsistencyCount() {
    dataInconsistencyFailedOpCount.incr();
  }

  @Override
  public void incrementInvalidACLCount() {
    invalidACLFailedOpCount.incr();
  }

  @Override
  public void incrementNoAuthCount() {
    noAuthFailedOpCount.incr();
  }

  @Override
  public void incrementOperationTimeoutCount() {
    operationTimeOutFailedOpCount.incr();
  }

  @Override
  public void incrementRuntimeInconsistencyCount() {
    runtimeInconsistencyFailedOpCount.incr();
  }

  @Override
  public void incrementSessionExpiredCount() {
    sessionExpiredFailedOpCount.incr();
  }

  @Override
  public void incrementSystemErrorCount() {
    systemErrorFailedOpCount.incr();
  }

  @Override
  public void incrementTotalFailedZKCalls() {
    totalFailedZKCalls.incr();
  }

  @Override
  public void recordReadOperationLatency(long latency) {
    readOpLatency.add(latency);
  }

  @Override
  public void recordWriteOperationLatency(long latency) {
    writeOpLatency.add(latency);
  }

  @Override
  public void recordSyncOperationLatency(long latency) {
    syncOpLatency.add(latency);
  }
}
