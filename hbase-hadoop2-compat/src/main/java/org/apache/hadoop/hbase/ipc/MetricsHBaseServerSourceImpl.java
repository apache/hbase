/**
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

package org.apache.hadoop.hbase.ipc;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableHistogram;

@InterfaceAudience.Private
public class MetricsHBaseServerSourceImpl extends BaseSourceImpl
    implements MetricsHBaseServerSource {

  private final MetricsHBaseServerWrapper wrapper;
  private final MutableCounterLong authorizationSuccesses;
  private final MutableCounterLong authorizationFailures;
  private final MutableCounterLong authenticationSuccesses;
  private final MutableCounterLong authenticationFailures;
  private final MutableCounterLong sentBytes;
  private final MutableCounterLong receivedBytes;
  private MutableHistogram queueCallTime;
  private MutableHistogram processCallTime;

  public MetricsHBaseServerSourceImpl(String metricsName,
                                      String metricsDescription,
                                      String metricsContext,
                                      String metricsJmxContext,
                                      MetricsHBaseServerWrapper wrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.wrapper = wrapper;

    this.authorizationSuccesses = this.getMetricsRegistry().newCounter(AUTHORIZATION_SUCCESSES_NAME,
        AUTHORIZATION_SUCCESSES_DESC, 0l);
    this.authorizationFailures = this.getMetricsRegistry().newCounter(AUTHORIZATION_FAILURES_NAME,
        AUTHORIZATION_FAILURES_DESC, 0l);

    this.authenticationSuccesses = this.getMetricsRegistry().newCounter(
        AUTHENTICATION_SUCCESSES_NAME, AUTHENTICATION_SUCCESSES_DESC, 0l);
    this.authenticationFailures = this.getMetricsRegistry().newCounter(AUTHENTICATION_FAILURES_NAME,
        AUTHENTICATION_FAILURES_DESC, 0l);
    this.sentBytes = this.getMetricsRegistry().newCounter(SENT_BYTES_NAME,
        SENT_BYTES_DESC, 0l);
    this.receivedBytes = this.getMetricsRegistry().newCounter(RECEIVED_BYTES_NAME,
        RECEIVED_BYTES_DESC, 0l);
    this.queueCallTime = this.getMetricsRegistry().newHistogram(QUEUE_CALL_TIME_NAME,
        QUEUE_CALL_TIME_DESC);
    this.processCallTime = this.getMetricsRegistry().newHistogram(PROCESS_CALL_TIME_NAME,
        PROCESS_CALL_TIME_DESC);
  }

  @Override
  public void authorizationSuccess() {
    authorizationSuccesses.incr();
  }

  @Override
  public void authorizationFailure() {
    authorizationFailures.incr();
  }

  @Override
  public void authenticationFailure() {
    authenticationFailures.incr();
  }

  @Override
  public void authenticationSuccess() {
    authenticationSuccesses.incr();
  }

  @Override
  public void sentBytes(long count) {
    this.sentBytes.incr(count);
  }

  @Override
  public void receivedBytes(int count) {
    this.receivedBytes.incr(count);
  }

  @Override
  public void dequeuedCall(int qTime) {
    queueCallTime.add(qTime);
  }

  @Override
  public void processedCall(int processingTime) {
    processCallTime.add(processingTime);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);

    if (wrapper != null) {
      mrb.addGauge(Interns.info(QUEUE_SIZE_NAME, QUEUE_SIZE_DESC), wrapper.getTotalQueueSize())
          .addGauge(Interns.info(GENERAL_QUEUE_NAME, GENERAL_QUEUE_DESC),
              wrapper.getGeneralQueueLength())
          .addGauge(Interns.info(REPLICATION_QUEUE_NAME,
              REPLICATION_QUEUE_DESC), wrapper.getReplicationQueueLength())
          .addGauge(Interns.info(PRIORITY_QUEUE_NAME, PRIORITY_QUEUE_DESC),
              wrapper.getPriorityQueueLength())
          .addGauge(Interns.info(NUM_OPEN_CONNECTIONS_NAME,
              NUM_OPEN_CONNECTIONS_DESC), wrapper.getNumOpenConnections())
          .addGauge(Interns.info(NUM_ACTIVE_HANDLER_NAME,
              NUM_ACTIVE_HANDLER_DESC), wrapper.getActiveRpcHandlerCount());
    }

    metricsRegistry.snapshot(mrb, all);
  }
}
