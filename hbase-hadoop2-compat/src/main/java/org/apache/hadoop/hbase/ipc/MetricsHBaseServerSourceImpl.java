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
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

@InterfaceAudience.Private
public class MetricsHBaseServerSourceImpl extends BaseSourceImpl
    implements MetricsHBaseServerSource {


  private final MetricsHBaseServerWrapper wrapper;
  private final MutableFastCounter authorizationSuccesses;
  private final MutableFastCounter authorizationFailures;
  private final MutableFastCounter authenticationSuccesses;
  private final MutableFastCounter authenticationFailures;
  private final MutableFastCounter authenticationFallbacks;
  private final MutableFastCounter sentBytes;
  private final MutableFastCounter receivedBytes;

  private final MutableFastCounter exceptions;
  private final MutableFastCounter exceptionsOOO;
  private final MutableFastCounter exceptionsBusy;
  private final MutableFastCounter exceptionsUnknown;
  private final MutableFastCounter exceptionsSanity;
  private final MutableFastCounter exceptionsNSRE;
  private final MutableFastCounter exceptionsMoved;
  private final MutableFastCounter exceptionsMultiTooLarge;


  private MetricHistogram queueCallTime;
  private MetricHistogram processCallTime;
  private MetricHistogram totalCallTime;
  private MetricHistogram requestSize;
  private MetricHistogram responseSize;

  public MetricsHBaseServerSourceImpl(String metricsName,
                                      String metricsDescription,
                                      String metricsContext,
                                      String metricsJmxContext,
                                      MetricsHBaseServerWrapper wrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.wrapper = wrapper;

    this.authorizationSuccesses = this.getMetricsRegistry().newCounter(AUTHORIZATION_SUCCESSES_NAME,
        AUTHORIZATION_SUCCESSES_DESC, 0L);
    this.authorizationFailures = this.getMetricsRegistry().newCounter(AUTHORIZATION_FAILURES_NAME,
        AUTHORIZATION_FAILURES_DESC, 0L);

    this.exceptions = this.getMetricsRegistry().newCounter(EXCEPTIONS_NAME, EXCEPTIONS_DESC, 0L);
    this.exceptionsOOO = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_OOO_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsBusy = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_BUSY_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsUnknown = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_UNKNOWN_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsSanity = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_SANITY_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsMoved = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_MOVED_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsNSRE = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_NSRE_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsMultiTooLarge = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_MULTI_TOO_LARGE_NAME, EXCEPTIONS_MULTI_TOO_LARGE_DESC, 0L);

    this.authenticationSuccesses = this.getMetricsRegistry().newCounter(
        AUTHENTICATION_SUCCESSES_NAME, AUTHENTICATION_SUCCESSES_DESC, 0L);
    this.authenticationFailures = this.getMetricsRegistry().newCounter(AUTHENTICATION_FAILURES_NAME,
        AUTHENTICATION_FAILURES_DESC, 0L);
    this.authenticationFallbacks = this.getMetricsRegistry().newCounter(
        AUTHENTICATION_FALLBACKS_NAME, AUTHENTICATION_FALLBACKS_DESC, 0L);
    this.sentBytes = this.getMetricsRegistry().newCounter(SENT_BYTES_NAME,
        SENT_BYTES_DESC, 0L);
    this.receivedBytes = this.getMetricsRegistry().newCounter(RECEIVED_BYTES_NAME,
        RECEIVED_BYTES_DESC, 0L);
    this.queueCallTime = this.getMetricsRegistry().newTimeHistogram(QUEUE_CALL_TIME_NAME,
        QUEUE_CALL_TIME_DESC);
    this.processCallTime = this.getMetricsRegistry().newTimeHistogram(PROCESS_CALL_TIME_NAME,
        PROCESS_CALL_TIME_DESC);
    this.totalCallTime = this.getMetricsRegistry().newTimeHistogram(TOTAL_CALL_TIME_NAME,
        TOTAL_CALL_TIME_DESC);
    this.requestSize = this.getMetricsRegistry().newSizeHistogram(REQUEST_SIZE_NAME,
        REQUEST_SIZE_DESC);
    this.responseSize = this.getMetricsRegistry().newSizeHistogram(RESPONSE_SIZE_NAME,
              RESPONSE_SIZE_DESC);
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
  public void authenticationFallback() {
    authenticationFallbacks.incr();
  }

  @Override
  public void exception() {
    exceptions.incr();
  }

  @Override
  public void outOfOrderException() {
    exceptionsOOO.incr();
  }

  @Override
  public void failedSanityException() {
    exceptionsSanity.incr();
  }

  @Override
  public void movedRegionException() {
    exceptionsMoved.incr();
  }

  @Override
  public void notServingRegionException() {
    exceptionsNSRE.incr();
  }

  @Override
  public void unknownScannerException() {
    exceptionsUnknown.incr();
  }

  @Override
  public void tooBusyException() {
    exceptionsBusy.incr();
  }

  @Override
  public void multiActionTooLargeException() {
    exceptionsMultiTooLarge.incr();
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
  public void sentResponse(long count) { this.responseSize.add(count); }

  @Override
  public void receivedRequest(long count) { this.requestSize.add(count); }

  @Override
  public void dequeuedCall(int qTime) {
    queueCallTime.add(qTime);
  }

  @Override
  public void processedCall(int processingTime) {
    processCallTime.add(processingTime);
  }

  @Override
  public void queuedAndProcessedCall(int totalTime) {
    totalCallTime.add(totalTime);
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
              NUM_ACTIVE_HANDLER_DESC), wrapper.getActiveRpcHandlerCount())
          .addCounter(Interns.info(NUM_GENERAL_CALLS_DROPPED_NAME,
              NUM_GENERAL_CALLS_DROPPED_DESC), wrapper.getNumGeneralCallsDropped())
          .addCounter(Interns.info(NUM_LIFO_MODE_SWITCHES_NAME,
              NUM_LIFO_MODE_SWITCHES_DESC), wrapper.getNumLifoModeSwitches());
    }

    metricsRegistry.snapshot(mrb, all);
  }
}
