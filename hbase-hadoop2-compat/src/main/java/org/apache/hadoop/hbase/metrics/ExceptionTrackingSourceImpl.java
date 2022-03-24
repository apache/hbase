/*
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

package org.apache.hadoop.hbase.metrics;

import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Common base implementation for metrics sources which need to track exceptions thrown or
 * received.
 */
@InterfaceAudience.Private
public class ExceptionTrackingSourceImpl extends BaseSourceImpl
    implements ExceptionTrackingSource {
  protected MutableFastCounter exceptions;
  protected MutableFastCounter exceptionsOOO;
  protected MutableFastCounter exceptionsBusy;
  protected MutableFastCounter exceptionsUnknown;
  protected MutableFastCounter exceptionsScannerReset;
  protected MutableFastCounter exceptionsSanity;
  protected MutableFastCounter exceptionsNSRE;
  protected MutableFastCounter exceptionsMoved;
  protected MutableFastCounter exceptionsMultiTooLarge;
  protected MutableFastCounter exceptionsCallQueueTooBig;
  protected MutableFastCounter exceptionsQuotaExceeded;
  protected MutableFastCounter exceptionsRpcThrottling;
  protected MutableFastCounter exceptionRequestTooBig;
  protected MutableFastCounter otherExceptions;

  public ExceptionTrackingSourceImpl(String metricsName, String metricsDescription,
                                     String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void init() {
    super.init();
    this.exceptions = this.getMetricsRegistry().newCounter(EXCEPTIONS_NAME, EXCEPTIONS_DESC, 0L);
    this.exceptionsOOO = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_OOO_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsBusy = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_BUSY_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsUnknown = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_UNKNOWN_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsScannerReset = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_SCANNER_RESET_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsSanity = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_SANITY_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsMoved = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_MOVED_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsNSRE = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_NSRE_NAME, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsMultiTooLarge = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_MULTI_TOO_LARGE_NAME, EXCEPTIONS_MULTI_TOO_LARGE_DESC, 0L);
    this.exceptionsCallQueueTooBig = this.getMetricsRegistry()
        .newCounter(EXCEPTIONS_CALL_QUEUE_TOO_BIG, EXCEPTIONS_CALL_QUEUE_TOO_BIG_DESC, 0L);
    this.exceptionsQuotaExceeded = this.getMetricsRegistry()
      .newCounter(EXCEPTIONS_QUOTA_EXCEEDED, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionsRpcThrottling = this.getMetricsRegistry()
      .newCounter(EXCEPTIONS_RPC_THROTTLING, EXCEPTIONS_TYPE_DESC, 0L);
    this.exceptionRequestTooBig = this.getMetricsRegistry()
      .newCounter(EXCEPTIONS_REQUEST_TOO_BIG, EXCEPTIONS_TYPE_DESC, 0L);
    this.otherExceptions = this.getMetricsRegistry()
      .newCounter(OTHER_EXCEPTIONS, EXCEPTIONS_TYPE_DESC, 0L);
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
  public void scannerResetException() {
    exceptionsScannerReset.incr();
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
  public void callQueueTooBigException() {
    exceptionsCallQueueTooBig.incr();
  }

  @Override
  public void quotaExceededException() {
    exceptionsQuotaExceeded.incr();
  }

  @Override
  public void rpcThrottlingException() {
    exceptionsRpcThrottling.incr();
  }

  @Override
  public void requestTooBigException() {
    exceptionRequestTooBig.incr();
  }

  @Override
  public void otherExceptions() {
    otherExceptions.incr();
  }
}
