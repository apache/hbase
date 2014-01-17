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

package org.apache.hadoop.hbase.rest;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MetricMutableCounterLong;

/**
 * Hadoop One implementation of a metrics2 source that will export metrics from the Rest server to
 * the hadoop metrics2 subsystem.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
public class MetricsRESTSourceImpl extends BaseSourceImpl implements MetricsRESTSource {

  private MetricMutableCounterLong request;
  private MetricMutableCounterLong sucGet;
  private MetricMutableCounterLong sucPut;
  private MetricMutableCounterLong sucDel;
  private MetricMutableCounterLong sucScan;
  private MetricMutableCounterLong fGet;
  private MetricMutableCounterLong fPut;
  private MetricMutableCounterLong fDel;
  private MetricMutableCounterLong fScan;

  public MetricsRESTSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, CONTEXT, JMX_CONTEXT);
  }

  public MetricsRESTSourceImpl(String metricsName,
                               String metricsDescription,
                               String metricsContext,
                               String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void init() {
    super.init();
    request = getMetricsRegistry().getLongCounter(REQUEST_KEY, 0l);

    sucGet = getMetricsRegistry().getLongCounter(SUCCESSFUL_GET_KEY, 0l);
    sucPut = getMetricsRegistry().getLongCounter(SUCCESSFUL_PUT_KEY, 0l);
    sucDel = getMetricsRegistry().getLongCounter(SUCCESSFUL_DELETE_KEY, 0l);
    sucScan = getMetricsRegistry().getLongCounter(SUCCESSFUL_SCAN_KEY, 0L);

    fGet = getMetricsRegistry().getLongCounter(FAILED_GET_KEY, 0l);
    fPut = getMetricsRegistry().getLongCounter(FAILED_PUT_KEY, 0l);
    fDel = getMetricsRegistry().getLongCounter(FAILED_DELETE_KEY, 0l);
    fScan = getMetricsRegistry().getLongCounter(FAILED_SCAN_KEY, 0l);
  }

  @Override
  public void incrementRequests(int inc) {
    request.incr(inc);
  }

  @Override
  public void incrementSucessfulGetRequests(int inc) {
    sucGet.incr(inc);
  }

  @Override
  public void incrementSucessfulPutRequests(int inc) {
    sucPut.incr(inc);
  }

  @Override
  public void incrementSucessfulDeleteRequests(int inc) {
    sucDel.incr(inc);
  }

  @Override
  public void incrementFailedGetRequests(int inc) {
    fGet.incr(inc);
  }

  @Override
  public void incrementFailedPutRequests(int inc) {
    fPut.incr(inc);
  }

  @Override
  public void incrementFailedDeleteRequests(int inc) {
    fDel.incr(inc);
  }

  @Override
  public void incrementSucessfulScanRequests(int inc) {
    sucScan.incr(inc);
  }

  @Override
  public void incrementFailedScanRequests(int inc) {
    fScan.incr(inc);
  }
}
