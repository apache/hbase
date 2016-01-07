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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rest.metrics;

import org.apache.hadoop.hbase.metrics.MetricsRate;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.jvm.JvmMetrics;
import org.apache.hadoop.metrics.util.MetricsRegistry;

public class RESTMetrics implements Updater {
  private final MetricsRecord metricsRecord;
  private final MetricsRegistry registry = new MetricsRegistry();
  private final RESTStatistics restStatistics;

  private MetricsRate requests = new MetricsRate("requests", registry);
  private MetricsRate sucessfulGetCount =
      new MetricsRate("sucessful.get.count", registry);
  private MetricsRate sucessfulPutCount =
      new MetricsRate("sucessful.put.count", registry);
  private MetricsRate sucessfulDeleteCount =
      new MetricsRate("sucessful.delete.count", registry);
  
  private MetricsRate failedGetCount =
      new MetricsRate("failed.get.count", registry);
  private MetricsRate failedPutCount =
      new MetricsRate("failed.put.count", registry);
  private MetricsRate failedDeleteCount =
      new MetricsRate("failed.delete.count", registry);

  public RESTMetrics() {
    MetricsContext context = MetricsUtil.getContext("rest");
    metricsRecord = MetricsUtil.createRecord(context, "rest");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("REST", name);
    context.registerUpdater(this);
    JvmMetrics.init("rest", name);
    // expose the MBean for metrics
    restStatistics = new RESTStatistics(registry);

  }

  public void shutdown() {
    if (restStatistics != null) {
      restStatistics.shutdown();
    }
  }

  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   * @param unused 
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      requests.pushMetric(metricsRecord);
      sucessfulGetCount.pushMetric(metricsRecord);
      sucessfulPutCount.pushMetric(metricsRecord);
      sucessfulDeleteCount.pushMetric(metricsRecord);
      failedGetCount.pushMetric(metricsRecord);
      failedPutCount.pushMetric(metricsRecord);
      failedDeleteCount.pushMetric(metricsRecord);
    }
    this.metricsRecord.update();
  }
  
  public void resetAllMinMax() {
    // Nothing to do
  }

  /**
   * @return Count of requests.
   */
  public float getRequests() {
    return requests.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    requests.inc(inc);
  }
  
  /**
   * @return Count of sucessfulGetCount.
   */
  public float getSucessfulGetCount() {
    return sucessfulGetCount.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to sucessfulGetCount.
   */
  public void incrementSucessfulGetRequests(final int inc) {
    sucessfulGetCount.inc(inc);
  }
  
  /**
   * @return Count of sucessfulGetCount.
   */
  public float getSucessfulPutCount() {
    return sucessfulPutCount.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to sucessfulPutCount.
   */
  public void incrementSucessfulPutRequests(final int inc) {
    sucessfulPutCount.inc(inc);
  }
  
  /**
   * @return Count of failedPutCount.
   */
  public float getFailedPutCount() {
    return failedPutCount.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to failedPutCount.
   */
  public void incrementFailedPutRequests(final int inc) {
    failedPutCount.inc(inc);
  }
  
  /**
   * @return Count of failedGetCount.
   */
  public float getFailedGetCount() {
    return failedGetCount.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to failedGetCount.
   */
  public void incrementFailedGetRequests(final int inc) {
    failedGetCount.inc(inc);
  }
  
  /**
   * @return Count of sucessfulGetCount.
   */
  public float getSucessfulDeleteCount() {
    return sucessfulDeleteCount.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to sucessfulDeleteCount.
   */
  public void incrementSucessfulDeleteRequests(final int inc) {
    sucessfulDeleteCount.inc(inc);
  }

  /**
   * @return Count of failedDeleteCount.
   */
  public float getFailedDeleteCount() {
    return failedDeleteCount.getPreviousIntervalValue();
  }
  
  /**
   * @param inc How much to add to failedDeleteCount.
   */
  public void incrementFailedDeleteRequests(final int inc) {
    failedDeleteCount.inc(inc);
  }
  
}
