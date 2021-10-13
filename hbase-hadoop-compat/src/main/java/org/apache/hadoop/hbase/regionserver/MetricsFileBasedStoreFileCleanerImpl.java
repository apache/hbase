/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Implementation of {@link MetricsFileBasedStoreFileCleaner} to track metrics for a specific
 * RegionServer.
 */
@InterfaceAudience.Private public class MetricsFileBasedStoreFileCleanerImpl extends BaseSourceImpl
  implements MetricsFileBasedStoreFileCleaner {

  private MutableFastCounter fileBasedStoreFileCleanerDeletes;
  private MutableFastCounter fileBasedStoreFileCleanerFailedDeletes;
  private MutableFastCounter fileBasedStoreFileCleanerRuns;
  private MutableTimeHistogram fileBasedStoreFileCleanerTimer;

  public MetricsFileBasedStoreFileCleanerImpl(String metricsName, String metricsDescription,
    String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    fileBasedStoreFileCleanerDeletes = getMetricsRegistry().newCounter(DELETES, DELETES_DESC, 0l);
    fileBasedStoreFileCleanerFailedDeletes = getMetricsRegistry().newCounter(FAILED_DELETES, FAILED_DELETES_DESC, 0l);
    fileBasedStoreFileCleanerRuns = getMetricsRegistry().newCounter(RUNS, RUNS_DESC, 0l);
    fileBasedStoreFileCleanerTimer = getMetricsRegistry().newTimeHistogram(RUNTIME, RUNTIME_DESC);
  }

  public MetricsFileBasedStoreFileCleanerImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  @Override public void incrementFileBasedStoreFileCleanerDeletes(long deletes) {
    fileBasedStoreFileCleanerDeletes.incr(deletes);
  }

  @Override public void incrementFileBasedStoreFileCleanerFailedDeletes(long failedDeletes) {
    fileBasedStoreFileCleanerFailedDeletes.incr(failedDeletes);
  }

  @Override public void incrementFileBasedStoreFileCleanerRuns() {
    fileBasedStoreFileCleanerRuns.incr();
  }

  @Override public void updateFileBasedStoreFileCleanerTimer(long millis) {
    fileBasedStoreFileCleanerTimer.add(millis);
  }
}
