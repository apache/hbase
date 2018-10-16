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

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Container class for commonly collected metrics for most operations. Instantiate this class to
 * collect submitted count, failed count and time histogram for an operation.
 */
@InterfaceAudience.Private
public class OperationMetrics {
  private static final String SUBMITTED_COUNT = "SubmittedCount";
  private static final String TIME = "Time";
  private static final String FAILED_COUNT = "FailedCount";

  final private Counter submittedCounter;
  final private Histogram timeHisto;
  final private Counter failedCounter;

  public OperationMetrics(final MetricRegistry registry, final String metricNamePrefix) {
    Preconditions.checkNotNull(registry);
    Preconditions.checkNotNull(metricNamePrefix);

    /**
     * TODO: As of now, Metrics description cannot be added/ registered with
     * {@link MetricRegistry}. As metric names are unambiguous but concise, descriptions of
     * metrics need to be made available someplace for users.
     */
    submittedCounter = registry.counter(metricNamePrefix + SUBMITTED_COUNT);
    timeHisto = registry.histogram(metricNamePrefix + TIME);
    failedCounter = registry.counter(metricNamePrefix + FAILED_COUNT);
  }

  public Counter getSubmittedCounter() {
    return submittedCounter;
  }

  public Histogram getTimeHisto() {
    return timeHisto;
  }

  public Counter getFailedCounter() {
    return failedCounter;
  }
}
