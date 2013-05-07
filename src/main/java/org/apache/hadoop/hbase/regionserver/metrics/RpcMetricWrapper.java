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

package org.apache.hadoop.hbase.regionserver.metrics;

import org.apache.hadoop.hbase.util.Histogram;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.MetricsRecord;

/**
 * This is a container class for metrics associated with RPC calls. It keeps a
 * rate metric, which provides the min/max/avg for the metric. It also contains
 * a percentile metric, which provides the approx value at a specific
 * percentile (in this case, 95.0), in the distribution of the metric.
 */
public class RpcMetricWrapper {
  public static final double percentile = PercentileMetric.P95;
  private PercentileMetric percentileMetric;
  private MetricsTimeVaryingRate rateMetric;

  /**
   * @param key The key for RpcMetricWrapper
   * @param metricsRegistry The appropriate metric registry
   */
  public RpcMetricWrapper(String key, MetricsRegistry metricsRegistry) {
    this.percentileMetric =
            new PercentileMetric((key + "_p" + (long)percentile),
                    metricsRegistry, new Histogram(), percentile,
                    PercentileMetric.HISTOGRAM_NUM_BUCKETS_DEFAULT);
    this.rateMetric = new MetricsTimeVaryingRate(key, metricsRegistry);
  }

  /**
   * Increment the metric by <code>amt</code>.
   * @param amt
   */
  public void inc(int amt) {
    rateMetric.inc(amt);
    percentileMetric.addValueInHistogram(amt);
  }

  /**
   * Push the metric to the MetricsRecord.
   * @param metricsRecord
   */
  public void pushMetric(MetricsRecord metricsRecord) {
    percentileMetric.pushMetric(metricsRecord);
    rateMetric.pushMetric(metricsRecord);
  }

  /**
   * Reset the metric
   */
  public void resetMetric() {
    percentileMetric.refresh();
    rateMetric.resetMinMax();
  }
}
