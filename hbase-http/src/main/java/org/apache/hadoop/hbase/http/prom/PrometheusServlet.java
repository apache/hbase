/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.http.prom;

import static org.apache.hadoop.hbase.http.prom.PrometheusUtils.toPrometheusName;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hbase.metrics.Counter;
import org.apache.hadoop.hbase.metrics.Gauge;
import org.apache.hadoop.hbase.metrics.Histogram;
import org.apache.hadoop.hbase.metrics.Meter;
import org.apache.hadoop.hbase.metrics.Metric;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.Timer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private public class PrometheusServlet extends HttpServlet {

  //Strings used to create metrics names.
  String NUM_OPS_METRIC_NAME = "_num_ops";
  String MIN_METRIC_NAME = "_min";
  String MAX_METRIC_NAME = "_max";
  String MEAN_METRIC_NAME = "_mean";
  String MEDIAN_METRIC_NAME = "_median";
  String TWENTY_FIFTH_PERCENTILE_METRIC_NAME = "_25th_percentile";
  String SEVENTY_FIFTH_PERCENTILE_METRIC_NAME = "_75th_percentile";
  String NINETIETH_PERCENTILE_METRIC_NAME = "_90th_percentile";
  String NINETY_FIFTH_PERCENTILE_METRIC_NAME = "_95th_percentile";
  String NINETY_EIGHTH_PERCENTILE_METRIC_NAME = "_98th_percentile";
  String NINETY_NINETH_PERCENTILE_METRIC_NAME = "_99th_percentile";
  String NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC_NAME = "_99.9th_percentile";

  Logger LOG = LoggerFactory.getLogger(PrometheusServlet.class);

  @Override protected void doGet(HttpServletRequest req, HttpServletResponse resp)
    throws IOException {

    final PrintWriter op = resp.getWriter();
    final List<Pair<String, Number>> expandedMetrics = new LinkedList<>();

    MetricRegistries.global().getMetricRegistries()
      .forEach(mr -> snapshotAllMetrics(mr, expandedMetrics));
    expandedMetrics.forEach(
      p -> op.append(p.getFirst()).append(" ").append(p.getSecond().toString()).append('\n'));
  }

  public void snapshotAllMetrics(MetricRegistry metricRegistry,
    List<Pair<String, Number>> builder) {
    Map<String, Metric> metrics = metricRegistry.getMetrics();

    for (Map.Entry<String, Metric> e : metrics.entrySet()) {

      String name = toPrometheusName(metricRegistry.getMetricRegistryInfo().getMetricsName(), e.getKey());
      Metric metric = e.getValue();

      if (metric instanceof Gauge) {
        addGauge(name, (Gauge<Number>) metric, builder);
      } else if (metric instanceof Counter) {
        addCounter(name, (Counter) metric, builder);
      } else if (metric instanceof Histogram) {
        addHistogram(name, (Histogram) metric, builder);
      } else if (metric instanceof Meter) {
        addMeter(name, (Meter) metric, builder);
      } else if (metric instanceof Timer) {
        addTimer(name, (Timer) metric, builder);
      } else {
        LOG.info("Ignoring unknown Metric class " + metric.getClass().getName());
      }
    }
  }

  private void addGauge(String name, Gauge<Number> gauge, List<Pair<String, Number>> builder) {
    builder.add(new Pair<>(name, gauge.getValue()));
  }

  private void addCounter(String name, Counter counter, List<Pair<String, Number>> builder) {
    builder.add(new Pair<>(name, counter.getCount()));
  }

  private void addHistogram(String name, Histogram histogram, List<Pair<String, Number>> builder) {
    Snapshot snapshot = histogram.snapshot();

    builder.add(new Pair<>(name + NUM_OPS_METRIC_NAME, histogram.getCount()));
    builder.add(new Pair<>(name + MIN_METRIC_NAME, snapshot.getMin()));
    builder.add(new Pair<>(name + MAX_METRIC_NAME, snapshot.getMax()));
    builder.add(new Pair<>(name + MEAN_METRIC_NAME, snapshot.getMean()));
    builder
      .add(new Pair<>(name + TWENTY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.get25thPercentile()));
    builder.add(new Pair<>(name + MEDIAN_METRIC_NAME, snapshot.getMedian()));
    builder
      .add(new Pair<>(name + SEVENTY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.get75thPercentile()));
    builder.add(new Pair<>(name + NINETIETH_PERCENTILE_METRIC_NAME, snapshot.get90thPercentile()));
    builder
      .add(new Pair<>(name + NINETY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.get95thPercentile()));
    builder
      .add(new Pair<>(name + NINETY_EIGHTH_PERCENTILE_METRIC_NAME, snapshot.get98thPercentile()));
    builder
      .add(new Pair<>(name + NINETY_NINETH_PERCENTILE_METRIC_NAME, snapshot.get99thPercentile()));
    builder.add(new Pair<>(name + NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC_NAME,
      snapshot.get999thPercentile()));
  }

  private void addMeter(String name, Meter meter, List<Pair<String, Number>> builder) {
    builder.add(new Pair<>(name + "_count", meter.getCount()));
    builder.add(new Pair<>(name + "_mean_rate", meter.getMeanRate()));
    builder.add(new Pair<>(name + "_1min_rate", meter.getOneMinuteRate()));
    builder.add(new Pair<>(name + "_5min_rate", meter.getFiveMinuteRate()));
    builder.add(new Pair<>(name + "_15min_rate", meter.getFifteenMinuteRate()));
  }

  private void addTimer(String name, Timer timer, List<Pair<String, Number>> builder) {
    addMeter(name, timer.getMeter(), builder);
    addHistogram(name, timer.getHistogram(), builder);
  }
}
