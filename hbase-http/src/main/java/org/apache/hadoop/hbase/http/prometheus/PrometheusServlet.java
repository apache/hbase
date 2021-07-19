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

package org.apache.hadoop.hbase.http.prometheus;

import static org.apache.hadoop.hbase.http.prometheus.PrometheusUtils.toPrometheusName;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
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
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class PrometheusServlet extends HttpServlet {

  //Strings used to create metrics names.
  private final String NUM_OPS_METRIC_NAME = "_num_ops";
  private final String MIN_METRIC_NAME = "_min";
  private final String MAX_METRIC_NAME = "_max";
  private final String MEAN_METRIC_NAME = "_mean";
  private final String MEDIAN_METRIC_NAME = "_median";
  private final String TWENTY_FIFTH_PERCENTILE_METRIC_NAME = "_25th_percentile";
  private final String SEVENTY_FIFTH_PERCENTILE_METRIC_NAME = "_75th_percentile";
  private final String NINETIETH_PERCENTILE_METRIC_NAME = "_90th_percentile";
  private final String NINETY_FIFTH_PERCENTILE_METRIC_NAME = "_95th_percentile";
  private final String NINETY_EIGHTH_PERCENTILE_METRIC_NAME = "_98th_percentile";
  private final String NINETY_NINETH_PERCENTILE_METRIC_NAME = "_99th_percentile";
  private final String NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC_NAME = "_99.9th_percentile";

  private final Logger LOG = LoggerFactory.getLogger(PrometheusServlet.class);

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
    throws IOException {
    final PrintWriter op = resp.getWriter();
    writeMetrics(MetricRegistries.global().getMetricRegistries(), op);
  }

  @VisibleForTesting
  void writeMetrics(Collection<MetricRegistry> metricRegistries, PrintWriter pw) {
    final List<Pair<String, Number>> metrics = new LinkedList<>();
    metricRegistries.forEach(mr -> snapshotAllMetrics(mr, metrics));
    metrics.forEach(p -> pw.append(p.getFirst()).append(" ").println(p.getSecond()));
    pw.flush();
  }

  private void snapshotAllMetrics(MetricRegistry metricRegistry,
    List<Pair<String, Number>> metricList) {
    Map<String, Metric> metrics = metricRegistry.getMetrics();

    for (Map.Entry<String, Metric> e : metrics.entrySet()) {

      String name =
        toPrometheusName(metricRegistry.getMetricRegistryInfo().getMetricsName(), e.getKey());
      Metric metric = e.getValue();

      if (metric instanceof Gauge) {
        addGauge(name, (Gauge<Number>) metric, metricList);
      } else if (metric instanceof Counter) {
        addCounter(name, (Counter) metric, metricList);
      } else if (metric instanceof Histogram) {
        addHistogram(name, (Histogram) metric, metricList);
      } else if (metric instanceof Meter) {
        addMeter(name, (Meter) metric, metricList);
      } else if (metric instanceof Timer) {
        addTimer(name, (Timer) metric, metricList);
      } else {
        LOG.info("Ignoring unknown Metric class " + metric.getClass().getName());
      }
    }
  }

  private void addGauge(String name, Gauge<Number> gauge, List<Pair<String, Number>> metricList) {
    metricList.add(new Pair<>(name, gauge.getValue()));
  }

  private void addCounter(String name, Counter counter, List<Pair<String, Number>> metricList) {
    metricList.add(new Pair<>(name, counter.getCount()));
  }

  private void addHistogram(String name, Histogram histogram, List<Pair<String, Number>> metricList) {
    Snapshot snapshot = histogram.snapshot();

    metricList.add(new Pair<>(name + NUM_OPS_METRIC_NAME, histogram.getCount()));
    metricList.add(new Pair<>(name + MIN_METRIC_NAME, snapshot.getMin()));
    metricList.add(new Pair<>(name + MAX_METRIC_NAME, snapshot.getMax()));
    metricList.add(new Pair<>(name + MEAN_METRIC_NAME, snapshot.getMean()));
    metricList
      .add(new Pair<>(name + TWENTY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.get25thPercentile()));
    metricList.add(new Pair<>(name + MEDIAN_METRIC_NAME, snapshot.getMedian()));
    metricList
      .add(new Pair<>(name + SEVENTY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.get75thPercentile()));
    metricList.add(new Pair<>(name + NINETIETH_PERCENTILE_METRIC_NAME, snapshot.get90thPercentile()));
    metricList
      .add(new Pair<>(name + NINETY_FIFTH_PERCENTILE_METRIC_NAME, snapshot.get95thPercentile()));
    metricList
      .add(new Pair<>(name + NINETY_EIGHTH_PERCENTILE_METRIC_NAME, snapshot.get98thPercentile()));
    metricList
      .add(new Pair<>(name + NINETY_NINETH_PERCENTILE_METRIC_NAME, snapshot.get99thPercentile()));
    metricList.add(new Pair<>(name + NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC_NAME,
      snapshot.get999thPercentile()));
  }

  private void addMeter(String name, Meter meter, List<Pair<String, Number>> metricList) {
    metricList.add(new Pair<>(name + "_count", meter.getCount()));
    metricList.add(new Pair<>(name + "_mean_rate", meter.getMeanRate()));
    metricList.add(new Pair<>(name + "_1min_rate", meter.getOneMinuteRate()));
    metricList.add(new Pair<>(name + "_5min_rate", meter.getFiveMinuteRate()));
    metricList.add(new Pair<>(name + "_15min_rate", meter.getFifteenMinuteRate()));
  }

  private void addTimer(String name, Timer timer, List<Pair<String, Number>> metricList) {
    addMeter(name, timer.getMeter(), metricList);
    addHistogram(name, timer.getHistogram(), metricList);
  }
}
