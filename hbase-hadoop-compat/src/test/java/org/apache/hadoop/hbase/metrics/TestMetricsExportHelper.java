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

import java.util.Collection;
import org.apache.hadoop.hbase.testclassification.MetricsTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.impl.MetricsExportHelper;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MetricsTests.TAG)
@Tag(SmallTests.TAG)
public class TestMetricsExportHelper {

  @Test
  public void testExportHelper() {
    DefaultMetricsSystem.initialize("exportHelperTestSystem");
    DefaultMetricsSystem.instance().start();

    String metricsName = "exportMetricsTestGrp";
    String gaugeName = "exportMetricsTestGauge";
    String counterName = "exportMetricsTestCounter";

    BaseSourceImpl baseSource = new BaseSourceImpl(metricsName, "", metricsName, metricsName);

    baseSource.setGauge(gaugeName, 0);
    baseSource.incCounters(counterName, 1);

    Collection<MetricsRecord> metrics = MetricsExportHelper.export();
    DefaultMetricsSystem.instance().stop();

    Assertions.assertTrue(metrics.stream().anyMatch(mr -> mr.name().equals(metricsName)));
    Assertions.assertTrue(contains(metrics, metricsName, gaugeName),
      gaugeName + " is missing in the export");
    Assertions.assertTrue(contains(metrics, metricsName, counterName),
      counterName + " is missing in the export");
  }

  private boolean contains(Collection<MetricsRecord> metrics, String metricsName,
    String metricName) {
    return metrics.stream().filter(mr -> mr.name().equals(metricsName)).anyMatch(mr -> {
      for (AbstractMetric metric : mr.metrics()) {
        if (metric.name().equals(metricName)) {
          return true;
        }
      }
      return false;
    });
  }
}
