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

import junit.framework.TestCase;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.junit.experimental.categories.Category;

import java.util.HashMap;

/**
 * A dummy MetricsRecord class which essentially just maintains a map of
 * metric name to value (Long, in this case). We will use it to check if the
 * RpcMetricsWrapper class properly sets values in the actual metrics record.
 */
class DummyMetricsRecord implements MetricsRecord {
  private HashMap<String, Long> metricMap = new HashMap<String, Long>();

  /** Do not need to implement these functions */
  public String getRecordName() { return ""; }
  public void setTag(String key, String value) { }
  public void setTag(String tagName, int tagValue) {}
  public void setTag(String tagName, long tagValue) {}
  public void setTag(String tagName, short tagValue) {}
  public void setTag(String tagName, byte tagValue) {}
  public void removeTag(String tagName) {}
  public void setMetric(String metricName, int metricValue) {}
  public void setMetric(String metricName, short metricValue) {}
  public void setMetric(String metricName, byte metricValue) {}
  public void setMetric(String metricName, float metricValue) {}
  public void incrMetric(String metricName, long metricValue) {}
  public void incrMetric(String metricName, short metricValue) {}
  public void incrMetric(String metricName, byte metricValue) {}
  public void incrMetric(String metricName, float metricValue) {}
  public void update() {}
  public void remove() {}

  /**
   * Get the metric value
   * @param metricName
   * @return
   */
  public Long getMetric(String metricName) {
    return metricMap.get(metricName);
  }

  /**
   * Set the metric value
   * @param metricName
   * @param metricValue
   */
  public void setMetric(String metricName, long metricValue) {
    metricMap.put(metricName, Long.valueOf(metricValue));
  }

  /**
   * Increment the metric
   * @param metricName
   * @param metricValue
   */
  public void incrMetric(String metricName, int metricValue) {
    Long value = getMetric(metricName);
    if (value == null) {
      setMetric(metricName, (long)metricValue);
    } else {
      setMetric(metricName, value.longValue() + (long)metricValue);
    }
  }
  @Override
  public void incrMetric(String arg0, double arg1) {
    // TODO Auto-generated method stub

  }
  @Override
  public void setMetric(String arg0, double arg1) {
    // TODO Auto-generated method stub

  }
}

@Category(SmallTests.class)
public class TestRpcMetricWrapper extends TestCase {
  private MetricsRegistry registry = new MetricsRegistry();

  /**
   *  Test if the RpcMetricWrapper pushes the metric name properly to the
   *  metric record object.
   */
  public void testRpcMetricPush() {
    String metricName = "foo";
    DummyMetricsRecord dummyMetricsRecord = new DummyMetricsRecord();
    RpcMetricWrapper rpcMetricWrapper =
            new RpcMetricWrapper(metricName, registry);

    for (int i = 1; i <= 100; i++) {
      rpcMetricWrapper.inc(i);
    }
    rpcMetricWrapper.pushMetric(dummyMetricsRecord);

    assertEquals(50, dummyMetricsRecord.getMetric("foo_avg_time").longValue());
    assertEquals(1, dummyMetricsRecord.getMetric("foo_min").longValue());
    assertEquals(100, dummyMetricsRecord.getMetric("foo_max").longValue());
    assertEquals(100, dummyMetricsRecord.getMetric("foo_num_ops").longValue());
    assertEquals(96, dummyMetricsRecord.getMetric("foo_p95").longValue());
  }
}
