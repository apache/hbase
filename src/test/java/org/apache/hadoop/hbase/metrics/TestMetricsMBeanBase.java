/**
 *
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;
import com.yammer.metrics.stats.Snapshot;

import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import junit.framework.TestCase;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMetricsMBeanBase extends TestCase {

  private class TestStatistics extends MetricsMBeanBase {
    public TestStatistics(MetricsRegistry registry) {
      super(registry, "TestStatistics");
    }
  }

  private MetricsRegistry registry;
  private MetricsRecord metricsRecord;
  private TestStatistics stats;
  private MetricsRate metricsRate;
  private MetricsIntValue intValue;
  private MetricsTimeVaryingRate varyRate;

  public void setUp() {
    this.registry = new MetricsRegistry();
    this.metricsRate = new MetricsRate("metricsRate", registry, "test");
    this.intValue = new MetricsIntValue("intValue", registry, "test");
    this.varyRate = new MetricsTimeVaryingRate("varyRate", registry, "test");
    this.stats = new TestStatistics(registry);
    MetricsContext context = MetricsUtil.getContext("hbase");
    this.metricsRecord = MetricsUtil.createRecord(context, "test");
    this.metricsRecord.setTag("TestStatistics", "test");
    //context.registerUpdater(this);

  }

  public void tearDown() {

  }

  public void testGetAttribute() throws Exception {
    this.metricsRate.inc(2);
    Thread.sleep(1000);
    this.metricsRate.pushMetric(this.metricsRecord);
    this.intValue.set(5);
    this.intValue.pushMetric(this.metricsRecord);
    this.varyRate.inc(10);
    this.varyRate.inc(50);
    this.varyRate.pushMetric(this.metricsRecord);


    assertEquals( 2.0, (Float)this.stats.getAttribute("metricsRate"), 0.005 );
    assertEquals( 5, this.stats.getAttribute("intValue") );
    assertEquals( 10L, this.stats.getAttribute("varyRateMinTime") );
    assertEquals( 50L, this.stats.getAttribute("varyRateMaxTime") );
    assertEquals( 30L, this.stats.getAttribute("varyRateAvgTime") );
    assertEquals( 2, this.stats.getAttribute("varyRateNumOps") );
  }

  public void testGetMBeanInfo() {
    MBeanInfo info = this.stats.getMBeanInfo();
    MBeanAttributeInfo[] attributes = info.getAttributes();
    assertEquals( 6, attributes.length );

    Map<String,MBeanAttributeInfo> attributeByName =
        new HashMap<String,MBeanAttributeInfo>(attributes.length);
    for (MBeanAttributeInfo attr : attributes)
      attributeByName.put(attr.getName(), attr);

    assertAttribute( attributeByName.get("metricsRate"),
        "metricsRate", "java.lang.Float", "test");
    assertAttribute( attributeByName.get("intValue"),
        "intValue", "java.lang.Integer", "test");
    assertAttribute( attributeByName.get("varyRateMinTime"),
        "varyRateMinTime", "java.lang.Long", "test");
    assertAttribute( attributeByName.get("varyRateMaxTime"),
        "varyRateMaxTime", "java.lang.Long", "test");
    assertAttribute( attributeByName.get("varyRateAvgTime"),
        "varyRateAvgTime", "java.lang.Long", "test");
    assertAttribute( attributeByName.get("varyRateNumOps"),
        "varyRateNumOps", "java.lang.Integer", "test");
  }

  public void testMetricsMBeanBaseHistogram()
      throws ReflectionException, AttributeNotFoundException, MBeanException {
    MetricsRegistry mr = new MetricsRegistry();
    MetricsHistogram histo = mock(MetricsHistogram.class);
    Snapshot snap = mock(Snapshot.class);

    //Set up the mocks
    String histoName = "MockHisto";
    when(histo.getName()).thenReturn(histoName);
    when(histo.getCount()).thenReturn(20l);
    when(histo.getMin()).thenReturn(1l);
    when(histo.getMax()).thenReturn(999l);
    when(histo.getMean()).thenReturn(500.2);
    when(histo.getStdDev()).thenReturn(1.2);
    when(histo.getSnapshot()).thenReturn(snap);

    when(snap.getMedian()).thenReturn(490.0);
    when(snap.get75thPercentile()).thenReturn(550.0);
    when(snap.get95thPercentile()).thenReturn(900.0);
    when(snap.get99thPercentile()).thenReturn(990.0);

    mr.add("myTestHisto", histo);

    MetricsMBeanBase mBeanBase = new MetricsMBeanBase(mr, "test");

    assertEquals(new Long(20), mBeanBase
        .getAttribute(histoName + MetricsHistogram.NUM_OPS_METRIC_NAME));
    assertEquals(new Long(1), mBeanBase
        .getAttribute(histoName + MetricsHistogram.MIN_METRIC_NAME));
    assertEquals(new Long(999), mBeanBase
        .getAttribute(histoName + MetricsHistogram.MAX_METRIC_NAME));
    assertEquals(new Float(500.2), mBeanBase
        .getAttribute(histoName + MetricsHistogram.MEAN_METRIC_NAME));
    assertEquals(new Float(1.2), mBeanBase
        .getAttribute(histoName + MetricsHistogram.STD_DEV_METRIC_NAME));

    assertEquals(new Float(490.0), mBeanBase
        .getAttribute(histoName + MetricsHistogram.MEDIAN_METRIC_NAME));
    assertEquals(new Float(550.0), mBeanBase
        .getAttribute(histoName + MetricsHistogram.SEVENTY_FIFTH_PERCENTILE_METRIC_NAME));
    assertEquals(new Float(900.0), mBeanBase
        .getAttribute(histoName + MetricsHistogram.NINETY_FIFTH_PERCENTILE_METRIC_NAME));
    assertEquals(new Float(990.0), mBeanBase
        .getAttribute(histoName + MetricsHistogram.NINETY_NINETH_PERCENTILE_METRIC_NAME));
  }

  protected void assertAttribute(MBeanAttributeInfo attr, String name,
      String type, String description) {

    assertEquals(attr.getName(), name);
    assertEquals(attr.getType(), type);
    assertEquals(attr.getDescription(), description);
  }


  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

