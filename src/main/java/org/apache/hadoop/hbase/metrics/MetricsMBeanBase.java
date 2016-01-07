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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;
import org.apache.hadoop.hbase.metrics.histogram.MetricsHistogram;

import com.yammer.metrics.stats.Snapshot;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsDynamicMBeanBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;

/**
 * Extends the Hadoop MetricsDynamicMBeanBase class to provide JMX support for
 * custom HBase MetricsBase implementations.  MetricsDynamicMBeanBase ignores
 * registered MetricsBase instance that are not instances of one of the
 * org.apache.hadoop.metrics.util implementations.
 *
 */
public class MetricsMBeanBase extends MetricsDynamicMBeanBase {

  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.hbase.metrics");

  protected final MetricsRegistry registry;
  protected final String description;
  protected int registryLength;
  /** HBase MetricsBase implementations that MetricsDynamicMBeanBase does
   * not understand
   */
  protected Map<String, MetricsBase> extendedAttributes =
      new ConcurrentHashMap<String, MetricsBase>();
  protected MBeanInfo extendedInfo;

  protected MetricsMBeanBase( MetricsRegistry mr, String description ) {
    super(copyMinusHBaseMetrics(mr), description);
    this.registry = mr;
    this.description = description;
    this.init();
  }

  /*
   * @param mr MetricsRegistry.
   * @return A copy of the passed MetricsRegistry minus the hbase metrics
   */
  private static MetricsRegistry copyMinusHBaseMetrics(final MetricsRegistry mr) {
    MetricsRegistry copy = new MetricsRegistry();
    for (MetricsBase metric : mr.getMetricsList()) {
      if (metric instanceof MetricsRate || metric instanceof MetricsString ||
          metric instanceof MetricsHistogram || metric instanceof ExactCounterMetric) {
        continue;
      }
      copy.add(metric.getName(), metric);
    }
    return copy;
  }

  protected void init() {
    List<MBeanAttributeInfo> attributes = new ArrayList<MBeanAttributeInfo>();
    MBeanInfo parentInfo = super.getMBeanInfo();
    List<String> parentAttributes = new ArrayList<String>();
    for (MBeanAttributeInfo attr : parentInfo.getAttributes()) {
      attributes.add(attr);
      parentAttributes.add(attr.getName());
    }

    this.registryLength = this.registry.getMetricsList().size();

    for (MetricsBase metric : this.registry.getMetricsList()) {
      if (metric.getName() == null || parentAttributes.contains(metric.getName()))
        continue;

      // add on custom HBase metric types
      if (metric instanceof MetricsRate) {
        attributes.add( new MBeanAttributeInfo(metric.getName(),
            "java.lang.Float", metric.getDescription(), true, false, false) );
        extendedAttributes.put(metric.getName(), metric);
      } else if (metric instanceof MetricsString) {
        attributes.add( new MBeanAttributeInfo(metric.getName(),
            "java.lang.String", metric.getDescription(), true, false, false) );
        extendedAttributes.put(metric.getName(), metric);
        LOG.info("MetricsString added: " + metric.getName());
      } else if (metric instanceof MetricsHistogram) {

        String metricName = metric.getName() + MetricsHistogram.NUM_OPS_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Long", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);

        metricName = metric.getName() + MetricsHistogram.MIN_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Long", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);

        metricName = metric.getName() + MetricsHistogram.MAX_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Long", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);

        metricName = metric.getName() + MetricsHistogram.MEAN_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Float", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);

        metricName = metric.getName() + MetricsHistogram.STD_DEV_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Float", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);

        metricName = metric.getName() + MetricsHistogram.MEDIAN_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Float", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);

        metricName = metric.getName() + MetricsHistogram.SEVENTY_FIFTH_PERCENTILE_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Float", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);

        metricName = metric.getName() + MetricsHistogram.NINETY_FIFTH_PERCENTILE_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Float", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);

        metricName = metric.getName() + MetricsHistogram.NINETY_NINETH_PERCENTILE_METRIC_NAME;
        attributes.add(new MBeanAttributeInfo(metricName,
            "java.lang.Float", metric.getDescription(), true, false, false));
        extendedAttributes.put(metricName, metric);
      }
      // else, its probably a hadoop metric already registered. Skip it.
    }

    LOG.info("new MBeanInfo");
    this.extendedInfo = new MBeanInfo( this.getClass().getName(),
        this.description, attributes.toArray( new MBeanAttributeInfo[0] ),
        parentInfo.getConstructors(), parentInfo.getOperations(),
        parentInfo.getNotifications() );
  }

  private void checkAndUpdateAttributes() {
    if (this.registryLength != this.registry.getMetricsList().size())
      this.init();
  }

  @Override
  public Object getAttribute( String name )
      throws AttributeNotFoundException, MBeanException,
      ReflectionException {

    if (name == null) {
      throw new IllegalArgumentException("Attribute name is NULL");
    }

    /*
     * Ugly.  Since MetricsDynamicMBeanBase implementation is private,
     * we need to first check the parent class for the attribute.
     * In case that the MetricsRegistry contents have changed, this will
     * allow the parent to update it's internal structures (which we rely on
     * to update our own.
     */
    try {
      return super.getAttribute(name);
    } catch (AttributeNotFoundException ex) {

      checkAndUpdateAttributes();

      MetricsBase metric = this.extendedAttributes.get(name);
      if (metric != null) {
        if (metric instanceof MetricsRate) {
          return ((MetricsRate) metric).getPreviousIntervalValue();
        } else if (metric instanceof MetricsString) {
          return ((MetricsString)metric).getValue();
        } else if (metric instanceof MetricsHistogram)  {
          MetricsHistogram hist = (MetricsHistogram) metric;
          if (name.endsWith(MetricsHistogram.NUM_OPS_METRIC_NAME)) {
            return hist.getCount();
          } else if (name.endsWith(MetricsHistogram.MIN_METRIC_NAME)) {
            return hist.getMin();
          } else if (name.endsWith(MetricsHistogram.MAX_METRIC_NAME)) {
            return hist.getMax();
          } else if (name.endsWith(MetricsHistogram.MEAN_METRIC_NAME)) {
            return (float) hist.getMean();
          } else if (name.endsWith(MetricsHistogram.STD_DEV_METRIC_NAME)) {
            return (float) hist.getStdDev();
          } else if (name.endsWith(MetricsHistogram.MEDIAN_METRIC_NAME)) {
            Snapshot s = hist.getSnapshot();
            return (float) s.getMedian();
          } else if (name.endsWith(MetricsHistogram.SEVENTY_FIFTH_PERCENTILE_METRIC_NAME)) {
            Snapshot s = hist.getSnapshot();
            return (float) s.get75thPercentile();
          } else if (name.endsWith(MetricsHistogram.NINETY_FIFTH_PERCENTILE_METRIC_NAME)) {
            Snapshot s = hist.getSnapshot();
            return (float) s.get95thPercentile();
          } else if (name.endsWith(MetricsHistogram.NINETY_NINETH_PERCENTILE_METRIC_NAME)) {
            Snapshot s = hist.getSnapshot();
            return (float) s.get99thPercentile();
          }

        } else {
          LOG.warn( String.format("unknown metrics type %s for attribute %s",
                        metric.getClass().getName(), name) );
        }
      }
    }

    throw new AttributeNotFoundException();
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    return this.extendedInfo;
  }

}
