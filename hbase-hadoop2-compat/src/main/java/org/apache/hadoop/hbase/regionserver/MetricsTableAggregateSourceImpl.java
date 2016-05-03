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

package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

@InterfaceAudience.Private
public class MetricsTableAggregateSourceImpl extends BaseSourceImpl
implements MetricsTableAggregateSource {

  private static final Log LOG = LogFactory.getLog(MetricsTableAggregateSourceImpl.class);
  private ConcurrentHashMap<String, MetricsTableSource> tableSources = new ConcurrentHashMap<>();

  public MetricsTableAggregateSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsTableAggregateSourceImpl(String metricsName,
      String metricsDescription,
      String metricsContext,
      String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void register(String table, MetricsTableSource source) {
    tableSources.put(table, source);
  }

  @Override
  public void deregister(String table) {
    try {
      tableSources.remove(table);
    } catch (Exception e) {
      // Ignored. If this errors out it means that someone is double
      // closing the region source and the region is already nulled out.
      LOG.info(
        "Error trying to remove " + table + " from " + this.getClass().getSimpleName(),
        e);
    }
  }

  /**
   * Yes this is a get function that doesn't return anything.  Thanks Hadoop for breaking all
   * expectations of java programmers.  Instead of returning anything Hadoop metrics expects
   * getMetrics to push the metrics into the collector.
   *
   * @param collector the collector
   * @param all       get all the metrics regardless of when they last changed.
   */
  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder mrb = collector.addRecord(metricsName);

    if (tableSources != null) {
      for (MetricsTableSource tableMetricSource : tableSources.values()) {
        if (tableMetricSource instanceof MetricsTableSourceImpl) {
          ((MetricsTableSourceImpl) tableMetricSource).snapshot(mrb, all);
        }
      }
      mrb.addGauge(Interns.info(NUM_TABLES, NUMBER_OF_TABLES_DESC), tableSources.size());
      metricsRegistry.snapshot(mrb, all);
    }
  }
}
