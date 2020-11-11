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

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsTableAggregateSourceImpl extends BaseSourceImpl
        implements MetricsTableAggregateSource {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsTableAggregateSourceImpl.class);
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

  private void register(MetricsTableSource source) {
    source.registerMetrics();
  }

  @Override
  public void deleteTableSource(String table) {
    try {
      MetricsTableSource source = tableSources.remove(table);
      if (source != null) {
        source.close();
      }
    } catch (Exception e) {
      // Ignored. If this errors out it means that someone is double
      // closing the user source and the user metrics is already nulled out.
      LOG.info("Error trying to remove " + table + " from " + getClass().getSimpleName(), e);
    }
  }

  @Override
  public MetricsTableSource getOrCreateTableSource(String table,
      MetricsTableWrapperAggregate wrapper) {
    MetricsTableSource source = tableSources.get(table);
    if (source != null) {
      return source;
    }
    MetricsTableSource newSource = CompatibilitySingletonFactory
      .getInstance(MetricsRegionServerSourceFactory.class).createTable(table, wrapper);
    return tableSources.computeIfAbsent(table, k -> {
      // register the new metrics now
      newSource.registerMetrics();
      return newSource;
    });
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
