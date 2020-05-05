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

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.impl.JmxCacheBuster;
import org.apache.hadoop.metrics2.lib.MetricsExecutorImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class MetricsStoreAggregateSourceImpl extends BaseSourceImpl
    implements MetricsStoreAggregateSource {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsStoreAggregateSourceImpl.class);

  private final MetricsExecutorImpl executor = new MetricsExecutorImpl();

  private final Set<MetricsStoreSource> storeSources =
      Collections.newSetFromMap(new ConcurrentHashMap<MetricsStoreSource, Boolean>());

  public MetricsStoreAggregateSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }


  public MetricsStoreAggregateSourceImpl(String metricsName,
                                          String metricsDescription,
                                          String metricsContext,
                                          String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);

    // Every few mins clean the JMX cache.
    executor.getExecutor().scheduleWithFixedDelay(new Runnable() {
      public void run() {
        JmxCacheBuster.clearJmxCache();
      }
    }, 5, 5, TimeUnit.MINUTES);
  }

  public MetricRegistry getMetricRegistry() {
    return registry;
  }

  @Override
  public void register(MetricsStoreSource source) {
    storeSources.add(source);
    clearCache();
  }

  @Override
  public void deregister(MetricsStoreSource toRemove) {
    storeSources.remove(toRemove);
    clearCache();
  }

  private synchronized void clearCache() {
    JmxCacheBuster.clearJmxCache();
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
    if (storeSources != null) {
      for (MetricsStoreSource storeMetricSource : storeSources) {
        if (storeMetricSource instanceof MetricsStoreSourceImpl) {
          ((MetricsStoreSourceImpl) storeMetricSource).snapshot(mrb, all);
        }
      }
      metricsRegistry.snapshot(mrb, all);
      // snapshot the hbase-metrics based counters also
      if (metricsAdapter != null) {
        metricsAdapter.snapshotAllMetrics(registry, mrb);
      }
    }
  }
}
