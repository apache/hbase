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
package org.apache.hadoop.hbase.metrics.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hbase.metrics.MetricRegistries;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.metrics.MetricRegistryInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsExecutor;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.JmxCacheBuster;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystemHelper;
import org.apache.hadoop.metrics2.lib.MetricsExecutorImpl;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class acts as an adapter to export the MetricRegistry's in the global registry. Each
 * MetricRegistry will be registered or unregistered from the metric2 system. The collection will
 * be performed via the MetricsSourceAdapter and the MetricRegistry will collected like a
 * BaseSource instance for a group of metrics  (like WAL, RPC, etc) with the MetricRegistryInfo's
 * JMX context.
 *
 * <p>Developer note:
 * Unlike the current metrics2 based approach, the new metrics approach
 * (hbase-metrics-api and hbase-metrics modules) work by having different MetricRegistries that are
 * initialized and used from the code that lives in their respective modules (hbase-server, etc).
 * There is no need to define BaseSource classes and do a lot of indirection. The MetricRegistry'es
 * will be in the global MetricRegistriesImpl, and this class will iterate over
 * MetricRegistries.global() and register adapters to the metrics2 subsystem. These adapters then
 * report the actual values by delegating to
 * {@link HBaseMetrics2HadoopMetricsAdapter#snapshotAllMetrics(MetricRegistry, MetricsCollector)}.
 *
 * We do not initialize the Hadoop Metrics2 system assuming that other BaseSources already do so
 * (see BaseSourceImpl). Once the last BaseSource is moved to the new system, the metric2
 * initialization should be moved here.
 * </p>
 */
@InterfaceAudience.Private
public final class GlobalMetricRegistriesAdapter {

  private static final Logger LOG = LoggerFactory.getLogger(GlobalMetricRegistriesAdapter.class);

  private class MetricsSourceAdapter implements MetricsSource {
    private final MetricRegistry registry;
    MetricsSourceAdapter(MetricRegistry registry) {
      this.registry = registry;
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
      metricsAdapter.snapshotAllMetrics(registry, collector);
    }
  }

  private final MetricsExecutor executor;
  private final AtomicBoolean stopped;
  private final DefaultMetricsSystemHelper helper;
  private final HBaseMetrics2HadoopMetricsAdapter metricsAdapter;
  private final HashMap<MetricRegistryInfo, MetricsSourceAdapter> registeredSources;

  private GlobalMetricRegistriesAdapter() {
    this.executor = new MetricsExecutorImpl();
    this.stopped = new AtomicBoolean(false);
    this.metricsAdapter = new HBaseMetrics2HadoopMetricsAdapter();
    this.registeredSources = new HashMap<>();
    this.helper = new DefaultMetricsSystemHelper();
    executor.getExecutor().scheduleAtFixedRate(() -> this.doRun(), 10, 10, TimeUnit.SECONDS);
  }

  /**
   * Make sure that this global MetricSource for hbase-metrics module based metrics are initialized.
   * This should be called only once.
   */
  public static GlobalMetricRegistriesAdapter init() {
    return new GlobalMetricRegistriesAdapter();
  }

  public void stop() {
    stopped.set(true);
  }

  private void doRun() {
    if (stopped.get()) {
      executor.stop();
      return;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("doRun called: " + registeredSources);
    }

    Collection<MetricRegistry> registries = MetricRegistries.global().getMetricRegistries();
    for (MetricRegistry registry : registries) {
      MetricRegistryInfo info = registry.getMetricRegistryInfo();

      if (info.isExistingSource()) {
        // If there is an already existing BaseSource for this MetricRegistry, skip it here. These
        // types of registries are there only due to existing BaseSource implementations in the
        // source code (like MetricsRegionServer, etc). This is to make sure that we can transition
        // iteratively to the new hbase-metrics system. These type of MetricRegistry metrics will be
        // exported from the BaseSource.getMetrics() call directly because there is already a
        // MetricRecordBuilder there (see MetricsRegionServerSourceImpl).
        continue;
      }

      if (!registeredSources.containsKey(info)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Registering adapter for the MetricRegistry: " + info.getMetricsJmxContext());
        }
        // register this as a MetricSource under different JMX Context'es.
        MetricsSourceAdapter adapter = new MetricsSourceAdapter(registry);
        LOG.info("Registering " + info.getMetricsJmxContext() + " " + info.getMetricsDescription());
        DefaultMetricsSystem.instance().register(info.getMetricsJmxContext(),
            info.getMetricsDescription(), adapter);
        registeredSources.put(info, adapter);
        // next collection will collect the newly registered MetricSource. Doing this here leads to
        // ConcurrentModificationException.
      }
    }

    boolean removed = false;
    // Remove registered sources if it is removed from the global registry
    for (Iterator<Entry<MetricRegistryInfo, MetricsSourceAdapter>> it =
         registeredSources.entrySet().iterator(); it.hasNext();) {
      Entry<MetricRegistryInfo, MetricsSourceAdapter> entry = it.next();
      MetricRegistryInfo info = entry.getKey();
      Optional<MetricRegistry> found = MetricRegistries.global().get(info);
      if (!found.isPresent()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Removing adapter for the MetricRegistry: " + info.getMetricsJmxContext());
        }
        synchronized(DefaultMetricsSystem.instance()) {
          DefaultMetricsSystem.instance().unregisterSource(info.getMetricsJmxContext());
          helper.removeSourceName(info.getMetricsJmxContext());
          helper.removeObjectName(info.getMetricsJmxContext());
          it.remove();
          removed = true;
        }
      }
    }
    if (removed) {
      JmxCacheBuster.clearJmxCache();
    }
  }
}
