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

package org.apache.hadoop.hbase.regionserver.metrics;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 *
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 * for example: rpcQueueTime.inc(time)
 *
 */
public class RegionServerDynamicMetrics implements Updater {
  private static final String UNABLE_TO_CLEAR = "Unable to clear RegionServerDynamicMetrics";
  
  private MetricsRecord metricsRecord;
  private MetricsContext context;
  private final RegionServerDynamicStatistics rsDynamicStatistics;
  private Method updateMbeanInfoIfMetricsListChanged = null;
  private HRegionServer regionServer;
  private static final Log LOG =
    LogFactory.getLog(RegionServerDynamicStatistics.class);
  
  private boolean reflectionInitialized = false;
  private boolean needsUpdateMessage = false;
  private Field recordMetricMapField;
  private Field registryMetricMapField;

  /**
   * The metrics variables are public:
   *  - they can be set directly by calling their set/inc methods
   *  -they can also be read directly - e.g. JMX does this.
   */
  public final MetricsRegistry registry = new MetricsRegistry();

  private RegionServerDynamicMetrics(HRegionServer regionServer) {
    this.context = MetricsUtil.getContext("hbase");
    this.metricsRecord = MetricsUtil.createRecord(
                            this.context,
                            "RegionServerDynamicStatistics");
    context.registerUpdater(this);
    this.rsDynamicStatistics = new RegionServerDynamicStatistics(this.registry);
    this.regionServer = regionServer;
    try {
      updateMbeanInfoIfMetricsListChanged =
        this.rsDynamicStatistics.getClass().getSuperclass()
        .getDeclaredMethod("updateMbeanInfoIfMetricsListChanged",
            new Class[]{});
      updateMbeanInfoIfMetricsListChanged.setAccessible(true);
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  public static RegionServerDynamicMetrics newInstance(HRegionServer regionServer) {
    RegionServerDynamicMetrics metrics =
      new RegionServerDynamicMetrics(regionServer);
    return metrics;
  }

  public synchronized void setNumericMetric(String name, long amt) {
    MetricsLongValue m = (MetricsLongValue)registry.get(name);
    if (m == null) {
      m = new MetricsLongValue(name, this.registry);
      this.needsUpdateMessage = true;
    }
    m.set(amt);
  }

  public synchronized void incrTimeVaryingMetric(
      String name,
      long amt,
      int numOps) {
    MetricsTimeVaryingRate m = (MetricsTimeVaryingRate)registry.get(name);
    if (m == null) {
      m = new MetricsTimeVaryingRate(name, this.registry);
      this.needsUpdateMessage = true;
    }
    if (numOps > 0) {
      m.inc(numOps, amt);
    }
  }
  
  /**
   * Clear all metrics this exposes. 
   * Uses reflection to clear them from hadoop metrics side as well.
   */
  @SuppressWarnings("rawtypes")
  public void clear() {
    this.needsUpdateMessage = true;
    // If this is the first clear use reflection to get the two maps that hold copies of our 
    // metrics on the hadoop metrics side. We have to use reflection because there is not 
    // remove metrics on the hadoop side. If we can't get them then clearing old metrics 
    // is not possible and bailing out early is our best option.
    if (!this.reflectionInitialized) {
      this.reflectionInitialized = true;
      try {
        this.recordMetricMapField = this.metricsRecord.getClass().getDeclaredField("metricTable");
        this.recordMetricMapField.setAccessible(true);
      } catch (SecurityException e) {
        LOG.debug(UNABLE_TO_CLEAR);
        return;
      } catch (NoSuchFieldException e) {
        LOG.debug(UNABLE_TO_CLEAR);
        return;
      }

      try {
        this.registryMetricMapField = this.registry.getClass().getDeclaredField("metricsList");
        this.registryMetricMapField.setAccessible(true);
      } catch (SecurityException e) {
        LOG.debug(UNABLE_TO_CLEAR);
        return;
      } catch (NoSuchFieldException e) {
        LOG.debug(UNABLE_TO_CLEAR);
        return;
      } 
    }

    
    //If we found both fields then try and clear the maps.
    if (this.recordMetricMapField != null && this.registryMetricMapField != null) {
      try {
        Map recordMap = (Map) this.recordMetricMapField.get(this.metricsRecord);
        recordMap.clear();
        Map registryMap = (Map) this.registryMetricMapField.get(this.registry);
        registryMap.clear();
      } catch (IllegalArgumentException e) {
        LOG.debug(UNABLE_TO_CLEAR);
      } catch (IllegalAccessException e) {
        LOG.debug(UNABLE_TO_CLEAR);
      }
    } else {
      LOG.debug(UNABLE_TO_CLEAR);
    }
  }

  /**
   * Push the metrics to the monitoring subsystem on doUpdate() call.
   * @param context ctx
   */
  public void doUpdates(MetricsContext context) {
    /* get dynamically created numeric metrics, and push the metrics */
    for (Entry<String, AtomicLong> entry : RegionMetricsStorage.getNumericMetrics().entrySet()) {
      this.setNumericMetric(entry.getKey(), entry.getValue().getAndSet(0));
    }

    /* export estimated size of all response queues */
    if (regionServer != null) {
      long responseQueueSize = regionServer.getResponseQueueSize();
      this.setNumericMetric("responseQueuesSize", responseQueueSize);
    }

    /* get dynamically created numeric metrics, and push the metrics.
     * These ones aren't to be reset; they are cumulative. */
    for (Entry<String, AtomicLong> entry : RegionMetricsStorage.getNumericPersistentMetrics().entrySet()) {
      this.setNumericMetric(entry.getKey(), entry.getValue().get());
    }
    /* get dynamically created time varying metrics, and push the metrics */
    for (Entry<String, Pair<AtomicLong, AtomicInteger>> entry :
        RegionMetricsStorage.getTimeVaryingMetrics().entrySet()) {
      Pair<AtomicLong, AtomicInteger> value = entry.getValue();
      this.incrTimeVaryingMetric(entry.getKey(),
          value.getFirst().getAndSet(0),
          value.getSecond().getAndSet(0));
    }

    // If there are new metrics sending this message to jmx tells it to update everything.
    // This is not ideal we should just move to metrics2 that has full support for dynamic metrics.
    if (needsUpdateMessage) {
      try {
        if (updateMbeanInfoIfMetricsListChanged != null) {
          updateMbeanInfoIfMetricsListChanged.invoke(this.rsDynamicStatistics,
              new Object[]{});
        }
      } catch (Exception e) {
        LOG.error(e);
      }
      needsUpdateMessage = false;
    }


    synchronized (registry) {
      // Iterate through the registry to propagate the different rpc metrics.
      for (String metricName : registry.getKeyList() ) {
        MetricsBase value = registry.get(metricName);
        value.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (rsDynamicStatistics != null)
      rsDynamicStatistics.shutdown();
  }
}
