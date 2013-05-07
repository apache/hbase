/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.ipc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.metrics.RpcMetricWrapper;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * This class is for maintaining  the various RPC statistics
 * and publishing them through the metrics interfaces.
 * This also registers the JMX MBean for RPC.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values;
 * for example:
 *  <p> {@link #rpcQueueTime}.inc(time)
 *
 */
public class HBaseRpcMetrics implements Updater {
  private MetricsRecord metricsRecord;
  private static Log LOG = LogFactory.getLog(HBaseRpcMetrics.class);
  private final HBaseRPCStatistics rpcStatistics;

  public HBaseRpcMetrics(String hostName, String port) {
    MetricsContext context = MetricsUtil.getContext("rpc");
    metricsRecord = MetricsUtil.createRecord(context, "metrics");

    metricsRecord.setTag("port", port);

    LOG.info("Initializing RPC Metrics with hostName="
        + hostName + ", port=" + port);

    context.registerUpdater(this);

    rpcStatistics = new HBaseRPCStatistics(this.registry, hostName, port);
  }

  /**
   * A metric name to RPCMetric object map. We do not keep RpcMetricWrapper objects
   * in the registry, because firstly the RpcMetricWrapper class does not extend
   * MetricsBase (since it is a container class, and not an atomic metric),
   * and secondly we do not want to confuse between the RpcMetricWrapper name and the
   * name of the rate metric contained within a RpcMetricWrapper object, when
   * querying the registry.
   */
  private final ConcurrentHashMap<String, RpcMetricWrapper> rpcMetricWrapperMap =
    new ConcurrentHashMap<String, RpcMetricWrapper>();
  /**
   * The metrics variables are public:
   *  - they can be set directly by calling their set/inc methods
   *  -they can also be read directly - e.g. JMX does this.
   */
  public final MetricsRegistry registry = new MetricsRegistry();

  public MetricsTimeVaryingRate rpcQueueTime = new MetricsTimeVaryingRate("RpcQueueTime", registry);
  public MetricsTimeVaryingRate rpcProcessingTime = new MetricsTimeVaryingRate("RpcProcessingTime", registry);

  private RpcMetricWrapper get(String name) {
    return rpcMetricWrapperMap.get(name);
  }

  private RpcMetricWrapper create(String name) {
    RpcMetricWrapper m = new RpcMetricWrapper(name, this.registry);
    rpcMetricWrapperMap.put(name, m);
    return m;
  }

  /**
   * Increment a metric. This would increment two different metrics.
   * The first one being the standard MetricsTimeVaryingRate, and the
   * other will be a PercentMetric.
   * @param name
   * @param amt
   */
  public void inc(String name, int amt) {
    RpcMetricWrapper m = get(name);
    if (m == null) {
      synchronized (this) {
        if ((m = get(name)) == null) {
          m = create(name);
        }
      }
    }
    m.inc(amt);
  }

  /**
   * Push the metrics to the monitoring subsystem on doUpdate() call.
   * @param context ctx
   */
  public void doUpdates(MetricsContext context) {
    rpcQueueTime.pushMetric(metricsRecord);
    rpcQueueTime.resetMinMax();
    
    rpcProcessingTime.pushMetric(metricsRecord);
    rpcProcessingTime.resetMinMax();

    synchronized (rpcMetricWrapperMap) {
      // Iterate through the rpcMetricWrapperMap to propagate the different rpc metrics.

      for (String metricName : rpcMetricWrapperMap.keySet()) {
        RpcMetricWrapper value = get(metricName);
        value.pushMetric(metricsRecord);
        value.resetMetric();
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (rpcStatistics != null)
      rpcStatistics.shutdown();
  }
}
