/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.thrift;

import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingLong;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * This class is for maintaining the various statistics of thrift server
 * and publishing them through the metrics interfaces.
 */
public class ThriftMetrics implements Updater {
  public final static Log LOG = LogFactory.getLog(ThriftMetrics.class);
  public final static String CONTEXT_NAME = "thriftserver";

  private final MetricsContext context;
  private final MetricsRecord metricsRecord;
  private final MetricsRegistry registry = new MetricsRegistry();
  private final long slowResponseTime;
  public static final String SLOW_RESPONSE_NANO_SEC =
    "hbase.thrift.slow.response.nano.second";
  public static final long DEFAULT_SLOW_RESPONSE_NANO_SEC = 10 * 1000 * 1000;

  private final MetricsIntValue callQueueLen =
      new MetricsIntValue("callQueueLen", registry);
  private final MetricsTimeVaryingRate numRowKeysInBatchGet =
      new MetricsTimeVaryingRate("numRowKeysInBatchGet", registry);
  private final MetricsTimeVaryingRate numRowKeysInBatchMutate =
      new MetricsTimeVaryingRate("numRowKeysInBatchMutate", registry);
  private final MetricsTimeVaryingRate timeInQueue =
      new MetricsTimeVaryingRate("timeInQueue", registry);
  private MetricsTimeVaryingRate thriftCall =
      new MetricsTimeVaryingRate("thriftCall", registry);
  private MetricsTimeVaryingRate slowThriftCall =
      new MetricsTimeVaryingRate("slowThriftCall", registry);

  public ThriftMetrics(int port, Configuration conf, Class<?> iface) {
    slowResponseTime = conf.getLong(
        SLOW_RESPONSE_NANO_SEC, DEFAULT_SLOW_RESPONSE_NANO_SEC);
    context = MetricsUtil.getContext(CONTEXT_NAME);
    metricsRecord = MetricsUtil.createRecord(context, CONTEXT_NAME);

    metricsRecord.setTag("port", port + "");

    LOG.info("Initializing RPC Metrics with port=" + port);

    context.registerUpdater(this);

    createMetricsForMethods(iface);
  }

  public void incTimeInQueue(long time) {
    timeInQueue.inc(time);
  }

  public void setCallQueueLen(int len) {
    callQueueLen.set(len);
  }

  public void incNumRowKeysInBatchGet(int diff) {
    numRowKeysInBatchGet.inc(diff);
  }

  public void incNumRowKeysInBatchMutate(int diff) {
    numRowKeysInBatchMutate.inc(diff);
  }

  public void incMethodTime(String name, int time) {
    MetricsTimeVaryingRate methodTimeMetrc = getMethodTimeMetrics(name);
    if (methodTimeMetrc == null) {
      LOG.warn(
          "Got incMethodTime() request for method that doesnt exist: " + name);
      return; // ignore methods that dont exist.
    }

    // inc method specific processTime
    methodTimeMetrc.inc(time);

    // inc general processTime
    thriftCall.inc(time);
    if (time > slowResponseTime) {
      slowThriftCall.inc(time);
    }
  }

  private void createMetricsForMethods(Class<?> iface) {
    LOG.debug("Creating metrics for interface " + iface.toString());
    for (Method m : iface.getDeclaredMethods()) {
      if (getMethodTimeMetrics(m.getName()) == null)
        LOG.debug("Creating metrics for method:" + m.getName());
        createMethodTimeMetrics(m.getName());
    }
  }

  private MetricsTimeVaryingRate getMethodTimeMetrics(String key) {
    return (MetricsTimeVaryingRate) registry.get(key);
  }

  private MetricsTimeVaryingRate createMethodTimeMetrics(String key) {
    return new MetricsTimeVaryingRate(key, this.registry);
  }

  /**
   * Push the metrics to the monitoring subsystem on doUpdate() call.
   */
  public void doUpdates(final MetricsContext context) {
    // getMetricsList() and pushMetric() are thread safe methods
    for (MetricsBase m : registry.getMetricsList()) {
      m.pushMetric(metricsRecord);
    }
    metricsRecord.update();
  }
}
