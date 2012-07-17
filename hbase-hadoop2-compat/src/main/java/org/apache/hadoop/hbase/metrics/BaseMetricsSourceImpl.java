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

package org.apache.hadoop.hbase.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.HBaseMetricsFactory;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Hadoop 2 implementation of BaseMetricsSource for */
public class BaseMetricsSourceImpl implements BaseMetricsSource, MetricsSource {

  private static boolean defaultMetricsSystemInited = false;
  public static final String HBASE_METRICS_SYSTEM_NAME = "hbase";

  public ConcurrentMap<String, MutableGaugeLong>
      gauges = new ConcurrentHashMap<String, MutableGaugeLong>();
  public ConcurrentMap<String, MutableCounterLong> counters =
      new ConcurrentHashMap<String, MutableCounterLong>();

  protected String metricsContext;
  protected String metricsName;
  protected String metricsDescription;

  public BaseMetricsSourceImpl(String metricsName,
                               String metricsDescription,
                               String metricsContext) {
    this.metricsContext = metricsContext;
    this.metricsName = metricsName;
    this.metricsDescription = metricsDescription;

    if (!defaultMetricsSystemInited) {
      //Not too worried about mutlithread here as all it does is spam the logs.
      defaultMetricsSystemInited = true;
      DefaultMetricsSystem.initialize(HBASE_METRICS_SYSTEM_NAME);
    }
    DefaultMetricsSystem.instance().register(this.metricsContext, this.metricsDescription, this);

  }

  /**
   * Set a single gauge to a value.
   *
   * @param gaugeName gauge name
   * @param value     the new value of the gauge.
   */
  public void setGauge(String gaugeName, long value) {
    MutableGaugeLong gaugeInt = getLongGauge(gaugeName, value);
    gaugeInt.set(value);
  }

  /**
   * Add some amount to a gauge.
   *
   * @param gaugeName The name of the gauge to increment.
   * @param delta     The amount to increment the gauge by.
   */
  public void incGauge(String gaugeName, long delta) {
    MutableGaugeLong gaugeInt = getLongGauge(gaugeName, 0l);
    gaugeInt.incr(delta);
  }

  /**
   * Decrease the value of a named gauge.
   *
   * @param gaugeName The name of the gauge.
   * @param delta     the ammount to subtract from a gauge value.
   */
  public void decGauge(String gaugeName, long delta) {
    MutableGaugeLong gaugeInt = getLongGauge(gaugeName, 0l);
    gaugeInt.decr(delta);
  }

  /**
   * Increment a named counter by some value.
   *
   * @param key   the name of the counter
   * @param delta the ammount to increment
   */
  public void incCounters(String key, long delta) {
    MutableCounterLong counter = getLongCounter(key, 0l);
    counter.incr(delta);

  }

  /**
   * Remove a named gauge.
   *
   * @param key
   */
  public void removeGauge(String key) {
    gauges.remove(key);
  }

  /**
   * Remove a named counter.
   *
   * @param key
   */
  public void removeCounter(String key) {
    counters.remove(key);
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder rb =
        metricsCollector.addRecord(this.metricsName).setContext(metricsContext);

    for (Map.Entry<String, MutableCounterLong> entry : counters.entrySet()) {
      entry.getValue().snapshot(rb, all);
    }
    for (Map.Entry<String, MutableGaugeLong> entry : gauges.entrySet()) {
      entry.getValue().snapshot(rb, all);
    }

  }

  /**
   * Get a MetricMutableGaugeLong from the storage.  If it is not there atomically put it.
   *
   * @param gaugeName              name of the gauge to create or get.
   * @param potentialStartingValue value of the new counter if we have to create it.
   * @return
   */
  private MutableGaugeLong getLongGauge(String gaugeName, long potentialStartingValue) {
    //Try and get the guage.
    MutableGaugeLong gaugeInt = gauges.get(gaugeName);

    //If it's not there then try and put a new one in the storage.
    if (gaugeInt == null) {

      //Create the potential new gauge.
      MutableGaugeLong newGauge = HBaseMetricsFactory.newGauge(gaugeName,
          "",
          potentialStartingValue);

      // Try and put the gauge in.  This is atomic.
      gaugeInt = gauges.putIfAbsent(gaugeName, newGauge);

      //If the value we get back is null then the put was successful and we will return that.
      //otherwise gaugeInt should contain the thing that was in before the put could be completed.
      if (gaugeInt == null) {
        gaugeInt = newGauge;
      }
    }
    return gaugeInt;
  }

  /**
   * Get a MetricMutableCounterLong from the storage.  If it is not there atomically put it.
   *
   * @param counterName            Name of the counter to get
   * @param potentialStartingValue starting value if we have to create a new counter
   * @return
   */
  private MutableCounterLong getLongCounter(String counterName, long potentialStartingValue) {
    //See getLongGauge for description on how this works.
    MutableCounterLong counter = counters.get(counterName);
    if (counter == null) {
      MutableCounterLong newCounter =
          HBaseMetricsFactory.newCounter(counterName, "", potentialStartingValue);
      counter = counters.putIfAbsent(counterName, newCounter);
      if (counter == null) {
        counter = newCounter;
      }
    }
    return counter;
  }

}
