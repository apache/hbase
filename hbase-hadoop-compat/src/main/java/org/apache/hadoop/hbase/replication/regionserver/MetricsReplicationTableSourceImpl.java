/*
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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.hadoop.metrics2.lib.MutableFastCounter;
import org.apache.hadoop.metrics2.lib.MutableHistogram;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the metric source for table level replication metrics.
 * We can easy monitor some useful table level replication metrics such as
 * ageOfLastShippedOp and shippedBytes
 */
@InterfaceAudience.Private
public class MetricsReplicationTableSourceImpl implements MetricsReplicationTableSource {

  private final MetricsReplicationSourceImpl rms;
  private final String tableName;
  private final String ageOfLastShippedOpKey;
  private String keyPrefix;

  private final String shippedBytesKey;

  private final MutableHistogram ageOfLastShippedOpHist;
  private final MutableFastCounter shippedBytesCounter;

  public MetricsReplicationTableSourceImpl(MetricsReplicationSourceImpl rms, String tableName) {
    this.rms = rms;
    this.tableName = tableName;
    this.keyPrefix = "source." + this.tableName + ".";

    ageOfLastShippedOpKey = this.keyPrefix + "ageOfLastShippedOp";
    ageOfLastShippedOpHist = rms.getMetricsRegistry().newTimeHistogram(ageOfLastShippedOpKey);

    shippedBytesKey = this.keyPrefix + "shippedBytes";
    shippedBytesCounter = rms.getMetricsRegistry().getCounter(shippedBytesKey, 0L);
  }

  @Override
  public void setLastShippedAge(long age) {
    ageOfLastShippedOpHist.add(age);
  }

  @Override
  public void incrShippedBytes(long size) {
    shippedBytesCounter.incr(size);
  }

  @Override
  public void clear() {
    rms.removeMetric(ageOfLastShippedOpKey);
    rms.removeMetric(shippedBytesKey);
  }

  @Override
  public long getLastShippedAge() {
    return ageOfLastShippedOpHist.getMax();
  }

  @Override
  public long getShippedBytes() {
    return shippedBytesCounter.value();
  }

  @Override
  public void init() {
    rms.init();
  }

  @Override
  public void setGauge(String gaugeName, long value) {
    rms.setGauge(this.keyPrefix + gaugeName, value);
  }

  @Override
  public void incGauge(String gaugeName, long delta) {
    rms.incGauge(this.keyPrefix + gaugeName, delta);
  }

  @Override
  public void decGauge(String gaugeName, long delta) {
    rms.decGauge(this.keyPrefix + gaugeName, delta);
  }

  @Override
  public void removeMetric(String key) {
    rms.removeMetric(this.keyPrefix + key);
  }

  @Override
  public void incCounters(String counterName, long delta) {
    rms.incCounters(this.keyPrefix + counterName, delta);
  }

  @Override
  public void updateHistogram(String name, long value) {
    rms.updateHistogram(this.keyPrefix + name, value);
  }

  @Override
  public String getMetricsContext() {
    return rms.getMetricsContext();
  }

  @Override
  public String getMetricsDescription() {
    return rms.getMetricsDescription();
  }

  @Override
  public String getMetricsJmxContext() {
    return rms.getMetricsJmxContext();
  }

  @Override
  public String getMetricsName() {
    return rms.getMetricsName();
  }
}
