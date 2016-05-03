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

package org.apache.hadoop.metrics2.lib;

import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

/**
 * Extended histogram implementation with metric range counters.
 */
@InterfaceAudience.Private
public abstract class MutableRangeHistogram extends MutableHistogram {

  public MutableRangeHistogram(MetricsInfo info) {
    this(info.name(), info.description());
  }

  public MutableRangeHistogram(String name, String description) {
    super(name, description);    
  }
  
  /**
   * Returns the type of range histogram size or time 
   */
  public abstract String getRangeType();
  
  /**
   * Returns the ranges to be counted 
   */
  public abstract long[] getRange();
  
  /**
   * Returns the range counts 
   */
  public abstract AtomicLongArray getRangeVals();

  @Override
  public void add(final long val) {
    super.add(val);
    updateBand(val);
  }

  private void updateBand(final long val) {
    int i;
    for (i=0; i<getRange().length && val > getRange()[i]; i++);
    getRangeVals().incrementAndGet(i);
  }
  
  @Override
  public void snapshot(MetricsRecordBuilder metricsRecordBuilder, boolean all) {
    if (all || changed()) {
      clearChanged();
      updateSnapshotMetrics(metricsRecordBuilder);
      updateSnapshotRangeMetrics(metricsRecordBuilder);
    }
  }
  
  public void updateSnapshotRangeMetrics(MetricsRecordBuilder metricsRecordBuilder) {
    long prior = 0;
    for (int i = 0; i < getRange().length; i++) {
      long val = getRangeVals().get(i);
      if (val > 0) {
        metricsRecordBuilder.addCounter(
          Interns.info(name + "_" + getRangeType() + "_" + prior + "-" + getRange()[i], desc), val);
      }
      prior = getRange()[i];
    }
    long val = getRangeVals().get(getRange().length);
    if (val > 0) {
      metricsRecordBuilder.addCounter(
        Interns.info(name + "_" + getRangeType() + "_" + getRange()[getRange().length - 1] + "-inf", desc),
        getRangeVals().get(getRange().length));
    }
  }  
}
