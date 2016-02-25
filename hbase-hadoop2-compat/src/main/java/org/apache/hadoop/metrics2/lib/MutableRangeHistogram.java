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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.FastLongHistogram;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
/**
 * Extended histogram implementation with metric range counters.
 */
@InterfaceAudience.Private
public abstract class MutableRangeHistogram extends MutableHistogram implements MetricHistogram {

  public MutableRangeHistogram(MetricsInfo info) {
    this(info.name(), info.description());
  }

  public MutableRangeHistogram(String name, String description) {
    this(name, description, Integer.MAX_VALUE << 2);
  }

  public MutableRangeHistogram(String name, String description, long expectedMax) {
    super(name, description, expectedMax);
  }

  /**
   * Returns the type of range histogram size or time 
   */
  public abstract String getRangeType();
  
  /**
   * Returns the ranges to be counted 
   */
  public abstract long[] getRanges();

  
  @Override
  public synchronized void snapshot(MetricsRecordBuilder metricsRecordBuilder, boolean all) {
    // Get a reference to the old histogram.
    FastLongHistogram histo = histogram.reset();
    if (histo != null) {
      updateSnapshotMetrics(metricsRecordBuilder, histo);
      updateSnapshotRangeMetrics(metricsRecordBuilder, histo);
    }
  }

  public void updateSnapshotRangeMetrics(MetricsRecordBuilder metricsRecordBuilder,
                                         FastLongHistogram histogram) {
    long priorRange = 0;
    long cumNum = 0;

    final long[] ranges = getRanges();
    final String rangeType = getRangeType();
    for (int i = 0; i < ranges.length - 1; i++) {
      long val = histogram.getNumAtOrBelow(ranges[i]);
      if (val - cumNum > 0) {
        metricsRecordBuilder.addCounter(
            Interns.info(name + "_" + rangeType + "_" + priorRange + "-" + ranges[i], desc),
            val - cumNum);
      }
      priorRange = ranges[i];
      cumNum = val;
    }
    long val = histogram.getCount();
    if (val - cumNum > 0) {
      metricsRecordBuilder.addCounter(
          Interns.info(name + "_" + rangeType + "_" + ranges[ranges.length - 1] + "-inf", desc),
          val - cumNum);
    }
  }  
}
