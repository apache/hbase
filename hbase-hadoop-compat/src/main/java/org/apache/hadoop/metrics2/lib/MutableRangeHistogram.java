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

import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Extended histogram implementation with metric range counters.
 */
@InterfaceAudience.Private
public abstract class MutableRangeHistogram extends MutableHistogram implements MetricHistogram {

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
  public abstract long[] getRanges();

  
  @Override
  public synchronized void snapshot(MetricsRecordBuilder metricsRecordBuilder, boolean all) {
    // Get a reference to the old histogram.
    Snapshot snapshot = histogram.snapshot();
    if (snapshot != null) {
      updateSnapshotMetrics(name, desc, histogram, snapshot, metricsRecordBuilder);
      updateSnapshotRangeMetrics(metricsRecordBuilder, snapshot);
    }
  }

  public void updateSnapshotRangeMetrics(MetricsRecordBuilder metricsRecordBuilder,
                                         Snapshot snapshot) {
    long priorRange = 0;
    long cumNum = 0;

    final long[] ranges = getRanges();
    final String rangeType = getRangeType();
    for (int i = 0; i < ranges.length; i++) {
      long val = snapshot.getCountAtOrBelow(ranges[i]);
      if (val - cumNum > 0) {
        metricsRecordBuilder.addCounter(
            Interns.info(name + "_" + rangeType + "_" + priorRange + "-" + ranges[i], desc),
            val - cumNum);
      }
      priorRange = ranges[i];
      cumNum = val;
    }
    long val = snapshot.getCount();
    if (val - cumNum > 0) {
      metricsRecordBuilder.addCounter(
          Interns.info(name + "_" + rangeType + "_" + priorRange + "-inf", desc),
          val - cumNum);
    }
  }

  @Override public long getCount() {
    return histogram.getCount();
  }
}
