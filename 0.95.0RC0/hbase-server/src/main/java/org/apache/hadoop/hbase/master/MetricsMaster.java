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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.master.MetricsMasterSource;
import org.apache.hadoop.hbase.master.MetricsMasterSourceFactory;
import org.apache.hadoop.hbase.master.MetricsMasterWrapper;

/**
 * This class is for maintaining the various master statistics
 * and publishing them through the metrics interfaces.
 * <p>
 * This class has a number of metrics variables that are publicly accessible;
 * these variables (objects) have methods to update their values.
 */
@InterfaceStability.Evolving
@InterfaceAudience.Private
public class MetricsMaster {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private MetricsMasterSource masterSource;

  public MetricsMaster(MetricsMasterWrapper masterWrapper) {
    masterSource = CompatibilitySingletonFactory.getInstance(MetricsMasterSourceFactory.class).create(masterWrapper);
  }

  // for unit-test usage
  public MetricsMasterSource getMetricsSource() {
    return masterSource;
  }

  /**
   * Record a single instance of a split
   * @param time time that the split took
   * @param size length of original HLogs that were split
   */
  public synchronized void addSplit(long time, long size) {
    masterSource.updateSplitTime(time);
    masterSource.updateSplitSize(size);
  }

  /**
   * @param inc How much to add to requests.
   */
  public void incrementRequests(final int inc) {
    masterSource.incRequests(inc);

  }

  /**
   * set new value for number of regions in transition.
   * @param ritCount
   */
  public void updateRITCount(int ritCount) {
    masterSource.setRIT(ritCount);
  }

  /**
   * update RIT count that are in this state for more than the threshold
   * as defined by the property rit.metrics.threshold.time.
   * @param ritCountOverThreshold
   */
  public void updateRITCountOverThreshold(int ritCountOverThreshold) {
    masterSource.setRITCountOverThreshold(ritCountOverThreshold);
  }
  /**
   * update the timestamp for oldest region in transition metrics.
   * @param timestamp
   */
  public void updateRITOldestAge(long timestamp) {
    masterSource.setRITOldestAge(timestamp);
  }
}
