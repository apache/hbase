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

package org.apache.hadoop.hbase.replication.regionserver.metrics;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * This class is for maintaining the various replication statistics for a sink and publishing them
 * through the metrics interfaces.
 */
@InterfaceAudience.Private
public class ReplicationSinkMetrics {

  public static final String SINK_AGE_OF_LAST_APPLIED_OP = "sink.ageOfLastAppliedOp";
  public static final String SINK_APPLIED_BATCHES = "sink.appliedBatches";
  public static final String SINK_APPLIED_OPS = "sink.appliedOps";

  private ReplicationMetricsSource rms;

  public ReplicationSinkMetrics() {
    rms = CompatibilitySingletonFactory.getInstance(ReplicationMetricsSource.class);
  }

  /**
   * Set the age of the last applied operation
   *
   * @param timestamp The timestamp of the last operation applied.
   */
  public void setAgeOfLastAppliedOp(long timestamp) {
    long age = EnvironmentEdgeManager.currentTimeMillis() - timestamp;
    rms.setGauge(SINK_AGE_OF_LAST_APPLIED_OP, age);
  }

  /**
   * Convience method to change metrics when a batch of operations are applied.
   *
   * @param batchSize
   */
  public void applyBatch(long batchSize) {
    rms.incCounters(SINK_APPLIED_BATCHES, 1);
    rms.incCounters(SINK_APPLIED_OPS, batchSize);
  }

}
