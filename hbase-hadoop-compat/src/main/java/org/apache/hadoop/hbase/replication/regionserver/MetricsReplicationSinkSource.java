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

import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsReplicationSinkSource {
  public static final String SINK_AGE_OF_LAST_APPLIED_OP = "sink.ageOfLastAppliedOp";
  public static final String SINK_APPLIED_BATCHES = "sink.appliedBatches";
  public static final String SINK_FAILED_BATCHES = "sink.failedBatches";
  public static final String SINK_APPLIED_OPS = "sink.appliedOps";
  public static final String SINK_APPLIED_HFILES = "sink.appliedHFiles";

  void setLastAppliedOpAge(long age);

  void incrAppliedBatches(long batches);

  void incrAppliedOps(long batchsize);

  void incrFailedBatches();

  long getLastAppliedOpAge();

  void incrAppliedHFiles(long hfileSize);

  long getSinkAppliedOps();

  long getFailedBatches();
}
