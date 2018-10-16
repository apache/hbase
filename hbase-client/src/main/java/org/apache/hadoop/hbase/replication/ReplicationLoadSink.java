/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A HBase ReplicationLoad to present MetricsSink information
 */
@InterfaceAudience.Public
public class ReplicationLoadSink {
  private final long ageOfLastAppliedOp;
  private final long timestampsOfLastAppliedOp;

  // TODO: add the builder for this class
  @InterfaceAudience.Private
  public ReplicationLoadSink(long age, long timestamp) {
    this.ageOfLastAppliedOp = age;
    this.timestampsOfLastAppliedOp = timestamp;
  }

  public long getAgeOfLastAppliedOp() {
    return this.ageOfLastAppliedOp;
  }

  /**
   * @deprecated Since hbase-2.0.0. Will be removed in 3.0.0.
   * @see #getTimestampsOfLastAppliedOp()
   */
  @Deprecated
  public long getTimeStampsOfLastAppliedOp() {
    return getTimestampsOfLastAppliedOp();
  }

  public long getTimestampsOfLastAppliedOp() {
    return this.timestampsOfLastAppliedOp;
  }
}
