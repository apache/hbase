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
package org.apache.hadoop.hbase.replication;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A HBase ReplicationLoad to present MetricsSink information
 */
@InterfaceAudience.Public
public class ReplicationLoadSink {
  private final long ageOfLastAppliedOp;
  private final long timestampsOfLastAppliedOp;
  private final long timestampStarted;
  private final long totalOpsProcessed;

  public ReplicationLoadSink(long age, long timestamp, long timestampStarted,
    long totalOpsProcessed) {
    this.ageOfLastAppliedOp = age;
    this.timestampsOfLastAppliedOp = timestamp;
    this.timestampStarted = timestampStarted;
    this.totalOpsProcessed = totalOpsProcessed;
  }

  public long getAgeOfLastAppliedOp() {
    return this.ageOfLastAppliedOp;
  }

  /**
   * Get the timestamp of the last applied operation.
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

  public long getTimestampStarted() {
    return timestampStarted;
  }

  public long getTotalOpsProcessed() {
    return totalOpsProcessed;
  }

  @InterfaceAudience.Private
  public static ReplicationLoadSinkBuilder newBuilder() {
    return new ReplicationLoadSinkBuilder();
  }

  @InterfaceAudience.Private
  public static final class ReplicationLoadSinkBuilder {
    private long ageOfLastAppliedOp;
    private long timestampsOfLastAppliedOp;
    private long timestampStarted;
    private long totalOpsProcessed;

    private ReplicationLoadSinkBuilder() {
    }

    public ReplicationLoadSinkBuilder setAgeOfLastAppliedOp(long ageOfLastAppliedOp) {
      this.ageOfLastAppliedOp = ageOfLastAppliedOp;
      return this;
    }

    public ReplicationLoadSinkBuilder setTimestampsOfLastAppliedOp(long timestampsOfLastAppliedOp) {
      this.timestampsOfLastAppliedOp = timestampsOfLastAppliedOp;
      return this;
    }

    public ReplicationLoadSinkBuilder setTimestampStarted(long timestampStarted) {
      this.timestampStarted = timestampStarted;
      return this;
    }

    public ReplicationLoadSinkBuilder setTotalOpsProcessed(long totalOpsProcessed) {
      this.totalOpsProcessed = totalOpsProcessed;
      return this;
    }

    public ReplicationLoadSink build() {
      return new ReplicationLoadSink(ageOfLastAppliedOp, timestampsOfLastAppliedOp,
        timestampStarted, totalOpsProcessed);
    }
  }
}
