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
 * A HBase ReplicationLoad to present MetricsSource information
 */
@InterfaceAudience.Public
public class ReplicationLoadSource {
  private final String peerID;
  private final long ageOfLastShippedOp;
  private final int sizeOfLogQueue;
  private final long timestampOfLastShippedOp;
  private final long replicationLag;

  // TODO: add the builder for this class
  @InterfaceAudience.Private
  public ReplicationLoadSource(String id, long age, int size, long timestamp, long lag) {
    this.peerID = id;
    this.ageOfLastShippedOp = age;
    this.sizeOfLogQueue = size;
    this.timestampOfLastShippedOp = timestamp;
    this.replicationLag = lag;
  }

  public String getPeerID() {
    return this.peerID;
  }

  public long getAgeOfLastShippedOp() {
    return this.ageOfLastShippedOp;
  }

  public long getSizeOfLogQueue() {
    return this.sizeOfLogQueue;
  }

  /**
   * @deprecated Since 2.0.0. Will be removed in 3.0.0.
   * @see #getTimestampOfLastShippedOp()
   */
  @Deprecated
  public long getTimeStampOfLastShippedOp() {
    return getTimestampOfLastShippedOp();
  }

  public long getTimestampOfLastShippedOp() {
    return this.timestampOfLastShippedOp;
  }

  public long getReplicationLag() {
    return this.replicationLag;
  }
}
