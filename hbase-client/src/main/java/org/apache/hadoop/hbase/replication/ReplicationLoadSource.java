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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A HBase ReplicationLoad to present MetricsSource information
 */
@InterfaceAudience.Private
public class ReplicationLoadSource {
  private String peerID;
  private long ageOfLastShippedOp;
  private int sizeOfLogQueue;
  private long timeStampOfLastShippedOp;
  private long replicationLag;

  public ReplicationLoadSource(String id, long age, int size, long timeStamp, long lag) {
    this.peerID = id;
    this.ageOfLastShippedOp = age;
    this.sizeOfLogQueue = size;
    this.timeStampOfLastShippedOp = timeStamp;
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

  public long getTimeStampOfLastShippedOp() {
    return this.timeStampOfLastShippedOp;
  }

  public long getReplicationLag() {
    return this.replicationLag;
  }
}