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
public final class ReplicationLoadSource {
  private final String peerID;
  private final long ageOfLastShippedOp;
  private final int sizeOfLogQueue;
  private final long timestampOfLastShippedOp;
  private final long replicationLag;
  private long timeStampOfNextToReplicate;
  private String queueId;
  private boolean recovered;
  private boolean running;
  private boolean editsSinceRestart;
  private long editsRead;
  private long oPsShipped;

  @InterfaceAudience.Private
  private ReplicationLoadSource(String id, long age, int size, long timestamp,
      long timeStampOfNextToReplicate, long lag, String queueId, boolean recovered, boolean running,
      boolean editsSinceRestart, long editsRead, long oPsShipped) {
    this.peerID = id;
    this.ageOfLastShippedOp = age;
    this.sizeOfLogQueue = size;
    this.timestampOfLastShippedOp = timestamp;
    this.replicationLag = lag;
    this.timeStampOfNextToReplicate = timeStampOfNextToReplicate;
    this.queueId = queueId;
    this.recovered = recovered;
    this.running = running;
    this.editsSinceRestart = editsSinceRestart;
    this.editsRead = editsRead;
    this.oPsShipped = oPsShipped;
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

  public long getTimeStampOfNextToReplicate() {
    return this.timeStampOfNextToReplicate;
  }

  public String getQueueId() {
    return queueId;
  }

  public boolean isRecovered() {
    return recovered;
  }

  public boolean isRunning() {
    return running;
  }

  public boolean hasEditsSinceRestart() {
    return editsSinceRestart;
  }

  public long getEditsRead() {
    return editsRead;
  }

  public long getOPsShipped() {
    return oPsShipped;
  }

  public static ReplicationLoadSourceBuilder newBuilder(){
    return new ReplicationLoadSourceBuilder();
  }

  public static final class ReplicationLoadSourceBuilder {

    private String peerID;
    private long ageOfLastShippedOp;
    private int sizeOfLogQueue;
    private long timestampOfLastShippedOp;
    private long replicationLag;
    private long timeStampOfNextToReplicate;
    private String queueId;
    private boolean recovered;
    private boolean running;
    private boolean editsSinceRestart;
    private long editsRead;
    private long oPsShipped;

    private ReplicationLoadSourceBuilder(){

    }

    public ReplicationLoadSourceBuilder setTimeStampOfNextToReplicate(
        long timeStampOfNextToReplicate) {
      this.timeStampOfNextToReplicate = timeStampOfNextToReplicate;
      return this;
    }

    public ReplicationLoadSourceBuilder setPeerID(String peerID) {
      this.peerID = peerID;
      return this;
    }

    public ReplicationLoadSourceBuilder setAgeOfLastShippedOp(long ageOfLastShippedOp) {
      this.ageOfLastShippedOp = ageOfLastShippedOp;
      return this;
    }

    public ReplicationLoadSourceBuilder setSizeOfLogQueue(int sizeOfLogQueue) {
      this.sizeOfLogQueue = sizeOfLogQueue;
      return this;
    }

    public ReplicationLoadSourceBuilder setTimestampOfLastShippedOp(long timestampOfLastShippedOp) {
      this.timestampOfLastShippedOp = timestampOfLastShippedOp;
      return this;
    }

    public ReplicationLoadSourceBuilder setReplicationLag(long replicationLag) {
      this.replicationLag = replicationLag;
      return this;
    }

    public ReplicationLoadSourceBuilder setQueueId(String queueId) {
      this.queueId = queueId;
      return this;
    }

    public ReplicationLoadSourceBuilder setRecovered(boolean recovered) {
      this.recovered = recovered;
      return this;
    }

    public ReplicationLoadSourceBuilder setRunning(boolean running) {
      this.running = running;
      return this;
    }

    public ReplicationLoadSourceBuilder setEditsSinceRestart(boolean editsSinceRestart) {
      this.editsSinceRestart = editsSinceRestart;
      return this;
    }

    public ReplicationLoadSourceBuilder setEditsRead(long editsRead) {
      this.editsRead = editsRead;
      return this;
    }

    public ReplicationLoadSourceBuilder setoPsShipped(long oPsShipped) {
      this.oPsShipped = oPsShipped;
      return this;
    }

    public ReplicationLoadSource build(){
      return new ReplicationLoadSource(peerID, ageOfLastShippedOp, sizeOfLogQueue,
          timestampOfLastShippedOp, timeStampOfNextToReplicate, replicationLag, queueId, recovered,
          running, editsSinceRestart, editsRead, oPsShipped);
    }
  }
}