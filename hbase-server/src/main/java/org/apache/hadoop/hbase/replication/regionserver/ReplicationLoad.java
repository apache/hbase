/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.replication.regionserver;

import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.hbase.util.Strings;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;

/**
 * This class is used for exporting some of the info from replication metrics
 */
@InterfaceAudience.Private
public class ReplicationLoad {

  // Empty load instance.
  public static final ReplicationLoad EMPTY_REPLICATIONLOAD = new ReplicationLoad();
  private MetricsSink sinkMetrics;

  private List<ClusterStatusProtos.ReplicationLoadSource> replicationLoadSourceEntries;
  private ClusterStatusProtos.ReplicationLoadSink replicationLoadSink;

  /** default constructor */
  public ReplicationLoad() {
    super();
  }

  /**
   * buildReplicationLoad
   * @param sources List of ReplicationSource instances for which metrics should be reported
   * @param skMetrics
   */

  public void buildReplicationLoad(final List<ReplicationSourceInterface> sources,
      final MetricsSink skMetrics) {
    this.sinkMetrics = skMetrics;

    // build the SinkLoad
    ClusterStatusProtos.ReplicationLoadSink.Builder rLoadSinkBuild =
        ClusterStatusProtos.ReplicationLoadSink.newBuilder();
    rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp());
    rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp());
    rLoadSinkBuild.setTimestampStarted(sinkMetrics.getStartTimestamp());
    rLoadSinkBuild.setTotalOpsProcessed(sinkMetrics.getAppliedOps());
    this.replicationLoadSink = rLoadSinkBuild.build();

    this.replicationLoadSourceEntries = new ArrayList<>();
    for (ReplicationSourceInterface source : sources) {
      MetricsSource sm = source.getSourceMetrics();
      // Get the actual peer id
      String peerId = sm.getPeerID();
      String[] parts = peerId.split("-", 2);
      peerId = parts.length != 1 ? parts[0] : peerId;

      long ageOfLastShippedOp = sm.getAgeOfLastShippedOp();
      int sizeOfLogQueue = sm.getSizeOfLogQueue();
      long editsRead = sm.getReplicableEdits();
      long oPsShipped = sm.getOpsShipped();
      long timeStampOfLastShippedOp = sm.getTimestampOfLastShippedOp();
      long timeStampOfNextToReplicate = sm.getTimeStampNextToReplicate();
      long replicationLag = sm.getReplicationDelay();
      ClusterStatusProtos.ReplicationLoadSource.Builder rLoadSourceBuild =
          ClusterStatusProtos.ReplicationLoadSource.newBuilder();
      rLoadSourceBuild.setPeerID(peerId);
      rLoadSourceBuild.setAgeOfLastShippedOp(ageOfLastShippedOp);
      rLoadSourceBuild.setSizeOfLogQueue(sizeOfLogQueue);
      rLoadSourceBuild.setTimeStampOfLastShippedOp(timeStampOfLastShippedOp);
      rLoadSourceBuild.setReplicationLag(replicationLag);
      rLoadSourceBuild.setTimeStampOfNextToReplicate(timeStampOfNextToReplicate);
      rLoadSourceBuild.setEditsRead(editsRead);
      rLoadSourceBuild.setOPsShipped(oPsShipped);
      if (source instanceof ReplicationSource){
        ReplicationSource replSource = (ReplicationSource)source;
        rLoadSourceBuild.setRecovered(replSource.getReplicationQueueInfo().isQueueRecovered());
        rLoadSourceBuild.setQueueId(replSource.getReplicationQueueInfo().getQueueId());
        rLoadSourceBuild.setRunning(replSource.isWorkerRunning());
        rLoadSourceBuild.setEditsSinceRestart(timeStampOfNextToReplicate>0);
      }

      this.replicationLoadSourceEntries.add(rLoadSourceBuild.build());
    }
  }

  /**
   * sourceToString
   * @return a string contains sourceReplicationLoad information
   */
  public String sourceToString() {
    StringBuilder sb = new StringBuilder();

    for (ClusterStatusProtos.ReplicationLoadSource rls :
        this.replicationLoadSourceEntries) {

      sb = Strings.appendKeyValue(sb, "\n           PeerID", rls.getPeerID());
      sb = Strings.appendKeyValue(sb, "AgeOfLastShippedOp", rls.getAgeOfLastShippedOp());
      sb = Strings.appendKeyValue(sb, "SizeOfLogQueue", rls.getSizeOfLogQueue());
      sb =
          Strings.appendKeyValue(sb, "TimestampsOfLastShippedOp",
              (new Date(rls.getTimeStampOfLastShippedOp()).toString()));
      sb = Strings.appendKeyValue(sb, "Replication Lag", rls.getReplicationLag());
    }

    return sb.toString();
  }

  /**
   * sinkToString
   * @return a string contains sinkReplicationLoad information
   */
  public String sinkToString() {
    if (this.replicationLoadSink == null) return null;

    StringBuilder sb = new StringBuilder();
    sb =
        Strings.appendKeyValue(sb, "AgeOfLastAppliedOp",
          this.replicationLoadSink.getAgeOfLastAppliedOp());
    sb =
        Strings.appendKeyValue(sb, "TimestampsOfLastAppliedOp",
          (new Date(this.replicationLoadSink.getTimeStampsOfLastAppliedOp()).toString()));

    return sb.toString();
  }

  public ClusterStatusProtos.ReplicationLoadSink getReplicationLoadSink() {
    return this.replicationLoadSink;
  }

  public List<ClusterStatusProtos.ReplicationLoadSource> getReplicationLoadSourceEntries() {
    return this.replicationLoadSourceEntries;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.sourceToString() + System.getProperty("line.separator") + this.sinkToString();
  }

}
