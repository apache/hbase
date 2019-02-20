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
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Strings;

/**
 * This class is used for exporting some of the info from replication metrics
 */
@InterfaceAudience.Private
public class ReplicationLoad {

  // Empty load instance.
  public static final ReplicationLoad EMPTY_REPLICATIONLOAD = new ReplicationLoad();

  private List<MetricsSource> sourceMetricsList;
  private MetricsSink sinkMetrics;

  private List<ClusterStatusProtos.ReplicationLoadSource> replicationLoadSourceList;
  private ClusterStatusProtos.ReplicationLoadSink replicationLoadSink;

  /** default constructor */
  public ReplicationLoad() {
    super();
  }

  /**
   * buildReplicationLoad
   * @param srMetricsList
   * @param skMetrics
   */

  public void buildReplicationLoad(final List<MetricsSource> srMetricsList,
      final MetricsSink skMetrics) {
    this.sourceMetricsList = srMetricsList;
    this.sinkMetrics = skMetrics;

    // build the SinkLoad
    ClusterStatusProtos.ReplicationLoadSink.Builder rLoadSinkBuild =
        ClusterStatusProtos.ReplicationLoadSink.newBuilder();
    rLoadSinkBuild.setAgeOfLastAppliedOp(sinkMetrics.getAgeOfLastAppliedOp());
    rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getTimestampOfLastAppliedOp());
    this.replicationLoadSink = rLoadSinkBuild.build();

    // build the SourceLoad List
    Map<String, ClusterStatusProtos.ReplicationLoadSource> replicationLoadSourceMap =
        new HashMap<>();
    for (MetricsSource sm : this.sourceMetricsList) {
      // Get the actual peer id
      String peerId = sm.getPeerID();
      String[] parts = peerId.split("-", 2);
      peerId = parts.length != 1 ? parts[0] : peerId;

      long ageOfLastShippedOp = sm.getAgeOfLastShippedOp();
      int sizeOfLogQueue = sm.getSizeOfLogQueue();
      long timeStampOfLastShippedOp = sm.getTimestampOfLastShippedOp();
      long replicationLag =
          calculateReplicationDelay(ageOfLastShippedOp, timeStampOfLastShippedOp, sizeOfLogQueue);

      ClusterStatusProtos.ReplicationLoadSource rLoadSource = replicationLoadSourceMap.get(peerId);
      if (rLoadSource != null) {
        ageOfLastShippedOp = Math.max(rLoadSource.getAgeOfLastShippedOp(), ageOfLastShippedOp);
        sizeOfLogQueue += rLoadSource.getSizeOfLogQueue();
        timeStampOfLastShippedOp = Math.min(rLoadSource.getTimeStampOfLastShippedOp(),
          timeStampOfLastShippedOp);
        replicationLag = Math.max(rLoadSource.getReplicationLag(), replicationLag);
      }
      ClusterStatusProtos.ReplicationLoadSource.Builder rLoadSourceBuild =
          ClusterStatusProtos.ReplicationLoadSource.newBuilder();
      rLoadSourceBuild.setPeerID(peerId);
      rLoadSourceBuild.setAgeOfLastShippedOp(ageOfLastShippedOp);
      rLoadSourceBuild.setSizeOfLogQueue(sizeOfLogQueue);
      rLoadSourceBuild.setTimeStampOfLastShippedOp(timeStampOfLastShippedOp);
      rLoadSourceBuild.setReplicationLag(replicationLag);

      replicationLoadSourceMap.put(peerId, rLoadSourceBuild.build());
    }
    this.replicationLoadSourceList = new ArrayList<>(replicationLoadSourceMap.values());
  }

  static long calculateReplicationDelay(long ageOfLastShippedOp,
      long timeStampOfLastShippedOp, int sizeOfLogQueue) {
    long replicationLag;
    long timePassedAfterLastShippedOp;
    if (timeStampOfLastShippedOp == 0) { //replication not start yet, set to Long.MAX_VALUE
      return Long.MAX_VALUE;
    } else {
      timePassedAfterLastShippedOp =
          EnvironmentEdgeManager.currentTime() - timeStampOfLastShippedOp;
    }
    if (sizeOfLogQueue > 1) {
      // err on the large side
      replicationLag = Math.max(ageOfLastShippedOp, timePassedAfterLastShippedOp);
    } else if (timePassedAfterLastShippedOp < 2 * ageOfLastShippedOp) {
      replicationLag = ageOfLastShippedOp; // last shipped happen recently
    } else {
      // last shipped may happen last night,
      // so NO real lag although ageOfLastShippedOp is non-zero
      replicationLag = 0;
    }
    return replicationLag;
  }

  /**
   * sourceToString
   * @return a string contains sourceReplicationLoad information
   */
  public String sourceToString() {
    if (this.sourceMetricsList == null) return null;

    StringBuilder sb = new StringBuilder();

    for (ClusterStatusProtos.ReplicationLoadSource rls : this.replicationLoadSourceList) {

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

  public List<ClusterStatusProtos.ReplicationLoadSource> getReplicationLoadSourceList() {
    return this.replicationLoadSourceList;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return this.sourceToString() + System.getProperty("line.separator") + this.sinkToString();
  }

}
