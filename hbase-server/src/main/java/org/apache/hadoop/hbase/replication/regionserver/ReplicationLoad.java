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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
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
    rLoadSinkBuild.setTimeStampsOfLastAppliedOp(sinkMetrics.getTimeStampOfLastAppliedOp());
    this.replicationLoadSink = rLoadSinkBuild.build();

    // build the SourceLoad List
    this.replicationLoadSourceList = new ArrayList<ClusterStatusProtos.ReplicationLoadSource>();
    for (MetricsSource sm : this.sourceMetricsList) {
      long ageOfLastShippedOp = sm.getAgeOfLastShippedOp();
      int sizeOfLogQueue = sm.getSizeOfLogQueue();
      long timeStampOfLastShippedOp = sm.getTimeStampOfLastShippedOp();
      long replicationLag;
      long timePassedAfterLastShippedOp =
          EnvironmentEdgeManager.currentTime() - timeStampOfLastShippedOp;
      if (sizeOfLogQueue != 0) {
        // err on the large side
        replicationLag = Math.max(ageOfLastShippedOp, timePassedAfterLastShippedOp);
      } else if (timePassedAfterLastShippedOp < 2 * ageOfLastShippedOp) {
        replicationLag = ageOfLastShippedOp; // last shipped happen recently
      } else {
        // last shipped may happen last night,
        // so NO real lag although ageOfLastShippedOp is non-zero
        replicationLag = 0;
      }

      ClusterStatusProtos.ReplicationLoadSource.Builder rLoadSourceBuild =
          ClusterStatusProtos.ReplicationLoadSource.newBuilder();
      rLoadSourceBuild.setPeerID(sm.getPeerID());
      rLoadSourceBuild.setAgeOfLastShippedOp(ageOfLastShippedOp);
      rLoadSourceBuild.setSizeOfLogQueue(sizeOfLogQueue);
      rLoadSourceBuild.setTimeStampOfLastShippedOp(timeStampOfLastShippedOp);
      rLoadSourceBuild.setReplicationLag(replicationLag);

      this.replicationLoadSourceList.add(rLoadSourceBuild.build());
    }

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
          Strings.appendKeyValue(sb, "TimeStampsOfLastShippedOp",
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
        Strings.appendKeyValue(sb, "TimeStampsOfLastAppliedOp",
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