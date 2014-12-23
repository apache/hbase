package org.apache.hadoop.hbase.consensus.rpc;

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


import com.facebook.swift.codec.ThriftConstructor;
import com.facebook.swift.codec.ThriftEnum;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import org.apache.hadoop.hbase.regionserver.DataStoreState;

import javax.annotation.concurrent.Immutable;

@Immutable
@ThriftStruct
public final class PeerStatus implements Comparable<PeerStatus> {

  @ThriftEnum
  public static enum RAFT_STATE {
    INVALID,
    LEADER,
    FOLLOWER,
    CANDIDATE,
    HALT
  }

  private final String id;
  private final int rank;
  private final long term;
  private final RAFT_STATE paxosState;
  private final LogState logState;
  private final String metrics;
  private final DataStoreState dataStoreState;

  private String peerAddress;

  @ThriftConstructor
  public PeerStatus(
    @ThriftField(1) final String id,
    @ThriftField(2) final int rank,
    @ThriftField(3) final long term,
    @ThriftField(4) final RAFT_STATE paxosState,
    @ThriftField(5) final LogState logState,
    @ThriftField(6) final String metrics,
    @ThriftField(7) final DataStoreState dataStoreState) {
    this.id = id;
    this.rank = rank;
    this.term = term;
    this.paxosState = paxosState;
    this.logState = logState;
    this.metrics = metrics;
    this.dataStoreState = dataStoreState;
  }

  @ThriftField(1)
  public String getId() {
    return id;
  }

  @ThriftField(2)
  public int getRank() {
    return rank;
  }

  @ThriftField(3)
  public long getTerm() {
    return term;
  }

  @ThriftField(4)
  public RAFT_STATE getPaxosState() {
    return paxosState;
  }

  @ThriftField(5)
  public LogState getLogState() {
    return logState;
  }

  @ThriftField(6)
  public String getMetrics() {
    return metrics;
  }

  @ThriftField(7)
  public DataStoreState getDataStoreState() {
    return dataStoreState;
  }

  public String getPeerAddress() {
    return peerAddress;
  }

  public void setPeerAddress(String peerAddress) {
    this.peerAddress = peerAddress;
  }

  @Override
  public String toString() {
    return "Peer : " + peerAddress + " {" + "id=" + id + "-" + rank
           + "term=" + term + ", " + "paxosState=" + paxosState + ", "
           + "logState=" + logState + ", " + "dataStoreState=" + dataStoreState
           + "}";
  }

  @Override
  public int compareTo(PeerStatus peer) {
    return Integer.compare(this.rank, peer.getRank());
  }
}
