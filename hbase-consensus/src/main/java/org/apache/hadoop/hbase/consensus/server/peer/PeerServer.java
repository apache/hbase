package org.apache.hadoop.hbase.consensus.server.peer;

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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.consensus.rpc.AppendRequest;
import org.apache.hadoop.hbase.consensus.rpc.VoteRequest;
import org.apache.hadoop.hbase.regionserver.RaftEventListener;

public interface PeerServer extends PeerServerMutableContext {
  public void sendAppendEntries(AppendRequest request);
  public void sendRequestVote(VoteRequest request);
  public int getRank();
  public void setRank(int rank);
  public String getPeerServerName();
  public Configuration getConf();
  public void initialize();
  public void stop();
  public void registerDataStoreEventListener(RaftEventListener listener);
}
