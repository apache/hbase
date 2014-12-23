package org.apache.hadoop.hbase.consensus.raft.states;

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


import org.apache.hadoop.hbase.consensus.fsm.StateType;

public enum RaftStateType implements StateType {
  NONE,

  START,
  BECOME_FOLLOWER,
  FOLLOWER,

  CANDIDATE,

  BECOME_LEADER,
  LEADER,
  CHANGE_QUORUM_MEMBERSHIP,

  SEND_VOTE_REQUEST,
  HANDLE_VOTE_REQUEST,
  HANDLE_VOTE_RESPONSE,
  HANDLE_QUORUM_MEMBERSHIP_CHANGE_REQUEST,

  SEND_APPEND_REQUEST,
  RESEND_APPEND_REQUEST,
  HANDLE_APPEND_REQUEST,
  HANDLE_APPEND_RESPONSE,

  HANDLE_RESEED_REQUEST,

  PROGRESS_TIMEOUT,

  ACK_CLIENT,

  HALT
}
