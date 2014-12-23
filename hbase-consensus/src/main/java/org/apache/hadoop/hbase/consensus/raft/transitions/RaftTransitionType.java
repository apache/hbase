package org.apache.hadoop.hbase.consensus.raft.transitions;

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


import org.apache.hadoop.hbase.consensus.fsm.TransitionType;

public enum RaftTransitionType implements TransitionType {
  NONE,
  UNCONDITIONAL,

  ON_START,
  ON_PROGRESS_TIMEOUT,

  ON_VOTE_REQUEST,
  ON_VOTE_RESPONSE,

  ON_VOTE_SUCCEEDED,
  ON_VOTE_FAILED,
  ON_VOTE_NOT_COMPLETED,

  ON_APPEND_REQUEST,
  ON_APPEND_RESPONSE,

  ON_APPEND_SUCCEEDED,
  ON_NEED_STEPDOWN,
  ON_APPEND_RETRY,
  ON_APPEND_NOT_COMPLETED,
  ON_APPEND_TIMEOUT,

  ON_REPLICATED_ENTRIES,

  IS_LEADER,
  IS_CANDIDATE,
  IS_FOLLOWER,

  ON_QUORUM_MEMBERSHIP_CHANGE_REQUEST,
  IS_QUORUM_MEMBERSHIP_CHANGE_IN_PROGRESS,
  QUORUM_MEMBERSHIP_CHANGE_NOT_IN_PROGRESS,

  ON_TRANSACTION_LOG_NOT_ACCESSIBLE,

  ON_RESEED_REQUEST,

  ON_HALT,
  MAX
}
