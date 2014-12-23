package org.apache.hadoop.hbase.consensus.server.peer.transition;

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

public enum PeerServerTransitionType implements TransitionType {
  NONE,
  UNCONDITIONAL,
  ON_START,
  ON_VOTE_REQUEST,
  ON_VOTE_RESPONSE,

  ON_APPEND_REQUEST,
  ON_APPEND_RESPONSE,

  ON_APPEND_NACK_RESPONSE,
  ON_APPEND_ACK_RESPONSE,
  ON_PEER_IS_REACHABLE,
  ON_RPC_ERROR,

  ON_HALT,
  MAX
}
