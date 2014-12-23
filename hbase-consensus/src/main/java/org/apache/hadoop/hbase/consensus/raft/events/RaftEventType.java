package org.apache.hadoop.hbase.consensus.raft.events;

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


import org.apache.hadoop.hbase.consensus.fsm.EventType;

public enum RaftEventType implements EventType {
  NONE,
  START,
  VOTE_REQUEST_RECEIVED,
  VOTE_RESPONSE_RECEIVED,
  APPEND_REQUEST_RECEIVED,
  APPEND_RESPONSE_RECEIVED,
  RESEED_REQUEST_RECEIVED,
  REPLICATE_ENTRIES,
  PROGRESS_TIMEOUT,
  TERM_TIMEOUT,
  HALT,
  QUORUM_MEMBERSHIP_CHANGE,
  MAX
}
