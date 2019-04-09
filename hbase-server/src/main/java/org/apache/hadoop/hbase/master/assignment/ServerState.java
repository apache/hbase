/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.assignment;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Server State.
 */
@InterfaceAudience.Private
public enum ServerState {
  /**
   * Initial state. Available.
   */
  ONLINE,

  /**
   * Indicate that the server has crashed, i.e., we have already scheduled a SCP for it.
   */
  CRASHED,

  /**
   * Only server which carries meta can have this state. We will split wal for meta and then
   * assign meta first before splitting other wals.
   */
  SPLITTING_META,

  /**
   * Indicate that the meta splitting is done. We need this state so that the UnassignProcedure
   * for meta can safely quit. See the comments in UnassignProcedure.remoteCallFailed for more
   * details.
   */
  SPLITTING_META_DONE,

  /**
   * Server expired/crashed. Currently undergoing WAL splitting.
   */
  SPLITTING,

  /**
   * WAL splitting done. This state will be used to tell the UnassignProcedure that it can safely
   * quit. See the comments in UnassignProcedure.remoteCallFailed for more details.
   */
  OFFLINE
}