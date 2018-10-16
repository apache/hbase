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
package org.apache.hadoop.hbase.procedure;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.errorhandling.ForeignException;

/**
 * RPCs for the coordinator to run a barriered procedure with subprocedures executed at
 * distributed members.
 * @see ProcedureCoordinator
 */
@InterfaceAudience.Private
public interface ProcedureCoordinatorRpcs extends Closeable {

  /**
   * Initialize and start threads necessary to connect an implementation's rpc mechanisms.
   * @param listener
   * @return true if succeed, false if encountered initialization errors.
   */
  boolean start(final ProcedureCoordinator listener);

  /**
   * Notify the members that the coordinator has aborted the procedure and that it should release
   * barrier resources.
   *
   * @param procName name of the procedure that was aborted
   * @param cause the reason why the procedure needs to be aborted
   * @throws IOException if the rpcs can't reach the other members of the procedure (and can't
   *           recover).
   */
  void sendAbortToMembers(Procedure procName, ForeignException cause) throws IOException;

  /**
   * Notify the members to acquire barrier for the procedure
   *
   * @param procName name of the procedure to start
   * @param info information that should be passed to all members
   * @param members names of the members requested to reach the acquired phase
   * @throws IllegalArgumentException if the procedure was already marked as failed
   * @throws IOException if we can't reach the remote notification mechanism
   */
  void sendGlobalBarrierAcquire(Procedure procName, byte[] info, List<String> members)
      throws IOException, IllegalArgumentException;

  /**
   * Notify members that all members have acquired their parts of the barrier and that they can
   * now execute under the global barrier.
   *
   * Must come after calling {@link #sendGlobalBarrierAcquire(Procedure, byte[], List)}
   *
   * @param procName name of the procedure to start
   * @param members members to tell we have reached in-barrier phase
   * @throws IOException if we can't reach the remote notification mechanism
   */
  void sendGlobalBarrierReached(Procedure procName, List<String> members) throws IOException;

  /**
   * Notify Members to reset the distributed state for procedure
   * @param procName name of the procedure to reset
   * @throws IOException if the remote notification mechanism cannot be reached
   */
  void resetMembers(Procedure procName) throws IOException;
}
