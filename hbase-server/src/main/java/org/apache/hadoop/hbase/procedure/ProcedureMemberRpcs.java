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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.errorhandling.ForeignException;

/**
 * This is the notification interface for Procedures that encapsulates message passing from
 * members to a coordinator.  Each of these calls should send a message to the coordinator.
 */
@InterfaceAudience.Private
public interface ProcedureMemberRpcs extends Closeable {

  /**
   * Initialize and start any threads or connections the member needs.
   */
  void start(final String memberName, final ProcedureMember member);

  /**
   * Each subprocedure is being executed on a member.  This is the identifier for the member.
   * @return the member name
   */
  String getMemberName();

  /**
   * Notify the coordinator that we aborted the specified {@link Subprocedure}
   *
   * @param sub the {@link Subprocedure} we are aborting
   * @param cause the reason why the member's subprocedure aborted
   * @throws IOException thrown when the rpcs can't reach the other members of the procedure (and
   *  thus can't recover).
   */
  void sendMemberAborted(Subprocedure sub, ForeignException cause) throws IOException;

  /**
   * Notify the coordinator that the specified {@link Subprocedure} has acquired the locally required
   * barrier condition.
   *
   * @param sub the specified {@link Subprocedure}
   * @throws IOException if we can't reach the coordinator
   */
  void sendMemberAcquired(Subprocedure sub) throws IOException;

  /**
   * Notify the coordinator that the specified {@link Subprocedure} has completed the work that
   * needed to be done under the global barrier.
   *
   * @param sub the specified {@link Subprocedure}
   * @param data the data the member returns to the coordinator along with the notification
   * @throws IOException if we can't reach the coordinator
   */
  void sendMemberCompleted(Subprocedure sub, byte[] data) throws IOException;
}
