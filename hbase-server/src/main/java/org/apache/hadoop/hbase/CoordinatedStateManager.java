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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.coordination.SplitLogManagerCoordination;
import org.apache.hadoop.hbase.coordination.SplitLogWorkerCoordination;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * Implementations of this interface will keep and return to clients
 * implementations of classes providing API to execute
 * coordinated operations. This interface is client-side, so it does NOT
 * include methods to retrieve the particular interface implementations.
 *
 * For each coarse-grained area of operations there will be a separate
 * interface with implementation, providing API for relevant operations
 * requiring coordination.
 */
@InterfaceAudience.Private
public interface CoordinatedStateManager {
  /**
   * Method to retrieve coordination for split log worker
   */
  SplitLogWorkerCoordination getSplitLogWorkerCoordination();

  /**
   * Method to retrieve coordination for split log manager
   */
  SplitLogManagerCoordination getSplitLogManagerCoordination();
  /**
   * Method to retrieve {@link org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs}
   */
  ProcedureCoordinatorRpcs getProcedureCoordinatorRpcs(String procType, String coordNode)
      throws IOException;

  /**
   * Method to retrieve {@link org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs}
   */
  ProcedureMemberRpcs getProcedureMemberRpcs(String procType) throws KeeperException;

}
