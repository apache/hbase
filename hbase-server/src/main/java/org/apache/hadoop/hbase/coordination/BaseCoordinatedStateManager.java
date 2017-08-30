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
package org.apache.hadoop.hbase.coordination;

import java.io.IOException;

import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.Server;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs;
import org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs;
import org.apache.zookeeper.KeeperException;

/**
 * Base class for {@link org.apache.hadoop.hbase.CoordinatedStateManager} implementations.
 * Defines methods to retrieve coordination objects for relevant areas. CoordinatedStateManager
 * reference returned from Server interface has to be casted to this type to
 * access those methods.
 */
@InterfaceAudience.Private
public abstract class BaseCoordinatedStateManager implements CoordinatedStateManager {

  @Override
  public void initialize(Server server) {
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public Server getServer() {
    return null;
  }

  /**
   * Method to retrieve coordination for split log worker
   */
  public abstract  SplitLogWorkerCoordination getSplitLogWorkerCoordination();

  /**
   * Method to retrieve coordination for split log manager
   */
  public abstract SplitLogManagerCoordination getSplitLogManagerCoordination();
  /**
   * Method to retrieve {@link org.apache.hadoop.hbase.procedure.ProcedureCoordinatorRpcs}
   */
  public abstract ProcedureCoordinatorRpcs
    getProcedureCoordinatorRpcs(String procType, String coordNode) throws IOException;

  /**
   * Method to retrieve {@link org.apache.hadoop.hbase.procedure.ProcedureMemberRpcs}
   */
  public abstract ProcedureMemberRpcs
    getProcedureMemberRpcs(String procType) throws KeeperException;

}
