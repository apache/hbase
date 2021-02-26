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

import java.io.IOException;

import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.zookeeper.KeeperException;

/**
* A life-cycle management interface for globally barriered procedures on master.
* See the following doc on details of globally barriered procedure:
* https://issues.apache.org/jira/secure/attachment/12555103/121127-global-barrier-proc.pdf
*
* To implement a custom globally barriered procedure, user needs to extend two classes:
* {@link MasterProcedureManager} and {@link RegionServerProcedureManager}. Implementation of
* {@link MasterProcedureManager} is loaded into {@link org.apache.hadoop.hbase.master.HMaster}
* process via configuration parameter 'hbase.procedure.master.classes', while implementation of
* {@link RegionServerProcedureManager} is loaded into
* {@link org.apache.hadoop.hbase.regionserver.HRegionServer} process via
* configuration parameter 'hbase.procedure.regionserver.classes'.
*
* An example of globally barriered procedure implementation is
* {@link org.apache.hadoop.hbase.master.snapshot.SnapshotManager} and
* {@link org.apache.hadoop.hbase.regionserver.snapshot.RegionServerSnapshotManager}.
*
* A globally barriered procedure is identified by its signature (usually it is the name of the
* procedure znode). During the initialization phase, the initialize methods are called by both
* {@link org.apache.hadoop.hbase.master.HMaster}
* and {@link org.apache.hadoop.hbase.regionserver.HRegionServer} which create the procedure znode
* and register the listeners. A procedure can be triggered by its signature and an instant name
* (encapsulated in a {@link ProcedureDescription} object). When the servers are shutdown,
* the stop methods on both classes are called to clean up the data associated with the procedure.
*/
@InterfaceAudience.Private
public abstract class MasterProcedureManager extends ProcedureManager implements Stoppable {
  /**
   * Initialize a globally barriered procedure for master.
   *
   * @param master Master service interface
   * @throws KeeperException
   * @throws IOException
   * @throws UnsupportedOperationException
   */
  public abstract void initialize(MasterServices master, MetricsMaster metricsMaster)
      throws KeeperException, IOException, UnsupportedOperationException;

  /**
   * Execute a distributed procedure on cluster
   *
   * @param desc Procedure description
   * @throws IOException
   */
  public void execProcedure(ProcedureDescription desc) throws IOException {}

  /**
   * Execute a distributed procedure on cluster with return data.
   *
   * @param desc Procedure description
   * @return data returned from the procedure execution, null if no data
   * @throws IOException
   */
  public byte[] execProcedureWithRet(ProcedureDescription desc)
      throws IOException {
    return null;
  }

  /**
   * Check for required permissions before executing the procedure.
   * @throws IOException if permissions requirements are not met.
   */
  public abstract void checkPermissions(ProcedureDescription desc, AccessChecker accessChecker,
      User user) throws IOException;

  /**
   * Check if the procedure is finished successfully
   *
   * @param desc Procedure description
   * @return true if the specified procedure is finished successfully
   * @throws IOException
   */
  public abstract boolean isProcedureDone(ProcedureDescription desc) throws IOException;
}
