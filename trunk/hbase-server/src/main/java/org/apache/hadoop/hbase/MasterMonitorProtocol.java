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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.MasterMonitorService;
import org.apache.hadoop.hbase.security.TokenInfo;
import org.apache.hadoop.hbase.security.KerberosInfo;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterMonitorProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningResponse;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Protocol that a client uses to communicate with the Master (for monitoring purposes).
 */
@KerberosInfo(
  serverPrincipal = "hbase.master.kerberos.principal")
@TokenInfo("HBASE_AUTH_TOKEN")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface MasterMonitorProtocol extends
    MasterMonitorService.BlockingInterface, MasterProtocol {
  public static final long VERSION = 1L;

  /**
   * Used by the client to get the number of regions that have received the
   * updated schema
   *
   * @param controller Unused (set to null).
   * @param req GetSchemaAlterStatusRequest that contains:<br>
   * - tableName
   * @return GetSchemaAlterStatusResponse indicating the number of regions updated.
   *         yetToUpdateRegions is the regions that are yet to be updated totalRegions
   *         is the total number of regions of the table
   * @throws ServiceException
   */
  @Override
  public GetSchemaAlterStatusResponse getSchemaAlterStatus(
    RpcController controller, GetSchemaAlterStatusRequest req) throws ServiceException;

  /**
   * Get list of TableDescriptors for requested tables.
   * @param controller Unused (set to null).
   * @param req GetTableDescriptorsRequest that contains:<br>
   * - tableNames: requested tables, or if empty, all are requested
   * @return GetTableDescriptorsResponse
   * @throws ServiceException
   */
  @Override
  public GetTableDescriptorsResponse getTableDescriptors(
      RpcController controller, GetTableDescriptorsRequest req) throws ServiceException;

  /**
   * Return cluster status.
   * @param controller Unused (set to null).
   * @param req GetClusterStatusRequest
   * @return status object
   * @throws ServiceException
   */
  @Override
  public GetClusterStatusResponse getClusterStatus(RpcController controller, GetClusterStatusRequest req)
  throws ServiceException;

  /**
   * @param c Unused (set to null).
   * @param req IsMasterRunningRequest
   * @return IsMasterRunningRequest that contains:<br>
   * isMasterRunning: true if master is available
   * @throws ServiceException
   */
  @Override
  public IsMasterRunningResponse isMasterRunning(RpcController c, IsMasterRunningRequest req)
  throws ServiceException;
}
