/**
 * Copyright 2010 The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.security.TokenInfo;
import org.apache.hadoop.hbase.security.KerberosInfo;
import org.apache.hadoop.hbase.util.Pair;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Clients interact with the HMasterInterface to gain access to meta-level
 * HBase functionality, like finding an HRegionServer and creating/destroying
 * tables.
 *
 * <p>NOTE: if you change the interface, you must change the RPC version
 * number in HBaseRPCProtocolVersion
 *
 */
@KerberosInfo(
    serverPrincipal = "hbase.master.kerberos.principal")
@TokenInfo("HBASE_AUTH_TOKEN")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface HMasterInterface extends VersionedProtocol {
  /**
   * This Interfaces' version. Version changes when the Interface changes.
   */
  // All HBase Interfaces used derive from HBaseRPCProtocolVersion.  It
  // maintained a single global version number on all HBase Interfaces.  This
  // meant all HBase RPC was broke though only one of the three RPC Interfaces
  // had changed.  This has since been undone.
  // 29:  4/3/2010 - changed ClusterStatus serialization
  // 30: 3/20/2012 - HBASE-5589: Added offline method
  // 31: 5/8/2012 - HBASE-5445: Converted to PB-based calls
  public static final long VERSION = 31L;

  /**
   * @param c Unused (set to null).
   * @param req IsMasterRunningRequest
   * @return IsMasterRunningRequest that contains:<br>
   * isMasterRunning: true if master is available
   * @throws ServiceException
   */
  public IsMasterRunningResponse isMasterRunning(RpcController c, IsMasterRunningRequest req)
  throws ServiceException;

  // Admin tools would use these cmds

  /**
   * Creates a new table asynchronously.  If splitKeys are specified, then the
   * table will be created with an initial set of multiple regions.
   * If splitKeys is null, the table will be created with a single region.
   * @param desc table descriptor
   * @param splitKeys
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte [][] splitKeys)
  throws IOException;

  /**
   * Deletes a table
   * @param tableName table to delete
   * @throws IOException e
   */
  public void deleteTable(final byte [] tableName) throws IOException;

  /**
   * Used by the client to get the number of regions that have received the
   * updated schema
   *
   * @param tableName
   * @return Pair indicating the number of regions updated Pair.getFirst() is the
   *         regions that are yet to be updated Pair.getSecond() is the total number
   *         of regions of the table
   * @throws IOException
   */
  public Pair<Integer, Integer> getAlterStatus(byte[] tableName)
  throws IOException;

  /**
   * Adds a column to the specified table
   * @param tableName table to modify
   * @param column column descriptor
   * @throws IOException e
   */
  public void addColumn(final byte [] tableName, HColumnDescriptor column)
  throws IOException;

  /**
   * Modifies an existing column on the specified table
   * @param tableName table name
   * @param descriptor new column descriptor
   * @throws IOException e
   */
  public void modifyColumn(final byte [] tableName, HColumnDescriptor descriptor)
  throws IOException;


  /**
   * Deletes a column from the specified table. Table must be disabled.
   * @param tableName table to alter
   * @param columnName column family to remove
   * @throws IOException e
   */
  public void deleteColumn(final byte [] tableName, final byte [] columnName)
  throws IOException;

  /**
   * Puts the table on-line (only needed if table has been previously taken offline)
   * @param tableName table to enable
   * @throws IOException e
   */
  public void enableTable(final byte [] tableName) throws IOException;

  /**
   * Take table offline
   *
   * @param tableName table to take offline
   * @throws IOException e
   */
  public void disableTable(final byte [] tableName) throws IOException;

  /**
   * Modify a table's metadata
   *
   * @param tableName table to modify
   * @param htd new descriptor for table
   * @throws IOException e
   */
  public void modifyTable(byte[] tableName, HTableDescriptor htd)
  throws IOException;

  /**
   * Shutdown an HBase cluster.
   * @param controller Unused (set to null).
   * @param request ShutdownRequest
   * @return ShutdownResponse
   * @throws ServiceException
   */
  public ShutdownResponse shutdown(RpcController controller, ShutdownRequest request)
  throws ServiceException;

  /**
   * Stop HBase Master only.
   * Does not shutdown the cluster.
   * @param controller Unused (set to null).
   * @param request StopMasterRequest
   * @return StopMasterResponse
   * @throws ServiceException
   */
  public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request)
  throws ServiceException;

  /**
   * Return cluster status.
   * @return status object
   */
  public ClusterStatus getClusterStatus();

  /**
   * Offline a region from the assignment manager's in-memory state.  The
   * region should be in a closed state and there will be no attempt to
   * automatically reassign the region as in unassign.   This is a special
   * method, and should only be used by experts or hbck.
   * @param regionName Region to offline.  Will clear any existing RegionPlan
   * if one found.
   * @throws IOException
   */
  public void offline(final byte[] regionName) throws IOException;

  /**
   * Run the balancer.  Will run the balancer and if regions to move, it will
   * go ahead and do the reassignments.  Can NOT run for various reasons.  Check
   * logs.
   * @param c Unused (set to null).
   * @param request BalanceRequest
   * @return BalanceResponse that contains:<br>
   * - balancerRan: True if balancer ran and was able to tell the region servers to
   * unassign all the regions to balance (the re-assignment itself is async),
   * false otherwise.
   */
  public BalanceResponse balance(RpcController c, BalanceRequest request) throws ServiceException;

  /**
   * Turn the load balancer on or off.
   * @param controller Unused (set to null).
   * @param req SetBalancerRunningRequest that contains:<br>
   * - on: If true, enable balancer. If false, disable balancer.<br>
   * - synchronous: if true, wait until current balance() call, if outstanding, to return.
   * @return SetBalancerRunningResponse that contains:<br>
   * - prevBalanceValue: Previous balancer value
   * @throws ServiceException
   */
  public SetBalancerRunningResponse loadBalancerIs(RpcController controller, SetBalancerRunningRequest req)
  throws ServiceException;

  /**
   * Get array of all HTDs.
   * @return array of HTableDescriptor
   */
  public HTableDescriptor[] getHTableDescriptors();

  /**
   * Get array of HTDs for requested tables.
   * @param tableNames
   * @return array of HTableDescriptor
   */
  public HTableDescriptor[] getHTableDescriptors(List<String> tableNames);

  /**
   * Assign a region to a server chosen at random.
   * @param controller Unused (set to null).
   * @param req contains the region to assign.  Will use existing RegionPlan if one
   * found.
   * @throws ServiceException
   */
  public AssignRegionResponse assignRegion(RpcController controller, AssignRegionRequest req)
  throws ServiceException;

  /**
   * Unassign a region from current hosting regionserver.  Region will then be
   * assigned to a regionserver chosen at random.  Region could be reassigned
   * back to the same server.  Use {@link #moveRegion(RpcController,MoveRegionRequest}
   * if you want to control the region movement.
   * @param controller Unused (set to null).
   * @param req The request which contains:<br>
   * - region: Region to unassign. Will clear any existing RegionPlan
   * if one found.<br>
   * - force: If true, force unassign (Will remove region from
   * regions-in-transition too if present as well as from assigned regions --
   * radical!.If results in double assignment use hbck -fix to resolve.
   * @throws ServiceException
   */
  public UnassignRegionResponse unassignRegion(RpcController controller, UnassignRegionRequest req)
  throws ServiceException;

  /**
   * Move a region to a specified destination server.
   * @param controller Unused (set to null).
   * @param req The request which contains:<br>
   * - region: The encoded region name; i.e. the hash that makes
   * up the region name suffix: e.g. if regionname is
   * <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   * then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.<br>
   * - destServerName: The servername of the destination regionserver.  If
   * passed the empty byte array we'll assign to a random server.  A server name
   * is made of host, port and startcode.  Here is an example:
   * <code> host187.example.com,60020,1289493121758</code>.
   * @throws ServiceException that wraps a UnknownRegionException if we can't find a
   * region named <code>encodedRegionName</code>
   */
  public MoveRegionResponse moveRegion(RpcController controller, MoveRegionRequest req)
		  throws ServiceException;
}
