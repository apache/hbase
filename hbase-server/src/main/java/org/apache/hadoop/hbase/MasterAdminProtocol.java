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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AddColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.BalanceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CatalogScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.CreateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.DisableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.EnableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ListSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ListSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.MasterAdminService;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.ShutdownResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.StopMasterResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.TakeSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.TakeSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterAdminProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.security.KerberosInfo;
import org.apache.hadoop.hbase.security.TokenInfo;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Protocol that a client uses to communicate with the Master (for admin purposes).
 */
@KerberosInfo(
  serverPrincipal = "hbase.master.kerberos.principal")
@TokenInfo("HBASE_AUTH_TOKEN")
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface MasterAdminProtocol extends
    MasterAdminService.BlockingInterface, MasterProtocol {
  public static final long VERSION = 1L;

  /* Column-level */

  /**
   * Adds a column to the specified table
   * @param controller Unused (set to null).
   * @param req AddColumnRequest that contains:<br>
   * - tableName: table to modify<br>
   * - column: column descriptor
   * @throws ServiceException
   */
  @Override
  public AddColumnResponse addColumn(RpcController controller, AddColumnRequest req)
  throws ServiceException;

  /**
   * Deletes a column from the specified table. Table must be disabled.
   * @param controller Unused (set to null).
   * @param req DeleteColumnRequest that contains:<br>
   * - tableName: table to alter<br>
   * - columnName: column family to remove
   * @throws ServiceException
   */
  @Override
  public DeleteColumnResponse deleteColumn(RpcController controller, DeleteColumnRequest req)
  throws ServiceException;

  /**
   * Modifies an existing column on the specified table
   * @param controller Unused (set to null).
   * @param req ModifyColumnRequest that contains:<br>
   * - tableName: table name<br>
   * - descriptor: new column descriptor
   * @throws ServiceException  e
   */
  @Override
  public ModifyColumnResponse modifyColumn(RpcController controller, ModifyColumnRequest req)
  throws ServiceException;

  /* Region-level */

  /**
   * Move a region to a specified destination server.
   * @param controller Unused (set to null).
   * @param req The request that contains:<br>
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
  @Override
  public MoveRegionResponse moveRegion(RpcController controller, MoveRegionRequest req)
  throws ServiceException;

  /**
   * Assign a region to a server chosen at random.
   * @param controller Unused (set to null).
   * @param req contains the region to assign.  Will use existing RegionPlan if one
   * found.
   * @throws ServiceException
   */
  @Override
  public AssignRegionResponse assignRegion(RpcController controller, AssignRegionRequest req)
  throws ServiceException;

  /**
   * Unassign a region from current hosting regionserver.  Region will then be
   * assigned to a regionserver chosen at random.  Region could be reassigned
   * back to the same server.  Use {@link #moveRegion} if you want to
   * control the region movement.
   * @param controller Unused (set to null).
   * @param req The request that contains:<br>
   * - region: Region to unassign. Will clear any existing RegionPlan
   * if one found.<br>
   * - force: If true, force unassign (Will remove region from
   * regions-in-transition too if present as well as from assigned regions --
   * radical!.If results in double assignment use hbck -fix to resolve.
   * @throws ServiceException
   */
  @Override
  public UnassignRegionResponse unassignRegion(RpcController controller, UnassignRegionRequest req)
  throws ServiceException;

  /**
   * Offline a region from the assignment manager's in-memory state.  The
   * region should be in a closed state and there will be no attempt to
   * automatically reassign the region as in unassign.   This is a special
   * method, and should only be used by experts or hbck.
   * @param controller Unused (set to null).
   * @param request OfflineRegionRequest that contains:<br>
   * - region: Region to offline.  Will clear any existing RegionPlan
   * if one found.
   * @throws ServiceException
   */
  @Override
  public OfflineRegionResponse offlineRegion(RpcController controller, OfflineRegionRequest request)
  throws ServiceException;

  /* Table-level */

  /**
   * Creates a new table asynchronously.  If splitKeys are specified, then the
   * table will be created with an initial set of multiple regions.
   * If splitKeys is null, the table will be created with a single region.
   * @param controller Unused (set to null).
   * @param req CreateTableRequest that contains:<br>
   * - tablesSchema: table descriptor<br>
   * - splitKeys
   * @throws ServiceException
   */
  @Override
  public CreateTableResponse createTable(RpcController controller, CreateTableRequest req)
  throws ServiceException;

  /**
   * Deletes a table
   * @param controller Unused (set to null).
   * @param req DeleteTableRequest that contains:<br>
   * - tableName: table to delete
   * @throws ServiceException
   */
  @Override
  public DeleteTableResponse deleteTable(RpcController controller, DeleteTableRequest req)
  throws ServiceException;

  /**
   * Puts the table on-line (only needed if table has been previously taken offline)
   * @param controller Unused (set to null).
   * @param req EnableTableRequest that contains:<br>
   * - tableName: table to enable
   * @throws ServiceException
   */
  @Override
  public EnableTableResponse enableTable(RpcController controller, EnableTableRequest req)
  throws ServiceException;

  /**
   * Take table offline
   *
   * @param controller Unused (set to null).
   * @param req DisableTableRequest that contains:<br>
   * - tableName: table to take offline
   * @throws ServiceException
   */
  @Override
  public DisableTableResponse disableTable(RpcController controller, DisableTableRequest req)
  throws ServiceException;

  /**
   * Modify a table's metadata
   *
   * @param controller Unused (set to null).
   * @param req ModifyTableRequest that contains:<br>
   * - tableName: table to modify<br>
   * - tableSchema: new descriptor for table
   * @throws ServiceException
   */
  @Override
  public ModifyTableResponse modifyTable(RpcController controller, ModifyTableRequest req)
  throws ServiceException;

  /* Cluster-level */

  /**
   * Shutdown an HBase cluster.
   * @param controller Unused (set to null).
   * @param request ShutdownRequest
   * @return ShutdownResponse
   * @throws ServiceException
   */
  @Override
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
  @Override
  public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request)
  throws ServiceException;

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
  @Override
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
  @Override
  public SetBalancerRunningResponse setBalancerRunning(
      RpcController controller, SetBalancerRunningRequest req) throws ServiceException;

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

  /**
   * Run a scan of the catalog table
   * @param c Unused (set to null).
   * @param req CatalogScanRequest
   * @return CatalogScanResponse that contains the int return code corresponding
   *         to the number of entries cleaned
   * @throws ServiceException
   */
  @Override
  public CatalogScanResponse runCatalogScan(RpcController c,
      CatalogScanRequest req) throws ServiceException;

  /**
   * Enable/Disable the catalog janitor
   * @param c Unused (set to null).
   * @param req EnableCatalogJanitorRequest that contains:<br>
   * - enable: If true, enable catalog janitor. If false, disable janitor.<br>
   * @return EnableCatalogJanitorResponse that contains:<br>
   * - prevValue: true, if it was enabled previously; false, otherwise
   * @throws ServiceException
   */
  @Override
  public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController c,
      EnableCatalogJanitorRequest req) throws ServiceException;

  /**
   * Query whether the catalog janitor is enabled
   * @param c Unused (set to null).
   * @param req IsCatalogJanitorEnabledRequest
   * @return IsCatalogCatalogJanitorEnabledResponse that contains:<br>
   * - value: true, if it is enabled; false, otherwise
   * @throws ServiceException
   */
  @Override
  public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController c,
      IsCatalogJanitorEnabledRequest req) throws ServiceException;

  /**
   * Create a snapshot for the given table.
   * @param controller Unused (set to null).
   * @param snapshot description of the snapshot to take
   * @return empty response on success
   * @throws ServiceException if the snapshot cannot be taken
   */
  @Override
  public TakeSnapshotResponse snapshot(RpcController controller, TakeSnapshotRequest snapshot)
      throws ServiceException;

  /**
   * List existing snapshots.
   * @param controller Unused (set to null).
   * @param request information about the request (can be empty)
   * @return {@link ListSnapshotResponse} - a list of {@link SnapshotDescription}
   * @throws ServiceException if we cannot reach the filesystem
   */
  @Override
  public ListSnapshotResponse listSnapshots(RpcController controller, ListSnapshotRequest request)
      throws ServiceException;

  /**
   * Delete an existing snapshot. This method can also be used to clean up a aborted snapshot.
   * @param controller Unused (set to null).
   * @param snapshotName snapshot to delete
   * @return <tt>true</tt> if the snapshot was deleted, <tt>false</tt> if the snapshot didn't exist
   *         originally
   * @throws ServiceException if the filesystem cannot be reached
   */
  @Override
  public DeleteSnapshotResponse deleteSnapshot(RpcController controller,
      DeleteSnapshotRequest snapshotName) throws ServiceException;

  /**
   * Check to see if the snapshot is done.
   * @param controller Unused (set to null).
   * @param request name of the snapshot to check.
   * @throws ServiceException around possible exceptions:
   *           <ol>
   *           <li>{@link UnknownSnapshotException} if the passed snapshot name doesn't match the
   *           current snapshot <i>or</i> there is no previous snapshot.</li>
   *           <li>{@link SnapshotCreationException} if the snapshot couldn't complete because of
   *           errors</li>
   *           </ol>
   */
  @Override
  public IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
      IsSnapshotDoneRequest request) throws ServiceException;
}
