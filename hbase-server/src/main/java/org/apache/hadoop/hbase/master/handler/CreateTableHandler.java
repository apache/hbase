/**
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
package org.apache.hadoop.hbase.master.handler;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotAllMetaRegionsOnlineException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ModifyRegionUtils;
import org.apache.zookeeper.KeeperException;

/**
 * Handler to create a table.
 */
@InterfaceAudience.Private
public class CreateTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(CreateTableHandler.class);
  protected final MasterFileSystem fileSystemManager;
  protected final HTableDescriptor hTableDescriptor;
  protected final Configuration conf;
  private final AssignmentManager assignmentManager;
  private final CatalogTracker catalogTracker;
  private final TableLockManager tableLockManager;
  private final HRegionInfo [] newRegions;
  private final TableLock tableLock;
  private User activeUser;

  public CreateTableHandler(Server server, MasterFileSystem fileSystemManager,
      HTableDescriptor hTableDescriptor, Configuration conf, HRegionInfo [] newRegions,
      MasterServices masterServices) {
    super(server, EventType.C_M_CREATE_TABLE);

    this.fileSystemManager = fileSystemManager;
    this.hTableDescriptor = hTableDescriptor;
    this.conf = conf;
    this.newRegions = newRegions;
    this.catalogTracker = masterServices.getCatalogTracker();
    this.assignmentManager = masterServices.getAssignmentManager();
    this.tableLockManager = masterServices.getTableLockManager();

    this.tableLock = this.tableLockManager.writeLock(this.hTableDescriptor.getTableName()
        , EventType.C_M_CREATE_TABLE.toString());
  }

  public CreateTableHandler prepare()
      throws NotAllMetaRegionsOnlineException, TableExistsException, IOException {
    int timeout = conf.getInt("hbase.client.catalog.timeout", 10000);
    // Need hbase:meta availability to create a table
    try {
      if(catalogTracker.waitForMeta(timeout) == null) {
        throw new NotAllMetaRegionsOnlineException();
      }
      // If we are creating the table in service to an RPC request, record the
      // active user for later, so proper permissions will be applied to the
      // new table by the AccessController if it is active
      this.activeUser = RpcServer.getRequestUser();
      if (this.activeUser == null) {
        this.activeUser = UserProvider.instantiate(conf).getCurrent();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted waiting for meta availability", e);
      InterruptedIOException ie = new InterruptedIOException(e.getMessage());
      ie.initCause(e);
      throw ie;
    }

    //acquire the table write lock, blocking. Make sure that it is released.
    this.tableLock.acquire();
    boolean success = false;
    try {
      TableName tableName = this.hTableDescriptor.getTableName();
      if (MetaReader.tableExists(catalogTracker, tableName)) {
        throw new TableExistsException(tableName);
      }

      // If we have multiple client threads trying to create the table at the
      // same time, given the async nature of the operation, the table
      // could be in a state where hbase:meta table hasn't been updated yet in
      // the process() function.
      // Use enabling state to tell if there is already a request for the same
      // table in progress. This will introduce a new zookeeper call. Given
      // createTable isn't a frequent operation, that should be ok.
      //TODO: now that we have table locks, re-evaluate above
      try {
        // During master initialization, the ZK state could be inconsistent from failed DDL
        // in the past. If we fail here, it would prevent master to start.  We should force
        // setting the system table state regardless the table state.
        if (!((HMaster) this.server).isInitialized() && tableName.isSystemTable()) {
          this.assignmentManager.getZKTable().setEnablingTable(tableName);
        } else if (!this.assignmentManager.getZKTable().checkAndSetEnablingTable(tableName)) {
          throw new TableExistsException(tableName);
        }
      } catch (KeeperException e) {
        throw new IOException("Unable to ensure that the table will be" +
          " enabling because of a ZooKeeper issue", e);
      }
      success = true;
    } finally {
      if (!success) {
        releaseTableLock();
      }
    }
    return this;
  }

  @Override
  public String toString() {
    String name = "UnknownServerName";
    if(server != null && server.getServerName() != null) {
      name = server.getServerName().toString();
    }
    return getClass().getSimpleName() + "-" + name + "-" + getSeqid() + "-" +
      this.hTableDescriptor.getTableName();
  }

  @Override
  public void process() {
    TableName tableName = this.hTableDescriptor.getTableName();
    LOG.info("Create table " + tableName);

    try {
      final MasterCoprocessorHost cpHost = ((HMaster) this.server).getCoprocessorHost();
      if (cpHost != null) {
        cpHost.preCreateTableHandler(this.hTableDescriptor, this.newRegions);
      }
      handleCreateTable(tableName);
      completed(null);
      if (cpHost != null) {
        this.activeUser.runAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            cpHost.postCreateTableHandler(hTableDescriptor, newRegions);
            return null;
          }
        });
      }
    } catch (Throwable e) {
      LOG.error("Error trying to create the table " + tableName, e);
      completed(e);
    }
  }

  /**
   * Called after that process() is completed.
   * @param exception null if process() is successful or not null if something has failed.
   */
  protected void completed(final Throwable exception) {
    releaseTableLock();
    String msg = exception == null ? null : exception.getMessage();
    LOG.info("Table, " + this.hTableDescriptor.getTableName() + ", creation " +
        msg == null ? "successful" : "failed. " + msg);
    if (exception != null) {
      // Try deleting the enabling node in case of error
      // If this does not happen then if the client tries to create the table
      // again with the same Active master
      // It will block the creation saying TableAlreadyExists.
      try {
        this.assignmentManager.getZKTable().removeEnablingTable(
            this.hTableDescriptor.getTableName(), false);
      } catch (KeeperException e) {
        // Keeper exception should not happen here
        LOG.error("Got a keeper exception while removing the ENABLING table znode "
            + this.hTableDescriptor.getTableName(), e);
      }
    }
  }

  /**
   * Responsible of table creation (on-disk and META) and assignment.
   * - Create the table directory and descriptor (temp folder)
   * - Create the on-disk regions (temp folder)
   *   [If something fails here: we've just some trash in temp]
   * - Move the table from temp to the root directory
   *   [If something fails here: we've the table in place but some of the rows required
   *    present in META. (hbck needed)]
   * - Add regions to META
   *   [If something fails here: we don't have regions assigned: table disabled]
   * - Assign regions to Region Servers
   *   [If something fails here: we still have the table in disabled state]
   * - Update ZooKeeper with the enabled state
   */
  private void handleCreateTable(TableName tableName)
      throws IOException, KeeperException {
    Path tempdir = fileSystemManager.getTempDir();
    FileSystem fs = fileSystemManager.getFileSystem();

    // 1. Create Table Descriptor
    Path tempTableDir = FSUtils.getTableDir(tempdir, tableName);
    new FSTableDescriptors(this.conf).createTableDescriptorForTableDirectory(
      tempTableDir, this.hTableDescriptor, false);
    Path tableDir = FSUtils.getTableDir(fileSystemManager.getRootDir(), tableName);

    // 2. Create Regions
    List<HRegionInfo> regionInfos = handleCreateHdfsRegions(tempdir, tableName);
    // 3. Move Table temp directory to the hbase root location
    if (!fs.rename(tempTableDir, tableDir)) {
      throw new IOException("Unable to move table from temp=" + tempTableDir +
        " to hbase root=" + tableDir);
    }

    if (regionInfos != null && regionInfos.size() > 0) {
      // 4. Add regions to META
      addRegionsToMeta(this.catalogTracker, regionInfos);

      // 5. Trigger immediate assignment of the regions in round-robin fashion
      try {
        assignmentManager.getRegionStates().createRegionStates(regionInfos);
        assignmentManager.assign(regionInfos);
      } catch (InterruptedException e) {
        LOG.error("Caught " + e + " during round-robin assignment");
        InterruptedIOException ie = new InterruptedIOException(e.getMessage());
        ie.initCause(e);
        throw ie;
      }
    }

    // 6. Set table enabled flag up in zk.
    try {
      assignmentManager.getZKTable().setEnabledTable(tableName);
    } catch (KeeperException e) {
      throw new IOException("Unable to ensure that " + tableName + " will be" +
        " enabled because of a ZooKeeper issue", e);
    }

    // 7. Update the tabledescriptor cache.
    ((HMaster) this.server).getTableDescriptors().get(tableName);
  }

  private void releaseTableLock() {
    if (this.tableLock != null) {
      try {
        this.tableLock.release();
      } catch (IOException ex) {
        LOG.warn("Could not release the table lock", ex);
      }
    }
  }

  /**
   * Create the on-disk structure for the table, and returns the regions info.
   * @param tableRootDir directory where the table is being created
   * @param tableName name of the table under construction
   * @return the list of regions created
   */
  protected List<HRegionInfo> handleCreateHdfsRegions(final Path tableRootDir,
    final TableName tableName)
      throws IOException {
    return ModifyRegionUtils.createRegions(conf, tableRootDir,
        hTableDescriptor, newRegions, null);
  }

  /**
   * Add the specified set of regions to the hbase:meta table.
   */
  protected void addRegionsToMeta(final CatalogTracker ct, final List<HRegionInfo> regionInfos)
      throws IOException {
    MetaEditor.addRegionsToMeta(this.catalogTracker, regionInfos);
  }
}
