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
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.constraint.ConstraintException;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.BulkAssigner;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.zookeeper.KeeperException;
import org.cloudera.htrace.Trace;

/**
 * Handler to run disable of a table.
 */
@InterfaceAudience.Private
public class DisableTableHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(DisableTableHandler.class);
  private final TableName tableName;
  private final AssignmentManager assignmentManager;
  private final TableLockManager tableLockManager;
  private final CatalogTracker catalogTracker;
  private final boolean skipTableStateCheck;
  private TableLock tableLock;

  public DisableTableHandler(Server server, TableName tableName,
      CatalogTracker catalogTracker, AssignmentManager assignmentManager,
      TableLockManager tableLockManager, boolean skipTableStateCheck) {
    super(server, EventType.C_M_DISABLE_TABLE);
    this.tableName = tableName;
    this.assignmentManager = assignmentManager;
    this.catalogTracker = catalogTracker;
    this.tableLockManager = tableLockManager;
    this.skipTableStateCheck = skipTableStateCheck;
  }

  public DisableTableHandler prepare()
      throws TableNotFoundException, TableNotEnabledException, IOException {
    if(tableName.equals(TableName.META_TABLE_NAME)) {
      throw new ConstraintException("Cannot disable catalog table");
    }
    //acquire the table write lock, blocking
    this.tableLock = this.tableLockManager.writeLock(tableName,
        EventType.C_M_DISABLE_TABLE.toString());
    this.tableLock.acquire();

    boolean success = false;
    try {
      // Check if table exists
      if (!MetaReader.tableExists(catalogTracker, tableName)) {
        throw new TableNotFoundException(tableName);
      }

      // There could be multiple client requests trying to disable or enable
      // the table at the same time. Ensure only the first request is honored
      // After that, no other requests can be accepted until the table reaches
      // DISABLED or ENABLED.
      //TODO: reevaluate this since we have table locks now
      if (!skipTableStateCheck) {
        try {
          if (!this.assignmentManager.getZKTable().checkEnabledAndSetDisablingTable
            (this.tableName)) {
            LOG.info("Table " + tableName + " isn't enabled; skipping disable");
            throw new TableNotEnabledException(this.tableName);
          }
        } catch (KeeperException e) {
          throw new IOException("Unable to ensure that the table will be" +
            " disabling because of a ZooKeeper issue", e);
        }
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
        tableName;
  }

  @Override
  public void process() {
    try {
      LOG.info("Attempting to disable table " + this.tableName);
      MasterCoprocessorHost cpHost = ((HMaster) this.server)
          .getCoprocessorHost();
      if (cpHost != null) {
        cpHost.preDisableTableHandler(this.tableName);
      }
      handleDisableTable();
      if (cpHost != null) {
        cpHost.postDisableTableHandler(this.tableName);
      }
    } catch (IOException e) {
      LOG.error("Error trying to disable table " + this.tableName, e);
    } catch (KeeperException e) {
      LOG.error("Error trying to disable table " + this.tableName, e);
    } finally {
      releaseTableLock();
    }
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

  private void handleDisableTable() throws IOException, KeeperException {
    // Set table disabling flag up in zk.
    this.assignmentManager.getZKTable().setDisablingTable(this.tableName);
    boolean done = false;
    while (true) {
      // Get list of online regions that are of this table.  Regions that are
      // already closed will not be included in this list; i.e. the returned
      // list is not ALL regions in a table, its all online regions according
      // to the in-memory state on this master.
      final List<HRegionInfo> regions = this.assignmentManager
        .getRegionStates().getRegionsOfTable(tableName);
      if (regions.size() == 0) {
        done = true;
        break;
      }
      LOG.info("Offlining " + regions.size() + " regions.");
      BulkDisabler bd = new BulkDisabler(this.server, regions);
      try {
        if (bd.bulkAssign()) {
          done = true;
          break;
        }
      } catch (InterruptedException e) {
        LOG.warn("Disable was interrupted");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
        break;
      }
    }
    // Flip the table to disabled if success.
    if (done) this.assignmentManager.getZKTable().setDisabledTable(this.tableName);
    LOG.info("Disabled table, " + this.tableName + ", is done=" + done);
  }

  /**
   * Run bulk disable.
   */
  class BulkDisabler extends BulkAssigner {
    private final List<HRegionInfo> regions;

    BulkDisabler(final Server server, final List<HRegionInfo> regions) {
      super(server);
      this.regions = regions;
    }

    @Override
    protected void populatePool(ExecutorService pool) {
      RegionStates regionStates = assignmentManager.getRegionStates();
      for (HRegionInfo region: regions) {
        if (regionStates.isRegionInTransition(region)
            && !regionStates.isRegionInState(region, State.FAILED_CLOSE)) {
          continue;
        }
        final HRegionInfo hri = region;
        pool.execute(Trace.wrap("DisableTableHandler.BulkDisabler",new Runnable() {
          public void run() {
            assignmentManager.unassign(hri, true);
          }
        }));
      }
    }

    @Override
    protected boolean waitUntilDone(long timeout)
    throws InterruptedException {
      long startTime = System.currentTimeMillis();
      long remaining = timeout;
      List<HRegionInfo> regions = null;
      while (!server.isStopped() && remaining > 0) {
        Thread.sleep(waitingTimeForEvents);
        regions = assignmentManager.getRegionStates().getRegionsOfTable(tableName);
        LOG.debug("Disable waiting until done; " + remaining + " ms remaining; " + regions);
        if (regions.isEmpty()) break;
        remaining = timeout - (System.currentTimeMillis() - startTime);
      }
      return regions != null && regions.isEmpty();
    }
  }
}
