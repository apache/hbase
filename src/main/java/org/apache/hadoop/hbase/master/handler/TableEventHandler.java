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
package org.apache.hadoop.hbase.master.handler;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.ipc.HMasterInterface;
import org.apache.hadoop.hbase.master.BulkReOpen;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.monitoring.TaskMonitor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MasterSchemaChangeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base class for performing operations against tables.
 * Checks on whether the process can go forward are done in constructor rather
 * than later on in {@link #process()}.  The idea is to fail fast rather than
 * later down in an async invocation of {@link #process()} (which currently has
 * no means of reporting back issues once started).
 */
public abstract class TableEventHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(TableEventHandler.class);
  protected final MasterServices masterServices;
  protected HMasterInterface master = null;
  protected final byte [] tableName;
  protected final String tableNameStr;
  protected boolean instantAction = false;

  public TableEventHandler(EventType eventType, byte [] tableName, Server server,
      MasterServices masterServices, HMasterInterface masterInterface,
      boolean instantSchemaChange)
  throws IOException {
    super(server, eventType);
    this.masterServices = masterServices;
    this.tableName = tableName;
    this.masterServices.checkTableModifiable(tableName, eventType);
    this.tableNameStr = Bytes.toString(this.tableName);
    this.instantAction = instantSchemaChange;
    this.master = masterInterface;
  }

  @Override
  public void process() {
    try {
      LOG.info("Handling table operation " + eventType + " on table " +
          Bytes.toString(tableName));
      List<HRegionInfo> hris =
        MetaReader.getTableRegions(this.server.getCatalogTracker(),
          tableName);
      handleTableOperation(hris);
      handleSchemaChanges(hris);
    } catch (IOException e) {
      LOG.error("Error manipulating table " + Bytes.toString(tableName), e);
    } catch (KeeperException e) {
      LOG.error("Error manipulating table " + Bytes.toString(tableName), e);
    }
  }

  private void handleSchemaChanges(List<HRegionInfo> regions)
      throws IOException {
    if (instantAction && regions != null && !regions.isEmpty()) {
      handleInstantSchemaChanges(regions);
    } else {
      handleRegularSchemaChanges(regions);
    }
  }


  /**
   * Perform schema changes only if the table is in enabled state.
   * @return
   */
  private boolean canPerformSchemaChange() {
    return (eventType.isSchemaChangeEvent() && this.masterServices.
        getAssignmentManager().getZKTable().
        isEnabledTable(Bytes.toString(tableName)));
  }

  private void handleRegularSchemaChanges(List<HRegionInfo> regions)
      throws IOException {
    if (canPerformSchemaChange()) {
      this.masterServices.getAssignmentManager().setRegionsToReopen(regions);
      if (reOpenAllRegions(regions)) {
        LOG.info("Completed table operation " + eventType + " on table " +
            Bytes.toString(tableName));
      } else {
        LOG.warn("Error on reopening the regions");
      }
    }
  }

  public boolean reOpenAllRegions(List<HRegionInfo> regions) throws IOException {
    boolean done = false;
    LOG.info("Bucketing regions by region server...");
    HTable table = new HTable(masterServices.getConfiguration(), tableName);
    TreeMap<ServerName, List<HRegionInfo>> serverToRegions = Maps
        .newTreeMap();
    NavigableMap<HRegionInfo, ServerName> hriHserverMapping
        = table.getRegionLocations();
    List<HRegionInfo> reRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo hri : regions) {
      ServerName rsLocation = hriHserverMapping.get(hri);

      // Skip the offlined split parent region
      // See HBASE-4578 for more information.
      if (null == rsLocation) {
        LOG.info("Skip " + hri);
        continue;
      }
      if (!serverToRegions.containsKey(rsLocation)) {
        LinkedList<HRegionInfo> hriList = Lists.newLinkedList();
        serverToRegions.put(rsLocation, hriList);
      }
      reRegions.add(hri);
      serverToRegions.get(rsLocation).add(hri);
    }
    
    LOG.info("Reopening " + reRegions.size() + " regions on "
        + serverToRegions.size() + " region servers.");
    this.masterServices.getAssignmentManager().setRegionsToReopen(reRegions);
    BulkReOpen bulkReopen = new BulkReOpen(this.server, serverToRegions,
        this.masterServices.getAssignmentManager());
    while (true) {
      try {
        if (bulkReopen.bulkReOpen()) {
          done = true;
          break;
        } else {
          LOG.warn("Timeout before reopening all regions");
        }
      } catch (InterruptedException e) {
        LOG.warn("Reopen was interrupted");
        // Preserve the interrupt.
        Thread.currentThread().interrupt();
        break;
      }
    }
    return done;
  }

  /**
   * Check whether any of the regions from the list of regions is undergoing a split.
   * We simply check whether there is a unassigned node for any of the region and if so
   * we return as true.
   * @param regionInfos
   * @return
   */
  private boolean isSplitInProgress(List<HRegionInfo> regionInfos) {
    for (HRegionInfo hri : regionInfos) {
      ZooKeeperWatcher zkw = this.masterServices.getZooKeeper();
      String node = ZKAssign.getNodeName(zkw, hri.getEncodedName());
      try {
        if (ZKUtil.checkExists(zkw, node) != -1) {
          LOG.debug("Region " + hri.getRegionNameAsString() + " is unassigned. Assuming" +
          " that it is undergoing a split");
          return true;
        }
      } catch (KeeperException ke) {
        LOG.debug("KeeperException while determining splits in progress.", ke);
        // Assume no splits happening?
        return false;
      }
    }
    return false;
  }

  /**
   * Wait for region split transaction in progress (if any)
   * @param regions
   * @param status
   */
  private void waitForInflightSplit(List<HRegionInfo> regions, MonitoredTask status) {
    while (isSplitInProgress(regions)) {
      try {
        status.setStatus("Alter Schema is waiting for split region to complete.");
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  protected void handleInstantSchemaChanges(List<HRegionInfo> regions) {
    if (regions == null || regions.isEmpty()) {
      LOG.debug("Region size is null or empty. Ignoring alter request.");
      return;
    }
    MonitoredTask status = TaskMonitor.get().createStatus(
        "Handling alter table request for table = " + tableNameStr);
    if (canPerformSchemaChange()) {
      boolean prevBalanceSwitch = false;
      try {
        // turn off load balancer synchronously
        prevBalanceSwitch = master.synchronousBalanceSwitch(false);
        waitForInflightSplit(regions, status);
        MasterSchemaChangeTracker masterSchemaChangeTracker =
          this.masterServices.getSchemaChangeTracker();
        masterSchemaChangeTracker
        .createSchemaChangeNode(Bytes.toString(tableName),
            regions.size());
        while(!masterSchemaChangeTracker.doesSchemaChangeNodeExists(
            Bytes.toString(tableName))) {
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        status.markComplete("Created ZK node for handling the alter table request for table = "
            + tableNameStr);
      } catch (KeeperException e) {
        LOG.warn("Instant schema change failed for table " + tableNameStr, e);
        status.setStatus("Instant schema change failed for table " + tableNameStr
            + " Cause = " + e.getCause());

      } catch (IOException ioe) {
        LOG.warn("Instant schema change failed for table " + tableNameStr, ioe);
        status.setStatus("Instant schema change failed for table " + tableNameStr
            + " Cause = " + ioe.getCause());
      } finally {
        master.synchronousBalanceSwitch(prevBalanceSwitch);
      }
    }
  }

  /**
   * @return Table descriptor for this table
   * @throws TableExistsException
   * @throws FileNotFoundException
   * @throws IOException
   */
  HTableDescriptor getTableDescriptor()
  throws TableExistsException, FileNotFoundException, IOException {
    final String name = Bytes.toString(tableName);
    HTableDescriptor htd =
      this.masterServices.getTableDescriptors().get(name);
    if (htd == null) {
      throw new IOException("HTableDescriptor missing for " + name);
    }
    return htd;
  }

  byte [] hasColumnFamily(final HTableDescriptor htd, final byte [] cf)
  throws InvalidFamilyOperationException {
    if (!htd.hasFamily(cf)) {
      throw new InvalidFamilyOperationException("Column family '" +
        Bytes.toString(cf) + "' does not exist");
    }
    return cf;
  }

  protected abstract void handleTableOperation(List<HRegionInfo> regions)
  throws IOException, KeeperException;
}
