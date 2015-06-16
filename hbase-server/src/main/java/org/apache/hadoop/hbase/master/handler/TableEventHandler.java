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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CoordinatedStateException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InvalidFamilyOperationException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.master.BulkReOpen;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.TableLockManager.TableLock;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base class for performing operations against tables.
 * Checks on whether the process can go forward are done in constructor rather
 * than later on in {@link #process()}.  The idea is to fail fast rather than
 * later down in an async invocation of {@link #process()} (which currently has
 * no means of reporting back issues once started).
 */
@InterfaceAudience.Private
public abstract class TableEventHandler extends EventHandler {
  private static final Log LOG = LogFactory.getLog(TableEventHandler.class);
  protected final MasterServices masterServices;
  protected final TableName tableName;
  protected TableLock tableLock;
  private boolean isPrepareCalled = false;

  public TableEventHandler(EventType eventType, TableName tableName, Server server,
      MasterServices masterServices) {
    super(server, eventType);
    this.masterServices = masterServices;
    this.tableName = tableName;
  }

  public TableEventHandler prepare() throws IOException {
    //acquire the table write lock, blocking
    this.tableLock = masterServices.getTableLockManager()
        .writeLock(tableName, eventType.toString());
    this.tableLock.acquire();
    boolean success = false;
    try {
      try {
        this.masterServices.checkTableModifiable(tableName);
      } catch (TableNotDisabledException ex)  {
        if (isOnlineSchemaChangeAllowed()
            && eventType.isOnlineSchemaChangeSupported()) {
          LOG.debug("Ignoring table not disabled exception " +
              "for supporting online schema changes.");
        } else {
          throw ex;
        }
      }
      prepareWithTableLock();
      success = true;
    } finally {
      if (!success ) {
        releaseTableLock();
      }
    }
    this.isPrepareCalled = true;
    return this;
  }

  /** Called from prepare() while holding the table lock. Subclasses
   * can do extra initialization, and not worry about the releasing
   * the table lock. */
  protected void prepareWithTableLock() throws IOException {
  }

  private boolean isOnlineSchemaChangeAllowed() {
    return this.server.getConfiguration().getBoolean(
      "hbase.online.schema.update.enable", false);
  }

  @Override
  public void process() {
    if (!isPrepareCalled) {
      //For proper table locking semantics, the implementor should ensure to call
      //TableEventHandler.prepare() before calling process()
      throw new RuntimeException("Implementation should have called prepare() first");
    }
    try {
      LOG.info("Handling table operation " + eventType + " on table " +
          tableName);

      List<HRegionInfo> hris;
      if (TableName.META_TABLE_NAME.equals(tableName)) {
        hris = new MetaTableLocator().getMetaRegions(server.getZooKeeper());
      } else {
        hris = MetaTableAccessor.getTableRegions(server.getZooKeeper(),
          server.getConnection(), tableName);
      }
      handleTableOperation(hris);
      if (eventType.isOnlineSchemaChangeSupported() && this.masterServices.
          getAssignmentManager().getTableStateManager().isTableState(
          tableName, ZooKeeperProtos.Table.State.ENABLED)) {
        if (reOpenAllRegions(hris)) {
          LOG.info("Completed table operation " + eventType + " on table " +
              tableName);
        } else {
          LOG.warn("Error on reopening the regions");
        }
      }
      completed(null);
    } catch (IOException e) {
      LOG.error("Error manipulating table " + tableName, e);
      completed(e);
    } catch (CoordinatedStateException e) {
      LOG.error("Error manipulating table " + tableName, e);
      completed(e);
    } finally {
      releaseTableLock();
    }
  }

  protected void releaseTableLock() {
    if (this.tableLock != null) {
      try {
        this.tableLock.release();
      } catch (IOException ex) {
        LOG.warn("Could not release the table lock", ex);
      }
    }
  }

  /**
   * Called after that process() is completed.
   * @param exception null if process() is successful or not null if something has failed.
   */
  protected void completed(final Throwable exception) {
  }

  public boolean reOpenAllRegions(List<HRegionInfo> regions) throws IOException {
    boolean done = false;
    LOG.info("Bucketing regions by region server...");
    List<HRegionLocation> regionLocations = null;
    Connection connection = this.masterServices.getConnection();
    try (RegionLocator locator = connection.getRegionLocator(tableName)) {
      regionLocations = locator.getAllRegionLocations();
    }
    // Convert List<HRegionLocation> to Map<HRegionInfo, ServerName>.
    NavigableMap<HRegionInfo, ServerName> hri2Sn = new TreeMap<HRegionInfo, ServerName>();
    for (HRegionLocation location: regionLocations) {
      hri2Sn.put(location.getRegionInfo(), location.getServerName());
    }
    TreeMap<ServerName, List<HRegionInfo>> serverToRegions = Maps.newTreeMap();
    List<HRegionInfo> reRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo hri : regions) {
      ServerName sn = hri2Sn.get(hri);
      // Skip the offlined split parent region
      // See HBASE-4578 for more information.
      if (null == sn) {
        LOG.info("Skip " + hri);
        continue;
      }
      if (!serverToRegions.containsKey(sn)) {
        LinkedList<HRegionInfo> hriList = Lists.newLinkedList();
        serverToRegions.put(sn, hriList);
      }
      reRegions.add(hri);
      serverToRegions.get(sn).add(hri);
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
   * Gets a TableDescriptor from the masterServices.  Can Throw exceptions.
   *
   * @return Table descriptor for this table
   * @throws FileNotFoundException
   * @throws IOException
   */
  public HTableDescriptor getTableDescriptor()
  throws FileNotFoundException, IOException {
    HTableDescriptor htd =
      this.masterServices.getTableDescriptors().get(tableName);
    if (htd == null) {
      throw new IOException("HTableDescriptor missing for " + tableName);
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
    throws IOException, CoordinatedStateException;
}
