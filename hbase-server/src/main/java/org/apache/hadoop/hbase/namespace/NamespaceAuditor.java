/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.namespace;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.quotas.QuotaExceededException;

import com.google.common.annotations.VisibleForTesting;

/**
 * The Class NamespaceAuditor performs checks to ensure operations like table creation and region
 * splitting preserve namespace quota. The namespace quota can be specified while namespace
 * creation.
 */
@InterfaceAudience.Private
public class NamespaceAuditor {
  private static Log LOG = LogFactory.getLog(NamespaceAuditor.class);
  static final String NS_AUDITOR_INIT_TIMEOUT = "hbase.namespace.auditor.init.timeout";
  static final int DEFAULT_NS_AUDITOR_INIT_TIMEOUT = 120000;
  private NamespaceStateManager stateManager;
  private MasterServices masterServices;

  public NamespaceAuditor(MasterServices masterServices) {
    this.masterServices = masterServices;
    stateManager = new NamespaceStateManager(masterServices, masterServices.getZooKeeper());
  }

  public void start() throws IOException {
    stateManager.start();
    LOG.info("NamespaceAuditor started.");
  }

  /**
   * Check quota to create table. We add the table information to namespace state cache, assuming
   * the operation will pass. If the operation fails, then the next time namespace state chore runs
   * namespace state cache will be corrected.
   * @param tName - The table name to check quota.
   * @param regions - Number of regions that will be added.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void checkQuotaToCreateTable(TableName tName, int regions) throws IOException {
    if (stateManager.isInitialized()) {
      // We do this check to fail fast.
      if (MetaTableAccessor.tableExists(this.masterServices.getConnection(), tName)) {
        throw new TableExistsException(tName);
      }
      stateManager.checkAndUpdateNamespaceTableCount(tName, regions);
    } else {
      checkTableTypeAndThrowException(tName);
    }
  }
  
  /**
   * Check and update region count quota for an existing table.
   * @param tName - table name for which region count to be updated.
   * @param regions - Number of regions that will be added.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void checkQuotaToUpdateRegion(TableName tName, int regions) throws IOException {
    if (stateManager.isInitialized()) {
      stateManager.checkAndUpdateNamespaceRegionCount(tName, regions);
    } else {
      checkTableTypeAndThrowException(tName);
    }
  }

  private void checkTableTypeAndThrowException(TableName name) throws IOException {
    if (name.isSystemTable()) {
      LOG.debug("Namespace auditor checks not performed for table " + name.getNameAsString());
    } else {
      throw new HBaseIOException(name
          + " is being created even before namespace auditor has been initialized.");
    }
  }

  /**
   * Get region count for table
   * @param tName - table name
   * @return cached region count, or -1 if table status not found
   * @throws IOException Signals that the namespace auditor has not been initialized
   */
  public int getRegionCountOfTable(TableName tName) throws IOException {
    if (stateManager.isInitialized()) {
      NamespaceTableAndRegionInfo state = stateManager.getState(tName.getNamespaceAsString());
      return state != null ? state.getRegionCountOfTable(tName) : -1;
    }
    checkTableTypeAndThrowException(tName);
    return -1;
  }

  public void checkQuotaToSplitRegion(HRegionInfo hri) throws IOException {
    if (!stateManager.isInitialized()) {
      throw new IOException(
          "Split operation is being performed even before namespace auditor is initialized.");
    } else if (!stateManager.checkAndUpdateNamespaceRegionCount(hri.getTable(),
      hri.getRegionName(), 1)) {
      throw new QuotaExceededException("Region split not possible for :" + hri.getEncodedName()
          + " as quota limits are exceeded ");
    }
  }

  public void updateQuotaForRegionMerge(HRegionInfo hri) throws IOException {
    if (!stateManager.isInitialized()) {
      throw new IOException(
          "Merge operation is being performed even before namespace auditor is initialized.");
    } else if (!stateManager
        .checkAndUpdateNamespaceRegionCount(hri.getTable(), hri.getRegionName(), -1)) {
      throw new QuotaExceededException("Region split not possible for :" + hri.getEncodedName()
          + " as quota limits are exceeded ");
    }
  }

  public void addNamespace(NamespaceDescriptor ns) throws IOException {
    stateManager.addNamespace(ns.getName());
  }

  public void deleteNamespace(String namespace) throws IOException {
    stateManager.deleteNamespace(namespace);
  }

  public void removeFromNamespaceUsage(TableName tableName) throws IOException {
    stateManager.removeTable(tableName);
  }

  public void removeRegionFromNamespaceUsage(HRegionInfo hri) throws IOException {
    stateManager.removeRegionFromTable(hri);
  }

  /**
   * Used only for unit tests.
   * @param namespace The name of the namespace
   * @return An instance of NamespaceTableAndRegionInfo
   */
  @VisibleForTesting
  NamespaceTableAndRegionInfo getState(String namespace) {
    if (stateManager.isInitialized()) {
      return stateManager.getState(namespace);
    }
    return null;
  }

  /**
   * Checks if namespace auditor is initialized. Used only for testing.
   * @return true, if is initialized
   */
  public boolean isInitialized() {
    return stateManager.isInitialized();
  }
}
