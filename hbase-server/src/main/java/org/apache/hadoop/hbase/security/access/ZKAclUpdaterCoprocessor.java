

/*
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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;

import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.coprocessor.CoreCoprocessor;
import org.apache.hadoop.hbase.coprocessor.HasMasterServices;
import org.apache.hadoop.hbase.coprocessor.HasRegionServerServices;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessor;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;

import org.apache.hadoop.hbase.master.MasterServices;


import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

@CoreCoprocessor
@InterfaceAudience.Private
public class ZKAclUpdaterCoprocessor implements  MasterCoprocessor, RegionCoprocessor, MasterObserver, RegionObserver{

  private static final Logger LOG = LoggerFactory.getLogger(ZKAclUpdaterCoprocessor.class);

  private ZKPermissionWatcher zkPermissionWatcher;

  /**
   * flags if we are running on a region of the _acl_ table
   */
  private boolean aclRegion = false;
  private boolean initialized = false;

  /**
   * defined only for Endpoint implementation, so it can have way to access region services
   */
  private RegionCoprocessorEnvironment regionEnv;

  public Region getRegion() {
    return regionEnv != null ? regionEnv.getRegion() : null;
  }

  private void initialize(RegionCoprocessorEnvironment e) throws IOException {
    final Region region = e.getRegion();
    Configuration conf = e.getConfiguration();
    Map<byte[], ListMultimap<String, UserPermission>> tables = org.apache.hadoop.hbase.security.access.PermissionStorage.loadAll(region);
    // For each table, write out the table's permissions to the respective
    // znode for that table.
    for (Entry<byte[], ListMultimap<String, UserPermission>> t : tables.entrySet()) {
      byte[] entry = t.getKey();
      ListMultimap<String, UserPermission> perms = t.getValue();
      byte[] serialized = PermissionStorage.writePermissionsAsBytes(perms, conf);
      zkPermissionWatcher.writeToZookeeper(entry, serialized);
    }
    initialized = true;
  }

  /**
   * Writes all table ACLs for the tables in the given Map up into ZooKeeper znodes. This is called
   * to synchronize ACL changes following {@code _acl_} table updates.
   */
  private void updateACL(RegionCoprocessorEnvironment e, final Map<byte[], List<Cell>> familyMap) {
    Set<byte[]> entries = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
    for (Entry<byte[], List<Cell>> f : familyMap.entrySet()) {
      List<Cell> cells = f.getValue();
      for (Cell cell : cells) {
        if (CellUtil.matchingFamily(cell, PermissionStorage.ACL_LIST_FAMILY)) {
          entries.add(CellUtil.cloneRow(cell));
        }
      }
    }
    Configuration conf = regionEnv.getConfiguration();
    byte[] currentEntry = null;
    // TODO: Here we are already on the ACL region. (And it is single
    // region) We can even just get the region from the env and do get
    // directly. The short circuit connection would avoid the RPC overhead
    // so no socket communication, req write/read .. But we have the PB
    // to and fro conversion overhead. get req is converted to PB req
    // and results are converted to PB results 1st and then to POJOs
    // again. We could have avoided such at least in ACL table context..
    try (Table t = e.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
      for (byte[] entry : entries) {
        currentEntry = entry;
        ListMultimap<String, UserPermission> perms =
          PermissionStorage.getPermissions(conf, entry, t, null, null, null, false);
        byte[] serialized = PermissionStorage.writePermissionsAsBytes(perms, conf);
        zkPermissionWatcher.writeToZookeeper(entry, serialized);
      }
    } catch (IOException ex) {
      LOG.error("Failed updating permissions mirror for '" + (currentEntry == null ?
        "null" :
        Bytes.toString(currentEntry)) + "'", ex);
    }
  }

  /* ---- MasterObserver implementation ---- */
  @Override public void start(CoprocessorEnvironment env) throws IOException {
    CompoundConfiguration conf = new CompoundConfiguration();
    conf.add(env.getConfiguration());

    if (env instanceof MasterCoprocessorEnvironment) {
      // if running on HMaster
      MasterCoprocessorEnvironment mEnv = (MasterCoprocessorEnvironment) env;
      if (mEnv instanceof HasMasterServices) {
        MasterServices masterServices = ((HasMasterServices) mEnv).getMasterServices();
        zkPermissionWatcher = masterServices.getZKPermissionWatcher();
      }
    } else if (env instanceof RegionCoprocessorEnvironment) {
      // if running at region
      regionEnv = (RegionCoprocessorEnvironment) env;
      conf.addBytesMap(regionEnv.getRegion().getTableDescriptor().getValues());
      if (regionEnv instanceof HasRegionServerServices) {
        RegionServerServices rsServices =
          ((HasRegionServerServices) regionEnv).getRegionServerServices();
        zkPermissionWatcher = rsServices.getZKPermissionWatcher();
      }
    }

    if (zkPermissionWatcher == null) {
      throw new NullPointerException("ZKPermissionWatcher is null");
    }
  }

  @Override public void stop(CoprocessorEnvironment env) {
  }


  /*********************************** Observer/Service Getters ***********************************/
  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  @Override
  public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override
  public void postOpen(ObserverContext<? extends RegionCoprocessorEnvironment> c) {
    RegionCoprocessorEnvironment env = c.getEnvironment();
    final Region region = env.getRegion();
    if (region == null) {
      LOG.error("NULL region from RegionCoprocessorEnvironment in postOpen()");
      return;
    }
    if (PermissionStorage.isAclRegion(region)) {
      aclRegion = true;
      try {
        initialize(env);
      } catch (IOException ex) {
        // if we can't obtain permissions, it's better to fail
        // than perform checks incorrectly
        throw new RuntimeException("Failed to initialize permissions cache", ex);
      }
    } else {
      initialized = true;
    }
  }

  @Override
  public void postPut(final ObserverContext<? extends RegionCoprocessorEnvironment> c, final Put put,
    final WALEdit edit, final Durability durability) {
    if (aclRegion) {
      updateACL(c.getEnvironment(), put.getFamilyCellMap());
    }
  }

  @Override
  public void postDelete(final ObserverContext<? extends RegionCoprocessorEnvironment> c, final Delete delete,
    final WALEdit edit, final Durability durability) throws IOException {
    if (aclRegion) {
      updateACL(c.getEnvironment(), delete.getFamilyCellMap());
    }
  }

  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> c,
    final TableName tableName) throws IOException {
    zkPermissionWatcher.deleteTableACLNode(tableName);
  }
  @Override
  public void postDeleteNamespace(ObserverContext<MasterCoprocessorEnvironment> ctx,
    final String namespace) throws IOException {
    zkPermissionWatcher.deleteNamespaceACLNode(namespace);
  }

}
