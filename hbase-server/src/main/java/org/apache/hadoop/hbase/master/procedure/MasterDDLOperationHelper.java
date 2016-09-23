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

package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.fs.MasterStorage;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.BulkReOpen;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Helper class for schema change procedures
 */
@InterfaceAudience.Private
public final class MasterDDLOperationHelper {
  private static final Log LOG = LogFactory.getLog(MasterDDLOperationHelper.class);

  private MasterDDLOperationHelper() {}

  /**
   * Remove the column family from the file system
   **/
  public static void deleteColumnFamilyFromFileSystem(
      final MasterProcedureEnv env,
      final TableName tableName,
      List<HRegionInfo> regionInfoList,
      final byte[] familyName,
      boolean hasMob) throws IOException {
    final MasterStorage ms = env.getMasterServices().getMasterStorage();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing family=" + Bytes.toString(familyName) + " from table=" + tableName);
    }
    if (regionInfoList == null) {
      regionInfoList = ProcedureSyncWait.getRegionsFromMeta(env, tableName);
    }
    for (HRegionInfo hri : regionInfoList) {
      // Delete the family directory in FS for all the regions one by one
      ms.deleteFamilyFromStorage(hri, familyName, hasMob);
    }
  }

  /**
   * Reopen all regions from a table after a schema change operation.
   **/
  public static boolean reOpenAllRegions(
      final MasterProcedureEnv env,
      final TableName tableName,
      final List<HRegionInfo> regionInfoList) throws IOException {
    boolean done = false;
    LOG.info("Bucketing regions by region server...");
    List<HRegionLocation> regionLocations = null;
    Connection connection = env.getMasterServices().getConnection();
    try (RegionLocator locator = connection.getRegionLocator(tableName)) {
      regionLocations = locator.getAllRegionLocations();
    }
    // Convert List<HRegionLocation> to Map<HRegionInfo, ServerName>.
    NavigableMap<HRegionInfo, ServerName> hri2Sn = new TreeMap<HRegionInfo, ServerName>();
    for (HRegionLocation location : regionLocations) {
      hri2Sn.put(location.getRegionInfo(), location.getServerName());
    }
    TreeMap<ServerName, List<HRegionInfo>> serverToRegions = Maps.newTreeMap();
    List<HRegionInfo> reRegions = new ArrayList<HRegionInfo>();
    for (HRegionInfo hri : regionInfoList) {
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

    LOG.info("Reopening " + reRegions.size() + " regions on " + serverToRegions.size()
        + " region servers.");
    AssignmentManager am = env.getMasterServices().getAssignmentManager();
    am.setRegionsToReopen(reRegions);
    BulkReOpen bulkReopen = new BulkReOpen(env.getMasterServices(), serverToRegions, am);
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
   * Get the region info list of a table from meta if it is not already known by the caller.
   **/
  public static List<HRegionInfo> getRegionInfoList(
    final MasterProcedureEnv env,
    final TableName tableName,
    List<HRegionInfo> regionInfoList) throws IOException {
    if (regionInfoList == null) {
      regionInfoList = ProcedureSyncWait.getRegionsFromMeta(env, tableName);
    }
    return regionInfoList;
  }
}
