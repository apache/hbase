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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.zookeeper.KeeperException;

/**
 * This class contains helper methods that repair parts of hbase's filesystem
 * contents.
 */
public class HBaseFsckRepair {
  public static final Log LOG = LogFactory.getLog(HBaseFsckRepair.class);

  /**
   * Fix multiple assignment by doing silent closes on each RS hosting the region
   * and then force ZK unassigned node to OFFLINE to trigger assignment by
   * master.
   *
   * @param admin HBase admin used to undeploy
   * @param region Region to undeploy
   * @param servers list of Servers to undeploy from
   */
  public static void fixMultiAssignment(HBaseAdmin admin, HRegionInfo region,
      List<ServerName> servers)
  throws IOException, KeeperException, InterruptedException {
    HRegionInfo actualRegion = new HRegionInfo(region);

    // Close region on the servers silently
    for(ServerName server : servers) {
      closeRegionSilentlyAndWait(admin, server, actualRegion);
    }

    // Force ZK node to OFFLINE so master assigns
    forceOfflineInZK(admin, actualRegion);
  }

  /**
   * Fix unassigned by creating/transition the unassigned ZK node for this
   * region to OFFLINE state with a special flag to tell the master that this is
   * a forced operation by HBCK.
   *
   * This assumes that info is in META.
   *
   * @param conf
   * @param region
   * @throws IOException
   * @throws KeeperException
   */
  public static void fixUnassigned(HBaseAdmin admin, HRegionInfo region)
      throws IOException, KeeperException {
    HRegionInfo actualRegion = new HRegionInfo(region);

    // Force ZK node to OFFLINE so master assigns
    forceOfflineInZK(admin, actualRegion);
  }

  /**
   * In 0.90, this forces an HRI offline by setting the RegionTransitionData
   * in ZK to have HBCK_CODE_NAME as the server.  This is a special case in
   * the AssignmentManager that attempts an assign call by the master.
   *
   * @see org.apache.hadoop.hbase.master.AssignementManager#handleHBCK
   *
   * This doesn't seem to work properly in the updated version of 0.92+'s hbck
   * so we use assign to force the region into transition.  This has the
   * side-effect of requiring a HRegionInfo that considers regionId (timestamp)
   * in comparators that is addressed by HBASE-5563.
   */
  private static void forceOfflineInZK(HBaseAdmin admin, final HRegionInfo region)
  throws ZooKeeperConnectionException, KeeperException, IOException {
    admin.assign(region.getRegionName());
  }

  /*
   * Should we check all assignments or just not in RIT?
   */
  public static void waitUntilAssigned(HBaseAdmin admin,
      HRegionInfo region) throws IOException, InterruptedException {
    long timeout = admin.getConfiguration().getLong("hbase.hbck.assign.timeout", 120000);
    long expiration = timeout + System.currentTimeMillis();
    while (System.currentTimeMillis() < expiration) {
      try {
        Map<String, RegionState> rits=
            admin.getClusterStatus().getRegionsInTransition();

        if (rits.keySet() != null && !rits.keySet().contains(region.getEncodedName())) {
          // yay! no longer RIT
          return;
        }
        // still in rit
        LOG.info("Region still in transition, waiting for "
            + "it to become assigned: " + region);
      } catch (IOException e) {
        LOG.warn("Exception when waiting for region to become assigned,"
            + " retrying", e);
      }
      Thread.sleep(1000);
    }
    throw new IOException("Region " + region + " failed to move out of " +
        "transition within timeout " + timeout + "ms");
  }

  /**
   * Contacts a region server and waits up to hbase.hbck.close.timeout ms
   * (default 120s) to close the region.  This bypasses the active hmaster.
   */
  public static void closeRegionSilentlyAndWait(HBaseAdmin admin,
      ServerName server, HRegionInfo region) throws IOException, InterruptedException {
    HConnection connection = admin.getConnection();
    HRegionInterface rs = connection.getHRegionConnection(server.getHostname(),
        server.getPort());
    try {
      rs.closeRegion(region, false);
    } catch (IOException ioe) {
      LOG.warn("Exception when closing region: " + region.getRegionNameAsString(), ioe);
    }
    long timeout = admin.getConfiguration()
      .getLong("hbase.hbck.close.timeout", 120000);
    long expiration = timeout + System.currentTimeMillis();
    while (System.currentTimeMillis() < expiration) {
      try {
        HRegionInfo rsRegion = rs.getRegionInfo(region.getRegionName());
        if (rsRegion == null)
          return;
      } catch (IOException ioe) {
        return;
      }
      Thread.sleep(1000);
    }
    throw new IOException("Region " + region + " failed to close within"
        + " timeout " + timeout);
  }

  /**
   * Puts the specified HRegionInfo into META.
   */
  public static void fixMetaHoleOnline(Configuration conf,
      HRegionInfo hri) throws IOException {
    Put p = new Put(hri.getRegionName());
    p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
        Writables.getBytes(hri));
    HTable meta = new HTable(conf, HConstants.META_TABLE_NAME);
    meta.put(p);
    meta.close();
  }

  /**
   * Creates, flushes, and closes a new region.
   */
  public static HRegion createHDFSRegionDir(Configuration conf,
      HRegionInfo hri, HTableDescriptor htd) throws IOException {
    // Create HRegion
    Path root = FSUtils.getRootDir(conf);
    HRegion region = HRegion.createHRegion(hri, root, conf, htd);
    HLog hlog = region.getLog();

    // Close the new region to flush to disk. Close log file too.
    region.close();
    hlog.closeAndDelete();
    return region;
  }
}
