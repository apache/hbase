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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.master.AssignmentManager.RegionState;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;

/**
 * This class contains helper methods that repair parts of hbase's filesystem
 * contents.
 */
public class HBaseFsckRepair {
  public static final Log LOG = LogFactory.getLog(HBaseFsckRepair.class);

  /**
   * Fix dupe assignment by doing silent closes on each RS hosting the region
   * and then force ZK unassigned node to OFFLINE to trigger assignment by
   * master.
   *
   * @param conf
   * @param region
   * @param servers
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static void fixMultiAssignment(HBaseAdmin admin, HRegionInfo region,
      List<HServerAddress> servers) throws IOException, KeeperException,
      InterruptedException {

    HRegionInfo actualRegion = new HRegionInfo(region);

    // Close region on the servers silently
    for (HServerAddress server : servers) {
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
   * This forces an HRI offline by setting the RegionTransitionData in ZK to
   * have HBCK_CODE_NAME as the server.  This is a special case in the
   * AssignmentManager that attempts an assign call by the master.
   *
   * @see org.apache.hadoop.hbase.master.AssignementManager#handleHBCK
   */
  private static void forceOfflineInZK(HBaseAdmin admin, final HRegionInfo region)
  throws ZooKeeperConnectionException, KeeperException, IOException {
    HConnectionManager.execute(new HConnectable<Void>(admin.getConfiguration()) {
      @Override
      public Void connect(HConnection connection) throws IOException {
        try {
          ZKAssign.createOrForceNodeOffline(connection.getZooKeeperWatcher(),
              region, HConstants.HBCK_CODE_NAME);
        } catch (KeeperException ke) {
          throw new IOException(ke);
        }
        return null;
      }
    });
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
      HServerAddress server, HRegionInfo region) throws IOException, InterruptedException {
    HConnection connection = admin.getConnection();
    HRegionInterface rs = connection.getHRegionConnection(server);
    rs.closeRegion(region, false);
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
   * Replace the .regioninfo with a new one with the expected table desc,
   * then re-assign the region.
   */
  public static void fixTableDesc(final HBaseAdmin admin, final HServerAddress hsa,
      final HBaseFsck.HbckInfo hbi, final HTableDescriptor htd, final Path sidelineTableDir)
          throws IOException, KeeperException, InterruptedException {
    // at first, sideline the current .regioninfo
    Path regionDir = hbi.getHdfsRegionDir();
    Path regioninfoPath = new Path(regionDir, HRegion.REGIONINFO_FILE);
    Path sidelineRegionDir = new Path(sidelineTableDir, regionDir.getName());
    Path regioninfoSidelinePath = new Path(sidelineRegionDir, HRegion.REGIONINFO_FILE);
    FileSystem fs = FileSystem.get(admin.getConfiguration());
    fs.mkdirs(sidelineRegionDir);
    boolean success = fs.rename(regioninfoPath, regioninfoSidelinePath);
    if (!success) {
      String msg = "Unable to rename file " + regioninfoPath +  " to " + regioninfoSidelinePath;
      LOG.error(msg);
      throw new IOException(msg);
    }

    // then fix the table desc: create a new .regioninfo,
    //   offline the region and wait till it's assigned again.
    HRegionInfo hri = hbi.getHdfsHRI();
    hri.setTableDesc(htd);
    Path tmpDir = new Path(sidelineRegionDir, ".tmp");
    Path tmpPath = new Path(tmpDir, HRegion.REGIONINFO_FILE);

    FSDataOutputStream out = fs.create(tmpPath, true);
    try {
      hri.write(out);
      out.write('\n');
      out.write('\n');
      out.write(Bytes.toBytes(hri.toString()));
    } finally {
      out.close();
    }
    if (!fs.rename(tmpPath, regioninfoPath)) {
      throw new IOException("Unable to rename " + tmpPath + " to " +
        regioninfoPath);
    }

    if (hsa != null) {
      closeRegionSilentlyAndWait(admin, hsa, hri);
    }

    // Force ZK node to OFFLINE so master assigns
    forceOfflineInZK(admin, hri);
    waitUntilAssigned(admin, hri);
  }

  /**
   * Creates, flushes, and closes a new hdfs region dir
   */
  public static HRegion createHDFSRegionDir(Configuration conf,
      HRegionInfo hri) throws IOException {
    // Create HRegion
    Path root = FSUtils.getRootDir(conf);
    HRegion region = HRegion.createHRegion(hri, root, conf);
    HLog hlog = region.getLog();

    // Close the new region to flush to disk. Close log file too.
    region.close();
    hlog.closeAndDelete();
    return region;
  }
}
