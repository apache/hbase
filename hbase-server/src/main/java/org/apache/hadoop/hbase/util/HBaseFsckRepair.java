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
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterMetrics.Option;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains helper methods that repair parts of hbase's filesystem
 * contents.
 */
@InterfaceAudience.Private
public class HBaseFsckRepair {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFsckRepair.class);

  /**
   * Fix multiple assignment by doing silent closes on each RS hosting the region
   * and then force ZK unassigned node to OFFLINE to trigger assignment by
   * master.
   *
   * @param connection HBase connection to the cluster
   * @param region Region to undeploy
   * @param servers list of Servers to undeploy from
   */
  public static void fixMultiAssignment(Connection connection, RegionInfo region,
      List<ServerName> servers)
  throws IOException, KeeperException, InterruptedException {
    // Close region on the servers silently
    for(ServerName server : servers) {
      closeRegionSilentlyAndWait(connection, server, region);
    }

    // Force ZK node to OFFLINE so master assigns
    forceOfflineInZK(connection.getAdmin(), region);
  }

  /**
   * Fix unassigned by creating/transition the unassigned ZK node for this
   * region to OFFLINE state with a special flag to tell the master that this is
   * a forced operation by HBCK.
   *
   * This assumes that info is in META.
   *
   * @param admin
   * @param region
   * @throws IOException
   * @throws KeeperException
   */
  public static void fixUnassigned(Admin admin, RegionInfo region)
      throws IOException, KeeperException, InterruptedException {
    // Force ZK node to OFFLINE so master assigns
    forceOfflineInZK(admin, region);
  }

  /**
   * In 0.90, this forces an HRI offline by setting the RegionTransitionData
   * in ZK to have HBCK_CODE_NAME as the server.  This is a special case in
   * the AssignmentManager that attempts an assign call by the master.
   *
   * This doesn't seem to work properly in the updated version of 0.92+'s hbck
   * so we use assign to force the region into transition.  This has the
   * side-effect of requiring a RegionInfo that considers regionId (timestamp)
   * in comparators that is addressed by HBASE-5563.
   */
  private static void forceOfflineInZK(Admin admin, final RegionInfo region)
  throws ZooKeeperConnectionException, KeeperException, IOException, InterruptedException {
    admin.assign(region.getRegionName());
  }

  /*
   * Should we check all assignments or just not in RIT?
   */
  public static void waitUntilAssigned(Admin admin,
      RegionInfo region) throws IOException, InterruptedException {
    long timeout = admin.getConfiguration().getLong("hbase.hbck.assign.timeout", 120000);
    long expiration = timeout + EnvironmentEdgeManager.currentTime();
    while (EnvironmentEdgeManager.currentTime() < expiration) {
      try {
        boolean inTransition = false;
        for (RegionState rs : admin.getClusterMetrics(EnumSet.of(Option.REGIONS_IN_TRANSITION))
                                   .getRegionStatesInTransition()) {
          if (RegionInfo.COMPARATOR.compare(rs.getRegion(), region) == 0) {
            inTransition = true;
            break;
          }
        }
        if (!inTransition) {
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
   * Contacts a region server and waits up to hbase.hbck.close.timeout ms (default 120s) to close
   * the region. This bypasses the active hmaster.
   */
  public static void closeRegionSilentlyAndWait(Connection connection, ServerName server,
      RegionInfo region) throws IOException, InterruptedException {
    long timeout = connection.getConfiguration().getLong("hbase.hbck.close.timeout", 120000);
    // this is a bit ugly but it is only used in the old hbck and tests, so I think it is fine.
    try (AsyncClusterConnection asyncConn = ClusterConnectionFactory
      .createAsyncClusterConnection(connection.getConfiguration(), null, User.getCurrent())) {
      ServerManager.closeRegionSilentlyAndWait(asyncConn, server, region, timeout);
    }
  }

  /**
   * Puts the specified RegionInfo into META with replica related columns
   */
  public static void fixMetaHoleOnlineAndAddReplicas(Configuration conf,
      RegionInfo hri, Collection<ServerName> servers, int numReplicas) throws IOException {
    Connection conn = ConnectionFactory.createConnection(conf);
    Table meta = conn.getTable(TableName.META_TABLE_NAME);
    Put put = MetaTableAccessor.makePutFromRegionInfo(hri, EnvironmentEdgeManager.currentTime());
    if (numReplicas > 1) {
      Random r = new Random();
      ServerName[] serversArr = servers.toArray(new ServerName[servers.size()]);
      for (int i = 1; i < numReplicas; i++) {
        ServerName sn = serversArr[r.nextInt(serversArr.length)];
        // the column added here is just to make sure the master is able to
        // see the additional replicas when it is asked to assign. The
        // final value of these columns will be different and will be updated
        // by the actual regionservers that start hosting the respective replicas
        MetaTableAccessor.addLocation(put, sn, sn.getStartcode(), i);
      }
    }
    meta.put(put);
    meta.close();
    conn.close();
  }

  /**
   * Creates, flushes, and closes a new region.
   */
  public static HRegion createHDFSRegionDir(Configuration conf,
      RegionInfo hri, TableDescriptor htd) throws IOException {
    // Create HRegion
    Path root = FSUtils.getRootDir(conf);
    HRegion region = HRegion.createHRegion(hri, root, conf, htd, null);

    // Close the new region to flush to disk. Close log file too.
    region.close();
    return region;
  }

  /*
   * Remove parent
   */
  public static void removeParentInMeta(Configuration conf, RegionInfo hri) throws IOException {
    Connection conn = ConnectionFactory.createConnection(conf);
    MetaTableAccessor.deleteRegion(conn, hri);
  }
}
