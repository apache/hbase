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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.JVMClusterUtil;

public final class RegionReplicaTestHelper {

  private RegionReplicaTestHelper() {
  }

  // waits for all replicas to have region location
  static void waitUntilAllMetaReplicasAreReady(HBaseTestingUtility util,
    ConnectionRegistry registry) throws IOException {
    Configuration conf = util.getConfiguration();
    int regionReplicaCount =
      util.getAdmin().getDescriptor(TableName.META_TABLE_NAME).getRegionReplication();
    Waiter.waitFor(conf, conf.getLong("hbase.client.sync.wait.timeout.msec", 60000), 200, true,
      new ExplainingPredicate<IOException>() {
        @Override
        public String explainFailure() {
          return "Not all meta replicas get assigned";
        }

        @Override
        public boolean evaluate() {
          try {
            RegionLocations locs = registry.getMetaRegionLocations().get();
            if (locs.size() < regionReplicaCount) {
              return false;
            }
            for (int i = 0; i < regionReplicaCount; i++) {
              HRegionLocation loc = locs.getRegionLocation(i);
              // Wait until the replica is served by a region server. There could be delay between
              // the replica being available to the connection and region server opening it.
              Optional<ServerName> rsCarryingReplica =
                  getRSCarryingReplica(util, loc.getRegion().getTable(), i);
              if (!rsCarryingReplica.isPresent()) {
                return false;
              }
            }
            return true;
          } catch (Exception e) {
            TestZKConnectionRegistry.LOG.warn("Failed to get meta region locations", e);
            return false;
          }
        }
      });
  }

  static Optional<ServerName> getRSCarryingReplica(HBaseTestingUtility util, TableName tableName,
      int replicaId) {
    return util.getHBaseCluster().getRegionServerThreads().stream().map(t -> t.getRegionServer())
      .filter(rs -> rs.getRegions(tableName).stream()
        .anyMatch(r -> r.getRegionInfo().getReplicaId() == replicaId))
      .findAny().map(rs -> rs.getServerName());
  }

  /**
   * Return the new location.
   */
  static ServerName moveRegion(HBaseTestingUtility util, HRegionLocation currentLoc)
      throws Exception {
    ServerName serverName = currentLoc.getServerName();
    RegionInfo regionInfo = currentLoc.getRegion();
    TableName tableName = regionInfo.getTable();
    int replicaId = regionInfo.getReplicaId();
    ServerName newServerName = util.getHBaseCluster().getRegionServerThreads().stream()
      .map(t -> t.getRegionServer().getServerName()).filter(sn -> !sn.equals(serverName)).findAny()
      .get();
    util.getAdmin().move(regionInfo.getEncodedNameAsBytes(), newServerName);
    util.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        Optional<ServerName> newServerName = getRSCarryingReplica(util, tableName, replicaId);
        return newServerName.isPresent() && !newServerName.get().equals(serverName);
      }

      @Override
      public String explainFailure() throws Exception {
        return regionInfo.getRegionNameAsString() + " is still on " + serverName;
      }
    });
    return newServerName;
  }

  interface Locator {
    RegionLocations getRegionLocations(TableName tableName, int replicaId, boolean reload)
        throws Exception;

    void updateCachedLocationOnError(HRegionLocation loc, Throwable error) throws Exception;
  }

  static void testLocator(HBaseTestingUtility util, TableName tableName, Locator locator)
      throws Exception {
    RegionLocations locs =
      locator.getRegionLocations(tableName, RegionReplicaUtil.DEFAULT_REPLICA_ID, false);
    assertEquals(3, locs.size());
    for (int i = 0; i < 3; i++) {
      HRegionLocation loc = locs.getRegionLocation(i);
      assertNotNull(loc);
      ServerName serverName = getRSCarryingReplica(util, tableName, i).get();
      assertEquals(serverName, loc.getServerName());
    }
    ServerName newServerName = moveRegion(util, locs.getDefaultRegionLocation());
    // The cached location should not be changed
    assertEquals(locs.getDefaultRegionLocation().getServerName(),
      locator.getRegionLocations(tableName, RegionReplicaUtil.DEFAULT_REPLICA_ID, false)
        .getDefaultRegionLocation().getServerName());
    // should get the new location when reload = true
    // when meta replica LoadBalance mode is enabled, it may delay a bit.
    util.waitFor(3000, new ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        ServerName sn = locator.getRegionLocations(tableName, RegionReplicaUtil.DEFAULT_REPLICA_ID,
          true).getDefaultRegionLocation().getServerName();
        return newServerName.equals(sn);
      }

      @Override
      public String explainFailure() throws Exception {
        return "New location does not show up in meta (replica) region";
      }
    });

    assertEquals(newServerName,
      locator.getRegionLocations(tableName, RegionReplicaUtil.DEFAULT_REPLICA_ID, true)
        .getDefaultRegionLocation().getServerName());
    // the cached location should be replaced
    assertEquals(newServerName,
      locator.getRegionLocations(tableName, RegionReplicaUtil.DEFAULT_REPLICA_ID, false)
        .getDefaultRegionLocation().getServerName());

    ServerName newServerName1 = moveRegion(util, locs.getRegionLocation(1));
    ServerName newServerName2 = moveRegion(util, locs.getRegionLocation(2));

    // The cached location should not be change
    assertEquals(locs.getRegionLocation(1).getServerName(),
      locator.getRegionLocations(tableName, 1, false).getRegionLocation(1).getServerName());
    // clear the cached location for replica 1
    locator.updateCachedLocationOnError(locs.getRegionLocation(1), new NotServingRegionException());
    // the cached location for replica 2 should not be changed
    assertEquals(locs.getRegionLocation(2).getServerName(),
      locator.getRegionLocations(tableName, 2, false).getRegionLocation(2).getServerName());
    // should get the new location as we have cleared the old location
    assertEquals(newServerName1,
      locator.getRegionLocations(tableName, 1, false).getRegionLocation(1).getServerName());
    // as we will get the new location for replica 2 at once, we should also get the new location
    // for replica 2
    assertEquals(newServerName2,
      locator.getRegionLocations(tableName, 2, false).getRegionLocation(2).getServerName());
  }

  public static void assertReplicaDistributed(HBaseTestingUtility util, Table t)
    throws IOException {
    if (t.getDescriptor().getRegionReplication() <= 1) {
      return;
    }
    List<RegionInfo> regionInfos = new ArrayList<>();
    for (JVMClusterUtil.RegionServerThread rs : util.getMiniHBaseCluster()
      .getRegionServerThreads()) {
      regionInfos.clear();
      for (Region r : rs.getRegionServer().getRegions(t.getName())) {
        if (contains(regionInfos, r.getRegionInfo())) {
          fail("Replica regions should be assigned to different region servers");
        } else {
          regionInfos.add(r.getRegionInfo());
        }
      }
    }
  }

  private static boolean contains(List<RegionInfo> regionInfos, RegionInfo regionInfo) {
    for (RegionInfo info : regionInfos) {
      if (RegionReplicaUtil.isReplicasForSameRegion(info, regionInfo)) {
        return true;
      }
    }
    return false;
  }
}
