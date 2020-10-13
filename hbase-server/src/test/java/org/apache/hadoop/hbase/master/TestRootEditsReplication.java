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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocateType;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.region.RootStore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, MediumTests.class })
public class TestRootEditsReplication {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRootEditsReplication.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static byte[] CF = Bytes.toBytes("cf");

  private static TableDescriptor TD1 = TableDescriptorBuilder.newBuilder(TableName.valueOf("a"))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

  private static TableDescriptor TD2 = TableDescriptorBuilder.newBuilder(TableName.valueOf("b"))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

  @BeforeClass
  public static void setUp() throws Exception {
    // set a very large interval to actually disable full sync
    UTIL.getConfiguration().setInt(MetaLocationCache.SYNC_INTERVAL_SECONDS, Integer.MAX_VALUE);
    // enable root edits replication
    UTIL.getConfiguration().setBoolean(RootStore.REPLICATE_ROOT_EDITS, true);
    StartMiniClusterOption option = StartMiniClusterOption.builder().numMasters(1)
      .numAlwaysStandByMasters(1).numRegionServers(3).build();
    UTIL.startMiniCluster(option);
    // enable meta replica
    HBaseTestingUtility.setReplicas(UTIL.getAdmin(), TableName.META_TABLE_NAME, 3);
    UTIL.getAdmin().createTable(TD1);
    UTIL.getAdmin().createTable(TD2);
    UTIL.waitTableAvailable(TD1.getTableName());
    UTIL.waitTableAvailable(TD2.getTableName());
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    UTIL.shutdownMiniCluster();
  }

  private void compactMeta() throws IOException {
    for (JVMClusterUtil.RegionServerThread t : UTIL.getMiniHBaseCluster()
      .getRegionServerThreads()) {
      for (HRegion r : t.getRegionServer().getOnlineRegionsLocalContext()) {
        if (TableName.isMetaTableName(r.getRegionInfo().getTable())) {
          r.compact(true);
          for (HStore store : r.getStores()) {
            store.closeAndArchiveCompactedFiles();
          }
        }
      }
    }
  }

  private void waitForMetaCount(AssignmentManager am, int count) {
    UTIL.waitFor(30000,
      () -> am.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME).size() == count);
  }

  private void waitUntilReplicated(MetaLocationCache cache, List<RegionInfo> metaRegions,
    List<ServerName> metaLocs) {
    UTIL.waitFor(10000, () -> {
      for (int i = 0; i < metaRegions.size(); i++) {
        RegionInfo region = metaRegions.get(i);
        RegionLocations locs = cache.locateMeta(region.getStartKey(), RegionLocateType.CURRENT);
        if (locs == null) {
          return false;
        }
        HRegionLocation loc = locs.getRegionLocation(region.getReplicaId());
        if (loc == null || !metaLocs.get(i).equals(loc.getServerName())) {
          return false;
        }
      }
      return true;
    });
  }

  private void waitAllCacheCount(MetaLocationCache cache, int size) {
    UTIL.waitFor(10000, () -> cache.getAllMetaRegionLocations(false).size() == size);
  }

  @Test
  public void testReplicate() throws Exception {
    HMaster active = UTIL.getHBaseCluster().getMaster();
    AssignmentManager activeAM = active.getAssignmentManager();
    waitForMetaCount(activeAM, 3);
    HMaster backup = UTIL.getHBaseCluster().getMasterThreads().stream().map(t -> t.getMaster())
      .filter(h -> h != active).findFirst().get();
    List<RegionInfo> metaRegions =
      activeAM.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME);
    List<ServerName> metaLocs = metaRegions.stream()
      .map(ri -> activeAM.getRegionStates().getRegionStateNode(ri).getRegionLocation())
      .collect(Collectors.toList());

    // make sure we have done the initial sync
    MetaLocationCache cache = backup.getMetaLocationCache();
    waitUntilReplicated(cache, metaRegions, metaLocs);
    waitAllCacheCount(cache, 3);

    // move meta region to a new location, check whether we replicate the edits to backup master
    RegionInfo meta = metaRegions.get(0);
    ServerName currentLoc = metaLocs.get(0);
    ServerName newLoc = UTIL.getAdmin().getRegionServers().stream()
      .filter(s -> !s.equals(currentLoc)).findAny().get();
    active.getAssignmentManager().moveAsync(new RegionPlan(meta, metaLocs.get(0), newLoc)).get();
    assertEquals(newLoc, activeAM.getRegionStates().getRegionStateNode(meta).getRegionLocation());
    UTIL.waitFor(10000, () -> {
      RegionLocations locs = cache.locateMeta(HConstants.EMPTY_START_ROW, RegionLocateType.CURRENT);
      if (locs == null) {
        return false;
      }
      HRegionLocation loc = locs.getRegionLocation(meta.getReplicaId());
      return loc != null && newLoc.equals(loc.getServerName());
    });

    Admin admin = UTIL.getAdmin();
    // turn off catalog janitor
    admin.catalogJanitorSwitch(false);
    // split meta and check whether we replicate the edits to backup master
    admin.split(TableName.META_TABLE_NAME, Bytes.toBytes("b"));
    List<RegionInfo> splitMetaRegions =
      activeAM.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME);
    waitForMetaCount(activeAM, 6);
    List<ServerName> splitMetaRegionLocs = splitMetaRegions.stream()
      .map(ri -> activeAM.getRegionStates().getRegionStateNode(ri).getRegionLocation())
      .collect(Collectors.toList());
    // we still have a split parent
    waitAllCacheCount(cache, 7);
    waitUntilReplicated(cache, splitMetaRegions, splitMetaRegionLocs);

    assertEquals(1, cache.getAllMetaRegionLocations(false).stream()
      .filter(hrl -> hrl.getRegion().isSplitParent()).count());

    compactMeta();
    // clean the split parent
    assertEquals(1, admin.runCatalogJanitor());

    // the split parent should have been purged
    waitAllCacheCount(cache, 6);
    assertFalse(cache.getAllMetaRegionLocations(false).stream()
      .anyMatch(hrl -> hrl.getRegion().isSplitParent()));

    // set replica count to 2
    HBaseTestingUtility.setReplicas(UTIL.getAdmin(), TableName.META_TABLE_NAME, 2);

    waitForMetaCount(activeAM, 4);
    waitAllCacheCount(cache, 4);
    // remove replica 2
    splitMetaRegions.remove(5);
    splitMetaRegions.remove(2);
    splitMetaRegionLocs.remove(5);
    splitMetaRegionLocs.remove(2);
    waitUntilReplicated(cache, splitMetaRegions.subList(0, splitMetaRegions.size() - 2),
      splitMetaRegionLocs.subList(0, splitMetaRegionLocs.size() - 2));

    // merge meta and check whether we replicate the edits to backup master
    admin.mergeRegionsAsync(splitMetaRegions.stream().filter(RegionReplicaUtil::isDefaultReplica)
      .map(RegionInfo::getRegionName).toArray(byte[][]::new), false).get();
    waitForMetaCount(activeAM, 2);

    List<RegionInfo> mergedMetaRegions =
      activeAM.getRegionStates().getRegionsOfTable(TableName.META_TABLE_NAME);
    List<ServerName> mergedMetaLocs = mergedMetaRegions.stream()
      .map(ri -> activeAM.getRegionStates().getRegionStateNode(ri).getRegionLocation())
      .collect(Collectors.toList());
    waitAllCacheCount(cache, 2);
    waitUntilReplicated(cache, mergedMetaRegions, mergedMetaLocs);

    compactMeta();
    // clean the merged qualifier
    assertEquals(1, admin.runCatalogJanitor());
    // the gc procedure is run in background so we need to wait here, can not check directly
    UTIL.waitFor(10000, () -> !activeAM.getRegionStateStore().hasMergeRegions(
      mergedMetaRegions.stream().filter(RegionReplicaUtil::isDefaultReplica).findFirst().get()));
    Thread.sleep(2000);
    // it should have no effect as we only deleted two merge qualifiers
    assertEquals(2, cache.getAllMetaRegionLocations(false).size());
    for (int i = 0; i < mergedMetaRegions.size(); i++) {
      RegionInfo region = mergedMetaRegions.get(i);
      RegionLocations locs = cache.locateMeta(region.getStartKey(), RegionLocateType.CURRENT);
      HRegionLocation loc = locs.getRegionLocation(region.getReplicaId());
      assertEquals(mergedMetaLocs.get(i), loc.getServerName());
    }
  }
}
