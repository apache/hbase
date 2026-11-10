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
package org.apache.hadoop.hbase.mob;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that the master side obsolete MOB file cleanup does not archive MOB files which are
 * named after a split or merge parent region but are still referenced by the daughter regions.
 * <p>
 * A non user compaction does not rewrite MOB cells, it only carries the MOB references into the new
 * store file. So after the parent region is garbage collected, the MOB files whose name suffix is
 * the parent region are still live.
 */
@Tag(MediumTests.TAG)
public class TestMobFileCleanupUtilAfterSplitOrMerge {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestMobFileCleanupUtilAfterSplitOrMerge.class);

  private static final HBaseTestingUtil HTU = new HBaseTestingUtil();

  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final long MOB_THRESHOLD = 10;
  private static final int ROWS = 20;
  private static final byte[] SPLIT_KEY = Bytes.toBytes(ROWS / 2);

  private static Configuration conf;
  private static Admin admin;

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    conf = HTU.getConfiguration();
    // Disable the automatic MOB compaction and MOB cleaner chores, the test runs them explicitly
    conf.setLong(MobConstants.MOB_COMPACTION_CHORE_PERIOD, 0);
    conf.setLong(MobConstants.MOB_CLEANER_PERIOD, 0);
    // Every MOB file which exists before the cleanup is old enough to be archived
    conf.setLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, 0);
    // Archived files are deleted as soon as the HFile cleaner runs
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    // Always open the MOB file from the file system when reading a MOB cell
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
    HTU.startMiniCluster();
    admin = HTU.getAdmin();
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testReferencedMobFilesOfSplitParentAreKept(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    try (Table table = createMobTable(tableName, null)) {
      loadAndFlush(table);
      List<HRegion> parents = HTU.getMiniHBaseCluster().getRegions(tableName);
      assertEquals(1, parents.size());
      RegionInfo parent = parents.get(0).getRegionInfo();
      Set<String> parentMobFiles = listMobFiles(tableName);
      assertMobFilesNamedAfter(parentMobFiles, parent);

      admin.splitRegionAsync(parent.getRegionName(), SPLIT_KEY).get(2, TimeUnit.MINUTES);
      await().atMost(Duration.ofMinutes(1)).untilAsserted(
        () -> assertEquals(2, HTU.getMiniHBaseCluster().getRegions(tableName).size()));

      verifyCleanupKeepsReferencedMobFiles(table, parentMobFiles, parent);
    }
  }

  @Test
  public void testReferencedMobFilesOfMergeParentsAreKept(TestInfo testInfo) throws Exception {
    TableName tableName = TableName.valueOf(testInfo.getTestMethod().get().getName());
    try (Table table = createMobTable(tableName, new byte[][] { SPLIT_KEY })) {
      loadAndFlush(table);
      List<HRegion> parents = HTU.getMiniHBaseCluster().getRegions(tableName);
      assertEquals(2, parents.size());
      RegionInfo parentA = parents.get(0).getRegionInfo();
      RegionInfo parentB = parents.get(1).getRegionInfo();
      Set<String> parentMobFiles = listMobFiles(tableName);
      assertMobFilesNamedAfter(parentMobFiles, parentA, parentB);

      admin
        .mergeRegionsAsync(
          new byte[][] { parentA.getEncodedNameAsBytes(), parentB.getEncodedNameAsBytes() }, false)
        .get(2, TimeUnit.MINUTES);
      await().atMost(Duration.ofMinutes(1)).untilAsserted(
        () -> assertEquals(1, HTU.getMiniHBaseCluster().getRegions(tableName).size()));

      verifyCleanupKeepsReferencedMobFiles(table, parentMobFiles, parentA, parentB);
    }
  }

  private void verifyCleanupKeepsReferencedMobFiles(Table table, Set<String> parentMobFiles,
    RegionInfo... parents) throws Exception {
    TableName tableName = table.getName();
    List<HRegion> children = HTU.getMiniHBaseCluster().getRegions(tableName);
    // Wait for the system compactions triggered on region open to replace the reference files
    for (HRegion child : children) {
      waitUntilNoReferences(child);
    }
    // The children did not rewrite the MOB cells, they still refer to the parent MOB files
    assertEquals(parentMobFiles, getReferencedMobFiles(children));
    assertEquals(parentMobFiles, listMobFiles(tableName));

    // Garbage collect the parent regions
    FileSystem fs = HTU.getTestFileSystem();
    Path tableDir = CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(conf), tableName);
    await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
      admin.runCatalogJanitor();
      ProcedureTestingUtility
        .waitNoProcedureRunning(HTU.getMiniHBaseCluster().getMaster().getMasterProcedureExecutor());
      for (RegionInfo parent : parents) {
        Path parentDir = new Path(tableDir, parent.getEncodedName());
        assertFalse(fs.exists(parentDir), "Parent region dir not cleaned: " + parentDir);
      }
    });

    MobFileCleanupUtil.cleanupObsoleteMobFiles(conf, tableName, admin);
    Set<String> mobFilesAfterCleanup = listMobFiles(tableName);
    // Remove whatever has been archived, then read all the rows back
    HTU.getMiniHBaseCluster().getMaster().getHFileCleaner().triggerCleanerNow().get();

    assertAll(
      () -> assertEquals(parentMobFiles, mobFilesAfterCleanup,
        "MOB files still referenced by the children were archived"),
      () -> assertEquals(ROWS, verifyRows(table)));
  }

  private Table createMobTable(TableName tableName, byte[][] splitKeys) throws IOException {
    ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
      .setMobEnabled(true).setMobThreshold(MOB_THRESHOLD).setMaxVersions(1).build();
    TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(cfd).build();
    return HTU.createTable(htd, splitKeys);
  }

  private static byte[] value(int row) {
    return Bytes.toBytes(String.format("mob-value-%050d", row));
  }

  private void loadAndFlush(Table table) throws IOException {
    List<Put> puts = new ArrayList<>(ROWS);
    for (int i = 0; i < ROWS; i++) {
      puts.add(new Put(Bytes.toBytes(i)).addColumn(FAMILY, QUALIFIER, value(i)));
    }
    table.put(puts);
    admin.flush(table.getName());
  }

  private int verifyRows(Table table) throws IOException {
    int count = 0;
    try (ResultScanner scanner = table.getScanner(FAMILY)) {
      for (Result result; (result = scanner.next()) != null;) {
        int row = Bytes.toInt(result.getRow());
        assertArrayEquals(value(row), result.getValue(FAMILY, QUALIFIER));
        count++;
      }
    }
    return count;
  }

  private Set<String> listMobFiles(TableName tableName) throws IOException {
    Path mobFamilyDir = MobUtils.getMobFamilyPath(conf, tableName, Bytes.toString(FAMILY));
    Set<String> names = new HashSet<>();
    for (FileStatus status : HTU.getTestFileSystem().listStatus(mobFamilyDir)) {
      names.add(status.getPath().getName());
    }
    LOG.info("MOB files of {}: {}", tableName, names);
    return names;
  }

  private static void assertMobFilesNamedAfter(Set<String> mobFiles, RegionInfo... regions) {
    Set<String> regionNames = new HashSet<>();
    for (RegionInfo region : regions) {
      regionNames.add(region.getEncodedName());
    }
    assertFalse(mobFiles.isEmpty());
    for (String mobFile : mobFiles) {
      assertTrue(regionNames.contains(MobFileName.create(mobFile).getRegionName()), mobFile);
    }
  }

  private static Set<String> getReferencedMobFiles(List<HRegion> regions) {
    Set<String> referenced = new HashSet<>();
    for (HRegion region : regions) {
      for (HStoreFile sf : region.getStore(FAMILY).getStorefiles()) {
        byte[] refs = sf.getMetadataValue(HStoreFile.MOB_FILE_REFS);
        assertNotNull(refs, "No MOB_FILE_REFS in " + sf);
        referenced.addAll(MobUtils.deserializeMobFileRefs(refs).build().values());
      }
    }
    LOG.info("MOB files referenced by {}: {}", regions, referenced);
    return referenced;
  }

  private void waitUntilNoReferences(HRegion region) {
    HStore store = region.getStore(FAMILY);
    await().atMost(Duration.ofMinutes(1)).until(() -> {
      if (store.getStoreEngine().getCompactor().isCompacting()) {
        return false;
      }
      store.closeAndArchiveCompactedFiles();
      if (!store.hasReferences()) {
        return true;
      }
      // The compaction requested on region open may have been skipped, request it again. This is
      // still a system compaction, which does not rewrite the MOB cells.
      region.compact(true);
      return false;
    });
  }
}
