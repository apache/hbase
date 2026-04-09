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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mob file compaction base test. 1. Enables batch mode for regular MOB compaction, Sets batch size
 * to 7 regions. (Optional) 2. Disables periodic MOB compactions, sets minimum age to archive to 10
 * sec 3. Creates MOB table with 20 regions 4. Loads MOB data (randomized keys, 1000 rows), flushes
 * data. 5. Repeats 4. two more times 6. Verifies that we have 20 *3 = 60 mob files (equals to
 * number of regions x 3) 7. Runs major MOB compaction. 8. Verifies that number of MOB files in a
 * mob directory is 20 x4 = 80 9. Waits for a period of time larger than minimum age to archive 10.
 * Runs Mob cleaner chore 11 Verifies that number of MOB files in a mob directory is 20. 12 Runs
 * scanner and checks all 3 * 1000 rows.
 */
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: useFileBasedSFT={0}")
public class TestMobCompactionWithDefaults {
  private static final Logger LOG = LoggerFactory.getLogger(TestMobCompactionWithDefaults.class);

  protected HBaseTestingUtil HTU;
  protected static Configuration conf;
  protected static long minAgeToArchive = 10000;

  protected final static String famStr = "f1";
  protected final static byte[] fam = Bytes.toBytes(famStr);
  protected final static byte[] qualifier = Bytes.toBytes("q1");
  protected final static long mobLen = 10;
  protected final static byte[] mobVal = Bytes
    .toBytes("01234567890123456789012345678901234567890123456789012345678901234567890123456789");

  private String testMethodName;
  protected TableDescriptor tableDescriptor;
  private ColumnFamilyDescriptor familyDescriptor;
  protected Admin admin;
  protected TableName table = null;
  protected int numRegions = 20;
  protected int rows = 1000;

  protected Boolean useFileBasedSFT;

  public TestMobCompactionWithDefaults(Boolean useFileBasedSFT) {
    this.useFileBasedSFT = useFileBasedSFT;
  }

  public static Stream<Arguments> parameters() {
    Boolean[] data = { false, true };
    return Arrays.asList(data).stream().map(Arguments::of);
  }

  protected void htuStart() throws Exception {
    HTU = new HBaseTestingUtil();
    conf = HTU.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    // Disable automatic MOB compaction
    conf.setLong(MobConstants.MOB_COMPACTION_CHORE_PERIOD, 0);
    // Disable automatic MOB file cleaner chore
    conf.setLong(MobConstants.MOB_CLEANER_PERIOD, 0);
    // Set minimum age to archive to 10 sec
    conf.setLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, minAgeToArchive);
    // Set compacted file discharger interval to a half minAgeToArchive
    conf.setLong("hbase.hfile.compaction.discharger.interval", minAgeToArchive / 2);
    conf.setBoolean("hbase.regionserver.compaction.enabled", false);
    if (useFileBasedSFT) {
      conf.set(StoreFileTrackerFactory.TRACKER_IMPL,
        "org.apache.hadoop.hbase.regionserver.storefiletracker.FileBasedStoreFileTracker");
    }
    additonalConfigSetup();
    HTU.startMiniCluster();
  }

  protected void additonalConfigSetup() {
  }

  @BeforeEach
  public void setUp(TestInfo testInfo) throws Exception {
    testMethodName = testInfo.getTestMethod().get().getName();
    htuStart();
    admin = HTU.getAdmin();
    familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(fam).setMobEnabled(true)
      .setMobThreshold(mobLen).setMaxVersions(1).build();
    tableDescriptor = HTU.createModifyableTableDescriptor(TestMobUtils.getTableName(testMethodName))
      .setColumnFamily(familyDescriptor).build();
    RegionSplitter.UniformSplit splitAlgo = new RegionSplitter.UniformSplit();
    byte[][] splitKeys = splitAlgo.split(numRegions);
    table = HTU.createTable(tableDescriptor, splitKeys).getName();
  }

  private void loadData(TableName tableName, int num) {
    LOG.info("Started loading {} rows into {}", num, tableName);
    try (final Table table = HTU.getConnection().getTable(tableName)) {
      for (int i = 0; i < num; i++) {
        byte[] key = new byte[32];
        Bytes.random(key);
        Put p = new Put(key);
        p.addColumn(fam, qualifier, mobVal);
        table.put(p);
      }
      admin.flush(tableName);
      LOG.info("Finished loading {} rows into {}", num, tableName);
    } catch (Exception e) {
      LOG.error("MOB file compaction chore test FAILED", e);
      fail("MOB file compaction chore test FAILED");
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    admin.disableTable(tableDescriptor.getTableName());
    admin.deleteTable(tableDescriptor.getTableName());
    HTU.shutdownMiniCluster();
  }

  @TestTemplate
  public void baseTestMobFileCompaction() throws InterruptedException, IOException {
    LOG.info("MOB compaction " + description() + " started");
    loadAndFlushThreeTimes(rows, table, famStr);
    mobCompact(tableDescriptor, familyDescriptor);
    assertEquals(numRegions * 4, getNumberOfMobFiles(table, famStr),
      "Should have 4 MOB files per region due to 3xflush + compaction.");
    cleanupAndVerifyCounts(table, famStr, 3 * rows);
    LOG.info("MOB compaction " + description() + " finished OK");
  }

  @TestTemplate
  public void testMobFileCompactionAfterSnapshotClone() throws InterruptedException, IOException {
    final TableName clone = TableName.valueOf(TestMobUtils.getTableName(testMethodName) + "-clone");
    LOG.info("MOB compaction of cloned snapshot, " + description() + " started");
    loadAndFlushThreeTimes(rows, table, famStr);
    LOG.debug("Taking snapshot and cloning table {}", table);
    admin.snapshot(TestMobUtils.getTableName(testMethodName), table);
    admin.cloneSnapshot(TestMobUtils.getTableName(testMethodName), clone);
    assertEquals(3 * numRegions, getNumberOfMobFiles(clone, famStr),
      "Should have 3 hlinks per region in MOB area from snapshot clone");
    mobCompact(admin.getDescriptor(clone), familyDescriptor);
    assertEquals(4 * numRegions, getNumberOfMobFiles(clone, famStr),
      "Should have 3 hlinks + 1 MOB file per region due to clone + compact");
    cleanupAndVerifyCounts(clone, famStr, 3 * rows);
    LOG.info("MOB compaction of cloned snapshot, " + description() + " finished OK");
  }

  @TestTemplate
  public void testMobFileCompactionAfterSnapshotCloneAndFlush()
    throws InterruptedException, IOException {
    final TableName clone = TableName.valueOf(TestMobUtils.getTableName(testMethodName) + "-clone");
    LOG.info("MOB compaction of cloned snapshot after flush, " + description() + " started");
    loadAndFlushThreeTimes(rows, table, famStr);
    LOG.debug("Taking snapshot and cloning table {}", table);
    admin.snapshot(TestMobUtils.getTableName(testMethodName), table);
    admin.cloneSnapshot(TestMobUtils.getTableName(testMethodName), clone);
    assertEquals(3 * numRegions, getNumberOfMobFiles(clone, famStr),
      "Should have 3 hlinks per region in MOB area from snapshot clone");
    loadAndFlushThreeTimes(rows, clone, famStr);
    mobCompact(admin.getDescriptor(clone), familyDescriptor);
    assertEquals(7 * numRegions, getNumberOfMobFiles(clone, famStr),
      "Should have 7 MOB file per region due to clone + 3xflush + compact");
    cleanupAndVerifyCounts(clone, famStr, 6 * rows);
    LOG.info("MOB compaction of cloned snapshot w flush, " + description() + " finished OK");
  }

  protected void loadAndFlushThreeTimes(int rows, TableName table, String family)
    throws IOException {
    final long start = getNumberOfMobFiles(table, family);
    // Load and flush data 3 times
    loadData(table, rows);
    loadData(table, rows);
    loadData(table, rows);
    assertEquals(start + numRegions * 3, getNumberOfMobFiles(table, family),
      "Should have 3 more mob files per region from flushing.");
  }

  protected String description() {
    return "regular mode";
  }

  protected void enableCompactions() throws IOException {
    final List<String> serverList =
      admin.getRegionServers().stream().map(sn -> sn.getServerName()).collect(Collectors.toList());
    admin.compactionSwitch(true, serverList);
  }

  protected void disableCompactions() throws IOException {
    final List<String> serverList =
      admin.getRegionServers().stream().map(sn -> sn.getServerName()).collect(Collectors.toList());
    admin.compactionSwitch(false, serverList);
  }

  /**
   * compact the given table and return once it is done. should presume compactions are disabled
   * when called. should ensure compactions are disabled before returning.
   */
  protected void mobCompact(TableDescriptor tableDescriptor,
    ColumnFamilyDescriptor familyDescriptor) throws IOException, InterruptedException {
    LOG.debug("Major compact MOB table " + tableDescriptor.getTableName());
    enableCompactions();
    mobCompactImpl(tableDescriptor, familyDescriptor);
    waitUntilCompactionIsComplete(tableDescriptor.getTableName());
    disableCompactions();
  }

  /**
   * Call the API for compaction specific to the test set. should not wait for compactions to
   * finish. may assume compactions are enabled when called.
   */
  protected void mobCompactImpl(TableDescriptor tableDescriptor,
    ColumnFamilyDescriptor familyDescriptor) throws IOException, InterruptedException {
    admin.majorCompact(tableDescriptor.getTableName(), familyDescriptor.getName());
  }

  protected void waitUntilCompactionIsComplete(TableName table)
    throws IOException, InterruptedException {
    CompactionState state = admin.getCompactionState(table);
    while (state != CompactionState.NONE) {
      LOG.debug("Waiting for compaction on {} to complete. current state {}", table, state);
      Thread.sleep(100);
      state = admin.getCompactionState(table);
    }
    LOG.debug("done waiting for compaction on {}", table);
  }

  protected void cleanupAndVerifyCounts(TableName table, String family, int rows)
    throws InterruptedException, IOException {
    // We have guarantee, that compacted file discharger will run during this pause
    // because it has interval less than this wait time
    LOG.info("Waiting for {}ms", minAgeToArchive + 1000);

    Thread.sleep(minAgeToArchive + 1000);
    LOG.info("Cleaning up MOB files");

    // run cleaner chore on each RS
    for (ServerName sn : admin.getRegionServers()) {
      HTU.getMiniHBaseCluster().getRegionServer(sn).getRSMobFileCleanerChore().chore();
    }

    assertEquals(numRegions, getNumberOfMobFiles(table, family),
      "After cleaning, we should have 1 MOB file per region based on size.");

    LOG.debug("checking count of rows");
    long scanned = scanTable(table);
    assertEquals(rows, scanned, "Got the wrong number of rows in table " + table + " cf " + family);

  }

  protected long getNumberOfMobFiles(TableName tableName, String family) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path dir = MobUtils.getMobFamilyPath(conf, tableName, family);
    FileStatus[] stat = fs.listStatus(dir);
    for (FileStatus st : stat) {
      LOG.debug("MOB Directory content: {}", st.getPath());
    }
    LOG.debug("MOB Directory content total files: {}", stat.length);

    return stat.length;
  }

  protected long scanTable(TableName tableName) {
    try (final Table table = HTU.getConnection().getTable(tableName);
      final ResultScanner scanner = table.getScanner(fam)) {
      Result result;
      long counter = 0;
      while ((result = scanner.next()) != null) {
        assertTrue(Arrays.equals(result.getValue(fam, qualifier), mobVal));
        counter++;
      }
      return counter;
    } catch (Exception e) {
      LOG.error("MOB file compaction test FAILED", e);
      if (HTU != null) {
        fail(e.getMessage());
      } else {
        System.exit(-1);
      }
    }
    return 0;
  }
}
