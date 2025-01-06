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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mob file cleaner chore test. 1. Creates MOB table 2. Load MOB data and flushes it N times 3. Runs
 * major MOB compaction 4. Verifies that number of MOB files in a mob directory is N+1 5. Waits for
 * a period of time larger than minimum age to archive 6. Runs Mob cleaner chore 7 Verifies that
 * every old MOB file referenced from current RS was archived
 */
@Category(MediumTests.class)
public class TestRSMobFileCleanerChore {
  private static final Logger LOG = LoggerFactory.getLogger(TestRSMobFileCleanerChore.class);
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRSMobFileCleanerChore.class);

  private HBaseTestingUtility HTU;

  private final static String famStr = "f1";
  private final static byte[] fam = Bytes.toBytes(famStr);
  private final static byte[] qualifier = Bytes.toBytes("q1");
  private final static long mobLen = 10;
  private final static byte[] mobVal = Bytes
    .toBytes("01234567890123456789012345678901234567890123456789012345678901234567890123456789");

  private Configuration conf;
  private TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor;
  private ColumnFamilyDescriptor familyDescriptor;
  private Admin admin;
  private Table table = null;
  private RSMobFileCleanerChore chore;
  private long minAgeToArchive = 10000;

  public TestRSMobFileCleanerChore() {
  }

  @Before
  public void setUp() throws Exception {
    HTU = new HBaseTestingUtility();
    conf = HTU.getConfiguration();

    initConf();

    HTU.startMiniCluster();
    admin = HTU.getAdmin();
    familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(fam).setMobEnabled(true)
      .setMobThreshold(mobLen).setMaxVersions(1).build();
    tableDescriptor =
      HTU.createModifyableTableDescriptor("testMobCompactTable").setColumnFamily(familyDescriptor);
    table = HTU.createTable(tableDescriptor, Bytes.toByteArrays("1"));
  }

  private void initConf() {

    conf.setInt("hfile.format.version", 3);
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    conf.setInt("hbase.client.retries.number", 100);
    conf.setInt("hbase.hregion.max.filesize", 200000000);
    conf.setInt("hbase.hregion.memstore.flush.size", 800000);
    conf.setInt("hbase.hstore.blockingStoreFiles", 150);
    conf.setInt("hbase.hstore.compaction.throughput.lower.bound", 52428800);
    conf.setInt("hbase.hstore.compaction.throughput.higher.bound", 2 * 52428800);
    // conf.set(MobStoreEngine.DEFAULT_MOB_COMPACTOR_CLASS_KEY,
    // FaultyMobStoreCompactor.class.getName());
    // Disable automatic MOB compaction
    conf.setLong(MobConstants.MOB_COMPACTION_CHORE_PERIOD, 0);
    // Disable automatic MOB file cleaner chore
    conf.setLong(MobConstants.MOB_CLEANER_PERIOD, 0);
    // Set minimum age to archive to 10 sec
    conf.setLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, minAgeToArchive);
    // Set compacted file discharger interval to a half minAgeToArchive
    conf.setLong("hbase.hfile.compaction.discharger.interval", minAgeToArchive / 2);
  }

  private void loadData(int start, int num) {
    try {

      for (int i = 0; i < num; i++) {
        Put p = new Put(Bytes.toBytes(start + i));
        p.addColumn(fam, qualifier, mobVal);
        table.put(p);
      }
      admin.flush(table.getName());
    } catch (Exception e) {
      LOG.error("MOB file cleaner chore test FAILED", e);
      assertTrue(false);
    }
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(tableDescriptor.getTableName());
    admin.deleteTable(tableDescriptor.getTableName());
    HTU.shutdownMiniCluster();
  }

  @Test
  public void testMobFileCleanerChore() throws InterruptedException, IOException {
    loadData(0, 10);
    loadData(10, 10);
    // loadData(20, 10);
    long num = getNumberOfMobFiles(conf, table.getName(), new String(fam));
    assertEquals(2, num);
    // Major compact
    admin.majorCompact(tableDescriptor.getTableName(), fam);
    // wait until compaction is complete
    while (admin.getCompactionState(tableDescriptor.getTableName()) != CompactionState.NONE) {
      Thread.sleep(100);
    }

    num = getNumberOfMobFiles(conf, table.getName(), new String(fam));
    assertEquals(3, num);
    // We have guarantee, that compcated file discharger will run during this pause
    // because it has interval less than this wait time
    LOG.info("Waiting for {}ms", minAgeToArchive + 1000);

    Thread.sleep(minAgeToArchive + 1000);
    LOG.info("Cleaning up MOB files");

    ServerName serverUsed = null;
    List<RegionInfo> serverRegions = null;
    for (ServerName sn : admin.getRegionServers()) {
      serverRegions = admin.getRegions(sn);
      if (serverRegions != null && serverRegions.size() > 0) {
        // filtering out non test table regions
        serverRegions = serverRegions.stream().filter(r -> r.getTable() == table.getName())
          .collect(Collectors.toList());
        // if such one is found use this rs
        if (serverRegions.size() > 0) {
          serverUsed = sn;
        }
        break;
      }
    }

    chore = HTU.getMiniHBaseCluster().getRegionServer(serverUsed).getRSMobFileCleanerChore();

    chore.chore();

    num = getNumberOfMobFiles(conf, table.getName(), new String(fam));
    assertEquals(3 - serverRegions.size(), num);

    long scanned = scanTable();
    assertEquals(20, scanned);

    // creating a MOB file not referenced from the current RS
    Path extraMOBFile = MobTestUtil.generateMOBFileForRegion(conf, table.getName(),
      familyDescriptor, "nonExistentRegion");

    // verifying the new MOBfile is added
    num = getNumberOfMobFiles(conf, table.getName(), new String(fam));
    assertEquals(4 - serverRegions.size(), num);

    FileSystem fs = FileSystem.get(conf);
    assertTrue(fs.exists(extraMOBFile));

    LOG.info("Waiting for {}ms", minAgeToArchive + 1000);

    Thread.sleep(minAgeToArchive + 1000);
    LOG.info("Cleaning up MOB files");

    // running chore again
    chore.chore();

    // the chore should only archive old MOB files that were referenced from the current RS
    // the unrelated MOB file is still there
    num = getNumberOfMobFiles(conf, table.getName(), new String(fam));
    assertEquals(4 - serverRegions.size(), num);

    assertTrue(fs.exists(extraMOBFile));

    scanned = scanTable();
    assertEquals(20, scanned);
  }

  private long getNumberOfMobFiles(Configuration conf, TableName tableName, String family)
    throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Path dir = MobUtils.getMobFamilyPath(conf, tableName, family);
    FileStatus[] stat = fs.listStatus(dir);
    for (FileStatus st : stat) {
      LOG.debug("DDDD MOB Directory content: {} size={}", st.getPath(), st.getLen());
    }
    LOG.debug("MOB Directory content total files: {}", stat.length);

    return stat.length;
  }

  private long scanTable() {
    try {

      Result result;
      ResultScanner scanner = table.getScanner(fam);
      long counter = 0;
      while ((result = scanner.next()) != null) {
        assertTrue(Arrays.equals(result.getValue(fam, qualifier), mobVal));
        counter++;
      }
      return counter;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("MOB file cleaner chore test FAILED");
      if (HTU != null) {
        assertTrue(false);
      } else {
        System.exit(-1);
      }
    }
    return 0;
  }
}
