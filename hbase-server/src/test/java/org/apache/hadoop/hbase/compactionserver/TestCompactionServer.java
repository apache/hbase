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
package org.apache.hadoop.hbase.compactionserver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.compaction.CompactionOffloadManager;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.testclassification.CompactionServerTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Category({CompactionServerTests.class, MediumTests.class})
public class TestCompactionServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompactionServer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCompactionServer.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration CONF = TEST_UTIL.getConfiguration();
  private static HMaster MASTER;
  private static HCompactionServer COMPACTION_SERVER;
  private static ServerName COMPACTION_SERVER_NAME;
  private static TableName TABLENAME = TableName.valueOf("t");
  private static String FAMILY = "C";
  private static String COL ="c0";

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder().numCompactionServers(1).build());
    TEST_UTIL.getAdmin().switchCompactionOffload(true);
    MASTER = TEST_UTIL.getMiniHBaseCluster().getMaster();
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    COMPACTION_SERVER = TEST_UTIL.getMiniHBaseCluster().getCompactionServerThreads().get(0)
      .getCompactionServer();
    COMPACTION_SERVER_NAME = COMPACTION_SERVER.getServerName();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    TableDescriptor tableDescriptor =
        TableDescriptorBuilder.newBuilder(TABLENAME).setCompactionOffloadEnabled(true).build();
    TEST_UTIL.createTable(tableDescriptor, Bytes.toByteArrays(FAMILY),
      TEST_UTIL.getConfiguration());
    TEST_UTIL.waitTableAvailable(TABLENAME);
    COMPACTION_SERVER.requestCount.reset();
  }

  @After
  public void after() throws IOException {
    TEST_UTIL.deleteTableIfAny(TABLENAME);
  }

  private void doPutRecord(int start, int end, boolean flush) throws Exception {
    Table h = TEST_UTIL.getConnection().getTable(TABLENAME);
    for (int i = start; i <= end; i++) {
      Put p = new Put(Bytes.toBytes(i));
      p.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COL), Bytes.toBytes(i));
      h.put(p);
      if (i % 100 == 0 && flush) {
        TEST_UTIL.flush(TABLENAME);
      }
    }
    h.close();
  }

  private void doFillRecord(int start, int end, byte[] value) throws Exception {
    Table h = TEST_UTIL.getConnection().getTable(TABLENAME);
    for (int i = start; i <= end; i++) {
      Put p = new Put(Bytes.toBytes(i));
      p.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(COL), value);
      h.put(p);
    }
    h.close();
  }

  private void verifyRecord(int start, int end, boolean exist) throws Exception {
    Table h = TEST_UTIL.getConnection().getTable(TABLENAME);
    for (int i = start; i <= end; i++) {
      Get get = new Get(Bytes.toBytes(i));
      Result r = h.get(get);
      if (exist) {
        assertArrayEquals(Bytes.toBytes(i), r.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COL)));
      } else {
        assertNull(r.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(COL)));
      }
    }
    h.close();
  }

  @Test
  public void testCompaction() throws Exception {
    TEST_UTIL.getAdmin().compactionSwitch(false, new ArrayList<>());
    doPutRecord(1, 1000, true);
    int hFileCount = 0;
    for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLENAME)) {
      hFileCount += region.getStore(Bytes.toBytes(FAMILY)).getStorefilesCount();
    }
    assertEquals(10, hFileCount);
    TEST_UTIL.getAdmin().compactionSwitch(true, new ArrayList<>());
    TEST_UTIL.compact(TABLENAME, true);
    Thread.sleep(5000);
    TEST_UTIL.waitFor(60000,
      () -> COMPACTION_SERVER.requestCount.sum() > 0 && COMPACTION_SERVER.compactionThreadManager
          .getRunningCompactionTasks().values().size() == 0);
    hFileCount = 0;
    for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLENAME)) {
      hFileCount += region.getStore(Bytes.toBytes(FAMILY)).getStorefilesCount();
    }
    assertEquals(1, hFileCount);
    verifyRecord(1, 1000, true);
  }

  @Test
  public void testCompactionWithVersions() throws Exception {
    TEST_UTIL.getAdmin().compactionSwitch(false, new ArrayList<>());
    ColumnFamilyDescriptor cfd =
        ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(FAMILY)).setMaxVersions(3).build();
    TableDescriptor modifiedTableDescriptor = TableDescriptorBuilder.newBuilder(TABLENAME)
        .setColumnFamily(cfd).setCompactionOffloadEnabled(true).build();
    TEST_UTIL.getAdmin().modifyTable(modifiedTableDescriptor);
    TEST_UTIL.waitTableAvailable(TABLENAME);
    doFillRecord(1, 500, RandomUtils.nextBytes(20));
    doFillRecord(1, 500, RandomUtils.nextBytes(20));
    doFillRecord(1, 500, RandomUtils.nextBytes(20));
    TEST_UTIL.flush(TABLENAME);
    doPutRecord(1, 500, true);

    int kVCount = 0;
    for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLENAME)) {
      for (HStoreFile hStoreFile : region.getStore(Bytes.toBytes(FAMILY)).getStorefiles()) {
        kVCount += hStoreFile.getReader().getHFileReader().getTrailer().getEntryCount();
      }
    }
    assertEquals(2000, kVCount);
    TEST_UTIL.getAdmin().compactionSwitch(true, new ArrayList<>());
    TEST_UTIL.compact(TABLENAME, true);

    TEST_UTIL.waitFor(60000, () -> {
      int hFileCount = 0;
      for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLENAME)) {
        hFileCount += region.getStore(Bytes.toBytes(FAMILY)).getStorefilesCount();

      }
      return hFileCount == 1;
    });

    // To ensure do compaction on compaction server
    TEST_UTIL.waitFor(60000, () -> COMPACTION_SERVER.requestCount.sum() > 0);
    kVCount = 0;
    for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLENAME)) {
      for (HStoreFile hStoreFile : region.getStore(Bytes.toBytes(FAMILY)).getStorefiles()) {
        kVCount += hStoreFile.getReader().getHFileReader().getTrailer().getEntryCount();
      }
    }
    assertEquals(1500, kVCount);
    verifyRecord(1, 500, true);
  }

  @Test
  public void testCompactionServerDown() throws Exception {
    TEST_UTIL.getAdmin().compactionSwitch(false, new ArrayList<>());
    TEST_UTIL.getHBaseCluster().stopCompactionServer(0);
    TEST_UTIL.getHBaseCluster().waitOnCompactionServer(0);
    TEST_UTIL.waitFor(60000,
      () -> MASTER.getCompactionOffloadManager().getOnlineServersList().size() == 0);
    doPutRecord(1, 1000, true);
    int hFileCount = 0;
    for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLENAME)) {
      hFileCount += region.getStore(Bytes.toBytes(FAMILY)).getStorefilesCount();
    }
    assertEquals(10, hFileCount);
    TEST_UTIL.getAdmin().compactionSwitch(true, new ArrayList<>());
    TEST_UTIL.compact(TABLENAME, true);
    Thread.sleep(5000);
    TEST_UTIL.waitFor(60000, () -> {
      int hFile = 0;
      for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLENAME)) {
        hFile += region.getStore(Bytes.toBytes(FAMILY)).getStorefilesCount();
      }
      return hFile == 1;
    });
    verifyRecord(1, 1000, true);
    TEST_UTIL.getHBaseCluster().startCompactionServer();
    COMPACTION_SERVER = TEST_UTIL.getMiniHBaseCluster().getCompactionServerThreads().get(0)
      .getCompactionServer();
    COMPACTION_SERVER_NAME = COMPACTION_SERVER.getServerName();
    TEST_UTIL.waitFor(60000,
      () -> MASTER.getCompactionOffloadManager().getOnlineServersList().size() == 1);
  }

  @Test
  public void testCompactionServerReport() throws Exception {
    CompactionOffloadManager compactionOffloadManager = MASTER.getCompactionOffloadManager();
    TEST_UTIL.waitFor(60000, () -> !compactionOffloadManager.getOnlineServers().isEmpty()
        && null != compactionOffloadManager.getOnlineServers().get(COMPACTION_SERVER_NAME));
    // invoke compact
    TEST_UTIL.compact(TABLENAME, false);
    TEST_UTIL.waitFor(60000,
      () -> COMPACTION_SERVER.requestCount.sum() > 0
          && COMPACTION_SERVER.requestCount.sum() == compactionOffloadManager.getOnlineServers()
              .get(COMPACTION_SERVER_NAME).getTotalNumberOfRequests());
  }

  @Test
  public void testCompactionServerExpire() throws Exception {
    int initialNum = TEST_UTIL.getMiniHBaseCluster().getNumLiveCompactionServers();
    CONF.setInt(HConstants.COMPACTION_SERVER_PORT, HConstants.DEFAULT_COMPACTION_SERVER_PORT + 1);
    CONF.setInt(HConstants.COMPACTION_SERVER_INFO_PORT,
      HConstants.DEFAULT_COMPACTION_SERVER_INFOPORT + 1);
    HCompactionServer compactionServer = new HCompactionServer(CONF);
    compactionServer.start();
    ServerName compactionServerName = compactionServer.getServerName();

    CompactionOffloadManager compactionOffloadManager = MASTER.getCompactionOffloadManager();
    TEST_UTIL.waitFor(60000,
      () -> initialNum + 1 == compactionOffloadManager.getOnlineServersList().size()
          && null != compactionOffloadManager.getLoad(compactionServerName));

    compactionServer.stop("test");

    TEST_UTIL.waitFor(60000,
      () -> initialNum == compactionOffloadManager.getOnlineServersList().size());
    assertNull(compactionOffloadManager.getLoad(compactionServerName));
  }

  @Test
  public void testCompactionOffloadTableDescriptor() throws Exception {
    CompactionOffloadManager compactionOffloadManager = MASTER.getCompactionOffloadManager();
    TEST_UTIL.waitFor(6000, () -> !compactionOffloadManager.getOnlineServers().isEmpty()
        && null != compactionOffloadManager.getOnlineServers().get(COMPACTION_SERVER_NAME));

    TableDescriptor htd =
        TableDescriptorBuilder.newBuilder(TEST_UTIL.getAdmin().getDescriptor(TABLENAME))
            .setCompactionOffloadEnabled(false).build();
    TEST_UTIL.getAdmin().modifyTable(htd);
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLENAME);
    // invoke compact
    TEST_UTIL.compact(TABLENAME, false);
    TEST_UTIL.waitFor(6000, () -> COMPACTION_SERVER.requestCount.sum() == 0);

    htd = TableDescriptorBuilder.newBuilder(TEST_UTIL.getAdmin().getDescriptor(TABLENAME))
        .setCompactionOffloadEnabled(true).build();
    TEST_UTIL.getAdmin().modifyTable(htd);
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLENAME);
    // invoke compact
    TEST_UTIL.compact(TABLENAME, false);
    TEST_UTIL.waitFor(6000, () -> COMPACTION_SERVER.requestCount.sum() > 0);
  }
}
