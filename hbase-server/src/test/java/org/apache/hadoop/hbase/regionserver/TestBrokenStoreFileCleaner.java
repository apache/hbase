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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, RegionServerTests.class })
public class TestBrokenStoreFileCleaner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBrokenStoreFileCleaner.class);

  private final HBaseTestingUtil testUtil = new HBaseTestingUtil();
  private final static byte[] fam = Bytes.toBytes("cf_1");
  private final static byte[] qual1 = Bytes.toBytes("qf_1");
  private final static byte[] val = Bytes.toBytes("val");
  private final static String junkFileName = "409fad9a751c4e8c86d7f32581bdc156";
  TableName tableName;

  @Before
  public void setUp() throws Exception {
    testUtil.getConfiguration().set(StoreFileTrackerFactory.TRACKER_IMPL,
      "org.apache.hadoop.hbase.regionserver.storefiletracker.FileBasedStoreFileTracker");
    testUtil.getConfiguration().set(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_ENABLED,
      "true");
    testUtil.getConfiguration().set(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_TTL, "0");
    testUtil.getConfiguration().set(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_PERIOD,
      "15000000");
    testUtil.getConfiguration().set(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_DELAY, "0");
    testUtil.startMiniCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testDeletingJunkFile() throws Exception {
    tableName = TableName.valueOf(getClass().getSimpleName() + "testDeletingJunkFile");
    createTableWithData(tableName);

    HRegion region = testUtil.getMiniHBaseCluster().getRegions(tableName).get(0);
    ServerName sn = testUtil.getMiniHBaseCluster().getServerHoldingRegion(tableName,
      region.getRegionInfo().getRegionName());
    HRegionServer rs = testUtil.getMiniHBaseCluster().getRegionServer(sn);
    BrokenStoreFileCleaner cleaner = rs.getBrokenStoreFileCleaner();

    // create junk file
    HStore store = region.getStore(fam);
    Path cfPath = store.getRegionFileSystem().getStoreDir(store.getColumnFamilyName());
    Path junkFilePath = new Path(cfPath, junkFileName);

    FSDataOutputStream junkFileOS = store.getFileSystem().create(junkFilePath);
    junkFileOS.writeUTF("hello");
    junkFileOS.close();

    int storeFiles = store.getStorefilesCount();
    assertTrue(storeFiles > 0);

    // verify the file exist before the chore and missing afterwards
    assertTrue(store.getFileSystem().exists(junkFilePath));
    cleaner.chore();
    assertFalse(store.getFileSystem().exists(junkFilePath));

    // verify no storefile got deleted
    int currentStoreFiles = store.getStorefilesCount();
    assertEquals(currentStoreFiles, storeFiles);
  }

  @Test
  public void testSkippingCompactedFiles() throws Exception {
    tableName = TableName.valueOf(getClass().getSimpleName() + "testSkippningCompactedFiles");
    createTableWithData(tableName);

    HRegion region = testUtil.getMiniHBaseCluster().getRegions(tableName).get(0);

    ServerName sn = testUtil.getMiniHBaseCluster().getServerHoldingRegion(tableName,
      region.getRegionInfo().getRegionName());
    HRegionServer rs = testUtil.getMiniHBaseCluster().getRegionServer(sn);
    BrokenStoreFileCleaner cleaner = rs.getBrokenStoreFileCleaner();

    // run major compaction to generate compaced files
    region.compact(true);

    // make sure there are compacted files
    HStore store = region.getStore(fam);
    int compactedFiles = store.getCompactedFilesCount();
    assertTrue(compactedFiles > 0);

    cleaner.chore();

    // verify none of the compacted files were deleted
    int existingCompactedFiles = store.getCompactedFilesCount();
    assertEquals(compactedFiles, existingCompactedFiles);

    // verify adding a junk file does not break anything
    Path cfPath = store.getRegionFileSystem().getStoreDir(store.getColumnFamilyName());
    Path junkFilePath = new Path(cfPath, junkFileName);

    FSDataOutputStream junkFileOS = store.getFileSystem().create(junkFilePath);
    junkFileOS.writeUTF("hello");
    junkFileOS.close();

    assertTrue(store.getFileSystem().exists(junkFilePath));
    cleaner.setEnabled(true);
    cleaner.chore();
    assertFalse(store.getFileSystem().exists(junkFilePath));

    // verify compacted files are still intact
    existingCompactedFiles = store.getCompactedFilesCount();
    assertEquals(compactedFiles, existingCompactedFiles);
  }

  @Test
  public void testJunkFileTTL() throws Exception {
    tableName = TableName.valueOf(getClass().getSimpleName() + "testDeletingJunkFile");
    createTableWithData(tableName);

    HRegion region = testUtil.getMiniHBaseCluster().getRegions(tableName).get(0);
    ServerName sn = testUtil.getMiniHBaseCluster().getServerHoldingRegion(tableName,
      region.getRegionInfo().getRegionName());
    HRegionServer rs = testUtil.getMiniHBaseCluster().getRegionServer(sn);

    // create junk file
    HStore store = region.getStore(fam);
    Path cfPath = store.getRegionFileSystem().getStoreDir(store.getColumnFamilyName());
    Path junkFilePath = new Path(cfPath, junkFileName);

    FSDataOutputStream junkFileOS = store.getFileSystem().create(junkFilePath);
    junkFileOS.writeUTF("hello");
    junkFileOS.close();

    int storeFiles = store.getStorefilesCount();
    assertTrue(storeFiles > 0);

    // verify the file exist before the chore
    assertTrue(store.getFileSystem().exists(junkFilePath));

    // set a 5 sec ttl
    rs.getConfiguration().set(BrokenStoreFileCleaner.BROKEN_STOREFILE_CLEANER_TTL, "5000");
    BrokenStoreFileCleaner cleaner =
      new BrokenStoreFileCleaner(15000000, 0, rs, rs.getConfiguration(), rs);
    cleaner.chore();
    // file is still present after chore run
    assertTrue(store.getFileSystem().exists(junkFilePath));
    Thread.sleep(5000);
    cleaner.chore();
    assertFalse(store.getFileSystem().exists(junkFilePath));

    // verify no storefile got deleted
    int currentStoreFiles = store.getStorefilesCount();
    assertEquals(currentStoreFiles, storeFiles);
  }

  @Test
  public void testWhenRegionIsClosing() throws Exception {
    tableName = TableName.valueOf(getClass().getSimpleName() + "testWhenRegionIsClosing");
    createTableWithData(tableName);

    HRegion region = testUtil.getMiniHBaseCluster().getRegions(tableName).get(0);
    ServerName sn = testUtil.getMiniHBaseCluster().getServerHoldingRegion(tableName,
      region.getRegionInfo().getRegionName());
    HRegionServer rs = testUtil.getMiniHBaseCluster().getRegionServer(sn);

    HStore store = region.getStore(fam);
    int expectedStoreFiles = store.getStorefilesCount();
    assertTrue(expectedStoreFiles > 0);
    Path cfPath = store.getRegionFileSystem().getStoreDir(store.getColumnFamilyName());
    // because we use FILE SFT, there will be a .filelist dir under the store dir
    int totalFiles = store.getRegionFileSystem().getFileSystem().listStatus(cfPath).length - 1;
    assertEquals(expectedStoreFiles, totalFiles);

    HRegionServer mockedServer = mock(HRegionServer.class);
    HRegion mockedRegion = mock(HRegion.class);
    when(mockedRegion.isAvailable()).thenReturn(region.isAvailable());
    when(mockedRegion.getRegionFileSystem()).thenReturn(region.getRegionFileSystem());
    List<HRegion> mockedRegionsList = new ArrayList<>();
    mockedRegionsList.add(mockedRegion);
    when(mockedServer.getRegions()).thenReturn(mockedRegionsList);
    when(mockedServer.getServerName()).thenReturn(rs.getServerName());
    when(mockedRegion.getStores()).thenAnswer(i -> {
      region.close();
      return region.getStores();
    });

    BrokenStoreFileCleaner cleaner =
      new BrokenStoreFileCleaner(15000000, 0, rs, rs.getConfiguration(), mockedServer);

    cleaner.chore();

    // verify no storefile got deleted
    int currentStoreFiles =
      store.getRegionFileSystem().getFileSystem().listStatus(cfPath).length - 1;
    assertEquals(expectedStoreFiles, currentStoreFiles);
  }

  private Table createTableWithData(TableName tableName) throws IOException {
    Table table = testUtil.createTable(tableName, fam);
    try {
      for (int i = 1; i < 10; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(fam, qual1, val);
        table.put(p);
      }
      // flush them
      testUtil.getAdmin().flush(tableName);
      for (int i = 11; i < 20; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(fam, qual1, val);
        table.put(p);
      }
      // flush them
      testUtil.getAdmin().flush(tableName);
      for (int i = 21; i < 30; i++) {
        Put p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(fam, qual1, val);
        table.put(p);
      }
      // flush them
      testUtil.getAdmin().flush(tableName);
    } catch (IOException e) {
      table.close();
      throw e;
    }
    return table;
  }
}
