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
package org.apache.hadoop.hbase.master.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

@Category({ MasterTests.class, MediumTests.class })
public class TestLocalRegionOnTwoFileSystems {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLocalRegionOnTwoFileSystems.class);

  private static final HBaseCommonTestingUtility HFILE_UTIL = new HBaseCommonTestingUtility();

  private static final HBaseTestingUtility WAL_UTIL = new HBaseTestingUtility();

  private static byte[] CF = Bytes.toBytes("f");

  private static byte[] CQ = Bytes.toBytes("q");

  private static String REGION_DIR_NAME = "local";

  private static TableDescriptor TD =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("test:local"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

  private static int COMPACT_MIN = 4;

  private LocalRegion region;

  @BeforeClass
  public static void setUp() throws Exception {
    WAL_UTIL.startMiniCluster(3);
    Configuration conf = HFILE_UTIL.getConfiguration();
    conf.setBoolean(MemStoreLAB.USEMSLAB_KEY, false);
    Path rootDir = HFILE_UTIL.getDataTestDir();
    CommonFSUtils.setRootDir(conf, rootDir);
    Path walRootDir = WAL_UTIL.getDataTestDirOnTestFS();
    FileSystem walFs = WAL_UTIL.getTestFileSystem();
    CommonFSUtils.setWALRootDir(conf,
      walRootDir.makeQualified(walFs.getUri(), walFs.getWorkingDirectory()));

  }

  @AfterClass
  public static void tearDown() throws IOException {
    WAL_UTIL.shutdownMiniDFSCluster();
    WAL_UTIL.cleanupTestDir();
    HFILE_UTIL.cleanupTestDir();
  }

  private LocalRegion createLocalRegion(ServerName serverName) throws IOException {
    Server server = mock(Server.class);
    when(server.getConfiguration()).thenReturn(HFILE_UTIL.getConfiguration());
    when(server.getServerName()).thenReturn(serverName);
    LocalRegionParams params = new LocalRegionParams();
    params.server(server).regionDirName(REGION_DIR_NAME).tableDescriptor(TD)
      .flushSize(TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE).flushPerChanges(1_000_000)
      .flushIntervalMs(TimeUnit.MINUTES.toMillis(15)).compactMin(COMPACT_MIN).maxWals(32)
      .useHsync(false).ringBufferSlotCount(16).rollPeriodMs(TimeUnit.MINUTES.toMillis(15))
      .archivedWalSuffix(LocalStore.ARCHIVED_WAL_SUFFIX)
      .archivedHFileSuffix(LocalStore.ARCHIVED_HFILE_SUFFIX);
    return LocalRegion.create(params);
  }

  @Before
  public void setUpBeforeTest() throws IOException {
    Path rootDir = HFILE_UTIL.getDataTestDir();
    FileSystem fs = rootDir.getFileSystem(HFILE_UTIL.getConfiguration());
    fs.delete(rootDir, true);
    Path walRootDir = WAL_UTIL.getDataTestDirOnTestFS();
    FileSystem walFs = WAL_UTIL.getTestFileSystem();
    walFs.delete(walRootDir, true);
    region = createLocalRegion(ServerName.valueOf("localhost", 12345, System.currentTimeMillis()));
  }

  @After
  public void tearDownAfterTest() {
    region.close(true);
  }

  private int getStorefilesCount() {
    return Iterables.getOnlyElement(region.region.getStores()).getStorefilesCount();
  }

  @Test
  public void testFlushAndCompact() throws Exception {
    for (int i = 0; i < COMPACT_MIN - 1; i++) {
      final int index = i;
      region
        .update(r -> r.put(new Put(Bytes.toBytes(index)).addColumn(CF, CQ, Bytes.toBytes(index))));
      region.flush(true);
    }
    region.requestRollAll();
    region.waitUntilWalRollFinished();
    region.update(r -> r.put(
      new Put(Bytes.toBytes(COMPACT_MIN - 1)).addColumn(CF, CQ, Bytes.toBytes(COMPACT_MIN - 1))));
    region.flusherAndCompactor.requestFlush();

    HFILE_UTIL.waitFor(15000, () -> getStorefilesCount() == 1);

    // make sure the archived hfiles are on the root fs
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePathForRootDir(
      HFILE_UTIL.getDataTestDir(), region.region.getRegionInfo(), CF);
    FileSystem rootFs = storeArchiveDir.getFileSystem(HFILE_UTIL.getConfiguration());
    HFILE_UTIL.waitFor(15000, () -> {
      try {
        FileStatus[] fses = rootFs.listStatus(storeArchiveDir);
        return fses != null && fses.length == COMPACT_MIN;
      } catch (FileNotFoundException e) {
        return false;
      }
    });

    // make sure the archived wal files are on the wal fs
    Path walArchiveDir = new Path(CommonFSUtils.getWALRootDir(HFILE_UTIL.getConfiguration()),
      HConstants.HREGION_OLDLOGDIR_NAME);
    HFILE_UTIL.waitFor(15000, () -> {
      try {
        FileStatus[] fses = WAL_UTIL.getTestFileSystem().listStatus(walArchiveDir);
        return fses != null && fses.length == 1;
      } catch (FileNotFoundException e) {
        return false;
      }
    });
  }

  @Test
  public void testRecovery() throws IOException {
    int countPerRound = 100;
    for (int round = 0; round < 5; round++) {
      for (int i = 0; i < countPerRound; i++) {
        int row = round * countPerRound + i;
        Put put = new Put(Bytes.toBytes(row)).addColumn(CF, CQ, Bytes.toBytes(row));
        region.update(r -> r.put(put));
      }
      region.close(true);
      region = createLocalRegion(
        ServerName.valueOf("localhost", 12345, System.currentTimeMillis() + round + 1));
      try (RegionScanner scanner = region.getScanner(new Scan())) {
        List<Cell> cells = new ArrayList<>();
        boolean moreValues = true;
        for (int i = 0; i < (round + 1) * countPerRound; i++) {
          assertTrue(moreValues);
          moreValues = scanner.next(cells);
          assertEquals(1, cells.size());
          Result result = Result.create(cells);
          cells.clear();
          assertEquals(i, Bytes.toInt(result.getRow()));
          assertEquals(i, Bytes.toInt(result.getValue(CF, CQ)));
        }
        assertFalse(moreValues);
      }
    }
  }
}
