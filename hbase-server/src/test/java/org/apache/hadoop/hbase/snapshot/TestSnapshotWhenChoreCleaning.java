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

package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSVisitor;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test Case for HBASE-21387
 */
@Category({ MediumTests.class })
public class TestSnapshotWhenChoreCleaning {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotWhenChoreCleaning.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration CONF = TEST_UTIL.getConfiguration();
  private static final Logger LOG = LoggerFactory.getLogger(TestSnapshotClientRetries.class);
  private static final TableName TABLE_NAME = TableName.valueOf("testTable");
  private static final int MAX_SPLIT_KEYS_NUM = 100;
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] VALUE = Bytes.toBytes("value");
  private static Table TABLE;

  @Rule
  public TableNameTestRule testTable = new TableNameTestRule();

  @BeforeClass
  public static void setUp() throws Exception {
    // Set the hbase.snapshot.thread.pool.max to 1;
    CONF.setInt("hbase.snapshot.thread.pool.max", 1);
    // Enable snapshot
    CONF.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    // Start MiniCluster.
    TEST_UTIL.startMiniCluster(3);
    // Create talbe
    createTable();
  }

  private static byte[] integerToBytes(int i) {
    return Bytes.toBytes(String.format("%06d", i));
  }

  private static void createTable() throws IOException {
    byte[][] splitKeys = new byte[MAX_SPLIT_KEYS_NUM][];
    for (int i = 0; i < splitKeys.length; i++) {
      splitKeys[i] = integerToBytes(i);
    }
    TABLE = TEST_UTIL.createTable(TABLE_NAME, FAMILY, splitKeys);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private static void loadDataAndFlush() throws IOException {
    for (int i = 0; i < MAX_SPLIT_KEYS_NUM; i++) {
      Put put = new Put(integerToBytes(i)).addColumn(FAMILY, QUALIFIER,
        Bytes.add(VALUE, Bytes.toBytes(i)));
      TABLE.put(put);
    }
    TEST_UTIL.flush(TABLE_NAME);
  }

  private static List<Path> listHFileNames(final FileSystem fs, final Path tableDir)
      throws IOException {
    final List<Path> hfiles = new ArrayList<>();
    FSVisitor.visitTableStoreFiles(fs, tableDir, (region, family, hfileName) -> {
      hfiles.add(new Path(new Path(new Path(tableDir, region), family), hfileName));
    });
    Collections.sort(hfiles);
    return hfiles;
  }

  private static boolean isAnySnapshots(FileSystem fs) throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getSnapshotsDir(CommonFSUtils.getRootDir(CONF));
    FileStatus[] snapFiles = fs.listStatus(snapshotDir);
    if (snapFiles.length == 0) {
      return false;
    }
    Path firstPath = snapFiles[0].getPath();
    LOG.info("firstPath in isAnySnapshots: " + firstPath);
    if (snapFiles.length == 1 && firstPath.getName().equals(".tmp")) {
      FileStatus[] tmpSnapFiles = fs.listStatus(firstPath);
      return tmpSnapFiles != null && tmpSnapFiles.length > 0;
    }
    return true;
  }

  @Test
  public void testSnapshotWhenSnapshotHFileCleanerRunning() throws Exception {
    // Load data and flush to generate huge number of HFiles.
    loadDataAndFlush();

    SnapshotHFileCleaner cleaner = new SnapshotHFileCleaner();
    cleaner.init(ImmutableMap.of(HMaster.MASTER, TEST_UTIL.getHBaseCluster().getMaster()));
    cleaner.setConf(CONF);

    FileSystem fs = CommonFSUtils.getCurrentFileSystem(CONF);
    List<Path> fileNames =
        listHFileNames(fs, CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(CONF), TABLE_NAME));
    List<FileStatus> files = new ArrayList<>();
    for (Path fileName : fileNames) {
      files.add(fs.getFileStatus(fileName));
    }

    TEST_UTIL.getAdmin().snapshot("snapshotName_prev", TABLE_NAME);
    Assert.assertEquals(Lists.newArrayList(cleaner.getDeletableFiles(files)).size(), 0);
    TEST_UTIL.getAdmin().deleteSnapshot("snapshotName_prev");
    cleaner.getFileCacheForTesting().triggerCacheRefreshForTesting();
    Assert.assertEquals(Lists.newArrayList(cleaner.getDeletableFiles(files)).size(), 100);

    Runnable snapshotRunnable = () -> {
      try {
        // The thread will be busy on taking snapshot;
        for (int k = 0; k < 5; k++) {
          TEST_UTIL.getAdmin().snapshot("snapshotName_" + k, TABLE_NAME);
        }
      } catch (Exception e) {
        LOG.error("Snapshot failed: ", e);
      }
    };
    final AtomicBoolean success = new AtomicBoolean(true);
    Runnable cleanerRunnable = () -> {
      try {
        while (!isAnySnapshots(fs)) {
          LOG.info("Not found any snapshot, sleep 100ms");
          Thread.sleep(100);
        }
        for (int k = 0; k < 5; k++) {
          cleaner.getFileCacheForTesting().triggerCacheRefreshForTesting();
          Iterable<FileStatus> toDeleteFiles = cleaner.getDeletableFiles(files);
          List<FileStatus> deletableFiles = Lists.newArrayList(toDeleteFiles);
          LOG.info("Size of deletableFiles is: " + deletableFiles.size());
          for (int i = 0; i < deletableFiles.size(); i++) {
            LOG.debug("toDeleteFiles[{}] is: {}", i, deletableFiles.get(i));
          }
          if (deletableFiles.size() > 0) {
            success.set(false);
          }
        }
      } catch (Exception e) {
        LOG.error("Chore cleaning failed: ", e);
      }
    };
    Thread t1 = new Thread(snapshotRunnable);
    t1.start();
    Thread t2 = new Thread(cleanerRunnable);
    t2.start();
    t1.join();
    t2.join();
    Assert.assertTrue(success.get());
  }
}
