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
package org.apache.hadoop.hbase.mob.filecompactions;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMobFileCompactor {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private Configuration conf = null;
  private String tableNameAsString;
  private TableName tableName;
  private static HTable hTable;
  private static Admin admin;
  private static HTableDescriptor desc;
  private static HColumnDescriptor hcd1;
  private static HColumnDescriptor hcd2;
  private static FileSystem fs;
  private final static String family1 = "family1";
  private final static String family2 = "family2";
  private final static String qf1 = "qualifier1";
  private final static String qf2 = "qualifier2";
  private static byte[] KEYS = Bytes.toBytes("012");
  private static int regionNum = KEYS.length;
  private static int delRowNum = 1;
  private static int delCellNum = 6;
  private static int cellNumPerRow = 3;
  private static int rowNumPerFile = 2;
  private static ExecutorService pool;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.startMiniCluster(1);
    pool = createThreadPool(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    pool.shutdown();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    long tid = System.currentTimeMillis();
    tableNameAsString = "testMob" + tid;
    tableName = TableName.valueOf(tableNameAsString);
    hcd1 = new HColumnDescriptor(family1);
    hcd1.setMobEnabled(true);
    hcd1.setMobThreshold(0L);
    hcd1.setMaxVersions(4);
    hcd2 = new HColumnDescriptor(family2);
    hcd2.setMobEnabled(true);
    hcd2.setMobThreshold(0L);
    hcd2.setMaxVersions(4);
    desc = new HTableDescriptor(tableName);
    desc.addFamily(hcd1);
    desc.addFamily(hcd2);
    admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(desc, getSplitKeys());
    hTable = new HTable(conf, tableNameAsString);
    hTable.setAutoFlush(false, false);
  }

  @After
  public void tearDown() throws Exception {
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.close();
    hTable.close();
    fs.delete(TEST_UTIL.getDataTestDir(), true);
  }

  @Test
  public void testCompactionWithoutDelFiles() throws Exception {
    resetConf();
    int count = 4;
    // generate mob files
    loadData(count, rowNumPerFile);
    int rowNumPerRegion = count*rowNumPerFile;

    assertEquals("Before compaction: mob rows count", regionNum*rowNumPerRegion,
        countMobRows(hTable));
    assertEquals("Before compaction: mob file count", regionNum*count, countFiles(true, family1));
    assertEquals("Before compaction: del file count", 0, countFiles(false, family1));

    MobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs, tableName, hcd1, pool);
    compactor.compact();

    assertEquals("After compaction: mob rows count", regionNum*rowNumPerRegion,
        countMobRows(hTable));
    assertEquals("After compaction: mob file count", regionNum, countFiles(true, family1));
    assertEquals("After compaction: del file count", 0, countFiles(false, family1));
  }

  @Test
  public void testCompactionWithDelFiles() throws Exception {
    resetConf();
    int count = 4;
    // generate mob files
    loadData(count, rowNumPerFile);
    int rowNumPerRegion = count*rowNumPerFile;

    assertEquals("Before deleting: mob rows count", regionNum*rowNumPerRegion,
        countMobRows(hTable));
    assertEquals("Before deleting: mob cells count", regionNum*cellNumPerRow*rowNumPerRegion,
        countMobCells(hTable));
    assertEquals("Before deleting: family1 mob file count", regionNum*count,
        countFiles(true, family1));
    assertEquals("Before deleting: family2 mob file count", regionNum*count,
        countFiles(true, family2));

    createDelFile();

    assertEquals("Before compaction: mob rows count", regionNum*(rowNumPerRegion-delRowNum),
        countMobRows(hTable));
    assertEquals("Before compaction: mob cells count",
        regionNum*(cellNumPerRow*rowNumPerRegion-delCellNum), countMobCells(hTable));
    assertEquals("Before compaction: family1 mob file count", regionNum*count,
        countFiles(true, family1));
    assertEquals("Before compaction: family2 file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("Before compaction: family1 del file count", regionNum,
        countFiles(false, family1));
    assertEquals("Before compaction: family2 del file count", regionNum,
        countFiles(false, family2));

    // do the mob file compaction
    MobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs, tableName, hcd1, pool);
    compactor.compact();

    assertEquals("After compaction: mob rows count", regionNum*(rowNumPerRegion-delRowNum),
        countMobRows(hTable));
    assertEquals("After compaction: mob cells count",
        regionNum*(cellNumPerRow*rowNumPerRegion-delCellNum), countMobCells(hTable));
    assertEquals("After compaction: family1 mob file count", regionNum,
        countFiles(true, family1));
    assertEquals("After compaction: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("After compaction: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After compaction: family2 del file count", regionNum,
        countFiles(false, family2));
    assertRefFileNameEqual(family1);
  }

  private void assertRefFileNameEqual(String familyName) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(familyName));
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    ResultScanner results = hTable.getScanner(scan);
    Path mobFamilyPath = new Path(MobUtils.getMobRegionPath(TEST_UTIL.getConfiguration(),
        tableName), familyName);
    List<Path> actualFilePaths = new ArrayList<>();
    List<Path> expectFilePaths = new ArrayList<>();
    for (Result res : results) {
      for (Cell cell : res.listCells()) {
        byte[] referenceValue = CellUtil.cloneValue(cell);
        String fileName = Bytes.toString(referenceValue, Bytes.SIZEOF_INT,
            referenceValue.length - Bytes.SIZEOF_INT);
        Path targetPath = new Path(mobFamilyPath, fileName);
        if(!actualFilePaths.contains(targetPath)) {
          actualFilePaths.add(targetPath);
        }
      }
    }
    results.close();
    if (fs.exists(mobFamilyPath)) {
      FileStatus[] files = fs.listStatus(mobFamilyPath);
      for (FileStatus file : files) {
        if (!StoreFileInfo.isDelFile(file.getPath())) {
          expectFilePaths.add(file.getPath());
        }
      }
    }
    Collections.sort(actualFilePaths);
    Collections.sort(expectFilePaths);
    assertEquals(expectFilePaths, actualFilePaths);
  }

  @Test
  public void testCompactionWithDelFilesAndNotMergeAllFiles() throws Exception {
    resetConf();
    int mergeSize = 5000;
    // change the mob compaction merge size
    conf.setLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);

    int count = 4;
    // generate mob files
    loadData(count, rowNumPerFile);
    int rowNumPerRegion = count*rowNumPerFile;

    assertEquals("Before deleting: mob rows count", regionNum*rowNumPerRegion,
        countMobRows(hTable));
    assertEquals("Before deleting: mob cells count", regionNum*cellNumPerRow*rowNumPerRegion,
        countMobCells(hTable));
    assertEquals("Before deleting: mob file count", regionNum*count, countFiles(true, family1));

    int largeFilesCount = countLargeFiles(mergeSize, family1);
    createDelFile();

    assertEquals("Before compaction: mob rows count", regionNum*(rowNumPerRegion-delRowNum),
        countMobRows(hTable));
    assertEquals("Before compaction: mob cells count",
        regionNum*(cellNumPerRow*rowNumPerRegion-delCellNum), countMobCells(hTable));
    assertEquals("Before compaction: family1 mob file count", regionNum*count,
        countFiles(true, family1));
    assertEquals("Before compaction: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("Before compaction: family1 del file count", regionNum,
        countFiles(false, family1));
    assertEquals("Before compaction: family2 del file count", regionNum,
        countFiles(false, family2));

    // do the mob file compaction
    MobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs, tableName, hcd1, pool);
    compactor.compact();

    assertEquals("After compaction: mob rows count", regionNum*(rowNumPerRegion-delRowNum),
        countMobRows(hTable));
    assertEquals("After compaction: mob cells count",
        regionNum*(cellNumPerRow*rowNumPerRegion-delCellNum), countMobCells(hTable));
    // After the compaction, the files smaller than the mob compaction merge size
    // is merge to one file
    assertEquals("After compaction: family1 mob file count", largeFilesCount + regionNum,
        countFiles(true, family1));
    assertEquals("After compaction: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("After compaction: family1 del file count", regionNum,
        countFiles(false, family1));
    assertEquals("After compaction: family2 del file count", regionNum,
        countFiles(false, family2));
  }

  @Test
  public void testCompactionWithDelFilesAndWithSmallCompactionBatchSize() throws Exception {
    resetConf();
    int batchSize = 2;
    conf.setInt(MobConstants.MOB_FILE_COMPACTION_BATCH_SIZE, batchSize);
    int count = 4;
    // generate mob files
    loadData(count, rowNumPerFile);
    int rowNumPerRegion = count*rowNumPerFile;

    assertEquals("Before deleting: mob row count", regionNum*rowNumPerRegion,
        countMobRows(hTable));
    assertEquals("Before deleting: family1 mob file count", regionNum*count,
        countFiles(true, family1));
    assertEquals("Before deleting: family2 mob file count", regionNum*count,
        countFiles(true, family2));

    createDelFile();

    assertEquals("Before compaction: mob rows count", regionNum*(rowNumPerRegion-delRowNum),
        countMobRows(hTable));
    assertEquals("Before compaction: mob cells count",
        regionNum*(cellNumPerRow*rowNumPerRegion-delCellNum), countMobCells(hTable));
    assertEquals("Before compaction: family1 mob file count", regionNum*count,
        countFiles(true, family1));
    assertEquals("Before compaction: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("Before compaction: family1 del file count", regionNum,
        countFiles(false, family1));
    assertEquals("Before compaction: family2 del file count", regionNum,
        countFiles(false, family2));

    // do the mob file compaction
    MobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs, tableName, hcd1, pool);
    compactor.compact();

    assertEquals("After compaction: mob rows count", regionNum*(rowNumPerRegion-delRowNum),
        countMobRows(hTable));
    assertEquals("After compaction: mob cells count",
        regionNum*(cellNumPerRow*rowNumPerRegion-delCellNum), countMobCells(hTable));
    assertEquals("After compaction: family1 mob file count", regionNum*(count/batchSize),
        countFiles(true, family1));
    assertEquals("After compaction: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("After compaction: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After compaction: family2 del file count", regionNum,
        countFiles(false, family2));
  }

  @Test
  public void testCompactionWithHFileLink() throws IOException, InterruptedException {
    resetConf();
    int count = 4;
    // generate mob files
    loadData(count, rowNumPerFile);
    int rowNumPerRegion = count*rowNumPerFile;

    long tid = System.currentTimeMillis();
    byte[] snapshotName1 = Bytes.toBytes("snaptb-" + tid);
    // take a snapshot
    admin.snapshot(snapshotName1, tableName);

    createDelFile();

    assertEquals("Before compaction: mob rows count", regionNum*(rowNumPerRegion-delRowNum),
        countMobRows(hTable));
    assertEquals("Before compaction: mob cells count",
        regionNum*(cellNumPerRow*rowNumPerRegion-delCellNum), countMobCells(hTable));
    assertEquals("Before compaction: family1 mob file count", regionNum*count,
        countFiles(true, family1));
    assertEquals("Before compaction: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("Before compaction: family1 del file count", regionNum,
        countFiles(false, family1));
    assertEquals("Before compaction: family2 del file count", regionNum,
        countFiles(false, family2));

    // do the mob file compaction
    MobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs, tableName, hcd1, pool);
    compactor.compact();

    assertEquals("After first compaction: mob rows count", regionNum*(rowNumPerRegion-delRowNum),
        countMobRows(hTable));
    assertEquals("After first compaction: mob cells count",
        regionNum*(cellNumPerRow*rowNumPerRegion-delCellNum), countMobCells(hTable));
    assertEquals("After first compaction: family1 mob file count", regionNum,
        countFiles(true, family1));
    assertEquals("After first compaction: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("After first compaction: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After first compaction: family2 del file count", regionNum,
        countFiles(false, family2));
    assertEquals("After first compaction: family1 hfilelink count", 0, countHFileLinks(family1));
    assertEquals("After first compaction: family2 hfilelink count", 0, countHFileLinks(family2));

    admin.disableTable(tableName);
    // Restore from snapshot, the hfilelink will exist in mob dir
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);

    assertEquals("After restoring snapshot: mob rows count", regionNum*rowNumPerRegion,
        countMobRows(hTable));
    assertEquals("After restoring snapshot: mob cells count",
        regionNum*cellNumPerRow*rowNumPerRegion, countMobCells(hTable));
    assertEquals("After restoring snapshot: family1 mob file count", regionNum*count,
        countFiles(true, family1));
    assertEquals("After restoring snapshot: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("After restoring snapshot: family1 del file count", 0,
        countFiles(false, family1));
    assertEquals("After restoring snapshot: family2 del file count", 0,
        countFiles(false, family2));
    assertEquals("After restoring snapshot: family1 hfilelink count", regionNum*count,
        countHFileLinks(family1));
    assertEquals("After restoring snapshot: family2 hfilelink count", 0,
        countHFileLinks(family2));

    compactor.compact();

    assertEquals("After second compaction: mob rows count", regionNum*rowNumPerRegion,
        countMobRows(hTable));
    assertEquals("After second compaction: mob cells count",
        regionNum*cellNumPerRow*rowNumPerRegion, countMobCells(hTable));
    assertEquals("After second compaction: family1 mob file count", regionNum,
        countFiles(true, family1));
    assertEquals("After second compaction: family2 mob file count", regionNum*count,
        countFiles(true, family2));
    assertEquals("After second compaction: family1 del file count", 0, countFiles(false, family1));
    assertEquals("After second compaction: family2 del file count", 0, countFiles(false, family2));
    assertEquals("After second compaction: family1 hfilelink count", 0, countHFileLinks(family1));
    assertEquals("After second compaction: family2 hfilelink count", 0, countHFileLinks(family2));
  }

  /**
   * Gets the number of rows in the given table.
   * @param table to get the  scanner
   * @return the number of rows
   */
  private int countMobRows(final HTable table) throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count++;
    }
    results.close();
    return count;
  }

  /**
   * Gets the number of cells in the given table.
   * @param table to get the  scanner
   * @return the number of cells
   */
  private int countMobCells(final HTable table) throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      for (Cell cell : res.listCells()) {
        count++;
      }
    }
    results.close();
    return count;
  }

  /**
   * Gets the number of files in the mob path.
   * @param isMobFile gets number of the mob files or del files
   * @param familyName the family name
   * @return the number of the files
   */
  private int countFiles(boolean isMobFile, String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(
        MobUtils.getMobRegionPath(conf, tableName), familyName);
    int count = 0;
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = fs.listStatus(mobDirPath);
      for (FileStatus file : files) {
        if (isMobFile == true) {
          if (!StoreFileInfo.isDelFile(file.getPath())) {
            count++;
          }
        } else {
          if (StoreFileInfo.isDelFile(file.getPath())) {
            count++;
          }
        }
      }
    }
    return count;
  }

  /**
   * Gets the number of HFileLink in the mob path.
   * @param familyName the family name
   * @return the number of the HFileLink
   */
  private int countHFileLinks(String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(
        MobUtils.getMobRegionPath(conf, tableName), familyName);
    int count = 0;
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = fs.listStatus(mobDirPath);
      for (FileStatus file : files) {
        if (HFileLink.isHFileLink(file.getPath())) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * Gets the number of files.
   * @param size the size of the file
   * @param familyName the family name
   * @return the number of files large than the size
   */
  private int countLargeFiles(int size, String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(
        MobUtils.getMobRegionPath(conf, tableName), familyName);
    int count = 0;
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = fs.listStatus(mobDirPath);
      for (FileStatus file : files) {
        // ignore the del files in the mob path
        if ((!StoreFileInfo.isDelFile(file.getPath()))
            && (file.getLen() > size)) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * loads some data to the table.
   * @param count the mob file number
   */
  private void loadData(int fileNum, int rowNumPerFile) throws IOException,
      InterruptedException {
    if (fileNum <= 0) {
      throw new IllegalArgumentException();
    }
    for (byte k0 : KEYS) {
      byte[] k = new byte[] { k0 };
      for (int i = 0; i < fileNum * rowNumPerFile; i++) {
        byte[] key = Bytes.add(k, Bytes.toBytes(i));
        byte[] mobVal = makeDummyData(10 * (i + 1));
        Put put = new Put(key);
        put.setDurability(Durability.SKIP_WAL);
        put.add(Bytes.toBytes(family1), Bytes.toBytes(qf1), mobVal);
        put.add(Bytes.toBytes(family1), Bytes.toBytes(qf2), mobVal);
        put.add(Bytes.toBytes(family2), Bytes.toBytes(qf1), mobVal);
        hTable.put(put);
        if ((i + 1) % rowNumPerFile == 0) {
          hTable.flushCommits();
          admin.flush(tableName);
        }
      }
    }
  }

  /**
   * delete the row, family and cell to create the del file
   */
  private void createDelFile() throws IOException, InterruptedException {
    for (byte k0 : KEYS) {
      byte[] k = new byte[] { k0 };
      // delete a family
      byte[] key1 = Bytes.add(k, Bytes.toBytes(0));
      Delete delete1 = new Delete(key1);
      delete1.deleteFamily(Bytes.toBytes(family1));
      hTable.delete(delete1);
      // delete one row
      byte[] key2 = Bytes.add(k, Bytes.toBytes(2));
      Delete delete2 = new Delete(key2);
      hTable.delete(delete2);
      // delete one cell
      byte[] key3 = Bytes.add(k, Bytes.toBytes(4));
      Delete delete3 = new Delete(key3);
      delete3.deleteColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1));
      hTable.delete(delete3);
      hTable.flushCommits();
      admin.flush(tableName);
      List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(
          Bytes.toBytes(tableNameAsString));
      for (HRegion region : regions) {
        region.waitForFlushesAndCompactions();
        region.compactStores(true);
      }
    }
  }
  /**
   * Creates the dummy data with a specific size.
   * @param the size of data
   * @return the dummy data
   */
  private byte[] makeDummyData(int size) {
    byte[] dummyData = new byte[size];
    new Random().nextBytes(dummyData);
    return dummyData;
  }

  /**
   * Gets the split keys
   */
  public static byte[][] getSplitKeys() {
    byte[][] splitKeys = new byte[KEYS.length - 1][];
    for (int i = 0; i < splitKeys.length; ++i) {
      splitKeys[i] = new byte[] { KEYS[i + 1] };
    }
    return splitKeys;
  }

  private static ExecutorService createThreadPool(Configuration conf) {
    int maxThreads = 10;
    long keepAliveTime = 60;
    final SynchronousQueue<Runnable> queue = new SynchronousQueue<Runnable>();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads,
        keepAliveTime, TimeUnit.SECONDS, queue,
        Threads.newDaemonThreadFactory("MobFileCompactionChore"),
        new RejectedExecutionHandler() {
          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
              // waiting for a thread to pick up instead of throwing exceptions.
              queue.put(r);
            } catch (InterruptedException e) {
              throw new RejectedExecutionException(e);
            }
          }
        });
    ((ThreadPoolExecutor) pool).allowCoreThreadTimeOut(true);
    return pool;
  }

  /**
   * Resets the configuration.
   */
  private void resetConf() {
    conf.setLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD);
    conf.setInt(MobConstants.MOB_FILE_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_BATCH_SIZE);
  }
}
