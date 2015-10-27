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
package org.apache.hadoop.hbase.mob.compactions;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.security.Key;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMobCompactor {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = null;
  private TableName tableName;
  private static Connection conn;
  private BufferedMutator bufMut;
  private Table table;
  private static Admin admin;
  private HTableDescriptor desc;
  private HColumnDescriptor hcd1;
  private HColumnDescriptor hcd2;
  private static FileSystem fs;
  private static final String family1 = "family1";
  private static final String family2 = "family2";
  private static final String qf1 = "qualifier1";
  private static final String qf2 = "qualifier2";
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
    TEST_UTIL.getConfiguration()
      .setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, 5000);
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY,
      KeyProviderForTesting.class.getName());
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    TEST_UTIL.startMiniCluster(1);
    pool = createThreadPool(TEST_UTIL.getConfiguration());
    conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration(), pool);
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    pool.shutdown();
    conn.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  public void setUp(String tableNameAsString) throws IOException {
    tableName = TableName.valueOf(tableNameAsString);
    hcd1 = new HColumnDescriptor(family1);
    hcd1.setMobEnabled(true);
    hcd1.setMobThreshold(5);
    hcd2 = new HColumnDescriptor(family2);
    hcd2.setMobEnabled(true);
    hcd2.setMobThreshold(5);
    desc = new HTableDescriptor(tableName);
    desc.addFamily(hcd1);
    desc.addFamily(hcd2);
    admin.createTable(desc, getSplitKeys());
    table = conn.getTable(tableName);
    bufMut = conn.getBufferedMutator(tableName);
  }

  @Test(timeout = 300000)
  public void testMinorCompaction() throws Exception {
    resetConf();
    int mergeSize = 5000;
    // change the mob compaction merge size
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);

    // create a table with namespace
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("ns").build();
    String tableNameAsString = "ns:testMinorCompaction";
    admin.createNamespace(namespaceDescriptor);
    setUp(tableNameAsString);
    int count = 4;
    // generate mob files
    loadData(admin, bufMut, tableName, count, rowNumPerFile);
    int rowNumPerRegion = count * rowNumPerFile;

    assertEquals("Before deleting: mob rows count", regionNum * rowNumPerRegion,
      countMobRows(table));
    assertEquals("Before deleting: mob cells count", regionNum * cellNumPerRow * rowNumPerRegion,
      countMobCells(table));
    assertEquals("Before deleting: mob file count", regionNum * count,
      countFiles(tableName, true, family1));

    int largeFilesCount = countLargeFiles(mergeSize, tableName, family1);
    createDelFile(table, tableName, Bytes.toBytes(family1), Bytes.toBytes(qf1));

    assertEquals("Before compaction: mob rows count", regionNum * (rowNumPerRegion - delRowNum),
      countMobRows(table));
    assertEquals("Before compaction: mob cells count", regionNum
      * (cellNumPerRow * rowNumPerRegion - delCellNum), countMobCells(table));
    assertEquals("Before compaction: family1 mob file count", regionNum * count,
      countFiles(tableName, true, family1));
    assertEquals("Before compaction: family2 mob file count", regionNum * count,
      countFiles(tableName, true, family2));
    assertEquals("Before compaction: family1 del file count", regionNum,
      countFiles(tableName, false, family1));
    assertEquals("Before compaction: family2 del file count", regionNum,
      countFiles(tableName, false, family2));

    // do the mob file compaction
    MobCompactor compactor = new PartitionedMobCompactor(conf, fs, tableName, hcd1, pool);
    compactor.compact();

    assertEquals("After compaction: mob rows count", regionNum * (rowNumPerRegion - delRowNum),
      countMobRows(table));
    assertEquals("After compaction: mob cells count", regionNum
      * (cellNumPerRow * rowNumPerRegion - delCellNum), countMobCells(table));
    // After the compaction, the files smaller than the mob compaction merge size
    // is merge to one file
    assertEquals("After compaction: family1 mob file count", largeFilesCount + regionNum,
      countFiles(tableName, true, family1));
    assertEquals("After compaction: family2 mob file count", regionNum * count,
      countFiles(tableName, true, family2));
    assertEquals("After compaction: family1 del file count", regionNum,
      countFiles(tableName, false, family1));
    assertEquals("After compaction: family2 del file count", regionNum,
      countFiles(tableName, false, family2));
  }

  @Test(timeout = 300000)
  public void testCompactionWithHFileLink() throws IOException, InterruptedException {
    resetConf();
    String tableNameAsString = "testCompactionWithHFileLink";
    setUp(tableNameAsString);
    int count = 4;
    // generate mob files
    loadData(admin, bufMut, tableName, count, rowNumPerFile);
    int rowNumPerRegion = count * rowNumPerFile;

    long tid = System.currentTimeMillis();
    byte[] snapshotName1 = Bytes.toBytes("snaptb-" + tid);
    // take a snapshot
    admin.snapshot(snapshotName1, tableName);

    createDelFile(table, tableName, Bytes.toBytes(family1), Bytes.toBytes(qf1));

    assertEquals("Before compaction: mob rows count", regionNum * (rowNumPerRegion - delRowNum),
      countMobRows(table));
    assertEquals("Before compaction: mob cells count", regionNum
      * (cellNumPerRow * rowNumPerRegion - delCellNum), countMobCells(table));
    assertEquals("Before compaction: family1 mob file count", regionNum * count,
      countFiles(tableName, true, family1));
    assertEquals("Before compaction: family2 mob file count", regionNum * count,
      countFiles(tableName, true, family2));
    assertEquals("Before compaction: family1 del file count", regionNum,
      countFiles(tableName, false, family1));
    assertEquals("Before compaction: family2 del file count", regionNum,
      countFiles(tableName, false, family2));

    // do the mob compaction
    MobCompactor compactor = new PartitionedMobCompactor(conf, fs, tableName, hcd1, pool);
    compactor.compact();

    assertEquals("After first compaction: mob rows count", regionNum
      * (rowNumPerRegion - delRowNum), countMobRows(table));
    assertEquals("After first compaction: mob cells count", regionNum
      * (cellNumPerRow * rowNumPerRegion - delCellNum), countMobCells(table));
    assertEquals("After first compaction: family1 mob file count", regionNum,
      countFiles(tableName, true, family1));
    assertEquals("After first compaction: family2 mob file count", regionNum * count,
      countFiles(tableName, true, family2));
    assertEquals("After first compaction: family1 del file count", 0,
      countFiles(tableName, false, family1));
    assertEquals("After first compaction: family2 del file count", regionNum,
      countFiles(tableName, false, family2));
    assertEquals("After first compaction: family1 hfilelink count", 0, countHFileLinks(family1));
    assertEquals("After first compaction: family2 hfilelink count", 0, countHFileLinks(family2));

    admin.disableTable(tableName);
    // Restore from snapshot, the hfilelink will exist in mob dir
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);

    assertEquals("After restoring snapshot: mob rows count", regionNum * rowNumPerRegion,
      countMobRows(table));
    assertEquals("After restoring snapshot: mob cells count", regionNum * cellNumPerRow
      * rowNumPerRegion, countMobCells(table));
    assertEquals("After restoring snapshot: family1 mob file count", regionNum * count,
      countFiles(tableName, true, family1));
    assertEquals("After restoring snapshot: family2 mob file count", regionNum * count,
      countFiles(tableName, true, family2));
    assertEquals("After restoring snapshot: family1 del file count", 0,
      countFiles(tableName, false, family1));
    assertEquals("After restoring snapshot: family2 del file count", 0,
      countFiles(tableName, false, family2));
    assertEquals("After restoring snapshot: family1 hfilelink count", regionNum * count,
      countHFileLinks(family1));
    assertEquals("After restoring snapshot: family2 hfilelink count", 0, countHFileLinks(family2));

    compactor.compact();

    assertEquals("After second compaction: mob rows count", regionNum * rowNumPerRegion,
      countMobRows(table));
    assertEquals("After second compaction: mob cells count", regionNum * cellNumPerRow
      * rowNumPerRegion, countMobCells(table));
    assertEquals("After second compaction: family1 mob file count", regionNum,
      countFiles(tableName, true, family1));
    assertEquals("After second compaction: family2 mob file count", regionNum * count,
      countFiles(tableName, true, family2));
    assertEquals("After second compaction: family1 del file count", 0,
      countFiles(tableName, false, family1));
    assertEquals("After second compaction: family2 del file count", 0,
      countFiles(tableName, false, family2));
    assertEquals("After second compaction: family1 hfilelink count", 0, countHFileLinks(family1));
    assertEquals("After second compaction: family2 hfilelink count", 0, countHFileLinks(family2));
    assertRefFileNameEqual(family1);
  }

  @Test(timeout = 300000)
  public void testMajorCompactionFromAdmin() throws Exception {
    resetConf();
    int mergeSize = 5000;
    // change the mob compaction merge size
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);
    String tableNameAsString = "testMajorCompactionFromAdmin";
    SecureRandom rng = new SecureRandom();
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    rng.nextBytes(keyBytes);
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Key cfKey = new SecretKeySpec(keyBytes, algorithm);
    byte[] encryptionKey = EncryptionUtil.wrapKey(conf,
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()), cfKey);
    TableName tableName = TableName.valueOf(tableNameAsString);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor hcd1 = new HColumnDescriptor(family1);
    hcd1.setMobEnabled(true);
    hcd1.setMobThreshold(0);
    hcd1.setEncryptionType(algorithm);
    hcd1.setEncryptionKey(encryptionKey);
    HColumnDescriptor hcd2 = new HColumnDescriptor(family2);
    hcd2.setMobEnabled(true);
    hcd2.setMobThreshold(0);
    desc.addFamily(hcd1);
    desc.addFamily(hcd2);
    admin.createTable(desc, getSplitKeys());
    Table table = conn.getTable(tableName);
    BufferedMutator bufMut = conn.getBufferedMutator(tableName);
    int count = 4;
    // generate mob files
    loadData(admin, bufMut, tableName, count, rowNumPerFile);
    int rowNumPerRegion = count * rowNumPerFile;

    assertEquals("Before deleting: mob rows count", regionNum * rowNumPerRegion,
      countMobRows(table));
    assertEquals("Before deleting: mob cells count", regionNum * cellNumPerRow * rowNumPerRegion,
      countMobCells(table));
    assertEquals("Before deleting: mob file count", regionNum * count,
      countFiles(tableName, true, family1));

    createDelFile(table, tableName, Bytes.toBytes(family1), Bytes.toBytes(qf1));

    assertEquals("Before compaction: mob rows count", regionNum * (rowNumPerRegion - delRowNum),
      countMobRows(table));
    assertEquals("Before compaction: mob cells count", regionNum
      * (cellNumPerRow * rowNumPerRegion - delCellNum), countMobCells(table));
    assertEquals("Before compaction: family1 mob file count", regionNum * count,
      countFiles(tableName, true, family1));
    assertEquals("Before compaction: family2 mob file count", regionNum * count,
      countFiles(tableName, true, family2));
    assertEquals("Before compaction: family1 del file count", regionNum,
      countFiles(tableName, false, family1));
    assertEquals("Before compaction: family2 del file count", regionNum,
      countFiles(tableName, false, family2));

    // do the major mob compaction, it will force all files to compaction
    admin.majorCompact(tableName, hcd1.getName(), Admin.CompactType.MOB);

    waitUntilMobCompactionFinished(tableName);
    assertEquals("After compaction: mob rows count", regionNum * (rowNumPerRegion - delRowNum),
      countMobRows(table));
    assertEquals("After compaction: mob cells count", regionNum
      * (cellNumPerRow * rowNumPerRegion - delCellNum), countMobCells(table));
    assertEquals("After compaction: family1 mob file count", regionNum,
      countFiles(tableName, true, family1));
    assertEquals("After compaction: family2 mob file count", regionNum * count,
      countFiles(tableName, true, family2));
    assertEquals("After compaction: family1 del file count", 0,
      countFiles(tableName, false, family1));
    assertEquals("After compaction: family2 del file count", regionNum,
      countFiles(tableName, false, family2));
    Assert.assertTrue(verifyEncryption(tableName, family1));
    table.close();
  }

  @Test(timeout = 300000)
  public void testScannerOnBulkLoadRefHFiles() throws Exception {
    resetConf();
    setUp("testScannerOnBulkLoadRefHFiles");
    long ts = EnvironmentEdgeManager.currentTime();
    byte[] key0 = Bytes.toBytes("k0");
    byte[] key1 = Bytes.toBytes("k1");
    String value0 = "mobValue0";
    String value1 = "mobValue1";
    String newValue0 = "new";
    Put put0 = new Put(key0);
    put0.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), ts, Bytes.toBytes(value0));
    loadData(admin, bufMut, tableName, new Put[] { put0 });
    put0 = new Put(key0);
    put0.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), ts, Bytes.toBytes(newValue0));
    Put put1 = new Put(key1);
    put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), ts, Bytes.toBytes(value1));
    loadData(admin, bufMut, tableName, new Put[] { put0, put1 });
    // read the latest cell of key0.
    Get get = new Get(key0);
    Result result = table.get(get);
    Cell cell = result.getColumnLatestCell(hcd1.getName(), Bytes.toBytes(qf1));
    assertEquals("Before compaction: mob value of k0", newValue0,
      Bytes.toString(CellUtil.cloneValue(cell)));
    admin.majorCompact(tableName, hcd1.getName(), Admin.CompactType.MOB);
    waitUntilMobCompactionFinished(tableName);
    // read the latest cell of key0, the cell seqId in bulk loaded file is not reset in the
    // scanner. The cell that has "new" value is still visible.
    result = table.get(get);
    cell = result.getColumnLatestCell(hcd1.getName(), Bytes.toBytes(qf1));
    assertEquals("After compaction: mob value of k0", newValue0,
      Bytes.toString(CellUtil.cloneValue(cell)));
    // read the ref cell, not read further to the mob cell.
    get = new Get(key1);
    get.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(true));
    result = table.get(get);
    cell = result.getColumnLatestCell(hcd1.getName(), Bytes.toBytes(qf1));
    // the ref name is the new file
    Path mobFamilyPath =
      MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(), tableName, hcd1.getNameAsString());
    List<Path> paths = new ArrayList<Path>();
    if (fs.exists(mobFamilyPath)) {
      FileStatus[] files = fs.listStatus(mobFamilyPath);
      for (FileStatus file : files) {
        if (!StoreFileInfo.isDelFile(file.getPath())) {
          paths.add(file.getPath());
        }
      }
    }
    assertEquals("After compaction: number of mob files:", 1, paths.size());
    assertEquals("After compaction: mob file name:", MobUtils.getMobFileName(cell), paths.get(0)
      .getName());
  }

  @Test(timeout = 300000)
  public void testScannerAfterCompactions() throws Exception {
    resetConf();
    setUp("testScannerAfterCompactions");
    long ts = EnvironmentEdgeManager.currentTime();
    byte[] key0 = Bytes.toBytes("k0");
    byte[] key1 = Bytes.toBytes("k1");
    String value = "mobValue"; // larger than threshold
    String newValue = "new";
    Put put0 = new Put(key0);
    put0.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), ts, Bytes.toBytes(value));
    loadData(admin, bufMut, tableName, new Put[] { put0 });
    Put put1 = new Put(key1);
    put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), ts, Bytes.toBytes(value));
    loadData(admin, bufMut, tableName, new Put[] { put1 });
    put1 = new Put(key1);
    put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), ts, Bytes.toBytes(newValue));
    loadData(admin, bufMut, tableName, new Put[] { put1 }); // now two mob files
    admin.majorCompact(tableName);
    waitUntilCompactionFinished(tableName);
    admin.majorCompact(tableName, hcd1.getName(), Admin.CompactType.MOB);
    waitUntilMobCompactionFinished(tableName);
    // read the latest cell of key1.
    Get get = new Get(key1);
    Result result = table.get(get);
    Cell cell = result.getColumnLatestCell(hcd1.getName(), Bytes.toBytes(qf1));
    assertEquals("After compaction: mob value", "new", Bytes.toString(CellUtil.cloneValue(cell)));
  }

  private void waitUntilCompactionFinished(TableName tableName) throws IOException,
    InterruptedException {
    long finished = EnvironmentEdgeManager.currentTime() + 60000;
    CompactionState state = admin.getCompactionState(tableName);
    while (EnvironmentEdgeManager.currentTime() < finished) {
      if (state == CompactionState.NONE) {
        break;
      }
      state = admin.getCompactionState(tableName);
      Thread.sleep(10);
    }
    assertEquals(CompactionState.NONE, state);
  }

  private void waitUntilMobCompactionFinished(TableName tableName) throws IOException,
    InterruptedException {
    long finished = EnvironmentEdgeManager.currentTime() + 60000;
    CompactionState state = admin.getCompactionState(tableName, Admin.CompactType.MOB);
    while (EnvironmentEdgeManager.currentTime() < finished) {
      if (state == CompactionState.NONE) {
        break;
      }
      state = admin.getCompactionState(tableName, Admin.CompactType.MOB);
      Thread.sleep(10);
    }
    assertEquals(CompactionState.NONE, state);
  }

  /**
   * Gets the number of rows in the given table.
   * @param table to get the  scanner
   * @return the number of rows
   */
  private int countMobRows(final Table table) throws IOException {
    Scan scan = new Scan();
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    return TEST_UTIL.countRows(table, scan);
  }

  /**
   * Gets the number of cells in the given table.
   * @param table to get the  scanner
   * @return the number of cells
   */
  private int countMobCells(final Table table) throws IOException {
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
  private int countFiles(TableName tableName, boolean isMobFile, String familyName)
    throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableName, familyName);
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

  private boolean verifyEncryption(TableName tableName, String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableName, familyName);
    boolean hasFiles = false;
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = fs.listStatus(mobDirPath);
      hasFiles = files != null && files.length > 0;
      Assert.assertTrue(hasFiles);
      Path path = files[0].getPath();
      CacheConfig cacheConf = new CacheConfig(conf);
      StoreFile sf = new StoreFile(TEST_UTIL.getTestFileSystem(), path, conf, cacheConf,
        BloomType.NONE);
      HFile.Reader reader = sf.createReader().getHFileReader();
      byte[] encryptionKey = reader.getTrailer().getEncryptionKey();
      Assert.assertTrue(null != encryptionKey);
      Assert.assertTrue(reader.getFileContext().getEncryptionContext().getCipher().getName()
        .equals(HConstants.CIPHER_AES));
    }
    return hasFiles;
  }

  /**
   * Gets the number of HFileLink in the mob path.
   * @param familyName the family name
   * @return the number of the HFileLink
   */
  private int countHFileLinks(String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableName, familyName);
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
   * @param tableName the current table name
   * @param familyName the family name
   * @return the number of files large than the size
   */
  private int countLargeFiles(int size, TableName tableName, String familyName) throws IOException {
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableName, familyName);
    int count = 0;
    if (fs.exists(mobDirPath)) {
      FileStatus[] files = fs.listStatus(mobDirPath);
      for (FileStatus file : files) {
        // ignore the del files in the mob path
        if ((!StoreFileInfo.isDelFile(file.getPath())) && (file.getLen() > size)) {
          count++;
        }
      }
    }
    return count;
  }

  /**
   * loads some data to the table.
   */
  private void loadData(Admin admin, BufferedMutator table, TableName tableName, int fileNum,
    int rowNumPerFile) throws IOException, InterruptedException {
    if (fileNum <= 0) {
      throw new IllegalArgumentException();
    }
    for (int i = 0; i < fileNum * rowNumPerFile; i++) {
      for (byte k0 : KEYS) {
        byte[] k = new byte[] { k0 };
        byte[] key = Bytes.add(k, Bytes.toBytes(i));
        byte[] mobVal = makeDummyData(10 * (i + 1));
        Put put = new Put(key);
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), mobVal);
        put.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf2), mobVal);
        put.addColumn(Bytes.toBytes(family2), Bytes.toBytes(qf1), mobVal);
        table.mutate(put);
      }
      if ((i + 1) % rowNumPerFile == 0) {
        table.flush();
        admin.flush(tableName);
      }
    }
  }

  private void loadData(Admin admin, BufferedMutator table, TableName tableName, Put[] puts)
    throws IOException {
    table.mutate(Arrays.asList(puts));
    table.flush();
    admin.flush(tableName);
  }

  /**
   * delete the row, family and cell to create the del file
   */
  private void createDelFile(Table table, TableName tableName, byte[] family, byte[] qf)
    throws IOException, InterruptedException {
    for (byte k0 : KEYS) {
      byte[] k = new byte[] { k0 };
      // delete a family
      byte[] key1 = Bytes.add(k, Bytes.toBytes(0));
      Delete delete1 = new Delete(key1);
      delete1.addFamily(family);
      table.delete(delete1);
      // delete one row
      byte[] key2 = Bytes.add(k, Bytes.toBytes(2));
      Delete delete2 = new Delete(key2);
      table.delete(delete2);
      // delete one cell
      byte[] key3 = Bytes.add(k, Bytes.toBytes(4));
      Delete delete3 = new Delete(key3);
      delete3.addColumn(family, qf);
      table.delete(delete3);
    }
    admin.flush(tableName);
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(tableName);
    for (HRegion region : regions) {
      region.waitForFlushesAndCompactions();
      region.compact(true);
    }
  }
  /**
   * Creates the dummy data with a specific size.
   * @param size the size of value
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
  private byte[][] getSplitKeys() {
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

  private void assertRefFileNameEqual(String familyName) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes(familyName));
    // Do not retrieve the mob data when scanning
    scan.setAttribute(MobConstants.MOB_SCAN_RAW, Bytes.toBytes(Boolean.TRUE));
    ResultScanner results = table.getScanner(scan);
    Path mobFamilyPath = MobUtils.getMobFamilyPath(TEST_UTIL.getConfiguration(),
        tableName, familyName);
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

  /**
   * Resets the configuration.
   */
  private void resetConf() {
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD,
      MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD);
    conf.setInt(MobConstants.MOB_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE);
  }
}
