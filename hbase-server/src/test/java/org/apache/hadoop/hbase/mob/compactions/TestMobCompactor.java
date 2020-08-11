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
package org.apache.hadoop.hbase.mob.compactions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.Key;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.CompactType;
import org.apache.hadoop.hbase.client.CompactionState;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
import org.apache.hadoop.hbase.io.crypto.aes.AES;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestMobCompactor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobCompactor.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMobCompactor.class);
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

  private static long tsFor20150907Monday;
  private static long tsFor20151120Sunday;
  private static long tsFor20151128Saturday;
  private static long tsFor20151130Monday;
  private static long tsFor20151201Tuesday;
  private static long tsFor20151205Saturday;
  private static long tsFor20151228Monday;
  private static long tsFor20151231Thursday;
  private static long tsFor20160101Friday;
  private static long tsFor20160103Sunday;

  private static final byte[] mobKey01 = Bytes.toBytes("r01");
  private static final byte[] mobKey02 = Bytes.toBytes("r02");
  private static final byte[] mobKey03 = Bytes.toBytes("r03");
  private static final byte[] mobKey04 = Bytes.toBytes("r04");
  private static final byte[] mobKey05 = Bytes.toBytes("r05");
  private static final byte[] mobKey06 = Bytes.toBytes("r05");
  private static final byte[] mobKey1 = Bytes.toBytes("r1");
  private static final byte[] mobKey2 = Bytes.toBytes("r2");
  private static final byte[] mobKey3 = Bytes.toBytes("r3");
  private static final byte[] mobKey4 = Bytes.toBytes("r4");
  private static final byte[] mobKey5 = Bytes.toBytes("r5");
  private static final byte[] mobKey6 = Bytes.toBytes("r6");
  private static final byte[] mobKey7 = Bytes.toBytes("r7");
  private static final byte[] mobKey8 = Bytes.toBytes("r8");
  private static final String mobValue0 = "mobValue00000000000000000000000000";
  private static final String mobValue1 = "mobValue00000111111111111111111111";
  private static final String mobValue2 = "mobValue00000222222222222222222222";
  private static final String mobValue3 = "mobValue00000333333333333333333333";
  private static final String mobValue4 = "mobValue00000444444444444444444444";
  private static final String mobValue5 = "mobValue00000666666666666666666666";
  private static final String mobValue6 = "mobValue00000777777777777777777777";
  private static final String mobValue7 = "mobValue00000888888888888888888888";
  private static final String mobValue8 = "mobValue00000888888888888888888899";

  private static byte[] KEYS = Bytes.toBytes("012");
  private static int regionNum = KEYS.length;
  private static int delRowNum = 1;
  private static int delCellNum = 6;
  private static int cellNumPerRow = 3;
  private static int rowNumPerFile = 2;
  private static ExecutorService pool;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, 5000);
    TEST_UTIL.getConfiguration()
        .set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, KeyProviderForTesting.class.getName());
    TEST_UTIL.getConfiguration().set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    TEST_UTIL.getConfiguration().setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.hfile.compaction.discharger.interval", 100);
    TEST_UTIL.getConfiguration().setBoolean("hbase.online.schema.update.enable", true);
    TEST_UTIL.startMiniCluster(1);
    pool = createThreadPool(TEST_UTIL.getConfiguration());
    conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration(), pool);
    fs = TEST_UTIL.getTestFileSystem();
    conf = TEST_UTIL.getConfiguration();
    admin = TEST_UTIL.getAdmin();

    // Initialize timestamps for these days
    Calendar calendar =  Calendar.getInstance();
    calendar.set(2015, 8, 7, 10, 20);
    tsFor20150907Monday = calendar.getTimeInMillis();

    calendar.set(2015, 10, 20, 10, 20);
    tsFor20151120Sunday = calendar.getTimeInMillis();

    calendar.set(2015, 10, 28, 10, 20);
    tsFor20151128Saturday = calendar.getTimeInMillis();

    calendar.set(2015, 10, 30, 10, 20);
    tsFor20151130Monday = calendar.getTimeInMillis();

    calendar.set(2015, 11, 1, 10, 20);
    tsFor20151201Tuesday = calendar.getTimeInMillis();

    calendar.set(2015, 11, 5, 10, 20);
    tsFor20151205Saturday = calendar.getTimeInMillis();

    calendar.set(2015, 11, 28, 10, 20);
    tsFor20151228Monday = calendar.getTimeInMillis();

    calendar.set(2015, 11, 31, 10, 20);
    tsFor20151231Thursday = calendar.getTimeInMillis();

    calendar.set(2016, 0, 1, 10, 20);
    tsFor20160101Friday = calendar.getTimeInMillis();

    calendar.set(2016, 0, 3, 10, 20);
    tsFor20160103Sunday = calendar.getTimeInMillis();
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

  // Set up for mob compaction policy testing
  private void setUpForPolicyTest(String tableNameAsString, MobCompactPartitionPolicy type)
      throws IOException {
    tableName = TableName.valueOf(tableNameAsString);
    hcd1 = new HColumnDescriptor(family1);
    hcd1.setMobEnabled(true);
    hcd1.setMobThreshold(10);
    hcd1.setMobCompactPartitionPolicy(type);
    desc = new HTableDescriptor(tableName);
    desc.addFamily(hcd1);
    admin.createTable(desc);
    table = conn.getTable(tableName);
    bufMut = conn.getBufferedMutator(tableName);
  }

  // alter mob compaction policy
  private void alterForPolicyTest(final MobCompactPartitionPolicy type)
      throws Exception {

    hcd1.setMobCompactPartitionPolicy(type);
    desc.modifyFamily(hcd1);
    admin.modifyTable(tableName, desc);
    Pair<Integer, Integer> st;

    while (null != (st = admin.getAlterStatus(tableName)) && st.getFirst() > 0) {
      LOG.debug(st.getFirst() + " regions left to update");
      Thread.sleep(40);
    }
    LOG.info("alter status finished");
  }

  @Test
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

  @Test
  public void testMinorCompactionWithWeeklyPolicy() throws Exception {
    resetConf();
    int mergeSize = 5000;
    // change the mob compaction merge size
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);

    commonPolicyTestLogic("testMinorCompactionWithWeeklyPolicy",
        MobCompactPartitionPolicy.WEEKLY, false, 6,
        new String[] { "20150907", "20151120", "20151128", "20151130", "20151205", "20160103" },
        true);
  }

  @Test
  public void testMajorCompactionWithWeeklyPolicy() throws Exception {
    resetConf();

    commonPolicyTestLogic("testMajorCompactionWithWeeklyPolicy",
        MobCompactPartitionPolicy.WEEKLY, true, 5,
        new String[] { "20150907", "20151120", "20151128", "20151205", "20160103" }, true);
  }

  @Test
  public void testMinorCompactionWithMonthlyPolicy() throws Exception {
    resetConf();
    int mergeSize = 5000;
    // change the mob compaction merge size
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);

    commonPolicyTestLogic("testMinorCompactionWithMonthlyPolicy",
        MobCompactPartitionPolicy.MONTHLY, false, 4,
        new String[] { "20150907", "20151130", "20151231", "20160103" }, true);
  }

  @Test
  public void testMajorCompactionWithMonthlyPolicy() throws Exception {
    resetConf();

    commonPolicyTestLogic("testMajorCompactionWithMonthlyPolicy",
        MobCompactPartitionPolicy.MONTHLY, true, 4,
        new String[] {"20150907", "20151130", "20151231", "20160103"}, true);
  }

  @Test
  public void testMajorCompactionWithWeeklyFollowedByMonthly() throws Exception {
    resetConf();

    commonPolicyTestLogic("testMajorCompactionWithWeeklyFollowedByMonthly",
        MobCompactPartitionPolicy.WEEKLY, true, 5,
        new String[] { "20150907", "20151120", "20151128", "20151205", "20160103" }, true);

    commonPolicyTestLogic("testMajorCompactionWithWeeklyFollowedByMonthly",
        MobCompactPartitionPolicy.MONTHLY, true, 4,
        new String[] {"20150907", "20151128", "20151205", "20160103" }, false);
  }

  @Test
  public void testMajorCompactionWithWeeklyFollowedByMonthlyFollowedByWeekly() throws Exception {
    resetConf();

    commonPolicyTestLogic("testMajorCompactionWithWeeklyFollowedByMonthlyFollowedByWeekly",
        MobCompactPartitionPolicy.WEEKLY, true, 5,
        new String[] { "20150907", "20151120", "20151128", "20151205", "20160103" }, true);

    commonPolicyTestLogic("testMajorCompactionWithWeeklyFollowedByMonthlyFollowedByWeekly",
        MobCompactPartitionPolicy.MONTHLY, true, 4,
        new String[] { "20150907", "20151128", "20151205", "20160103" }, false);

    commonPolicyTestLogic("testMajorCompactionWithWeeklyFollowedByMonthlyFollowedByWeekly",
        MobCompactPartitionPolicy.WEEKLY, true, 4,
        new String[] { "20150907", "20151128", "20151205", "20160103" }, false);
  }

  @Test
  public void testMajorCompactionWithWeeklyFollowedByMonthlyFollowedByDaily() throws Exception {
    resetConf();

    commonPolicyTestLogic("testMajorCompactionWithWeeklyFollowedByMonthlyFollowedByDaily",
        MobCompactPartitionPolicy.WEEKLY, true, 5,
        new String[] { "20150907", "20151120", "20151128", "20151205", "20160103" }, true);

    commonPolicyTestLogic("testMajorCompactionWithWeeklyFollowedByMonthlyFollowedByDaily",
        MobCompactPartitionPolicy.MONTHLY, true, 4,
        new String[] { "20150907", "20151128", "20151205", "20160103" }, false);

    commonPolicyTestLogic("testMajorCompactionWithWeeklyFollowedByMonthlyFollowedByDaily",
        MobCompactPartitionPolicy.DAILY, true, 4,
        new String[] { "20150907", "20151128", "20151205", "20160103" }, false);
  }

  @Test
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

  @Test
  public void testMajorCompactionFromAdmin() throws Exception {
    resetConf();
    int mergeSize = 5000;
    // change the mob compaction merge size
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);
    SecureRandom rng = new SecureRandom();
    byte[] keyBytes = new byte[AES.KEY_LENGTH];
    rng.nextBytes(keyBytes);
    String algorithm = conf.get(HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    Key cfKey = new SecretKeySpec(keyBytes, algorithm);
    byte[] encryptionKey = EncryptionUtil.wrapKey(conf,
      conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()), cfKey);
    final TableName tableName = TableName.valueOf(name.getMethodName());
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
    admin.majorCompact(tableName, hcd1.getName(), CompactType.MOB);

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

  @Test
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
    admin.majorCompact(tableName, hcd1.getName(), CompactType.MOB);
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
    List<Path> paths = new ArrayList<>();
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

  /**
   * This case tests the following mob compaction and normal compaction scenario,
   * after mob compaction, the mob reference in new bulkloaded hfile will win even after it
   * is compacted with some other normal hfiles. This is to make sure the mvcc is included
   * after compaction for mob enabled store files.
   */
  @Test
  public void testGetAfterCompaction() throws Exception {
    resetConf();
    conf.setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    String famStr = "f1";
    byte[] fam = Bytes.toBytes(famStr);
    byte[] qualifier = Bytes.toBytes("q1");
    byte[] mobVal = Bytes.toBytes("01234567890");
    HTableDescriptor hdt = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    hdt.addCoprocessor(CompactTwoLatestHfilesCopro.class.getName());
    HColumnDescriptor hcd = new HColumnDescriptor(fam);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(10);
    hcd.setMaxVersions(1);
    hdt.addFamily(hcd);
    try {
      Table table = TEST_UTIL.createTable(hdt, null);
      HRegion r = TEST_UTIL.getMiniHBaseCluster().getRegions(hdt.getTableName()).get(0);
      Put p = new Put(Bytes.toBytes("r1"));
      p.addColumn(fam, qualifier, mobVal);
      table.put(p);
      // Create mob file mob1 and reference file ref1
      TEST_UTIL.flush(table.getName());
      // Make sure that it is flushed.
      FileSystem fs = r.getRegionFileSystem().getFileSystem();
      Path path = r.getRegionFileSystem().getStoreDir(famStr);
      waitUntilFilesShowup(fs, path, 1);

      p = new Put(Bytes.toBytes("r2"));
      p.addColumn(fam, qualifier, mobVal);
      table.put(p);
      // Create mob file mob2 and reference file ref2
      TEST_UTIL.flush(table.getName());
      waitUntilFilesShowup(fs, path, 2);
      // Do mob compaction to create mob3 and ref3
      TEST_UTIL.getAdmin().compact(hdt.getTableName(), fam, CompactType.MOB);
      waitUntilFilesShowup(fs, path, 3);

      // Compact ref3 and ref2 into ref4
      TEST_UTIL.getAdmin().compact(hdt.getTableName(), fam);
      waitUntilFilesShowup(fs, path, 2);

      // Sleep for some time, since TimeToLiveHFileCleaner is 0, the next run of
      // clean chore is guaranteed to clean up files in archive
      Thread.sleep(100);
      // Run cleaner to make sure that files in archive directory are cleaned up
      TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();

      // Get "r2"
      Get get = new Get(Bytes.toBytes("r2"));
      try {
        Result result = table.get(get);
        assertTrue(Arrays.equals(result.getValue(fam, qualifier), mobVal));
      } catch (IOException e) {
        assertTrue("The MOB file doesn't exist", false);
      }
    } finally {
      TEST_UTIL.deleteTable(hdt.getTableName());
    }
  }

  private void waitUntilFilesShowup(final FileSystem fs, final Path path, final int num)
    throws InterruptedException, IOException {
    FileStatus[] fileList = fs.listStatus(path);
    while (fileList.length != num) {
      Thread.sleep(50);
      fileList = fs.listStatus(path);
      for (FileStatus fileStatus: fileList) {
        LOG.info(Objects.toString(fileStatus));
      }
    }
  }

  /**
   * This copro overwrites the default compaction policy. It always chooses two latest hfiles and
   * compacts them into a new one.
   */
  public static class CompactTwoLatestHfilesCopro implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preCompactSelection(ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        List<? extends StoreFile> candidates, CompactionLifeCycleTracker tracker)
        throws IOException {
      int count = candidates.size();
      if (count >= 2) {
        for (int i = 0; i < count - 2; i++) {
          candidates.remove(0);
        }
        c.bypass();
      }
    }
  }

  private void waitUntilMobCompactionFinished(TableName tableName) throws IOException,
    InterruptedException {
    long finished = EnvironmentEdgeManager.currentTime() + 60000;
    CompactionState state = admin.getCompactionState(tableName, CompactType.MOB);
    while (EnvironmentEdgeManager.currentTime() < finished) {
      if (state == CompactionState.NONE) {
        break;
      }
      state = admin.getCompactionState(tableName, CompactType.MOB);
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
      count += res.size();
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
      HStoreFile sf = new HStoreFile(TEST_UTIL.getTestFileSystem(), path, conf, cacheConf,
        BloomType.NONE, true);
      sf.initReader();
      HFile.Reader reader = sf.getReader().getHFileReader();
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

  private void loadDataForPartitionPolicy(Admin admin, BufferedMutator table, TableName tableName)
      throws IOException {

    Put[] pArray = new Put[1000];

    for (int i = 0; i < 1000; i ++) {
      Put put0 = new Put(Bytes.toBytes("r0" + i));
      put0.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20151130Monday, Bytes.toBytes(mobValue0));
      pArray[i] = put0;
    }
    loadData(admin, bufMut, tableName, pArray);

    Put put06 = new Put(mobKey06);
    put06.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20151128Saturday, Bytes.toBytes(mobValue0));

    loadData(admin, bufMut, tableName, new Put[] { put06 });

    Put put1 = new Put(mobKey1);
    put1.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20151201Tuesday,
        Bytes.toBytes(mobValue1));
    loadData(admin, bufMut, tableName, new Put[] { put1 });

    Put put2 = new Put(mobKey2);
    put2.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20151205Saturday,
        Bytes.toBytes(mobValue2));
    loadData(admin, bufMut, tableName, new Put[] { put2 });

    Put put3 = new Put(mobKey3);
    put3.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20151228Monday,
        Bytes.toBytes(mobValue3));
    loadData(admin, bufMut, tableName, new Put[] { put3 });

    Put put4 = new Put(mobKey4);
    put4.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20151231Thursday,
        Bytes.toBytes(mobValue4));
    loadData(admin, bufMut, tableName, new Put[] { put4 });

    Put put5 = new Put(mobKey5);
    put5.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20160101Friday,
        Bytes.toBytes(mobValue5));
    loadData(admin, bufMut, tableName, new Put[] { put5 });

    Put put6 = new Put(mobKey6);
    put6.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20160103Sunday,
        Bytes.toBytes(mobValue6));
    loadData(admin, bufMut, tableName, new Put[] { put6 });

    Put put7 = new Put(mobKey7);
    put7.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20150907Monday,
        Bytes.toBytes(mobValue7));
    loadData(admin, bufMut, tableName, new Put[] { put7 });

    Put put8 = new Put(mobKey8);
    put8.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qf1), tsFor20151120Sunday,
        Bytes.toBytes(mobValue8));
    loadData(admin, bufMut, tableName, new Put[] { put8 });
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
    final SynchronousQueue<Runnable> queue = new SynchronousQueue<>();
    ThreadPoolExecutor pool =
      new ThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS, queue,
        new ThreadFactoryBuilder().setNameFormat("MobFileCompactionChore-pool-%d")
          .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build(),
        (r, executor) -> {
          try {
            // waiting for a thread to pick up instead of throwing exceptions.
            queue.put(r);
          } catch (InterruptedException e) {
            throw new RejectedExecutionException(e);
          }
        });
    pool.allowCoreThreadTimeOut(true);
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

  /**
   * Verify mob partition policy compaction values.
   */
  private void verifyPolicyValues() throws Exception {
    Get get = new Get(mobKey01);
    Result result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue0)));

    get = new Get(mobKey02);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue0)));

    get = new Get(mobKey03);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue0)));

    get = new Get(mobKey04);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue0)));

    get = new Get(mobKey05);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue0)));

    get = new Get(mobKey06);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue0)));

    get = new Get(mobKey1);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue1)));

    get = new Get(mobKey2);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue2)));

    get = new Get(mobKey3);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue3)));

    get = new Get(mobKey4);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue4)));

    get = new Get(mobKey5);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue5)));

    get = new Get(mobKey6);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue6)));

    get = new Get(mobKey7);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue7)));

    get = new Get(mobKey8);
    result = table.get(get);
    assertTrue(Arrays.equals(result.getValue(Bytes.toBytes(family1), Bytes.toBytes(qf1)),
        Bytes.toBytes(mobValue8)));
  }

  private void commonPolicyTestLogic (final String tableNameAsString,
      final MobCompactPartitionPolicy pType, final boolean majorCompact,
      final int expectedFileNumbers, final String[] expectedFileNames,
      final boolean setupAndLoadData
      ) throws Exception {
    if (setupAndLoadData) {
      setUpForPolicyTest(tableNameAsString, pType);

      loadDataForPartitionPolicy(admin, bufMut, tableName);
    } else {
      alterForPolicyTest(pType);
    }

    if (majorCompact) {
      admin.majorCompact(tableName, hcd1.getName(), CompactType.MOB);
    } else {
      admin.compact(tableName, hcd1.getName(), CompactType.MOB);
    }

    waitUntilMobCompactionFinished(tableName);

    // Run cleaner to make sure that files in archive directory are cleaned up
    TEST_UTIL.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();

    //check the number of files
    Path mobDirPath = MobUtils.getMobFamilyPath(conf, tableName, family1);
    FileStatus[] fileList = fs.listStatus(mobDirPath);

    assertTrue(fileList.length == expectedFileNumbers);

    // the file names are expected
    ArrayList<String> fileNames = new ArrayList<>(expectedFileNumbers);
    for (FileStatus file : fileList) {
      fileNames.add(MobFileName.getDateFromName(file.getPath().getName()));
    }
    int index = 0;
    for (String fileName : expectedFileNames) {
      index = fileNames.indexOf(fileName);
      assertTrue(index >= 0);
      fileNames.remove(index);
    }

    // Check daily mob files are removed from the mobdir, and only weekly mob files are there.
    // Also check that there is no data loss.

    verifyPolicyValues();
  }
 }
