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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.compactions.MobCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionDelPartition;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartition;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestPartitionedMobCompactor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestPartitionedMobCompactor.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestPartitionedMobCompactor.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String family = "family";
  private final static String qf = "qf";
  private final long DAY_IN_MS = 1000 * 60 * 60 * 24;
  private static byte[] KEYS = Bytes.toBytes("012");
  private HColumnDescriptor hcd = new HColumnDescriptor(family);
  private Configuration conf = TEST_UTIL.getConfiguration();
  private CacheConfig cacheConf = new CacheConfig(conf);
  private FileSystem fs;
  private List<FileStatus> mobFiles = new ArrayList<>();
  private List<Path> delFiles = new ArrayList<>();
  private List<FileStatus> allFiles = new ArrayList<>();
  private Path basePath;
  private String mobSuffix;
  private String delSuffix;
  private static ExecutorService pool;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
    // Inject our customized DistributedFileSystem
    TEST_UTIL.getConfiguration().setClass("fs.hdfs.impl", FaultyDistributedFileSystem.class,
        DistributedFileSystem.class);
    TEST_UTIL.startMiniCluster(1);
    pool = createThreadPool();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    pool.shutdown();
    TEST_UTIL.shutdownMiniCluster();
  }

  private void init(String tableName) throws Exception {
    fs = FileSystem.get(conf);
    Path testDir = CommonFSUtils.getRootDir(conf);
    Path mobTestDir = new Path(testDir, MobConstants.MOB_DIR_NAME);
    basePath = new Path(new Path(mobTestDir, tableName), family);
    mobSuffix = TEST_UTIL.getRandomUUID().toString().replaceAll("-", "");
    delSuffix = TEST_UTIL.getRandomUUID().toString().replaceAll("-", "") + "_del";
    allFiles.clear();
    mobFiles.clear();
    delFiles.clear();
  }

  @Test
  public void testCompactionSelectAllFilesWeeklyPolicy() throws Exception {
    String tableName = "testCompactionSelectAllFilesWeeklyPolicy";
    testCompactionAtMergeSize(tableName, MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD,
        CompactionType.ALL_FILES, false, false, new Date(), MobCompactPartitionPolicy.WEEKLY, 1);
  }

  @Test
  public void testCompactionSelectPartFilesWeeklyPolicy() throws Exception {
    String tableName = "testCompactionSelectPartFilesWeeklyPolicy";
    testCompactionAtMergeSize(tableName, 4000, CompactionType.PART_FILES, false, false,
        new Date(), MobCompactPartitionPolicy.WEEKLY, 1);
  }

  @Test
  public void testCompactionSelectPartFilesWeeklyPolicyWithPastWeek() throws Exception {
    String tableName = "testCompactionSelectPartFilesWeeklyPolicyWithPastWeek";
    Date dateLastWeek = new Date(System.currentTimeMillis() - (7 * DAY_IN_MS));
    testCompactionAtMergeSize(tableName, 700, CompactionType.PART_FILES, false, false, dateLastWeek,
        MobCompactPartitionPolicy.WEEKLY, 7);
  }

  @Test
  public void testCompactionSelectAllFilesWeeklyPolicyWithPastWeek() throws Exception {
    String tableName = "testCompactionSelectAllFilesWeeklyPolicyWithPastWeek";
    Date dateLastWeek = new Date(System.currentTimeMillis() - (7 * DAY_IN_MS));
    testCompactionAtMergeSize(tableName, 3000, CompactionType.ALL_FILES,
        false, false, dateLastWeek, MobCompactPartitionPolicy.WEEKLY, 7);
  }

  @Test
  public void testCompactionSelectAllFilesMonthlyPolicy() throws Exception {
    String tableName = "testCompactionSelectAllFilesMonthlyPolicy";
    Date dateLastWeek = new Date(System.currentTimeMillis() - (7 * DAY_IN_MS));
    testCompactionAtMergeSize(tableName, MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD,
        CompactionType.ALL_FILES, false, false, dateLastWeek,
        MobCompactPartitionPolicy.MONTHLY, 7);
  }

  @Test
  public void testCompactionSelectNoFilesWithinCurrentWeekMonthlyPolicy() throws Exception {
    String tableName = "testCompactionSelectNoFilesWithinCurrentWeekMonthlyPolicy";
    testCompactionAtMergeSize(tableName, MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD,
        CompactionType.PART_FILES, false, false, new Date(), MobCompactPartitionPolicy.MONTHLY, 1);
  }

  @Test
  public void testCompactionSelectPartFilesMonthlyPolicy() throws Exception {
    String tableName = "testCompactionSelectPartFilesMonthlyPolicy";
    testCompactionAtMergeSize(tableName, 4000, CompactionType.PART_FILES, false, false,
        new Date(), MobCompactPartitionPolicy.MONTHLY, 1);
  }

  @Test
  public void testCompactionSelectPartFilesMonthlyPolicyWithPastWeek() throws Exception {
    String tableName = "testCompactionSelectPartFilesMonthlyPolicyWithPastWeek";
    Date dateLastWeek = new Date(System.currentTimeMillis() - (7 * DAY_IN_MS));
    Calendar calendar =  Calendar.getInstance();
    Date firstDayOfCurrentMonth = MobUtils.getFirstDayOfMonth(calendar, new Date());
    CompactionType type = CompactionType.PART_FILES;
    long mergeSizeMultiFactor = 7;


    // The dateLastWeek may not really be last week, suppose that it runs at 2/1/2017, it is going
    // to be last month and the monthly policy is going to be applied here.
    if (dateLastWeek.before(firstDayOfCurrentMonth)) {
      type = CompactionType.ALL_FILES;
      mergeSizeMultiFactor *= 4;
    }

    testCompactionAtMergeSize(tableName, 700, type, false, false, dateLastWeek,
        MobCompactPartitionPolicy.MONTHLY, mergeSizeMultiFactor);
  }

  @Test
  public void testCompactionSelectAllFilesMonthlyPolicyWithPastWeek() throws Exception {
    String tableName = "testCompactionSelectAllFilesMonthlyPolicyWithPastWeek";
    Date dateLastWeek = new Date(System.currentTimeMillis() - (7 * DAY_IN_MS));

    testCompactionAtMergeSize(tableName, 3000, CompactionType.ALL_FILES,
        false, false, dateLastWeek, MobCompactPartitionPolicy.MONTHLY, 7);
  }

  @Test
  public void testCompactionSelectPartFilesMonthlyPolicyWithPastMonth() throws Exception {
    String tableName = "testCompactionSelectPartFilesMonthlyPolicyWithPastMonth";

    // back 5 weeks, it is going to be a past month
    Date dateLastMonth = new Date(System.currentTimeMillis() - (7 * 5 * DAY_IN_MS));
    testCompactionAtMergeSize(tableName, 200, CompactionType.PART_FILES, false, false, dateLastMonth,
        MobCompactPartitionPolicy.MONTHLY, 28);
  }

  @Test
  public void testCompactionSelectAllFilesMonthlyPolicyWithPastMonth() throws Exception {
    String tableName = "testCompactionSelectAllFilesMonthlyPolicyWithPastMonth";

    // back 5 weeks, it is going to be a past month
    Date dateLastMonth = new Date(System.currentTimeMillis() - (7 * 5 * DAY_IN_MS));
    testCompactionAtMergeSize(tableName, 750, CompactionType.ALL_FILES,
        false, false, dateLastMonth, MobCompactPartitionPolicy.MONTHLY, 28);
  }

  @Test
  public void testCompactionSelectWithAllFiles() throws Exception {
    String tableName = "testCompactionSelectWithAllFiles";
    // If there is only 1 file, it will not be compacted with _del files, so
    // It wont be CompactionType.ALL_FILES in this case, do not create with _del files.
    testCompactionAtMergeSize(tableName, MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD,
        CompactionType.ALL_FILES, false, false);
  }

  @Test
  public void testCompactionSelectWithPartFiles() throws Exception {
    String tableName = "testCompactionSelectWithPartFiles";
    testCompactionAtMergeSize(tableName, 4000, CompactionType.PART_FILES, false);
  }

  @Test
  public void testCompactionSelectWithForceAllFiles() throws Exception {
    String tableName = "testCompactionSelectWithForceAllFiles";
    testCompactionAtMergeSize(tableName, Long.MAX_VALUE, CompactionType.ALL_FILES, true);
  }

  private void testCompactionAtMergeSize(final String tableName,
      final long mergeSize, final CompactionType type, final boolean isForceAllFiles)
      throws Exception {
    testCompactionAtMergeSize(tableName, mergeSize, type, isForceAllFiles, true);
  }

  private void testCompactionAtMergeSize(final String tableName,
      final long mergeSize, final CompactionType type, final boolean isForceAllFiles,
      final boolean createDelFiles)
      throws Exception {
    Date date = new Date();
    testCompactionAtMergeSize(tableName, mergeSize, type, isForceAllFiles, createDelFiles, date);
  }

  private void testCompactionAtMergeSize(final String tableName,
      final long mergeSize, final CompactionType type, final boolean isForceAllFiles,
      final boolean createDelFiles, final Date date)
      throws Exception {
    testCompactionAtMergeSize(tableName, mergeSize, type, isForceAllFiles, createDelFiles, date,
        MobCompactPartitionPolicy.DAILY, 1);
  }

  private void testCompactionAtMergeSize(final String tableName,
      final long mergeSize, final CompactionType type, final boolean isForceAllFiles,
      final boolean createDelFiles, final Date date, final MobCompactPartitionPolicy policy,
      final long mergeSizeMultiFactor)
      throws Exception {
    resetConf();
    init(tableName);
    int count = 10;
    // create 10 mob files.
    createStoreFiles(basePath, family, qf, count, Type.Put, date);

    if (createDelFiles) {
      // create 10 del files
      createStoreFiles(basePath, family, qf, count, Type.Delete, date);
    }

    Calendar calendar =  Calendar.getInstance();
    Date firstDayOfCurrentWeek = MobUtils.getFirstDayOfWeek(calendar, new Date());

    listFiles();
    List<String> expectedStartKeys = new ArrayList<>();
    for(FileStatus file : mobFiles) {
      if(file.getLen() < mergeSize * mergeSizeMultiFactor) {
        String fileName = file.getPath().getName();
        String startKey = fileName.substring(0, 32);

        // If the policy is monthly and files are in current week, they will be skipped
        // in minor compcation.
        boolean skipCompaction = false;
        if (policy == MobCompactPartitionPolicy.MONTHLY) {
          String fileDateStr = MobFileName.getDateFromName(fileName);
          Date fileDate;
          try {
            fileDate = MobUtils.parseDate(fileDateStr);
          } catch (ParseException e)  {
            LOG.warn("Failed to parse date " + fileDateStr, e);
            fileDate = new Date();
          }
          if (!fileDate.before(firstDayOfCurrentWeek)) {
            skipCompaction = true;
          }
        }

        // If it is not an major mob compaction and del files are there,
        // these mob files wont be compacted.
        if (isForceAllFiles || (!createDelFiles && !skipCompaction)) {
          expectedStartKeys.add(startKey);
        }
      }
    }

    // Set the policy
    this.hcd.setMobCompactPartitionPolicy(policy);
    // set the mob compaction mergeable threshold
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);
    testSelectFiles(tableName, type, isForceAllFiles, expectedStartKeys);
    // go back to the default daily policy
    this.hcd.setMobCompactPartitionPolicy(MobCompactPartitionPolicy.DAILY);
  }

  @Test
  public void testCompactDelFilesWithDefaultBatchSize() throws Exception {
    testCompactDelFilesAtBatchSize(name.getMethodName(), MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE,
        MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
  }

  @Test
  public void testCompactDelFilesWithSmallBatchSize() throws Exception {
    testCompactDelFilesAtBatchSize(name.getMethodName(), 4, MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
  }

  @Test
  public void testCompactDelFilesChangeMaxDelFileCount() throws Exception {
    testCompactDelFilesAtBatchSize(name.getMethodName(), 4, 2);
  }

  @Test
  public void testCompactFilesWithDstDirFull() throws Exception {
    String tableName = name.getMethodName();
    fs = FileSystem.get(conf);
    FaultyDistributedFileSystem faultyFs = (FaultyDistributedFileSystem)fs;
    Path testDir = CommonFSUtils.getRootDir(conf);
    Path mobTestDir = new Path(testDir, MobConstants.MOB_DIR_NAME);
    basePath = new Path(new Path(mobTestDir, tableName), family);

    try {
      int count = 2;
      // create 2 mob files.
      createStoreFiles(basePath, family, qf, count, Type.Put, true, new Date());
      listFiles();

      TableName tName = TableName.valueOf(tableName);
      MobCompactor compactor = new PartitionedMobCompactor(conf, faultyFs, tName, hcd, pool);
      faultyFs.setThrowException(true);
      try {
        compactor.compact(allFiles, true);
      } catch (IOException e) {
        System.out.println("Expected exception, ignore");
      }

      // Verify that all the files in tmp directory are cleaned up
      Path tempPath = new Path(MobUtils.getMobHome(conf), MobConstants.TEMP_DIR_NAME);
      FileStatus[] ls = faultyFs.listStatus(tempPath);

      // Only .bulkload under this directory
      assertTrue(ls.length == 1);
      assertTrue(MobConstants.BULKLOAD_DIR_NAME.equalsIgnoreCase(ls[0].getPath().getName()));

      Path bulkloadPath = new Path(tempPath, new Path(MobConstants.BULKLOAD_DIR_NAME, new Path(
          tName.getNamespaceAsString(), tName.getQualifierAsString())));

      // Nothing in bulkLoad directory
      FileStatus[] lsBulkload = faultyFs.listStatus(bulkloadPath);
      assertTrue(lsBulkload.length == 0);

    } finally {
      faultyFs.setThrowException(false);
    }
  }

  /**
   * Create mulitple partition files
   */
  private void createMobFile(Path basePath) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    MobFileName mobFileName = null;
    int ii = 0;
    Date today = new Date();
    for (byte k0 : KEYS) {
      byte[] startRow = Bytes.toBytes(ii++);

      mobFileName = MobFileName.create(startRow, MobUtils.formatDate(today), mobSuffix);

      StoreFileWriter mobFileWriter =
          new StoreFileWriter.Builder(conf, cacheConf, fs).withFileContext(meta)
              .withFilePath(new Path(basePath, mobFileName.getFileName())).build();

      long now = System.currentTimeMillis();
      try {
        for (int i = 0; i < 10; i++) {
          byte[] key = Bytes.add(Bytes.toBytes(k0), Bytes.toBytes(i));
          byte[] dummyData = new byte[5000];
          new Random().nextBytes(dummyData);
          mobFileWriter.append(
              new KeyValue(key, Bytes.toBytes(family), Bytes.toBytes(qf), now, Type.Put, dummyData));
        }
      } finally {
        mobFileWriter.close();
      }
    }
  }

  /**
   * Create mulitple partition delete files
   */
  private void createMobDelFile(Path basePath, int startKey) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    MobFileName mobFileName = null;
    Date today = new Date();

    byte[] startRow = Bytes.toBytes(startKey);

    mobFileName = MobFileName.create(startRow, MobUtils.formatDate(today), delSuffix);

    StoreFileWriter mobFileWriter =
        new StoreFileWriter.Builder(conf, cacheConf, fs).withFileContext(meta)
            .withFilePath(new Path(basePath, mobFileName.getFileName())).build();

    long now = System.currentTimeMillis();
    try {
      byte[] key = Bytes.add(Bytes.toBytes(KEYS[startKey]), Bytes.toBytes(0));
      byte[] dummyData = new byte[5000];
      new Random().nextBytes(dummyData);
      mobFileWriter.append(
          new KeyValue(key, Bytes.toBytes(family), Bytes.toBytes(qf), now, Type.Delete, dummyData));
      key = Bytes.add(Bytes.toBytes(KEYS[startKey]), Bytes.toBytes(2));
      mobFileWriter.append(
          new KeyValue(key, Bytes.toBytes(family), Bytes.toBytes(qf), now, Type.Delete, dummyData));
      key = Bytes.add(Bytes.toBytes(KEYS[startKey]), Bytes.toBytes(4));
      mobFileWriter.append(
          new KeyValue(key, Bytes.toBytes(family), Bytes.toBytes(qf), now, Type.Delete, dummyData));

    } finally {
      mobFileWriter.close();
    }
  }

  @Test
  public void testCompactFilesWithoutDelFile() throws Exception {
    String tableName = "testCompactFilesWithoutDelFile";
    resetConf();
    init(tableName);

    createMobFile(basePath);

    listFiles();

    PartitionedMobCompactor compactor = new PartitionedMobCompactor(conf, fs,
        TableName.valueOf(tableName), hcd, pool) {
      @Override
      public List<Path> compact(List<FileStatus> files, boolean isForceAllFiles)
          throws IOException {
        if (files == null || files.isEmpty()) {
          return null;
        }

        PartitionedMobCompactionRequest request = select(files, isForceAllFiles);

        // Make sure that there is no del Partitions
        assertTrue(request.getDelPartitions().size() == 0);

        // Make sure that when there is no startKey/endKey for partition.
        for (CompactionPartition p : request.getCompactionPartitions()) {
          assertTrue(p.getStartKey() == null);
          assertTrue(p.getEndKey() == null);
        }
        return null;
      }
    };

    compactor.compact(allFiles, true);
  }

  static class MyPartitionedMobCompactor extends PartitionedMobCompactor {
    int delPartitionSize = 0;
    int PartitionsIncludeDelFiles = 0;
    CacheConfig cacheConfig = null;

    MyPartitionedMobCompactor(Configuration conf, FileSystem fs, TableName tableName,
        ColumnFamilyDescriptor column, ExecutorService pool, final int delPartitionSize,
        final CacheConfig cacheConf, final int PartitionsIncludeDelFiles)
        throws IOException {
      super(conf, fs, tableName, column, pool);
      this.delPartitionSize = delPartitionSize;
      this.cacheConfig = cacheConf;
      this.PartitionsIncludeDelFiles = PartitionsIncludeDelFiles;
    }

    @Override public List<Path> compact(List<FileStatus> files, boolean isForceAllFiles)
        throws IOException {
      if (files == null || files.isEmpty()) {
        return null;
      }
      PartitionedMobCompactionRequest request = select(files, isForceAllFiles);

      assertTrue(request.getDelPartitions().size() == delPartitionSize);
      if (request.getDelPartitions().size() > 0) {
        for (CompactionPartition p : request.getCompactionPartitions()) {
          assertTrue(p.getStartKey() != null);
          assertTrue(p.getEndKey() != null);
        }
      }

      try {
        for (CompactionDelPartition delPartition : request.getDelPartitions()) {
          for (Path newDelPath : delPartition.listDelFiles()) {
            HStoreFile sf =
                new HStoreFile(fs, newDelPath, conf, this.cacheConfig, BloomType.NONE, true);
            // pre-create reader of a del file to avoid race condition when opening the reader in
            // each partition.
            sf.initReader();
            delPartition.addStoreFile(sf);
          }
        }

        // Make sure that CompactionDelPartitions does not overlap
        CompactionDelPartition prevDelP = null;
        for (CompactionDelPartition delP : request.getDelPartitions()) {
          assertTrue(
              Bytes.compareTo(delP.getId().getStartKey(), delP.getId().getEndKey()) <= 0);

          if (prevDelP != null) {
            assertTrue(
                Bytes.compareTo(prevDelP.getId().getEndKey(), delP.getId().getStartKey()) < 0);
          }
        }

        int affectedPartitions = 0;

        // Make sure that only del files within key range for a partition is included in compaction.
        // compact the mob files by partitions in parallel.
        for (CompactionPartition partition : request.getCompactionPartitions()) {
          List<HStoreFile> delFiles = getListOfDelFilesForPartition(partition, request.getDelPartitions());
          if (!request.getDelPartitions().isEmpty()) {
            if (!((Bytes.compareTo(request.getDelPartitions().get(0).getId().getStartKey(),
                partition.getEndKey()) > 0) || (Bytes.compareTo(
                request.getDelPartitions().get(request.getDelPartitions().size() - 1).getId()
                    .getEndKey(), partition.getStartKey()) < 0))) {

              if (delFiles.size() > 0) {
                assertTrue(delFiles.size() == 1);
                affectedPartitions += delFiles.size();
                assertTrue(Bytes.compareTo(partition.getStartKey(),
                  CellUtil.cloneRow(delFiles.get(0).getLastKey().get())) <= 0);
                assertTrue(Bytes.compareTo(partition.getEndKey(),
                  CellUtil.cloneRow(delFiles.get(delFiles.size() - 1).getFirstKey().get())) >= 0);
              }
            }
          }
        }
        // The del file is only included in one partition
        assertTrue(affectedPartitions == PartitionsIncludeDelFiles);
      } finally {
        for (CompactionDelPartition delPartition : request.getDelPartitions()) {
          for (HStoreFile storeFile : delPartition.getStoreFiles()) {
            try {
              storeFile.closeStoreFile(true);
            } catch (IOException e) {
              LOG.warn("Failed to close the reader on store file " + storeFile.getPath(), e);
            }
          }
        }
      }

      return null;
    }
  }

  @Test
  public void testCompactFilesWithOneDelFile() throws Exception {
    String tableName = "testCompactFilesWithOneDelFile";
    resetConf();
    init(tableName);

    // Create only del file.
    createMobFile(basePath);
    createMobDelFile(basePath, 2);

    listFiles();

    MyPartitionedMobCompactor compactor = new MyPartitionedMobCompactor(conf, fs,
        TableName.valueOf(tableName), hcd, pool, 1, cacheConf, 1);

    compactor.compact(allFiles, true);
  }

  @Test
  public void testCompactFilesWithMultiDelFiles() throws Exception {
    String tableName = "testCompactFilesWithMultiDelFiles";
    resetConf();
    init(tableName);

    // Create only del file.
    createMobFile(basePath);
    createMobDelFile(basePath, 0);
    createMobDelFile(basePath, 1);
    createMobDelFile(basePath, 2);

    listFiles();

    MyPartitionedMobCompactor compactor = new MyPartitionedMobCompactor(conf, fs,
        TableName.valueOf(tableName), hcd, pool, 3, cacheConf, 3);
    compactor.compact(allFiles, true);
  }

  private void testCompactDelFilesAtBatchSize(String tableName, int batchSize,
      int delfileMaxCount)  throws Exception {
    resetConf();
    init(tableName);
    // create 20 mob files.
    createStoreFiles(basePath, family, qf, 20, Type.Put, new Date());
    // create 13 del files
    createStoreFiles(basePath, family, qf, 13, Type.Delete, new Date());
    listFiles();

    // set the max del file count
    conf.setInt(MobConstants.MOB_DELFILE_MAX_COUNT, delfileMaxCount);
    // set the mob compaction batch size
    conf.setInt(MobConstants.MOB_COMPACTION_BATCH_SIZE, batchSize);
    testCompactDelFiles(tableName, 1, 13, false);
  }

  /**
   * Tests the selectFiles
   * @param tableName the table name
   * @param type the expected compaction type
   * @param isForceAllFiles whether all the mob files are selected
   * @param expected the expected start keys
   */
  private void testSelectFiles(String tableName, final CompactionType type,
    final boolean isForceAllFiles, final List<String> expected) throws IOException {
    PartitionedMobCompactor compactor = new PartitionedMobCompactor(conf, fs,
      TableName.valueOf(tableName), hcd, pool) {
      @Override
      public List<Path> compact(List<FileStatus> files, boolean isForceAllFiles)
        throws IOException {
        if (files == null || files.isEmpty()) {
          return null;
        }
        PartitionedMobCompactionRequest request = select(files, isForceAllFiles);

        // Make sure that when there is no del files, there will be no startKey/endKey for partition.
        if (request.getDelPartitions().size() == 0) {
          for (CompactionPartition p : request.getCompactionPartitions()) {
            assertTrue(p.getStartKey() == null);
            assertTrue(p.getEndKey() == null);
          }
        }

        // Make sure that CompactionDelPartitions does not overlap
        CompactionDelPartition prevDelP = null;
        for (CompactionDelPartition delP : request.getDelPartitions()) {
          assertTrue(Bytes.compareTo(delP.getId().getStartKey(),
              delP.getId().getEndKey()) <= 0);

          if (prevDelP != null) {
            assertTrue(Bytes.compareTo(prevDelP.getId().getEndKey(),
                delP.getId().getStartKey()) < 0);
          }
        }

        // Make sure that only del files within key range for a partition is included in compaction.
        // compact the mob files by partitions in parallel.
        for (CompactionPartition partition : request.getCompactionPartitions()) {
          List<HStoreFile> delFiles = getListOfDelFilesForPartition(partition, request.getDelPartitions());
          if (!request.getDelPartitions().isEmpty()) {
            if (!((Bytes.compareTo(request.getDelPartitions().get(0).getId().getStartKey(),
                partition.getEndKey()) > 0) || (Bytes.compareTo(
                request.getDelPartitions().get(request.getDelPartitions().size() - 1).getId()
                    .getEndKey(), partition.getStartKey()) < 0))) {
              if (delFiles.size() > 0) {
                assertTrue(Bytes.compareTo(partition.getStartKey(),
                  delFiles.get(0).getFirstKey().get().getRowArray()) >= 0);
                assertTrue(Bytes.compareTo(partition.getEndKey(),
                  delFiles.get(delFiles.size() - 1).getLastKey().get().getRowArray()) <= 0);
              }
            }
          }
        }

        // assert the compaction type
        assertEquals(type, request.type);
        // assert get the right partitions
        compareCompactedPartitions(expected, request.compactionPartitions);
        // assert get the right del files
        compareDelFiles(request.getDelPartitions());
        return null;
      }
    };
    compactor.compact(allFiles, isForceAllFiles);
  }

  /**
   * Tests the compacteDelFile
   * @param tableName the table name
   * @param expectedFileCount the expected file count
   * @param expectedCellCount the expected cell count
   * @param isForceAllFiles whether all the mob files are selected
   */
  private void testCompactDelFiles(String tableName, final int expectedFileCount,
      final int expectedCellCount, boolean isForceAllFiles) throws IOException {
    PartitionedMobCompactor compactor = new PartitionedMobCompactor(conf, fs,
      TableName.valueOf(tableName), hcd, pool) {
      @Override
      protected List<Path> performCompaction(PartitionedMobCompactionRequest request)
          throws IOException {
        List<Path> delFilePaths = new ArrayList<>();
        for (CompactionDelPartition delPartition: request.getDelPartitions()) {
          for (Path p : delPartition.listDelFiles()) {
            delFilePaths.add(p);
          }
        }
        List<Path> newDelPaths = compactDelFiles(request, delFilePaths);
        // assert the del files are merged.
        assertEquals(expectedFileCount, newDelPaths.size());
        assertEquals(expectedCellCount, countDelCellsInDelFiles(newDelPaths));
        return null;
      }
    };
    compactor.compact(allFiles, isForceAllFiles);
  }

  /**
   * Lists the files in the path
   */
  private void listFiles() throws IOException {
    for (FileStatus file : fs.listStatus(basePath)) {
      allFiles.add(file);
      if (file.getPath().getName().endsWith("_del")) {
        delFiles.add(file.getPath());
      } else {
        mobFiles.add(file);
      }
    }
  }

  /**
   * Compares the compacted partitions.
   * @param partitions the collection of CompactedPartitions
   */
  private void compareCompactedPartitions(List<String> expected,
      Collection<CompactionPartition> partitions) {
    List<String> actualKeys = new ArrayList<>();
    for (CompactionPartition partition : partitions) {
      actualKeys.add(partition.getPartitionId().getStartKey());
    }
    Collections.sort(expected);
    Collections.sort(actualKeys);
    assertEquals(expected.size(), actualKeys.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i), actualKeys.get(i));
    }
  }

  /**
   * Compares the del files.
   * @param delPartitions all del partitions
   */
  private void compareDelFiles(List<CompactionDelPartition> delPartitions) {
    Map<Path, Path> delMap = new HashMap<>();
    for (CompactionDelPartition delPartition : delPartitions) {
      for (Path f : delPartition.listDelFiles()) {
        delMap.put(f, f);
      }
    }
    for (Path f : delFiles) {
      assertTrue(delMap.containsKey(f));
    }
  }

  /**
   * Creates store files.
   * @param basePath the path to create file
   * @family the family name
   * @qualifier the column qualifier
   * @count the store file number
   * @type the key type
   */
  private void createStoreFiles(Path basePath, String family, String qualifier, int count,
      Type type, final Date date) throws IOException {
    createStoreFiles(basePath, family, qualifier, count, type, false, date);
  }

  private void createStoreFiles(Path basePath, String family, String qualifier, int count,
      Type type, boolean sameStartKey, final Date date) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    String startKey = "row_";
    MobFileName mobFileName = null;
    for (int i = 0; i < count; i++) {
      byte[] startRow;
      if (sameStartKey) {
        // When creating multiple files under one partition, suffix needs to be different.
        startRow = Bytes.toBytes(startKey);
        mobSuffix = TEST_UTIL.getRandomUUID().toString().replaceAll("-", "");
        delSuffix = TEST_UTIL.getRandomUUID().toString().replaceAll("-", "") + "_del";
      } else {
        startRow = Bytes.toBytes(startKey + i);
      }
      if(type.equals(Type.Delete)) {
        mobFileName = MobFileName.create(startRow, MobUtils.formatDate(date), delSuffix);
      }
      if(type.equals(Type.Put)){
        mobFileName = MobFileName.create(startRow, MobUtils.formatDate(date), mobSuffix);
      }
      StoreFileWriter mobFileWriter = new StoreFileWriter.Builder(conf, cacheConf, fs)
      .withFileContext(meta).withFilePath(new Path(basePath, mobFileName.getFileName())).build();
      writeStoreFile(mobFileWriter, startRow, Bytes.toBytes(family), Bytes.toBytes(qualifier),
          type, (i+1)*1000);
    }
  }

  /**
   * Writes data to store file.
   * @param writer the store file writer
   * @param row the row key
   * @param family the family name
   * @param qualifier the column qualifier
   * @param type the key type
   * @param size the size of value
   */
  private static void writeStoreFile(final StoreFileWriter writer, byte[]row, byte[] family,
      byte[] qualifier, Type type, int size) throws IOException {
    long now = System.currentTimeMillis();
    try {
      byte[] dummyData = new byte[size];
      new Random().nextBytes(dummyData);
      writer.append(new KeyValue(row, family, qualifier, now, type, dummyData));
    } finally {
      writer.close();
    }
  }

  /**
   * Gets the number of del cell in the del files
   * @param paths the del file paths
   * @return the cell size
   */
  private int countDelCellsInDelFiles(List<Path> paths) throws IOException {
    List<HStoreFile> sfs = new ArrayList<>();
    int size = 0;
    for (Path path : paths) {
      HStoreFile sf = new HStoreFile(fs, path, conf, cacheConf, BloomType.NONE, true);
      sfs.add(sf);
    }
    List<KeyValueScanner> scanners = new ArrayList<>(StoreFileScanner.getScannersForStoreFiles(sfs,
      false, true, false, false, HConstants.LATEST_TIMESTAMP));
    long timeToPurgeDeletes = Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    long ttl = HStore.determineTTLFromFamily(hcd);
    ScanInfo scanInfo = new ScanInfo(conf, hcd, ttl, timeToPurgeDeletes, CellComparatorImpl.COMPARATOR);
    StoreScanner scanner = new StoreScanner(scanInfo, ScanType.COMPACT_RETAIN_DELETES, scanners);
    List<Cell> results = new ArrayList<>();
    boolean hasMore = true;

    while (hasMore) {
      hasMore = scanner.next(results);
      size += results.size();
      results.clear();
    }
    scanner.close();
    return size;
  }

  private static ExecutorService createThreadPool() {
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

  /**
   * Resets the configuration.
   */
  private void resetConf() {
    conf.setLong(MobConstants.MOB_COMPACTION_MERGEABLE_THRESHOLD,
      MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD);
    conf.setInt(MobConstants.MOB_DELFILE_MAX_COUNT, MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    conf.setInt(MobConstants.MOB_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE);
  }

  /**
   * The customized Distributed File System Implementation
   */
  static class FaultyDistributedFileSystem extends DistributedFileSystem {
    private volatile boolean throwException = false;

    public FaultyDistributedFileSystem() {
      super();
    }

    public void setThrowException(boolean throwException) {
      this.throwException = throwException;
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      if (throwException) {
        throw new IOException("No more files allowed");
      }
      return super.rename(src, dst);
    }
  }
}
