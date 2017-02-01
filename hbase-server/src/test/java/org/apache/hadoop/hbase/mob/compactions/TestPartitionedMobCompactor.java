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

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.MobCompactPartitionPolicy;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.compactions.MobCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.compactions.PartitionedMobCompactionRequest.CompactionPartition;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.AfterClass;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestPartitionedMobCompactor {
  private static final Log LOG = LogFactory.getLog(TestPartitionedMobCompactor.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String family = "family";
  private final static String qf = "qf";
  private final long DAY_IN_MS = 1000 * 60 * 60 * 24;
  private HColumnDescriptor hcd = new HColumnDescriptor(family);
  private Configuration conf = TEST_UTIL.getConfiguration();
  private CacheConfig cacheConf = new CacheConfig(conf);
  private FileSystem fs;
  private List<FileStatus> mobFiles = new ArrayList<>();
  private List<FileStatus> delFiles = new ArrayList<>();
  private List<FileStatus> allFiles = new ArrayList<>();
  private Path basePath;
  private String mobSuffix;
  private String delSuffix;
  private static ExecutorService pool;

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
    Path testDir = FSUtils.getRootDir(conf);
    Path mobTestDir = new Path(testDir, MobConstants.MOB_DIR_NAME);
    basePath = new Path(new Path(mobTestDir, tableName), family);
    mobSuffix = UUID.randomUUID().toString().replaceAll("-", "");
    delSuffix = UUID.randomUUID().toString().replaceAll("-", "") + "_del";
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
  public void testCompactionSelectToAvoidCompactOneFileWithDelete() throws Exception {
    String tableName = "testCompactionSelectToAvoidCompactOneFileWithDelete";
    // If there is only 1 file, it will not be compacted with _del files, so
    // It wont be CompactionType.ALL_FILES in this case, and expected compact file count will be 0.
    testCompactionAtMergeSize(tableName, MobConstants.DEFAULT_MOB_COMPACTION_MERGEABLE_THRESHOLD,
        CompactionType.PART_FILES, false);
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
    String tableName = "testCompactDelFilesWithDefaultBatchSize";
    testCompactDelFilesAtBatchSize(tableName, MobConstants.DEFAULT_MOB_COMPACTION_BATCH_SIZE,
        MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
  }

  @Test
  public void testCompactDelFilesWithSmallBatchSize() throws Exception {
    String tableName = "testCompactDelFilesWithSmallBatchSize";
    testCompactDelFilesAtBatchSize(tableName, 4, MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
  }

  @Test
  public void testCompactDelFilesChangeMaxDelFileCount() throws Exception {
    String tableName = "testCompactDelFilesWithSmallBatchSize";
    testCompactDelFilesAtBatchSize(tableName, 4, 2);
  }

  @Test
  public void testCompactFilesWithDstDirFull() throws Exception {
    String tableName = "testCompactFilesWithDstDirFull";
    fs = FileSystem.get(conf);
    FaultyDistributedFileSystem faultyFs = (FaultyDistributedFileSystem)fs;
    Path testDir = FSUtils.getRootDir(conf);
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
        // assert the compaction type
        Assert.assertEquals(type, request.type);
        // assert get the right partitions
        compareCompactedPartitions(expected, request.compactionPartitions);
        // assert get the right del files
        compareDelFiles(request.delFiles);
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
        List<Path> delFilePaths = new ArrayList<Path>();
        for (FileStatus delFile : request.delFiles) {
          delFilePaths.add(delFile.getPath());
        }
        List<Path> newDelPaths = compactDelFiles(request, delFilePaths);
        // assert the del files are merged.
        Assert.assertEquals(expectedFileCount, newDelPaths.size());
        Assert.assertEquals(expectedCellCount, countDelCellsInDelFiles(newDelPaths));
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
        delFiles.add(file);
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
    Assert.assertEquals(expected.size(), actualKeys.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(expected.get(i), actualKeys.get(i));
    }
  }

  /**
   * Compares the del files.
   * @param allDelFiles all the del files
   */
  private void compareDelFiles(Collection<FileStatus> allDelFiles) {
    int i = 0;
    for (FileStatus file : allDelFiles) {
      Assert.assertEquals(delFiles.get(i), file);
      i++;
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
        mobSuffix = UUID.randomUUID().toString().replaceAll("-", "");
        delSuffix = UUID.randomUUID().toString().replaceAll("-", "") + "_del";
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
    List<StoreFile> sfs = new ArrayList<StoreFile>();
    int size = 0;
    for(Path path : paths) {
      StoreFile sf = new StoreFile(fs, path, conf, cacheConf, BloomType.NONE);
      sfs.add(sf);
    }
    List scanners = StoreFileScanner.getScannersForStoreFiles(sfs, false, true,
        false, false, HConstants.LATEST_TIMESTAMP);
    Scan scan = new Scan();
    scan.setMaxVersions(hcd.getMaxVersions());
    long timeToPurgeDeletes = Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    long ttl = HStore.determineTTLFromFamily(hcd);
    ScanInfo scanInfo = new ScanInfo(conf, hcd, ttl, timeToPurgeDeletes, CellComparator.COMPARATOR);
    StoreScanner scanner = new StoreScanner(scan, scanInfo, ScanType.COMPACT_RETAIN_DELETES, null,
        scanners, 0L, HConstants.LATEST_TIMESTAMP);
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
    final SynchronousQueue<Runnable> queue = new SynchronousQueue<Runnable>();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads, keepAliveTime,
      TimeUnit.SECONDS, queue, Threads.newDaemonThreadFactory("MobFileCompactionChore"),
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
