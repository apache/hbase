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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
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
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobFileName;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.filecompactions.MobFileCompactionRequest.CompactionType;
import org.apache.hadoop.hbase.mob.filecompactions.PartitionedMobFileCompactionRequest.CompactionPartition;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestPartitionedMobFileCompactor {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static String family = "family";
  private final static String qf = "qf";
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
    TEST_UTIL.getConfiguration().setInt("hbase.master.info.port", 0);
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.info.port.auto", true);
    TEST_UTIL.getConfiguration().setInt("hfile.format.version", 3);
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
  public void testCompactionSelectWithAllFiles() throws Exception {
    resetConf();
    String tableName = "testCompactionSelectWithAllFiles";
    init(tableName);
    int count = 10;
    // create 10 mob files.
    createStoreFiles(basePath, family, qf, count, Type.Put);
    // create 10 del files
    createStoreFiles(basePath, family, qf, count, Type.Delete);
    listFiles();
    long mergeSize = MobConstants.DEFAULT_MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD;
    List<String> expectedStartKeys = new ArrayList<>();
    for(FileStatus file : mobFiles) {
      if(file.getLen() < mergeSize) {
        String fileName = file.getPath().getName();
        String startKey = fileName.substring(0, 32);
        expectedStartKeys.add(startKey);
      }
    }
    testSelectFiles(tableName, CompactionType.ALL_FILES, false, expectedStartKeys);
  }

  @Test
  public void testCompactionSelectWithPartFiles() throws Exception {
    resetConf();
    String tableName = "testCompactionSelectWithPartFiles";
    init(tableName);
    int count = 10;
    // create 10 mob files.
    createStoreFiles(basePath, family, qf, count, Type.Put);
    // create 10 del files
    createStoreFiles(basePath, family, qf, count, Type.Delete);
    listFiles();
    long mergeSize = 4000;
    List<String> expectedStartKeys = new ArrayList<>();
    for(FileStatus file : mobFiles) {
      if(file.getLen() < 4000) {
        String fileName = file.getPath().getName();
        String startKey = fileName.substring(0, 32);
        expectedStartKeys.add(startKey);
      }
    }
    // set the mob file compaction mergeable threshold
    conf.setLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);
    testSelectFiles(tableName, CompactionType.PART_FILES, false, expectedStartKeys);
  }

  @Test
  public void testCompactionSelectWithForceAllFiles() throws Exception {
    resetConf();
    String tableName = "testCompactionSelectWithForceAllFiles";
    init(tableName);
    int count = 10;
    // create 10 mob files.
    createStoreFiles(basePath, family, qf, count, Type.Put);
    // create 10 del files
    createStoreFiles(basePath, family, qf, count, Type.Delete);
    listFiles();
    long mergeSize = 4000;
    List<String> expectedStartKeys = new ArrayList<>();
    for(FileStatus file : mobFiles) {
      String fileName = file.getPath().getName();
      String startKey = fileName.substring(0, 32);
      expectedStartKeys.add(startKey);
    }
    // set the mob file compaction mergeable threshold
    conf.setLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD, mergeSize);
    testSelectFiles(tableName, CompactionType.ALL_FILES, true, expectedStartKeys);
  }

  @Test
  public void testCompactDelFilesWithDefaultBatchSize() throws Exception {
    resetConf();
    String tableName = "testCompactDelFilesWithDefaultBatchSize";
    init(tableName);
    // create 20 mob files.
    createStoreFiles(basePath, family, qf, 20, Type.Put);
    // create 13 del files
    createStoreFiles(basePath, family, qf, 13, Type.Delete);
    listFiles();
    testCompactDelFiles(tableName, 1, 13, false);
  }

  @Test
  public void testCompactDelFilesWithSmallBatchSize() throws Exception {
    resetConf();
    String tableName = "testCompactDelFilesWithSmallBatchSize";
    init(tableName);
    // create 20 mob files.
    createStoreFiles(basePath, family, qf, 20, Type.Put);
    // create 13 del files
    createStoreFiles(basePath, family, qf, 13, Type.Delete);
    listFiles();

    // set the mob file compaction batch size
    conf.setInt(MobConstants.MOB_FILE_COMPACTION_BATCH_SIZE, 4);
    testCompactDelFiles(tableName, 1, 13, false);
  }

  @Test
  public void testCompactDelFilesChangeMaxDelFileCount() throws Exception {
    resetConf();
    String tableName = "testCompactDelFilesWithSmallBatchSize";
    init(tableName);
    // create 20 mob files.
    createStoreFiles(basePath, family, qf, 20, Type.Put);
    // create 13 del files
    createStoreFiles(basePath, family, qf, 13, Type.Delete);
    listFiles();

    // set the max del file count
    conf.setInt(MobConstants.MOB_DELFILE_MAX_COUNT, 5);
    // set the mob file compaction batch size
    conf.setInt(MobConstants.MOB_FILE_COMPACTION_BATCH_SIZE, 2);
    testCompactDelFiles(tableName, 4, 13, false);
  }

  /**
   * Tests the selectFiles
   * @param tableName the table name
   * @param type the expected compaction type
   * @param expected the expected start keys
   */
  private void testSelectFiles(String tableName, final CompactionType type,
    final boolean isForceAllFiles, final List<String> expected) throws IOException {
    PartitionedMobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs,
      TableName.valueOf(tableName), hcd, pool) {
      @Override
      public List<Path> compact(List<FileStatus> files, boolean isForceAllFiles)
        throws IOException {
        if (files == null || files.isEmpty()) {
          return null;
        }
        PartitionedMobFileCompactionRequest request = select(files, isForceAllFiles);
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
   */
  private void testCompactDelFiles(String tableName, final int expectedFileCount,
      final int expectedCellCount, boolean isForceAllFiles) throws IOException {
    PartitionedMobFileCompactor compactor = new PartitionedMobFileCompactor(conf, fs,
      TableName.valueOf(tableName), hcd, pool) {
      @Override
      protected List<Path> performCompaction(PartitionedMobFileCompactionRequest request)
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
      Type type) throws IOException {
    HFileContext meta = new HFileContextBuilder().withBlockSize(8 * 1024).build();
    String startKey = "row_";
    MobFileName mobFileName = null;
    for (int i = 0; i < count; i++) {
      byte[] startRow = Bytes.toBytes(startKey + i) ;
      if(type.equals(Type.Delete)) {
        mobFileName = MobFileName.create(startRow, MobUtils.formatDate(
            new Date()), delSuffix);
      }
      if(type.equals(Type.Put)){
        mobFileName = MobFileName.create(Bytes.toBytes(startKey + i), MobUtils.formatDate(
            new Date()), mobSuffix);
      }
      StoreFile.Writer mobFileWriter = new StoreFile.WriterBuilder(conf, cacheConf, fs)
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
  private static void writeStoreFile(final StoreFile.Writer writer, byte[]row, byte[] family,
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
        false, null, HConstants.LATEST_TIMESTAMP);
    Scan scan = new Scan();
    scan.setMaxVersions(hcd.getMaxVersions());
    long timeToPurgeDeletes = Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
    long ttl = HStore.determineTTLFromFamily(hcd);
    ScanInfo scanInfo = new ScanInfo(hcd, ttl, timeToPurgeDeletes, KeyValue.COMPARATOR);
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
    conf.setLong(MobConstants.MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_MERGEABLE_THRESHOLD);
    conf.setInt(MobConstants.MOB_DELFILE_MAX_COUNT, MobConstants.DEFAULT_MOB_DELFILE_MAX_COUNT);
    conf.setInt(MobConstants.MOB_FILE_COMPACTION_BATCH_SIZE,
      MobConstants.DEFAULT_MOB_FILE_COMPACTION_BATCH_SIZE);
  }
}