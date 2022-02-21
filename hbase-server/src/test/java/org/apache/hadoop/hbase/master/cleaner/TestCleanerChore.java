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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, SmallTests.class})
public class TestCleanerChore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCleanerChore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestCleanerChore.class);
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static DirScanPool POOL;

  @BeforeClass
  public static void setup() {
    POOL = DirScanPool.getHFileCleanerScanPool(UTIL.getConfiguration());
  }

  @AfterClass
  public static void cleanup() throws Exception {
    // delete and recreate the test directory, ensuring a clean test dir between tests
    UTIL.cleanupTestDir();
    POOL.shutdownNow();
  }

  @Test
  public void testSavesFilesOnRequest() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, NeverDelete.class.getName());

    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);

    // create the directory layout in the directory to clean
    Path parent = new Path(testDir, "parent");
    Path file = new Path(parent, "someFile");
    fs.mkdirs(parent);
    // touch a new file
    fs.create(file).close();
    assertTrue("Test file didn't get created.", fs.exists(file));

    // run the chore
    chore.chore();

    // verify all the files were preserved
    assertTrue("File shouldn't have been deleted", fs.exists(file));
    assertTrue("directory shouldn't have been deleted", fs.exists(parent));
  }

  @Test
  public void retriesIOExceptionInStatus() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";

    Path child = new Path(testDir, "child");
    Path file = new Path(child, "file");
    fs.mkdirs(child);
    fs.create(file).close();
    assertTrue("test file didn't get created.", fs.exists(file));
    final AtomicBoolean fails = new AtomicBoolean(true);

    FilterFileSystem filtered = new FilterFileSystem(fs) {
      public FileStatus[] listStatus(Path f) throws IOException {
        if (fails.get()) {
          throw new IOException("whomp whomp.");
        }
        return fs.listStatus(f);
      }
    };

    AllValidPaths chore =
      new AllValidPaths("test-retry-ioe", stop, conf, filtered, testDir, confKey, POOL);

    // trouble talking to the filesystem
    Boolean result = chore.runCleaner();

    // verify that it couldn't clean the files.
    assertTrue("test rig failed to inject failure.", fs.exists(file));
    assertTrue("test rig failed to inject failure.", fs.exists(child));
    // and verify that it accurately reported the failure.
    assertFalse("chore should report that it failed.", result);

    // filesystem is back
    fails.set(false);
    result = chore.runCleaner();

    // verify everything is gone.
    assertFalse("file should have been destroyed.", fs.exists(file));
    assertFalse("directory should have been destroyed.", fs.exists(child));
    // and verify that it accurately reported success.
    assertTrue("chore should claim it succeeded.", result);
  }

  @Test
  public void testDeletesEmptyDirectories() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);

    // create the directory layout in the directory to clean
    Path parent = new Path(testDir, "parent");
    Path child = new Path(parent, "child");
    Path emptyChild = new Path(parent, "emptyChild");
    Path file = new Path(child, "someFile");
    fs.mkdirs(child);
    fs.mkdirs(emptyChild);
    // touch a new file
    fs.create(file).close();
    // also create a file in the top level directory
    Path topFile = new Path(testDir, "topFile");
    fs.create(topFile).close();
    assertTrue("Test file didn't get created.", fs.exists(file));
    assertTrue("Test file didn't get created.", fs.exists(topFile));

    // run the chore
    chore.chore();

    // verify all the files got deleted
    assertFalse("File didn't get deleted", fs.exists(topFile));
    assertFalse("File didn't get deleted", fs.exists(file));
    assertFalse("Empty directory didn't get deleted", fs.exists(child));
    assertFalse("Empty directory didn't get deleted", fs.exists(parent));
  }

  /**
   * Test to make sure that we don't attempt to ask the delegate whether or not we should preserve a
   * directory.
   * @throws Exception on failure
   */
  @Test
  public void testDoesNotCheckDirectories() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);
    // spy on the delegate to ensure that we don't check for directories
    AlwaysDelete delegate = (AlwaysDelete) chore.cleanersChain.get(0);
    AlwaysDelete spy = Mockito.spy(delegate);
    chore.cleanersChain.set(0, spy);

    // create the directory layout in the directory to clean
    Path parent = new Path(testDir, "parent");
    Path file = new Path(parent, "someFile");
    fs.mkdirs(parent);
    assertTrue("Test parent didn't get created.", fs.exists(parent));
    // touch a new file
    fs.create(file).close();
    assertTrue("Test file didn't get created.", fs.exists(file));

    FileStatus fStat = fs.getFileStatus(parent);
    chore.chore();
    // make sure we never checked the directory
    Mockito.verify(spy, Mockito.never()).isFileDeletable(fStat);
    Mockito.reset(spy);
  }

  @Test
  public void testStoppedCleanerDoesNotDeleteFiles() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);

    // also create a file in the top level directory
    Path topFile = new Path(testDir, "topFile");
    fs.create(topFile).close();
    assertTrue("Test file didn't get created.", fs.exists(topFile));

    // stop the chore
    stop.stop("testing stop");

    // run the chore
    chore.chore();

    // test that the file still exists
    assertTrue("File got deleted while chore was stopped", fs.exists(topFile));
  }

  /**
   * While cleaning a directory, all the files in the directory may be deleted, but there may be
   * another file added, in which case the directory shouldn't be deleted.
   * @throws IOException on failure
   */
  @Test
  public void testCleanerDoesNotDeleteDirectoryWithLateAddedFiles() throws IOException {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    final Path testDir = UTIL.getDataTestDir();
    final FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);
    // spy on the delegate to ensure that we don't check for directories
    AlwaysDelete delegate = (AlwaysDelete) chore.cleanersChain.get(0);
    AlwaysDelete spy = Mockito.spy(delegate);
    chore.cleanersChain.set(0, spy);

    // create the directory layout in the directory to clean
    final Path parent = new Path(testDir, "parent");
    Path file = new Path(parent, "someFile");
    fs.mkdirs(parent);
    // touch a new file
    fs.create(file).close();
    assertTrue("Test file didn't get created.", fs.exists(file));
    final Path addedFile = new Path(parent, "addedFile");

    // when we attempt to delete the original file, add another file in the same directory
    Mockito.doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        fs.create(addedFile).close();
        CommonFSUtils.logFileSystemState(fs, testDir, LOG);
        return (Boolean) invocation.callRealMethod();
      }
    }).when(spy).isFileDeletable(Mockito.any());

    // run the chore
    chore.chore();

    // make sure all the directories + added file exist, but the original file is deleted
    assertTrue("Added file unexpectedly deleted", fs.exists(addedFile));
    assertTrue("Parent directory deleted unexpectedly", fs.exists(parent));
    assertFalse("Original file unexpectedly retained", fs.exists(file));
    Mockito.verify(spy, Mockito.times(1)).isFileDeletable(Mockito.any());
    Mockito.reset(spy);
  }

  /**
   * The cleaner runs in a loop, where it first checks to see all the files under a directory can be
   * deleted. If they all can, then we try to delete the directory. However, a file may be added
   * that directory to after the original check. This ensures that we don't accidentally delete that
   * directory on and don't get spurious IOExceptions.
   * <p>
   * This was from HBASE-7465.
   * @throws Exception on failure
   */
  @Test
  public void testNoExceptionFromDirectoryWithRacyChildren() throws Exception {
    UTIL.cleanupTestDir();
    Stoppable stop = new StoppableImplementation();
    // need to use a localutil to not break the rest of the test that runs on the local FS, which
    // gets hosed when we start to use a minicluster.
    HBaseTestingUtil localUtil = new HBaseTestingUtil();
    Configuration conf = localUtil.getConfiguration();
    final Path testDir = UTIL.getDataTestDir();
    final FileSystem fs = UTIL.getTestFileSystem();
    LOG.debug("Writing test data to: " + testDir);
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);
    // spy on the delegate to ensure that we don't check for directories
    AlwaysDelete delegate = (AlwaysDelete) chore.cleanersChain.get(0);
    AlwaysDelete spy = Mockito.spy(delegate);
    chore.cleanersChain.set(0, spy);

    // create the directory layout in the directory to clean
    final Path parent = new Path(testDir, "parent");
    Path file = new Path(parent, "someFile");
    fs.mkdirs(parent);
    // touch a new file
    fs.create(file).close();
    assertTrue("Test file didn't get created.", fs.exists(file));
    final Path racyFile = new Path(parent, "addedFile");

    // when we attempt to delete the original file, add another file in the same directory
    Mockito.doAnswer(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        fs.create(racyFile).close();
        CommonFSUtils.logFileSystemState(fs, testDir, LOG);
        return (Boolean) invocation.callRealMethod();
      }
    }).when(spy).isFileDeletable(Mockito.any());

    // run the chore
    chore.chore();

    // make sure all the directories + added file exist, but the original file is deleted
    assertTrue("Added file unexpectedly deleted", fs.exists(racyFile));
    assertTrue("Parent directory deleted unexpectedly", fs.exists(parent));
    assertFalse("Original file unexpectedly retained", fs.exists(file));
    Mockito.verify(spy, Mockito.times(1)).isFileDeletable(Mockito.any());
  }

  @Test
  public void testDeleteFileWithCleanerEnabled() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);

    // Enable cleaner
    chore.setEnabled(true);

    // create the directory layout in the directory to clean
    Path parent = new Path(testDir, "parent");
    Path child = new Path(parent, "child");
    Path file = new Path(child, "someFile");
    fs.mkdirs(child);

    // touch a new file
    fs.create(file).close();
    assertTrue("Test file didn't get created.", fs.exists(file));

    // run the chore
    chore.chore();

    // verify all the files got deleted
    assertFalse("File didn't get deleted", fs.exists(file));
    assertFalse("Empty directory didn't get deleted", fs.exists(child));
    assertFalse("Empty directory didn't get deleted", fs.exists(parent));
  }

  @Test
  public void testDeleteFileWithCleanerDisabled() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);

    // Disable cleaner
    chore.setEnabled(false);

    // create the directory layout in the directory to clean
    Path parent = new Path(testDir, "parent");
    Path child = new Path(parent, "child");
    Path file = new Path(child, "someFile");
    fs.mkdirs(child);

    // touch a new file
    fs.create(file).close();
    assertTrue("Test file didn't get created.", fs.exists(file));

    // run the chore
    chore.chore();

    // verify all the files exist
    assertTrue("File got deleted with cleaner disabled", fs.exists(file));
    assertTrue("Directory got deleted", fs.exists(child));
    assertTrue("Directory got deleted", fs.exists(parent));
  }

  @Test
  public void testOnConfigurationChange() throws Exception {
    int availableProcessorNum = Runtime.getRuntime().availableProcessors();
    if (availableProcessorNum == 1) { // no need to run this test
      return;
    }

    // have at least 2 available processors/cores
    int initPoolSize = availableProcessorNum / 2;
    int changedPoolSize = availableProcessorNum;

    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());
    conf.set(CleanerChore.CHORE_POOL_SIZE, String.valueOf(initPoolSize));
    AllValidPaths chore =
      new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, POOL);
    chore.setEnabled(true);
    // Create subdirs under testDir
    int dirNums = 6;
    Path[] subdirs = new Path[dirNums];
    for (int i = 0; i < dirNums; i++) {
      subdirs[i] = new Path(testDir, "subdir-" + i);
      fs.mkdirs(subdirs[i]);
    }
    // Under each subdirs create 6 files
    for (Path subdir : subdirs) {
      createFiles(fs, subdir, 6);
    }
    // Start chore
    Thread t = new Thread(() -> chore.chore());
    t.setDaemon(true);
    t.start();
    // Change size of chore's pool
    conf.set(CleanerChore.CHORE_POOL_SIZE, String.valueOf(changedPoolSize));
    POOL.onConfigurationChange(conf);
    assertEquals(changedPoolSize, chore.getChorePoolSize());
    // Stop chore
    t.join();
  }

  @Test
  public void testOnConfigurationChangeLogCleaner() throws Exception {
    int availableProcessorNum = Runtime.getRuntime().availableProcessors();
    if (availableProcessorNum == 1) { // no need to run this test
      return;
    }

    DirScanPool pool = DirScanPool.getLogCleanerScanPool(UTIL.getConfiguration());

    // have at least 2 available processors/cores
    int initPoolSize = availableProcessorNum / 2;
    int changedPoolSize = availableProcessorNum;

    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());
    conf.set(CleanerChore.LOG_CLEANER_CHORE_SIZE, String.valueOf(initPoolSize));
    final AllValidPaths chore =
        new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey, pool);
    chore.setEnabled(true);
    // Create subdirs under testDir
    int dirNums = 6;
    Path[] subdirs = new Path[dirNums];
    for (int i = 0; i < dirNums; i++) {
      subdirs[i] = new Path(testDir, "subdir-" + i);
      fs.mkdirs(subdirs[i]);
    }
    // Under each subdirs create 6 files
    for (Path subdir : subdirs) {
      createFiles(fs, subdir, 6);
    }
    // Start chore
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        chore.chore();
      }
    });
    t.setDaemon(true);
    t.start();
    // Change size of chore's pool
    conf.set(CleanerChore.LOG_CLEANER_CHORE_SIZE, String.valueOf(changedPoolSize));
    pool.onConfigurationChange(conf);
    assertEquals(changedPoolSize, chore.getChorePoolSize());
    // Stop chore
    t.join();
  }

  @Test
  public void testMinimumNumberOfThreads() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());
    conf.set(CleanerChore.CHORE_POOL_SIZE, "2");
    int numProcs = Runtime.getRuntime().availableProcessors();
    // Sanity
    assertEquals(numProcs, CleanerChore.calculatePoolSize(Integer.toString(numProcs)));
    // The implementation does not allow us to set more threads than we have processors
    assertEquals(numProcs, CleanerChore.calculatePoolSize(Integer.toString(numProcs + 2)));
    // Force us into the branch that is multiplying 0.0 against the number of processors
    assertEquals(1, CleanerChore.calculatePoolSize("0.0"));
  }

  private void createFiles(FileSystem fs, Path parentDir, int numOfFiles) throws IOException {
    for (int i = 0; i < numOfFiles; i++) {
      int xMega = 1 + ThreadLocalRandom.current().nextInt(3); // size of each file is between 1~3M
      try (FSDataOutputStream fsdos = fs.create(new Path(parentDir, "file-" + i))) {
        for (int m = 0; m < xMega; m++) {
          byte[] M = new byte[1024 * 1024];
          Bytes.random(M);
          fsdos.write(M);
        }
      }
    }
  }

  private static class AllValidPaths extends CleanerChore<BaseHFileCleanerDelegate> {

    public AllValidPaths(String name, Stoppable s, Configuration conf, FileSystem fs,
      Path oldFileDir, String confkey, DirScanPool pool) {
      super(name, Integer.MAX_VALUE, s, conf, fs, oldFileDir, confkey, pool);
    }

    // all paths are valid
    @Override
    protected boolean validate(Path file) {
      return true;
    }
  }

  public static class AlwaysDelete extends BaseHFileCleanerDelegate {
    @Override
    public boolean isFileDeletable(FileStatus fStat) {
      return true;
    }
  }

  public static class NeverDelete extends BaseHFileCleanerDelegate {
    @Override
    public boolean isFileDeletable(FileStatus fStat) {
      return false;
    }
  }
}
