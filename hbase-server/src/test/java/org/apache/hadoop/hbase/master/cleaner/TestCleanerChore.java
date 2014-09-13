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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category({MasterTests.class, SmallTests.class})
public class TestCleanerChore {

  private static final Log LOG = LogFactory.getLog(TestCleanerChore.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @After
  public void cleanup() throws Exception {
    // delete and recreate the test directory, ensuring a clean test dir between tests
    UTIL.cleanupTestDir();
}


  @Test
  public void testSavesFilesOnRequest() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, NeverDelete.class.getName());

    AllValidPaths chore = new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey);

    // create the directory layout in the directory to clean
    Path parent = new Path(testDir, "parent");
    Path file = new Path(parent, "someFile");
    fs.mkdirs(parent);
    // touch a new file
    fs.create(file).close();
    assertTrue("Test file didn't get created.", fs.exists(file));

    // run the chore
    chore.chore();

    // verify all the files got deleted
    assertTrue("File didn't get deleted", fs.exists(file));
    assertTrue("Empty directory didn't get deleted", fs.exists(parent));
  }

  @Test
  public void testDeletesEmptyDirectories() throws Exception {
    Stoppable stop = new StoppableImplementation();
    Configuration conf = UTIL.getConfiguration();
    Path testDir = UTIL.getDataTestDir();
    FileSystem fs = UTIL.getTestFileSystem();
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore = new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey);

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

    AllValidPaths chore = new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey);
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

    AllValidPaths chore = new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey);

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

    AllValidPaths chore = new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey);
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
        FSUtils.logFileSystemState(fs, testDir, LOG);
        return (Boolean) invocation.callRealMethod();
      }
    }).when(spy).isFileDeletable(Mockito.any(FileStatus.class));

    // run the chore
    chore.chore();

    // make sure all the directories + added file exist, but the original file is deleted
    assertTrue("Added file unexpectedly deleted", fs.exists(addedFile));
    assertTrue("Parent directory deleted unexpectedly", fs.exists(parent));
    assertFalse("Original file unexpectedly retained", fs.exists(file));
    Mockito.verify(spy, Mockito.times(1)).isFileDeletable(Mockito.any(FileStatus.class));
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
    Stoppable stop = new StoppableImplementation();
    // need to use a localutil to not break the rest of the test that runs on the local FS, which
    // gets hosed when we start to use a minicluster.
    HBaseTestingUtility localUtil = new HBaseTestingUtility();
    Configuration conf = localUtil.getConfiguration();
    final Path testDir = UTIL.getDataTestDir();
    final FileSystem fs = UTIL.getTestFileSystem();
    LOG.debug("Writing test data to: " + testDir);
    String confKey = "hbase.test.cleaner.delegates";
    conf.set(confKey, AlwaysDelete.class.getName());

    AllValidPaths chore = new AllValidPaths("test-file-cleaner", stop, conf, fs, testDir, confKey);
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
        FSUtils.logFileSystemState(fs, testDir, LOG);
        return (Boolean) invocation.callRealMethod();
      }
    }).when(spy).isFileDeletable(Mockito.any(FileStatus.class));

    // attempt to delete the directory, which
    if (chore.checkAndDeleteDirectory(parent)) {
      throw new Exception(
          "Reported success deleting directory, should have failed when adding file mid-iteration");
    }

    // make sure all the directories + added file exist, but the original file is deleted
    assertTrue("Added file unexpectedly deleted", fs.exists(racyFile));
    assertTrue("Parent directory deleted unexpectedly", fs.exists(parent));
    assertFalse("Original file unexpectedly retained", fs.exists(file));
    Mockito.verify(spy, Mockito.times(1)).isFileDeletable(Mockito.any(FileStatus.class));
  }

  private static class AllValidPaths extends CleanerChore<BaseHFileCleanerDelegate> {

    public AllValidPaths(String name, Stoppable s, Configuration conf, FileSystem fs,
        Path oldFileDir, String confkey) {
      super(name, Integer.MAX_VALUE, s, conf, fs, oldFileDir, confkey);
    }

    // all paths are valid
    @Override
    protected boolean validate(Path file) {
      return true;
    }
  };

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
