/*
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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.fs.RegionStorage;
import org.apache.hadoop.hbase.fs.FSUtilsWithRetries;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.Progressable;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHRegionStorage {
  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestHRegionStorage.class);

  @Test
  public void testOnDiskRegionCreation() throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("testOnDiskRegionCreation");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();

    // Create a Region
    HRegionInfo hri = new HRegionInfo(TableName.valueOf("TestTable"));
    RegionStorage regionFs = RegionStorage.open(conf, fs, rootDir, hri, true);

    // Verify if the region is on disk
    Path regionDir = regionFs.getRegionDir();
    assertTrue("The region folder should be created", fs.exists(regionDir));

    // Verify the .regioninfo
    HRegionInfo hriVerify = RegionStorage.open(conf, regionDir, false).getRegionInfo();
    assertEquals(hri, hriVerify);

    // Open the region
    regionFs = RegionStorage.open(conf, fs, rootDir, hri, false);
    assertEquals(regionDir, regionFs.getRegionDir());

    // Delete the region
    RegionStorage.destroy(conf, fs, rootDir, hri);
    assertFalse("The region folder should be removed", fs.exists(regionDir));

    fs.delete(rootDir, true);
  }

  @Test
  public void testNonIdempotentOpsWithRetries() throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("testOnDiskRegionCreation");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();

    FSUtilsWithRetries regionFs = new FSUtilsWithRetries(conf, new MockFileSystemForCreate());
    boolean result = regionFs.createDir(new Path("/foo/bar"));
    assertTrue("Couldn't create the directory", result);

    regionFs = new FSUtilsWithRetries(conf, new MockFileSystem());
    result = regionFs.rename(new Path("/foo/bar"), new Path("/foo/bar2"));
    assertTrue("Couldn't rename the directory", result);

    regionFs = new FSUtilsWithRetries(conf, new MockFileSystem());
    result = regionFs.deleteDir(new Path("/foo/bar"));
    assertTrue("Couldn't delete the directory", result);
    fs.delete(rootDir, true);
  }

  static class MockFileSystemForCreate extends MockFileSystem {
    @Override
    public boolean exists(Path path) {
      return false;
    }
  }

  /**
   * a mock fs which throws exception for first 3 times, and then process the call (returns the
   * excepted result).
   */
  static class MockFileSystem extends FileSystem {
    int retryCount;
    final static int successRetryCount = 3;

    public MockFileSystem() {
      retryCount = 0;
    }

    @Override
    public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2) throws IOException {
      throw new IOException("");
    }

    @Override
    public FSDataOutputStream create(Path arg0, FsPermission arg1, boolean arg2, int arg3,
        short arg4, long arg5, Progressable arg6) throws IOException {
      LOG.debug("Create, " + retryCount);
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return null;
    }

    @Override
    public boolean delete(Path arg0) throws IOException {
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return true;
    }

    @Override
    public boolean delete(Path arg0, boolean arg1) throws IOException {
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return true;
    }

    @Override
    public FileStatus getFileStatus(Path arg0) throws IOException {
      FileStatus fs = new FileStatus();
      return fs;
    }

    @Override
    public boolean exists(Path path) {
      return true;
    }

    @Override
    public URI getUri() {
      throw new RuntimeException("Something bad happen");
    }

    @Override
    public Path getWorkingDirectory() {
      throw new RuntimeException("Something bad happen");
    }

    @Override
    public FileStatus[] listStatus(Path arg0) throws IOException {
      throw new IOException("Something bad happen");
    }

    @Override
    public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
      LOG.debug("mkdirs, " + retryCount);
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return true;
    }

    @Override
    public FSDataInputStream open(Path arg0, int arg1) throws IOException {
      throw new IOException("Something bad happen");
    }

    @Override
    public boolean rename(Path arg0, Path arg1) throws IOException {
      LOG.debug("rename, " + retryCount);
      if (retryCount++ < successRetryCount) throw new IOException("Something bad happen");
      return true;
    }

    @Override
    public void setWorkingDirectory(Path arg0) {
      throw new RuntimeException("Something bad happen");
    }
  }

  @Test
  public void testTempAndCommit() throws IOException {
    Path rootDir = TEST_UTIL.getDataTestDirOnTestFS("testTempAndCommit");
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    Configuration conf = TEST_UTIL.getConfiguration();

    // Create a Region
    String familyName = "cf";
    HRegionInfo hri = new HRegionInfo(TableName.valueOf("TestTable"));
    RegionStorage regionFs = RegionStorage.open(conf, fs, rootDir, hri, true);

    // New region, no store files
    Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(familyName);
    assertEquals(0, storeFiles != null ? storeFiles.size() : 0);

    // Create a new file in temp (no files in the family)
    Path buildPath = regionFs.createTempName();
    fs.createNewFile(buildPath);
    storeFiles = regionFs.getStoreFiles(familyName);
    assertEquals(0, storeFiles != null ? storeFiles.size() : 0);

    // commit the file
    Path dstPath = regionFs.commitStoreFile(familyName, buildPath);
    storeFiles = regionFs.getStoreFiles(familyName);
    assertEquals(0, storeFiles != null ? storeFiles.size() : 0);
    assertFalse(fs.exists(buildPath));

    fs.delete(rootDir, true);
  }
}
