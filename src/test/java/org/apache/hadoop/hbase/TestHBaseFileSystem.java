/*
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.util.Progressable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestHBaseFileSystem {
  public static final Log LOG = LogFactory.getLog(TestHBaseFileSystem.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("dfs.support.append", true);
    // The below config supported by 0.20-append and CDH3b2
    conf.setInt("dfs.client.block.recovery.retries", 2);
    TEST_UTIL.startMiniDFSCluster(3);
    Path hbaseRootDir =
      TEST_UTIL.getDFSCluster().getFileSystem().makeQualified(new Path("/hbase"));
    LOG.info("hbase.rootdir=" + hbaseRootDir);
    conf.set(HConstants.HBASE_DIR, hbaseRootDir.toString());
    conf.setInt("hdfs.client.retries.number", 10);
    HBaseFileSystem.setRetryCounts(conf);
  }

  
  @Test
  public void testNonIdempotentOpsWithRetries() throws IOException {
    LOG.info("testNonIdempotentOpsWithRetries");

    Path rootDir = new Path(TestHBaseFileSystem.conf.get(HConstants.HBASE_DIR));
    FileSystem fs = TEST_UTIL.getTestFileSystem();
    // Create a Region
    assertTrue(HBaseFileSystem.createPathOnFileSystem(fs, rootDir, 
      true) != null);

    boolean result = HBaseFileSystem.makeDirOnFileSystem(new MockFileSystemForCreate(), 
      new Path("/a"));
    assertTrue("Couldn't create the directory", result);

    try {
      HBaseFileSystem.createPathOnFileSystem(new MockFileSystemForCreate(), 
        new Path("/A"), false);
     assertTrue(false);// control should not come here.
    } catch (Exception e) {
      LOG.info(e);
    }

    result = HBaseFileSystem.renameDirForFileSystem(new MockFileSystem(), new Path("/a"),
      new Path("/b"));
    assertTrue("Couldn't rename the directory", result);

    result = HBaseFileSystem.deleteDirFromFileSystem(new MockFileSystem(), 
      new Path("/a"));

    assertTrue("Couldn't delete the directory", result);
    fs.delete(rootDir, true);
  }
  
  @Test
  public void testRenameAndSetModifyTime() throws Exception {
    assertTrue(FSUtils.isHDFS(conf));

    FileSystem fs = FileSystem.get(conf);
    Path testDir = TEST_UTIL.getDataTestDir("testArchiveFile");

    String file = UUID.randomUUID().toString();
    Path p = new Path(testDir, file);

    FSDataOutputStream out = fs.create(p);
    out.close();
    assertTrue("The created file should be present", FSUtils.isExists(fs, p));

    long expect = System.currentTimeMillis() + 1000;
    assertFalse(expect == fs.getFileStatus(p).getModificationTime());

    ManualEnvironmentEdge mockEnv = new ManualEnvironmentEdge();
    mockEnv.setValue(expect);
    EnvironmentEdgeManager.injectEdge(mockEnv);

    String dstFile = UUID.randomUUID().toString();
    Path dst = new Path(testDir , dstFile);

    assertTrue(HBaseFileSystem.renameAndSetModifyTime(fs, p, dst));
    assertFalse("The moved file should not be present", FSUtils.isExists(fs,
                                                                         p));
    assertTrue("The dst file should be present", FSUtils.isExists(fs, dst));

    assertEquals(expect, fs.getFileStatus(dst).getModificationTime());
  }

  
  static class MockFileSystemForCreate extends MockFileSystem {
    @Override
    public boolean exists(Path path) {
      if ("/A".equals(path.toString())) return true;
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

}
