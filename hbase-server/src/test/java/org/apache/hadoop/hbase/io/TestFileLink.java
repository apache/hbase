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

package org.apache.hadoop.hbase.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that FileLink switches between alternate locations
 * when the current location moves or gets deleted.
 */
@Category(MediumTests.class)
public class TestFileLink {
  /**
   * Test, on HDFS, that the FileLink is still readable
   * even when the current file gets renamed.
   */
  @Test
  public void testHDFSLinkReadDuringRename() throws Exception {
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    Configuration conf = testUtil.getConfiguration();
    conf.setInt("dfs.blocksize", 1024 * 1024);
    conf.setInt("dfs.client.read.prefetch.size", 2 * 1024 * 1024);

    testUtil.startMiniDFSCluster(1);
    MiniDFSCluster cluster = testUtil.getDFSCluster();
    FileSystem fs = cluster.getFileSystem();
    assertEquals("hdfs", fs.getUri().getScheme());

    try {
      testLinkReadDuringRename(fs, testUtil.getDefaultRootDirPath());
    } finally {
      testUtil.shutdownMiniCluster();
    }
  }

  /**
   * Test, on a local filesystem, that the FileLink is still readable
   * even when the current file gets renamed.
   */
  @Test
  public void testLocalLinkReadDuringRename() throws IOException {
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    FileSystem fs = testUtil.getTestFileSystem();
    assertEquals("file", fs.getUri().getScheme());
    testLinkReadDuringRename(fs, testUtil.getDataTestDir());
  }

  /**
   * Test that link is still readable even when the current file gets renamed.
   */
  private void testLinkReadDuringRename(FileSystem fs, Path rootDir) throws IOException {
    Path originalPath = new Path(rootDir, "test.file");
    Path archivedPath = new Path(rootDir, "archived.file");

    writeSomeData(fs, originalPath, 256 << 20, (byte)2);

    List<Path> files = new ArrayList<Path>();
    files.add(originalPath);
    files.add(archivedPath);

    FileLink link = new FileLink(files);
    FSDataInputStream in = link.open(fs);
    try {
      byte[] data = new byte[8192];
      long size = 0;

      // Read from origin
      int n = in.read(data);
      dataVerify(data, n, (byte)2);
      size += n;

      if (FSUtils.WINDOWS) {
        in.close();
      }

      // Move origin to archive
      assertFalse(fs.exists(archivedPath));
      fs.rename(originalPath, archivedPath);
      assertFalse(fs.exists(originalPath));
      assertTrue(fs.exists(archivedPath));

      if (FSUtils.WINDOWS) {
        in = link.open(fs); // re-read from beginning
        in.read(data);
      }

      // Try to read to the end
      while ((n = in.read(data)) > 0) {
        dataVerify(data, n, (byte)2);
        size += n;
      }

      assertEquals(256 << 20, size);
    } finally {
      in.close();
      if (fs.exists(originalPath)) fs.delete(originalPath, true);
      if (fs.exists(archivedPath)) fs.delete(archivedPath, true);
    }
  }

  /**
   * Test that link is still readable even when the current file gets deleted.
   *
   * NOTE: This test is valid only on HDFS.
   * When a file is deleted from a local file-system, it is simply 'unlinked'.
   * The inode, which contains the file's data, is not deleted until all
   * processes have finished with it.
   * In HDFS when the request exceed the cached block locations,
   * a query to the namenode is performed, using the filename,
   * and the deleted file doesn't exists anymore (FileNotFoundException).
   */
  @Test
  public void testHDFSLinkReadDuringDelete() throws Exception {
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    Configuration conf = testUtil.getConfiguration();
    conf.setInt("dfs.blocksize", 1024 * 1024);
    conf.setInt("dfs.client.read.prefetch.size", 2 * 1024 * 1024);

    testUtil.startMiniDFSCluster(1);
    MiniDFSCluster cluster = testUtil.getDFSCluster();
    FileSystem fs = cluster.getFileSystem();
    assertEquals("hdfs", fs.getUri().getScheme());

    try {
      List<Path> files = new ArrayList<Path>();
      for (int i = 0; i < 3; i++) {
        Path path = new Path(String.format("test-data-%d", i));
        writeSomeData(fs, path, 1 << 20, (byte)i);
        files.add(path);
      }

      FileLink link = new FileLink(files);
      FSDataInputStream in = link.open(fs);
      try {
        byte[] data = new byte[8192];
        int n;

        // Switch to file 1
        n = in.read(data);
        dataVerify(data, n, (byte)0);
        fs.delete(files.get(0), true);
        skipBuffer(in, (byte)0);

        // Switch to file 2
        n = in.read(data);
        dataVerify(data, n, (byte)1);
        fs.delete(files.get(1), true);
        skipBuffer(in, (byte)1);

        // Switch to file 3
        n = in.read(data);
        dataVerify(data, n, (byte)2);
        fs.delete(files.get(2), true);
        skipBuffer(in, (byte)2);

        // No more files available
        try {
          n = in.read(data);
          assert(n <= 0);
        } catch (FileNotFoundException e) {
          assertTrue(true);
        }
      } finally {
        in.close();
      }
    } finally {
      testUtil.shutdownMiniCluster();
    }
  }

  /**
   * Write up to 'size' bytes with value 'v' into a new file called 'path'.
   */
  private void writeSomeData (FileSystem fs, Path path, long size, byte v) throws IOException {
    byte[] data = new byte[4096];
    for (int i = 0; i < data.length; i++) {
      data[i] = v;
    }

    FSDataOutputStream stream = fs.create(path);
    try {
      long written = 0;
      while (written < size) {
        stream.write(data, 0, data.length);
        written += data.length;
      }
    } finally {
      stream.close();
    }
  }

  /**
   * Verify that all bytes in 'data' have 'v' as value.
   */
  private static void dataVerify(byte[] data, int n, byte v) {
    for (int i = 0; i < n; ++i) {
      assertEquals(v, data[i]);
    }
  }

  private static void skipBuffer(FSDataInputStream in, byte v) throws IOException {
    byte[] data = new byte[8192];
    try {
      int n;
      while ((n = in.read(data)) == data.length) {
        for (int i = 0; i < data.length; ++i) {
          if (data[i] != v)
            throw new Exception("File changed");
        }
      }
    } catch (Exception e) {
    }
  }
}
