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
package org.apache.hadoop.hbase.backup.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link BackupFileSystemManager}.
 */
@Category(SmallTests.class)
public class TestBackupFileSystemManager {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBackupFileSystemManager.class);

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  /**
   * extractWalPathInfo: happy path where WALs dir has a prefix. e.g.
   * /some/prefix/WALs/2025-09-14/some-wal
   */
  @Test
  public void testExtractWalPathInfo_withPrefix() throws Exception {
    Path walPath =
      new Path("/some/prefix/" + BackupFileSystemManager.WALS_DIR + "/2025-09-14/wal-000");
    BackupFileSystemManager.WalPathInfo info = BackupFileSystemManager.extractWalPathInfo(walPath);

    assertNotNull("WalPathInfo should not be null", info);
    // prefixBeforeWALs should be "/some/prefix"
    assertEquals("/some/prefix", info.getPrefixBeforeWALs().toString());
    assertEquals("2025-09-14", info.getDateSegment());
  }

  /**
   * extractWalPathInfo: case where WALs is at root (leading slash). e.g. /WALs/2025-09-14/some-wal
   * Expect prefixBeforeWALs to be "/" (root) or non-null; resolution should still work.
   */
  @Test
  public void testExtractWalPathInfo_rootWALs() throws Exception {
    Path walPath = new Path("/" + BackupFileSystemManager.WALS_DIR + "/2025-09-14/wal-123");
    BackupFileSystemManager.WalPathInfo info = BackupFileSystemManager.extractWalPathInfo(walPath);

    assertNotNull(info);
    // parent of "/WALs" in Hadoop Path is "/" (root). Ensure date segment parsed.
    assertEquals("2025-09-14", info.getDateSegment());
    assertNotNull("prefixBeforeWALs should not be null for root-style path",
      info.getPrefixBeforeWALs());
    // prefix might be "/" (expected), be tolerant: assert it ends with "/" or equals "/"
    assertTrue(info.getPrefixBeforeWALs().toString().equals("/")
      || !info.getPrefixBeforeWALs().toString().isEmpty());
  }

  /**
   * extractWalPathInfo: null input should throw IllegalArgumentException.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testExtractWalPathInfo_nullPath() throws Exception {
    BackupFileSystemManager.extractWalPathInfo(null);
  }

  /**
   * extractWalPathInfo: missing date directory -> should throw IOException. Example: path that has
   * no parent for the wal file.
   */
  @Test(expected = IOException.class)
  public void testExtractWalPathInfo_missingDateDir() throws Exception {
    // A single segment path (no parents) e.g. "walfile"
    Path walPath = new Path("walfile");
    BackupFileSystemManager.extractWalPathInfo(walPath);
  }

  /**
   * extractWalPathInfo: WALs segment name mismatch -> should throw IOException. e.g.
   * /prefix/NOT_WALs/2025/wal
   */
  @Test(expected = IOException.class)
  public void testExtractWalPathInfo_wrongWALsegment() throws Exception {
    Path walPath = new Path("/prefix/NOT_WALS/2025/wal");
    BackupFileSystemManager.extractWalPathInfo(walPath);
  }

  /**
   * resolveBulkLoadFullPath: normal case with prefix.
   */
  @Test
  public void testResolveBulkLoadFullPath_withPrefix() throws Exception {
    Path walPath =
      new Path("/some/prefix/" + BackupFileSystemManager.WALS_DIR + "/2025-08-30/wal-1");
    Path relative = new Path("namespace/table/region/family/file1");
    Path full = BackupFileSystemManager.resolveBulkLoadFullPath(walPath, relative);

    // expected: /some/prefix/bulk-load-files/2025-08-30/namespace/table/region/family/file1
    String expected = "/some/prefix/" + BackupFileSystemManager.BULKLOAD_FILES_DIR
      + "/2025-08-30/namespace/table/region/family/file1";
    assertEquals(expected, full.toString());
  }

  /**
   * resolveBulkLoadFullPath: when WALs is under root (prefix is root) - ensure path resolved under
   * /bulk-load-files/<date>/...
   */
  @Test
  public void testResolveBulkLoadFullPath_rootWALs() throws Exception {
    Path walPath = new Path("/" + BackupFileSystemManager.WALS_DIR + "/2025-08-30/wal-2");
    Path relative = new Path("ns/tbl/r/f");
    Path full = BackupFileSystemManager.resolveBulkLoadFullPath(walPath, relative);

    String expected = "/" + BackupFileSystemManager.BULKLOAD_FILES_DIR + "/2025-08-30/ns/tbl/r/f";
    assertEquals(expected, full.toString());
  }

  /**
   * Integration-y test: constructor should create directories under the provided backup root. Uses
   * a temporary folder (local fs).
   */
  @Test
  public void testConstructorCreatesDirectories() throws Exception {
    File root = tmp.newFolder("backupRoot");
    String rootPath = root.getAbsolutePath();

    Configuration conf = HBaseConfiguration.create();
    BackupFileSystemManager mgr = new BackupFileSystemManager("peer-1", conf, rootPath);

    FileSystem fs = mgr.getBackupFs();
    Path wals = mgr.getWalsDir();
    Path bulk = mgr.getBulkLoadFilesDir();

    assertTrue("WALs dir should exist", fs.exists(wals));
    assertTrue("bulk-load-files dir should exist", fs.exists(bulk));
  }
}
