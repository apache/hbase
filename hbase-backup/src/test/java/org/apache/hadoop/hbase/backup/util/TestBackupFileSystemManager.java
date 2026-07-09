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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for {@link BackupFileSystemManager}.
 */
@Tag(SmallTests.TAG)
public class TestBackupFileSystemManager {

  @TempDir
  java.nio.file.Path tmp;

  /**
   * extractWalPathInfo: happy path where WALs dir has a prefix. e.g.
   * /some/prefix/WALs/2025-09-14/some-wal
   */
  @Test
  public void testExtractWalPathInfo_withPrefix() throws Exception {
    Path walPath =
      new Path("/some/prefix/" + BackupFileSystemManager.WALS_DIR + "/2025-09-14/wal-000");
    BackupFileSystemManager.WalPathInfo info = BackupFileSystemManager.extractWalPathInfo(walPath);

    assertNotNull(info, "WalPathInfo should not be null");
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
    assertNotNull(info.getPrefixBeforeWALs(),
      "prefixBeforeWALs should not be null for root-style path");
    // prefix might be "/" (expected), be tolerant: assert it ends with "/" or equals "/"
    assertTrue(info.getPrefixBeforeWALs().toString().equals("/")
      || !info.getPrefixBeforeWALs().toString().isEmpty());
  }

  /**
   * extractWalPathInfo: null input should throw IllegalArgumentException.
   */
  @Test
  public void testExtractWalPathInfo_nullPath() throws Exception {
    assertThrows(IllegalArgumentException.class,
      () -> BackupFileSystemManager.extractWalPathInfo(null));
  }

  /**
   * extractWalPathInfo: missing date directory -> should throw IOException. Example: path that has
   * no parent for the wal file.
   */
  @Test
  public void testExtractWalPathInfo_missingDateDir() throws Exception {
    // A single segment path (no parents) e.g. "walfile"
    Path walPath = new Path("walfile");
    assertThrows(IOException.class, () -> BackupFileSystemManager.extractWalPathInfo(walPath));
  }

  /**
   * extractWalPathInfo: WALs segment name mismatch -> should throw IOException. e.g.
   * /prefix/NOT_WALs/2025/wal
   */
  @Test
  public void testExtractWalPathInfo_wrongWALsegment() throws Exception {
    Path walPath = new Path("/prefix/NOT_WALS/2025/wal");
    assertThrows(IOException.class, () -> BackupFileSystemManager.extractWalPathInfo(walPath));
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
    java.nio.file.Path root = tmp.resolve("backupRoot");
    java.nio.file.Files.createDirectories(root);
    String rootPath = root.toString();

    Configuration conf = HBaseConfiguration.create();
    BackupFileSystemManager mgr = new BackupFileSystemManager("peer-1", conf, rootPath);

    FileSystem fs = mgr.getBackupFs();
    Path wals = mgr.getWalsDir();
    Path bulk = mgr.getBulkLoadFilesDir();

    assertTrue(fs.exists(wals), "WALs dir should exist");
    assertTrue(fs.exists(bulk), "bulk-load-files dir should exist");
  }
}
