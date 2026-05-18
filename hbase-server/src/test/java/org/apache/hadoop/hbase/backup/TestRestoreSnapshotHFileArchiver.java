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
package org.apache.hadoop.hbase.backup;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RestoreSnapshotHFileArchiver} — validates that the root-directory safety check
 * correctly blocks operations targeting the production root directory and allows operations on temp
 * directories.
 */
@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestRestoreSnapshotHFileArchiver {

  private static Configuration createConf(String rootDir) {
    Configuration conf = HBaseConfiguration.create();
    conf.set("fs.defaultFS", "file:///");
    conf.set(HConstants.HBASE_DIR, rootDir);
    return conf;
  }

  /**
   * When the target path IS the production root dir, validation must throw.
   */
  @Test
  public void testBlocksExactRootDir() throws IOException {
    Configuration conf = createConf("/hbase");
    Path targetPath = new Path("/hbase");

    try {
      RestoreSnapshotHFileArchiver.validateNotProductionRootDir(conf, targetPath, "testOp",
        "detail");
      fail("Expected IOException when target equals production root dir");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("BLOCKED"),
        "Error message should mention BLOCKED");
      assertTrue(e.getMessage().contains("HBASE-29435"),
        "Error message should reference HBASE-29435");
    }
  }

  /**
   * When the target path is a child of the production root dir, validation must throw.
   */
  @Test
  public void testBlocksChildOfRootDir() throws IOException {
    Configuration conf = createConf("/hbase");
    Path targetPath = new Path("/hbase/data/default/MY_TABLE");

    try {
      RestoreSnapshotHFileArchiver.validateNotProductionRootDir(conf, targetPath, "archiveRegion",
        "region=abc123");
      fail("Expected IOException when target is under production root dir");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("BLOCKED"),
        "Error message should mention BLOCKED");
    }
  }

  /**
   * When the target path is a temp/restore dir outside the root, validation must pass.
   */
  @Test
  public void testAllowsTempDir() throws IOException {
    Configuration conf = createConf("/hbase");
    Path targetPath = new Path("/tmp/snapshot-restore/data/default/MY_TABLE");

    // Should not throw
    RestoreSnapshotHFileArchiver.validateNotProductionRootDir(conf, targetPath, "archiveRegion",
      "region=abc123");
  }

  /**
   * A path that is a sibling of the root dir (shares a prefix string but is not a child) must be
   * allowed. For example, /hbase-staging is not a child of /hbase.
   */
  @Test
  public void testAllowsSiblingOfRootDir() throws IOException {
    Configuration conf = createConf("/hbase");
    Path targetPath = new Path("/hbase-staging/data/default/MY_TABLE");

    // Should not throw — /hbase-staging is NOT under /hbase
    RestoreSnapshotHFileArchiver.validateNotProductionRootDir(conf, targetPath, "archiveRegion",
      "region=abc123");
  }

  /**
   * The archive subdirectory under root (/hbase/archive) should also be blocked, since it is a
   * child of the production root.
   */
  @Test
  public void testBlocksArchiveSubdir() throws IOException {
    Configuration conf = createConf("/hbase");
    Path targetPath = new Path("/hbase/archive/data/default/MY_TABLE");

    try {
      RestoreSnapshotHFileArchiver.validateNotProductionRootDir(conf, targetPath,
        "archiveStoreFile", "file=hfile123");
      fail("Expected IOException when target is under production root dir (archive subdir)");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("BLOCKED"),
        "Error message should mention BLOCKED");
    }
  }

  /**
   * Verify the check works with a custom (non-default) root directory.
   */
  @Test
  public void testBlocksCustomRootDir() throws IOException {
    Configuration conf = createConf("/custom/hbase/root");
    Path targetPath = new Path("/custom/hbase/root/data/default/MY_TABLE");

    try {
      RestoreSnapshotHFileArchiver.validateNotProductionRootDir(conf, targetPath, "archiveRegion",
        "region=xyz");
      fail("Expected IOException when target is under custom production root dir");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("BLOCKED"));
    }
  }

  /**
   * A temp dir with a completely different root must be allowed even with a custom root.
   */
  @Test
  public void testAllowsTempDirWithCustomRoot() throws IOException {
    Configuration conf = createConf("/custom/hbase/root");
    Path targetPath = new Path("/tmp/restore/data/default/MY_TABLE");

    // Should not throw
    RestoreSnapshotHFileArchiver.validateNotProductionRootDir(conf, targetPath, "archiveRegion",
      "region=xyz");
  }
}
