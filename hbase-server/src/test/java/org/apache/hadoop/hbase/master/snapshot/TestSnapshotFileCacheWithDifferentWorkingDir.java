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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.File;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

/**
 * Test that we correctly reload the cache, filter directories, etc.
 * while the temporary directory is on a different file system than the root directory
 */
@Category({MasterTests.class, LargeTests.class})
public class TestSnapshotFileCacheWithDifferentWorkingDir extends TestSnapshotFileCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotFileCacheWithDifferentWorkingDir.class);

  protected static String TEMP_DIR = Paths.get(".", UUID.randomUUID().toString()).toAbsolutePath().toString();

  @BeforeClass
  public static void startCluster() throws Exception {
    initCommon();

    // Set the snapshot working directory to be on another filesystem.
    conf.set(SnapshotDescriptionUtils.SNAPSHOT_WORKING_DIR,
      "file://" + new Path(TEMP_DIR, ".tmpDir").toUri());
    workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(rootDir, conf);
    workingFs = workingDir.getFileSystem(conf);
  }

  @AfterClass
  public static void stopCluster() throws Exception {
    UTIL.shutdownMiniDFSCluster();
    FileUtils.deleteDirectory(new File(TEMP_DIR));
  }
}
