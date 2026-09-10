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
package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.TestTemplate;

public class ExportSnapshotRetryTestBase extends ExportSnapshotTestBase {

  protected ExportSnapshotRetryTestBase(boolean mob) {
    super(mob);
  }

  /**
   * Check that ExportSnapshot will succeed if something fails but the retry succeed.
   */
  @TestTemplate
  public void testExportRetry() throws Exception {
    Path copyDir = TestExportSnapshotMisc.getLocalDestinationDir(TEST_UTIL);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(ExportSnapshot.Testing.CONF_TEST_FAILURE, true);
    conf.setInt(ExportSnapshot.Testing.CONF_TEST_FAILURE_COUNT, 2);
    conf.setInt("mapreduce.map.maxattempts", 3);
    testExportFileSystemState(conf, tableName, snapshotName, snapshotName, tableNumFiles,
      TEST_UTIL.getDefaultRootDirPath(), copyDir, true, false, getBypassRegionPredicate(), true,
      false);
  }

  /**
   * Check that ExportSnapshot will fail if we inject failure more times than MR will retry.
   */
  @TestTemplate
  public void testExportFailure() throws Exception {
    Path copyDir = TestExportSnapshotMisc.getLocalDestinationDir(TEST_UTIL);
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(ExportSnapshot.Testing.CONF_TEST_FAILURE, true);
    conf.setInt(ExportSnapshot.Testing.CONF_TEST_FAILURE_COUNT, 4);
    conf.setInt("mapreduce.map.maxattempts", 3);
    testExportFileSystemState(conf, tableName, snapshotName, snapshotName, tableNumFiles,
      TEST_UTIL.getDefaultRootDirPath(), copyDir, true, false, getBypassRegionPredicate(), false,
      false);
  }
}
