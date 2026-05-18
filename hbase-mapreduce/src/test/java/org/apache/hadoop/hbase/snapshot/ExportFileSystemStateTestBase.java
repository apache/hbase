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

import org.junit.jupiter.api.TestTemplate;

public class ExportFileSystemStateTestBase extends ExportSnapshotTestBase {

  protected ExportFileSystemStateTestBase(boolean mob) {
    super(mob);
  }

  /**
   * Verify if exported snapshot and copied files matches the original one.
   */
  @TestTemplate
  public void testExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles);
  }

  @TestTemplate
  public void testExportFileSystemStateWithSkipTmp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ExportSnapshot.CONF_SKIP_TMP, true);
    try {
      testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles);
    } finally {
      TEST_UTIL.getConfiguration().setBoolean(ExportSnapshot.CONF_SKIP_TMP, false);
    }
  }

  @TestTemplate
  public void testEmptyExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, emptySnapshotName, emptySnapshotName, 0);
  }
}
